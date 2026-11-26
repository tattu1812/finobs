"""Gộp cảnh báo liên quan thành sự cố (AI-A, Sprint 4).

Tất định (deterministic): chạy lại cùng danh sách cảnh báo và cấu hình luôn cho
cùng tập sự cố, cùng ID ổn định và cùng lý do gộp.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import yaml

from observability.rca.impact_bfs import are_lineage_connected, bfs_downstream_impact

DEFAULT_WINDOW_SECONDS = 600


def load_grouping_config(path: Path | None = None) -> dict:
    if path and path.is_file():
        return yaml.safe_load(path.read_text(encoding="utf-8"))
    return {"grouping": {"window_seconds": DEFAULT_WINDOW_SECONDS}}


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _alert_source(alert: dict) -> str:
    return alert.get("source") or alert.get("labels", {}).get("source") or alert.get("group", {}).get("source", "")


def _alert_symbol(alert: dict) -> str:
    return (
        alert.get("symbol")
        or alert.get("labels", {}).get("symbol")
        or alert.get("group", {}).get("symbol_canonical")
        or ""
    )


def _alert_asset(alert: dict) -> str:
    return alert.get("asset") or alert.get("labels", {}).get("asset") or ""


@dataclass
class Incident:
    incident_id: str
    alerts: list[dict]
    started_at: str
    ended_at: str
    affected_assets: list[str]
    sources: list[str]
    symbols: list[str]
    severity: str
    status: str = "open"
    grouping_reasons: list[str] = field(default_factory=list)
    impacted_nodes: list[dict] = field(default_factory=list)
    priority_score: float = 0.0

    def as_dict(self) -> dict:
        return {
            "incident_id": self.incident_id,
            "alerts": self.alerts,
            "alert_ids": [a.get("alert_id") for a in self.alerts],
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "affected_assets": self.affected_assets,
            "sources": self.sources,
            "symbols": self.symbols,
            "severity": self.severity,
            "status": self.status,
            "grouping_reasons": self.grouping_reasons,
            "impacted_nodes": self.impacted_nodes,
            "priority_score": self.priority_score,
        }


_SEV_ORDER = {"critical": 4, "high": 3, "medium": 2, "low": 1, "info": 0}


def _should_group(a: dict, b: dict, window_s: int, graph: dict | None) -> list[str]:
    """Trả về danh sách lý do gộp nếu a và b thuộc cùng sự cố, ngược lại trả []."""
    dt_a = _parse_ts(a["detected_at"])
    dt_b = _parse_ts(b["detected_at"])
    if abs((dt_b - dt_a).total_seconds()) > window_s:
        return []

    reasons: list[str] = []
    src_a, src_b = _alert_source(a), _alert_source(b)
    sym_a, sym_b = _alert_symbol(a), _alert_symbol(b)
    ast_a, ast_b = _alert_asset(a), _alert_asset(b)

    if src_a and src_b and src_a == src_b:
        reasons.append(f"same_source:{src_a}")
    if sym_a and sym_b and sym_a == sym_b and sym_a != "all":
        reasons.append(f"same_symbol:{sym_a}")
    if graph and ast_a and ast_b and are_lineage_connected(graph, ast_a, ast_b):
        reasons.append(f"lineage_connected:{ast_a}<->{ast_b}")

    if reasons:
        reasons.append(f"within_window:{window_s}s")
    return reasons


def group_alerts(
    alerts: list[dict],
    lineage_graph: dict | None = None,
    window_seconds: int = DEFAULT_WINDOW_SECONDS,
) -> list[Incident]:
    """Gộp danh sách cảnh báo thành các Incident tất định."""
    if not alerts:
        return []

    ordered = sorted(alerts, key=lambda x: (x["detected_at"], x.get("alert_id", "")))
    n = len(ordered)
    parent = list(range(n))
    edge_reasons: dict[tuple[int, int], list[str]] = {}

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(i: int, j: int) -> None:
        ri, rj = find(i), find(j)
        if ri != rj:
            parent[max(ri, rj)] = min(ri, rj)

    for i in range(n):
        for j in range(i + 1, n):
            reasons = _should_group(ordered[i], ordered[j], window_seconds, lineage_graph)
            if reasons:
                union(i, j)
                edge_reasons[(i, j)] = reasons

    clusters: dict[int, list[int]] = {}
    for i in range(n):
        clusters.setdefault(find(i), []).append(i)

    incidents: list[Incident] = []
    for root_idx in sorted(clusters):
        idxs = clusters[root_idx]
        comp_alerts = [ordered[i] for i in idxs]
        alert_ids = sorted(str(a.get("alert_id", i)) for i, a in zip(idxs, comp_alerts))
        digest = hashlib.sha256("|".join(alert_ids).encode("utf-8")).hexdigest()[:8].upper()
        inc_id = f"INC-{digest}"

        times = [a["detected_at"] for a in comp_alerts]
        assets = sorted({ast for a in comp_alerts if (ast := _alert_asset(a))})
        sources = sorted({src for a in comp_alerts if (src := _alert_source(a))})
        symbols = sorted({sym for a in comp_alerts if (sym := _alert_symbol(a)) and sym != "all"})
        max_sev = max(
            (a.get("severity", "low") for a in comp_alerts),
            key=lambda s: _SEV_ORDER.get(s, 0),
        )

        reasons_set: set[str] = set()
        for idx_pos, i in enumerate(idxs):
            for j in idxs[idx_pos + 1:]:
                reasons_set.update(edge_reasons.get((i, j), []))

        impacted: list[dict] = []
        if lineage_graph and assets:
            seen_imp: set[str] = set()
            for ast in assets:
                for node in bfs_downstream_impact(lineage_graph, f"dataset:{ast}"):
                    if node.node_id not in seen_imp:
                        seen_imp.add(node.node_id)
                        impacted.append(node.as_dict())

        incidents.append(Incident(
            incident_id=inc_id,
            alerts=comp_alerts,
            started_at=min(times),
            ended_at=max(times),
            affected_assets=assets,
            sources=sources,
            symbols=symbols,
            severity=max_sev,
            grouping_reasons=sorted(reasons_set) if reasons_set else ["single_alert"],
            impacted_nodes=impacted,
        ))

    return incidents
