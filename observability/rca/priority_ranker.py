"""Xếp hạng ưu tiên sự cố (AI-C, Sprint 4).

Công thức:
  score = severity_weight
        + max_tier_weight (gold > silver > bronze > raw)
        + per_impacted_asset * len(impacted_nodes)
        + per_source * len(sources)
        + per_minute_duration * duration_minutes
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import yaml

from observability.rca.impact_bfs import infer_tier
from observability.rca.incident_grouper import Incident

DEFAULT_WEIGHTS = {
    "severity": {"critical": 100.0, "high": 60.0, "medium": 30.0, "low": 10.0, "info": 1.0},
    "asset_tier": {"gold": 40.0, "silver": 25.0, "bronze": 15.0, "raw": 10.0, "unknown": 5.0},
    "per_impacted_asset": 5.0,
    "per_source": 15.0,
    "per_minute_duration": 1.5,
}


def load_weights(config_path: Path | None = None) -> dict:
    if config_path and config_path.is_file():
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        return raw.get("priority_weights", DEFAULT_WEIGHTS)
    return DEFAULT_WEIGHTS


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def score_incident(incident: Incident, weights: dict | None = None) -> float:
    w = weights or DEFAULT_WEIGHTS
    sev_score = float(w["severity"].get(incident.severity, 10.0))

    tiers = [infer_tier(a) for a in incident.affected_assets]
    tiers += [n.get("tier", "unknown") for n in incident.impacted_nodes]
    tier_score = max((float(w["asset_tier"].get(t, 5.0)) for t in tiers), default=5.0)

    impact_score = float(w.get("per_impacted_asset", 5.0)) * len(incident.impacted_nodes)
    source_score = float(w.get("per_source", 15.0)) * len(incident.sources)
    dur_minutes = max(0.0, (_parse_ts(incident.ended_at) - _parse_ts(incident.started_at)).total_seconds() / 60.0)
    dur_score = float(w.get("per_minute_duration", 1.5)) * dur_minutes

    total = round(sev_score + tier_score + impact_score + source_score + dur_score, 2)
    incident.priority_score = total
    return total


def rank_incidents(incidents: list[Incident], weights: dict | None = None) -> list[Incident]:
    for inc in incidents:
        score_incident(inc, weights)
    return sorted(incidents, key=lambda x: (-x.priority_score, x.started_at, x.incident_id))


def export_ranked_json(incidents: list[Incident]) -> str:
    """API-ready JSON output cho FastAPI / Grafana / EV."""
    return json.dumps([inc.as_dict() for inc in incidents], ensure_ascii=False, indent=2)
