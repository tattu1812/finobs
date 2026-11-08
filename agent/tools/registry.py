"""
5 tool cho agent chẩn đoán FinObs, cùng một cửa vào duy nhất: Toolbox.call(name, args).

    from agent.tools.registry import Toolbox
    tb = Toolbox.fake()                       # dữ liệu giả + Marquez giả
    tb = Toolbox.fake(marquez_url="http://localhost:5000")   # dữ liệu giả + Marquez thật
    tb.call("get_lineage", {"asset": "finobs:gold.ohlcv_1m"})
    tb.definitions()                          # [{name, description, input_schema}] cho LLM

Định nghĩa (mô tả + JSON Schema input/output) nằm ở agent/tools/schemas/<tên>.json.
call() kiểm input theo schema, và KHÔNG ném exception: lỗi trả về dạng
{"error": {"code", "message"}} (schemas/_error.json) để agent tự sửa lời gọi.

Hiện chỉ get_lineage đọc từ Marquez; 4 tool còn lại đọc FakeWorld. Khi có
metric store / Airflow thật, thay backend tương ứng, giữ nguyên schema.
"""

from __future__ import annotations

import copy
import json
from collections import Counter
from pathlib import Path
from statistics import fmean

from jsonschema import Draft202012Validator

from agent.tools import fake_world
from agent.tools.fake_world import FakeWorld, iso, parse_iso
from agent.tools.marquez import FakeTransport, HttpTransport, MarquezClient, MarquezError, split_node_id

SCHEMA_DIR = Path(__file__).resolve().parent / "schemas"
TOOL_NAMES = ["list_alerts", "query_metric", "get_lineage", "get_job_runs", "get_contract_violations"]
SEVERITY = ["low", "medium", "high", "critical"]


class ToolError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def load_spec(name: str) -> dict:
    return json.loads((SCHEMA_DIR / f"{name}.json").read_text(encoding="utf-8"))


def inline_refs(schema: dict) -> dict:
    """Thay "$ref": "#/$defs/x" bằng nội dung, bỏ $defs: một số API LLM không hỗ trợ $ref."""
    defs = schema.get("$defs", {})

    def walk(node):
        if isinstance(node, dict):
            if "$ref" in node:
                return walk(copy.deepcopy(defs[node["$ref"].split("/")[-1]]))
            return {k: walk(v) for k, v in node.items() if k != "$defs"}
        if isinstance(node, list):
            return [walk(v) for v in node]
        return node

    return walk(schema)


def _window(args: dict) -> tuple:
    start, end = parse_iso(args["start"]), parse_iso(args["end"])
    if end <= start:
        raise ToolError("invalid_input", "end phải sau start")
    return start, end


class Toolbox:
    def __init__(self, world: FakeWorld, marquez: MarquezClient):
        self.world = world
        self.marquez = marquez
        self.specs = {name: load_spec(name) for name in TOOL_NAMES}
        self._in = {n: Draft202012Validator(s["input_schema"]) for n, s in self.specs.items()}
        self.handlers = {n: getattr(self, f"_{n}") for n in TOOL_NAMES}

    @classmethod
    def fake(cls, marquez_url: str | None = None) -> Toolbox:
        transport = HttpTransport(marquez_url) if marquez_url else FakeTransport()
        return cls(fake_world.build(), MarquezClient(transport))

    # ------------------------------------------------------------ cửa vào
    def definitions(self) -> list[dict]:
        return [{"name": s["name"], "description": s["description"], "input_schema": inline_refs(s["input_schema"])}
                for s in self.specs.values()]

    def call(self, name: str, args: dict | None = None) -> dict:
        args = dict(args or {})
        if name not in self.handlers:
            return _error("unknown_tool", f"Không có tool '{name}'. Các tool: {', '.join(TOOL_NAMES)}")
        problems = sorted(self._in[name].iter_errors(args), key=lambda e: list(map(str, e.path)))
        if problems:
            return _error("invalid_input", "; ".join(
                f"{'/'.join(map(str, e.path)) or '<input>'}: {e.message}" for e in problems))
        try:
            return self.handlers[name](args)
        except ToolError as exc:
            return _error(exc.code, str(exc))
        except MarquezError as exc:
            code = "not_found" if "không có node" in str(exc) else "backend_unavailable"
            return _error(code, str(exc))

    # ------------------------------------------------------------ 1. list_alerts
    def _list_alerts(self, args: dict) -> dict:
        start, end = _window(args)
        floor = SEVERITY.index(args.get("min_severity", "low"))
        alerts = [
            a for a in self.world.alerts
            if start <= parse_iso(a["detected_at"]) < end
            and SEVERITY.index(a["severity"]) >= floor
            and args.get("asset") in (None, a["asset"])
        ]
        alerts.sort(key=lambda a: a["detected_at"])
        return {"count": len(alerts), "alerts": copy.deepcopy(alerts)}

    # ------------------------------------------------------------ 2. query_metric
    def _query_metric(self, args: dict) -> dict:
        start, end = _window(args)
        entry = self.world.metrics.get(args["metric"])
        if entry is None:
            raise ToolError("not_found", f"Không có metric '{args['metric']}'. Có: {', '.join(sorted(self.world.metrics))}")
        want = args.get("group", {})
        series = []
        for group, points in entry["series"]:
            if any(group.get(k) != v for k, v in want.items()):
                continue
            pts = [{"ts": iso(t), "value": v} for t, v in points if start <= t < end]
            vals = [p["value"] for p in pts if p["value"] is not None]
            series.append({
                "group": dict(group), "points": pts,
                "summary": {"count": len(vals), "min": min(vals, default=None), "max": max(vals, default=None),
                            "mean": round(fmean(vals), 4) if vals else None},
            })
        if not series:
            raise ToolError("not_found", f"Không có chuỗi nào của '{args['metric']}' khớp group {want}")
        return {"metric": args["metric"], "unit": entry["unit"], "start": args["start"], "end": args["end"],
                "series": series}

    # ------------------------------------------------------------ 3. get_lineage (Marquez)
    def _get_lineage(self, args: dict) -> dict:
        asset, direction, depth = args["asset"], args.get("direction", "upstream"), args.get("depth", 10)
        graph = self.marquez.lineage(asset, depth=depth)
        start = f"dataset:{asset}"
        dist: dict[str, int] = {start: 0}
        edges: set[tuple[str, str]] = set()
        for d, sign in (("upstream", -1), ("downstream", 1)):
            if direction in (d, "both"):
                found, used = graph.walk(start, d, depth)
                edges |= used
                for node, n in found.items():
                    dist.setdefault(node, sign * n)

        nodes = []
        for node_id, n in sorted(dist.items(), key=lambda kv: (abs(kv[1]), kv[0])):
            kind, ns, name = split_node_id(node_id)
            nodes.append({"id": node_id, "kind": kind, "namespace": ns, "name": name, "distance": n})
        others = [n for n in nodes if n["id"] != start]
        return {
            "asset": asset, "direction": direction, "nodes": nodes,
            "edges": [{"from": o, "to": t} for o, t in sorted(edges)],
            "datasets": [f"{n['namespace']}:{n['name']}" for n in others if n["kind"] == "dataset"],
            "jobs": [f"{n['namespace']}:{n['name']}" for n in others if n["kind"] == "job"],
        }

    # ------------------------------------------------------------ 4. get_job_runs
    def _get_job_runs(self, args: dict) -> dict:
        start, end = _window(args)
        job = args["job"].removeprefix("job:").split(":")[-1]
        runs = self.world.runs.get(job)
        if runs is None:
            raise ToolError("not_found", f"Không có job '{job}'. Có: {', '.join(sorted(self.world.runs))}")
        in_window = [r for r in runs if start <= parse_iso(r["started_at"]) < end]
        failures = [r["started_at"] for r in in_window if r["state"] == "FAILED"]
        newest = sorted(in_window, key=lambda r: r["started_at"], reverse=True)
        return {
            "job": f"{fake_world.NAMESPACE}:{job}",
            "total_in_window": len(in_window),
            "state_counts": dict(Counter(r["state"] for r in in_window)),
            "first_failure_at": min(failures, default=None),
            "runs": copy.deepcopy(newest[: args.get("limit", 10)]),
        }

    # ------------------------------------------------------------ 5. get_contract_violations
    def _get_contract_violations(self, args: dict) -> dict:
        start, end = _window(args)
        source = args["source"]
        checks = [c for c in self.world.violations.get(source, []) if start <= c["ts"] < end]
        checked = sum(c["checked"] for c in checks)
        invalid = sum(c["invalid"] for c in checks)
        bad = [c for c in checks if c["invalid"]]

        # Trong thế giới giả chỉ ETHUSDT của Binance vi phạm; mỗi bản ghi lỗi mắc cả 2 lỗi.
        symbol = "ETHUSDT" if source == "binance" else None
        samples = []
        for i, c in enumerate(bad[: args.get("sample_limit", 5)]):
            seq = 3_250_000_000 + int(c["ts"].timestamp()) % 100_000 + i
            samples.append({
                "event_id": f"{source}:{symbol}:{seq}",
                "checked_at": iso(c["ts"]),
                "errors": [e.format(seq=seq) for e in fake_world.VIOLATION_ERRORS],
            })
        return {
            "source": source, "start": args["start"], "end": args["end"],
            "checked": checked, "invalid": invalid,
            "invalid_rate": round(invalid / checked, 4) if checked else None,
            "first_violation_at": iso(bad[0]["ts"]) if bad else None,
            "by_error": ([{"error": e.replace("'{seq}'", "'<trade id>'"), "count": invalid}
                          for e in fake_world.VIOLATION_ERRORS] if invalid else []),
            "by_symbol": {symbol: invalid} if invalid else {},
            "samples": samples,
        }


def _error(code: str, message: str) -> dict:
    return {"error": {"code": code, "message": message}}
