"""SLO Engine (OB-B, Sprint 4): đọc YAML, tính SLI, error budget, burn rate và sinh Prometheus rules."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, time, timezone
from pathlib import Path
from typing import Iterable

import yaml
from jsonschema import Draft202012Validator

HERE = Path(__file__).resolve().parent
DEFAULT_SCHEMA_PATH = HERE / "slo_schema.json"
DEFAULT_SLOS_PATH = HERE / "slos.yaml"


def validate_slo_file(slo_path: Path = DEFAULT_SLOS_PATH, schema_path: Path = DEFAULT_SCHEMA_PATH) -> dict:
    doc = yaml.safe_load(slo_path.read_text(encoding="utf-8"))
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema)
    errors = [
        f"{'/'.join(map(str, e.absolute_path)) or '<root>'}: {e.message}"
        for e in validator.iter_errors(doc)
    ]
    if errors:
        raise ValueError("SLO YAML không hợp lệ:\n  - " + "\n  - ".join(errors))
    return doc


def is_in_trading_hours(ts: datetime, schedule: str) -> bool:
    """Kiểm tra mốc thời gian UTC có nằm trong giờ giao dịch hay không.

    - ``24x7``: luôn True (crypto).
    - ``hose_vn30``: Thứ 2–Thứ 6, 09:00–11:30 và 13:00–15:00 giờ VN (UTC+7),
      tức 02:00–04:30 và 06:00–08:00 UTC.
    """
    if schedule == "24x7":
        return True
    if schedule == "hose_vn30":
        utc = ts.astimezone(timezone.utc)
        if utc.weekday() >= 5:  # Thứ 7, CN
            return False
        t = utc.time()
        morning = time(2, 0) <= t <= time(4, 30)
        afternoon = time(6, 0) <= t <= time(8, 0)
        return morning or afternoon
    raise ValueError(f"Lịch giao dịch không hỗ trợ: {schedule}")


def _is_good(obs: dict, comparator: str, threshold: float) -> bool:
    """Dữ liệu thiếu (status != 'ok' hoặc value is None) KHÔNG bao giờ được tính là đạt."""
    if obs.get("status") != "ok" or obs.get("value") is None:
        return False
    val = float(obs["value"])
    if comparator == "lte":
        return val <= threshold
    if comparator == "gte":
        return val >= threshold
    if comparator == "eq":
        return val == threshold
    raise ValueError(f"Comparator không hợp lệ: {comparator}")


@dataclass
class SLOResult:
    name: str
    pillar: str
    target: float
    total_samples: int
    good_samples: int
    sli_current: float | None
    error_budget_total: float
    error_budget_remaining: float | None
    burn_rate: float | None

    def to_metrics(self, observed_at: str, source: str = "all", symbol: str = "all") -> list[dict]:
        dims = {"slo": self.name, "pillar": self.pillar, "source": source, "symbol": symbol}
        status = "ok" if self.sli_current is not None else "no_data"
        base = {
            "dimensions": dims,
            "observed_at": observed_at,
            "window_start": None,
            "window_end": None,
            "status": status,
            "sample_count": self.total_samples,
        }
        return [
            {**base, "metric_name": "slo_target", "value": self.target, "status": "ok"},
            {**base, "metric_name": "slo_sli_current", "value": self.sli_current},
            {**base, "metric_name": "slo_error_budget_remaining", "value": self.error_budget_remaining},
            {**base, "metric_name": "slo_burn_rate", "value": self.burn_rate},
        ]


def evaluate_slo(slo: dict, observations: Iterable[dict]) -> SLOResult:
    """Tính SLI, error budget còn lại và burn rate cho một SLO.

    Công thức:
      - ``error_budget_total = 1 - target``
      - ``bad_rate = 1 - (good_samples / total_samples)``
      - ``burn_rate = bad_rate / error_budget_total``
      - ``error_budget_remaining = max(0.0, 1.0 - burn_rate)`` (tỷ lệ budget còn lại trong [0, 1])
    """
    schedule = slo.get("trading_schedule", "24x7")
    comp = slo["sli"]["comparator"]
    thresh = float(slo["sli"]["threshold"])
    metric_name = slo["sli"]["metric_name"]
    target = float(slo["target"])
    budget = 1.0 - target

    relevant = []
    for obs in observations:
        if obs.get("metric_name") != metric_name:
            continue
        ts_str = obs.get("observed_at")
        if ts_str:
            dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            if not is_in_trading_hours(dt, schedule):
                continue
        relevant.append(obs)

    total = len(relevant)
    if total == 0:
        return SLOResult(
            name=slo["name"],
            pillar=slo["pillar"],
            target=target,
            total_samples=0,
            good_samples=0,
            sli_current=None,
            error_budget_total=round(budget, 6),
            error_budget_remaining=None,
            burn_rate=None,
        )

    good = sum(1 for o in relevant if _is_good(o, comp, thresh))
    sli = good / total
    bad_rate = 1.0 - sli
    burn_rate = (bad_rate / budget) if budget > 0 else (0.0 if bad_rate == 0 else float("inf"))
    remaining = max(0.0, 1.0 - burn_rate)

    return SLOResult(
        name=slo["name"],
        pillar=slo["pillar"],
        target=target,
        total_samples=total,
        good_samples=good,
        sli_current=round(sli, 6),
        error_budget_total=round(budget, 6),
        error_budget_remaining=round(remaining, 6),
        burn_rate=round(burn_rate, 4),
    )


def generate_prometheus_burn_rate_rules(slo_doc: dict) -> dict:
    """Sinh cấu trúc Prometheus alert rules YAML từ định nghĩa SLO."""
    rules = []
    for slo in slo_doc["slos"]:
        for win in slo["burn_rate_windows"]:
            alert_name = f"SLOBurnRate_{slo['name']}_{win['name']}"
            rules.append({
                "alert": alert_name,
                "expr": f'slo_burn_rate{{slo="{slo["name"]}"}} > {win["burn_rate_threshold"]}',
                "for": win["short_window"],
                "labels": {
                    "severity": win["severity"],
                    "pillar": slo["pillar"],
                    "slo": slo["name"],
                    "owner": slo["owner"],
                    "service": "finobs-observability",
                    "source": '{{ $labels.source }}',
                    "symbol": '{{ $labels.symbol }}',
                },
                "annotations": {
                    "summary": f"SLO {slo['name']} burn rate vượt ngưỡng {win['burn_rate_threshold']}x ({win['short_window']}/{win['long_window']})",
                    "description": slo.get("description", slo["name"]),
                },
            })
    return {"groups": [{"name": "finobs-slo-burn-rate-alerts", "rules": rules}]}


def write_prometheus_rules(out_path: Path, slo_path: Path = DEFAULT_SLOS_PATH) -> Path:
    doc = validate_slo_file(slo_path)
    rules_doc = generate_prometheus_burn_rate_rules(doc)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(yaml.safe_dump(rules_doc, sort_keys=False, allow_unicode=True), encoding="utf-8")
    return out_path
