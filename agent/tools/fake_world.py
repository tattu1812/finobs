"""
"Thế giới giả" cho agent: topology lineage + metric, alert, job run, vi phạm contract.

Dựng theo kịch bản evaluation/scenarios/S002_binance_schema_drift.yaml và kéo dài thêm
xuống hạ nguồn để có việc cho chẩn đoán:

  03:05  Binance đổi schema của ETHUSDT (đổi tên "p" -> "price", "t" thành chuỗi)
  03:06  job bronze_load bắt đầu FAILED vì cast lỗi -> bronze/silver/gold cũ dần
  03:12  Coinbase BTC-USD có đỉnh tải thật (sự kiện thị trường) -> alert gây nhiễu

Đáp án đúng cho một agent: gốc là finobs:raw.binance_trades (nguồn Binance), không
phải job bronze_load (job chỉ là nơi lỗi lộ ra) và không phải Coinbase.

Mọi số đều sinh tất định từ SEED, nên test và demo luôn cho cùng kết quả.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

NAMESPACE = "finobs"
SEED = 2
START = datetime(2026, 10, 27, 3, 0, tzinfo=timezone.utc)
MINUTES = 20
DRIFT_AT = START + timedelta(minutes=5)       # Binance đổi schema
FAIL_AT = START + timedelta(minutes=6)        # bronze_load bắt đầu lỗi
BURST_AT = START + timedelta(minutes=12)      # đỉnh tải thật của Coinbase


def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


def parse_iso(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


# ---------------------------------------------------------------- topology lineage
# job -> (inputs, outputs). Tên dataset khớp asset trong evaluation/scenarios/*.yaml.
JOBS: dict[str, tuple[list[str], list[str]]] = {
    "ingest_binance": ([], ["raw.binance_trades"]),
    "ingest_coinbase": ([], ["raw.coinbase_trades"]),
    "bronze_load": (["raw.binance_trades", "raw.coinbase_trades"], ["bronze.trades"]),
    "silver_dedup": (["bronze.trades"], ["silver.trades_dedup"]),
    "gold_ohlcv": (["silver.trades_dedup"], ["gold.ohlcv_1m"]),
    "gold_cross_source": (["silver.trades_dedup"], ["gold.cross_source_spread"]),
}


# ---------------------------------------------------------------- dữ liệu giả
@dataclass
class FakeWorld:
    metrics: dict[str, dict] = field(default_factory=dict)   # metric -> {unit, series: [(group, [(ts, v)])]}
    alerts: list[dict] = field(default_factory=list)
    runs: dict[str, list[dict]] = field(default_factory=dict)
    violations: dict[str, list[dict]] = field(default_factory=dict)  # source -> [{ts, checked, errors:[...]}]


def _minutes():
    return [START + timedelta(minutes=i) for i in range(MINUTES)]


def build() -> FakeWorld:
    rng = random.Random(SEED)
    w = FakeWorld()

    def add(metric: str, unit: str, group: dict, values: list[float | None]):
        entry = w.metrics.setdefault(metric, {"unit": unit, "series": []})
        entry["series"].append((group, list(zip(_minutes(), values))))

    # Số trade theo mã (đã khử trùng). ETHUSDT vẫn tới đủ, chỉ sai schema.
    base = {("binance", "BTC-USDT"): 2100, ("binance", "ETH-USDT"): 880,
            ("coinbase", "BTC-USD"): 310, ("coinbase", "ETH-USD"): 140}
    for (src, sym), mean in base.items():
        vals = []
        for ts in _minutes():
            v = mean * rng.uniform(0.85, 1.15)
            if src == "coinbase" and ts == BURST_AT:
                v *= 3.6
            vals.append(round(v))
        add("trade_count_1m", "trades", {"source": src, "symbol_canonical": sym}, vals)

    for src, p95 in (("binance", 32.0), ("coinbase", 255.0)):
        add("ingest_delay_p95_ms", "milliseconds", {"source": src},
            [round(p95 * rng.uniform(0.9, 1.1), 1) for _ in _minutes()])

    # Vi phạm contract: chỉ Binance, từ DRIFT_AT, bằng tỷ trọng ETHUSDT.
    for src in ("binance", "coinbase"):
        rates, counts, checks = [], [], []
        for i, ts in enumerate(_minutes()):
            checked = sum(s[1][i][1] for s in w.metrics["trade_count_1m"]["series"] if s[0]["source"] == src)
            bad = 0
            if src == "binance" and ts >= DRIFT_AT:
                bad = next(s[1][i][1] for s in w.metrics["trade_count_1m"]["series"]
                           if s[0]["symbol_canonical"] == "ETH-USDT")
            rates.append(round(bad / checked, 4))
            counts.append(bad)
            checks.append({"ts": ts, "checked": checked, "invalid": bad})
        add("contract_violation_rate_1m", "ratio", {"source": src}, rates)
        add("contract_violation_count_1m", "records", {"source": src}, counts)
        w.violations[src] = checks

    # Độ tươi của bảng (metric đề xuất): raw luôn tươi, từ bronze trở xuống cũ dần khi job lỗi.
    for asset in ("raw.binance_trades", "raw.coinbase_trades", "bronze.trades",
                  "silver.trades_dedup", "gold.ohlcv_1m", "gold.cross_source_spread"):
        vals = []
        for ts in _minutes():
            if asset.startswith("raw.") or ts < FAIL_AT:
                vals.append(round(rng.uniform(2, 8) if asset.startswith("raw.") else rng.uniform(40, 70), 1))
            else:
                vals.append(round((ts - FAIL_AT).total_seconds() + 60 + rng.uniform(0, 5), 1))
        add("table_freshness_seconds", "seconds", {"asset": f"{NAMESPACE}:{asset}"}, vals)

    # Job run: mỗi phút một lần.
    for job in JOBS:
        runs = []
        for i, ts in enumerate(_minutes()):
            failed = job == "bronze_load" and ts >= FAIL_AT
            skipped = job in ("silver_dedup", "gold_ohlcv", "gold_cross_source") and ts >= FAIL_AT
            runs.append({
                "run_id": f"{job}-{ts:%H%M}",
                "state": "FAILED" if failed else "COMPLETED",
                "started_at": iso(ts + timedelta(seconds=5)),
                "ended_at": iso(ts + timedelta(seconds=5 + rng.randint(3, 20))),
                "error_message": (
                    "CAST_INVALID_INPUT: column 'trade_id' from payload.t is STRING, expected BIGINT "
                    "(source=binance, symbol=ETHUSDT); missing field payload.p"
                ) if failed else None,
                "output_rows": 0 if (failed or skipped) else rng.randint(2000, 3500),
            })
        w.runs[job] = runs

    # Alert do detector (giả) phát ra; giá trị "observed" lấy đúng từ chuỗi metric ở trên.
    def alert(aid, metric, group, asset, ts, severity, baseline, message):
        observed = next(v for t, v in next(s for g, s in w.metrics[metric]["series"] if g == group) if t == ts)
        w.alerts.append({
            "alert_id": aid, "metric": metric, "group": group, "asset": f"{NAMESPACE}:{asset}",
            "detected_at": iso(ts), "severity": severity, "observed": float(observed),
            "baseline": baseline, "status": "open", "message": message,
        })

    alert("A001", "contract_violation_rate_1m", {"source": "binance"}, "raw.binance_trades",
          DRIFT_AT, "high", 0.0, "Tỷ lệ bản ghi Binance vi phạm contract tăng đột ngột từ 0")
    alert("A002", "table_freshness_seconds", {"asset": f"{NAMESPACE}:bronze.trades"}, "bronze.trades",
          FAIL_AT + timedelta(minutes=2), "medium", 60.0, "bronze.trades không được cập nhật quá 3 phút")
    alert("A003", "table_freshness_seconds", {"asset": f"{NAMESPACE}:gold.ohlcv_1m"}, "gold.ohlcv_1m",
          FAIL_AT + timedelta(minutes=3), "medium", 60.0, "gold.ohlcv_1m không được cập nhật quá 4 phút")
    alert("A004", "trade_count_1m", {"source": "coinbase", "symbol_canonical": "BTC-USD"},
          "raw.coinbase_trades", BURST_AT, "low", 310.0, "Số trade Coinbase BTC-USD tăng vọt so với trung bình")
    return w


# Lỗi contract mẫu, khớp đúng thông báo của platform/contracts/validate.py (jsonschema).
VIOLATION_ERRORS = [
    "payload: 'p' is a required property",
    "payload/t: '{seq}' is not of type 'integer'",
]
