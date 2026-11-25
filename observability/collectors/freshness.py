"""Hai metric trụ cột freshness trong observability/metric_catalog.yaml.

  ingest_delay_p95_ms          p95(ingest_time - event_time) của trade live trong 1 phút
  last_live_trade_age_seconds  observed_at - max(ingest_time) của trade live

Đầu vào giống trade_count.observations: dict event_id -> (ingest_time, record), đã kiểm
contract và khử trùng (giữ bản nhận sớm nhất). Trade snapshot của Coinbase bị loại.

Cửa sổ phút tính theo ingest_time, nhất quán với trade_count_1m. p95 dùng phân vị
"nearest rank": phần tử thứ ceil(0.95 * n) sau khi sắp xếp, không nội suy, để giá trị
luôn là một độ trễ đã đo thật. Độ trễ âm (đồng hồ máy thu chậm hơn sàn) được giữ nguyên:
đó chính là tín hiệu lệch đồng hồ mà kịch bản S003 cần thấy.
"""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import datetime, timedelta

from observability.collectors.trade_count import iso_utc, utc_time

DELAY_METRIC = "ingest_delay_p95_ms"
AGE_METRIC = "last_live_trade_age_seconds"


def is_live(record: dict) -> bool:
    return not record.get("transport", {}).get("is_snapshot", False)


def nearest_rank(values: list[float], q: float) -> float:
    if not values:
        raise ValueError("nearest_rank cần ít nhất một giá trị")
    ordered = sorted(values)
    return ordered[max(1, math.ceil(q * len(ordered))) - 1]


def _row(metric, source, symbol, observed_at, start, end, value, sample_count) -> dict:
    return {
        "metric_name": metric,
        "dimensions": {"source": source, "symbol_canonical": symbol},
        "observed_at": iso_utc(observed_at),
        "window_start": iso_utc(start) if start else None,
        "window_end": iso_utc(end) if end else None,
        "value": value,
        "status": "ok" if value is not None else "no_data",
        "sample_count": sample_count,
    }


def ingest_delay_p95(first: dict, window_end: datetime | None = None,
                     expected: set[tuple[str, str]] | None = None) -> list[dict]:
    """Một dòng mỗi (nguồn, mã, phút). expected: các cặp phải có dòng kể cả khi không có trade
    (giá trị null, status no_data, theo when_no_trade_in_window: null của catalog)."""
    delays: dict[tuple, list[float]] = defaultdict(list)
    for received, record in first.values():
        if not is_live(record):
            continue
        start = received.replace(second=0, microsecond=0)
        if window_end is not None and start + timedelta(minutes=1) != window_end:
            continue
        delay_ms = (received - utc_time(record["event_time"])).total_seconds() * 1000
        delays[(record["source"], record["symbol_canonical"], start)].append(delay_ms)

    if window_end is not None and expected:
        start = window_end - timedelta(minutes=1)
        for source, symbol in expected:
            delays.setdefault((source, symbol, start), [])

    rows = []
    for (source, symbol, start), values in sorted(delays.items()):
        end = start + timedelta(minutes=1)
        value = round(nearest_rank(values, 0.95), 3) if values else None
        rows.append(_row(DELAY_METRIC, source, symbol, end, start, end, value, len(values)))
    return rows


def last_live_trade_age(first: dict, observed_at: datetime,
                        expected: set[tuple[str, str]] | None = None) -> list[dict]:
    """Tuổi của trade live mới nhất tính tới observed_at (chỉ xét trade nhận trước observed_at)."""
    latest: dict[tuple[str, str], datetime] = {}
    for received, record in first.values():
        if not is_live(record) or received >= observed_at:
            continue
        key = (record["source"], record["symbol_canonical"])
        if key not in latest or received > latest[key]:
            latest[key] = received

    keys = set(latest) | (expected or set())
    rows = []
    for source, symbol in sorted(keys):
        last = latest.get((source, symbol))
        value = round((observed_at - last).total_seconds(), 3) if last else None
        rows.append(_row(AGE_METRIC, source, symbol, observed_at, None, None, value, None))
    return rows
