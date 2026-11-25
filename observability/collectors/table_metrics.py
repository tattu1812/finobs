"""Metric cấp bảng, đọc Iceberg qua Trino (Sprint 3: OB-4, OB-6).

  table_freshness_seconds      lineage/freshness  tuổi snapshot mới nhất của mỗi bảng trong pipeline
  trade_size_p95_5m            distribution       p95 khối lượng trade trong 5 phút (silver)
  return_1m_bps                distribution       lợi suất giá đóng cửa so với phút trước (gold)
  cross_source_spread_bps      distribution       độ lệch giá đóng cửa giữa các sàn (gold, đầu vào C1)

Metric theo phút được tính cho phút kết thúc SETTLE_MINUTES trước mốc hiện tại: gold của một phút
còn được cập nhật khi dữ liệu trễ tới, chờ 2 phút để giá trị ổn định.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from observability.collectors.trade_count import iso_utc
from observability.collectors.trino_client import TrinoClient

SETTLE_MINUTES = 2
TABLES = ["bronze.trades_raw", "bronze.binance_official_trades", "silver.trades",
          "gold.ohlcv_1m", "gold.cross_exchange_spread"]


def _ts(value: datetime) -> str:
    return f"TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S')} UTC'"


def _row(metric, dims, observed_at, start, end, value, samples=None) -> dict:
    return {
        "metric_name": metric, "dimensions": dims, "observed_at": iso_utc(observed_at),
        "window_start": iso_utc(start) if start else None, "window_end": iso_utc(end) if end else None,
        "value": None if value is None else float(value),
        "status": "ok" if value is not None else "no_data", "sample_count": samples,
    }


def table_freshness(trino: TrinoClient, observed_at: datetime) -> list[dict]:
    rows = []
    for table in TABLES:
        namespace, name = table.split(".")
        try:
            result = trino.query(f'SELECT to_unixtime(max(committed_at)) AS last FROM iceberg.{namespace}."{name}$snapshots"')
            last = result[0]["last"] if result else None
        except Exception:  # noqa: BLE001 — bảng chưa tồn tại (chưa chạy job) -> no_data
            last = None
        age = observed_at.timestamp() - last if last is not None else None
        rows.append(_row("table_freshness_seconds", {"asset": f"finobs:{table}"}, observed_at, None, None,
                         round(age, 3) if age is not None else None))
    return rows


def _safe(trino: TrinoClient, sql: str) -> list[dict]:
    """Bảng chưa tồn tại (job chưa chạy lần nào) thì bỏ metric đó, không làm hỏng các metric khác."""
    try:
        return trino.query(sql)
    except Exception:  # noqa: BLE001
        return []


def distribution(trino: TrinoClient, now: datetime) -> list[dict]:
    end = now.replace(second=0, microsecond=0) - timedelta(minutes=SETTLE_MINUTES)
    minute = end - timedelta(minutes=1)
    rows = []

    size = _safe(trino, f"""
        SELECT source, symbol_canonical, approx_percentile(CAST(qty AS double), 0.95) AS p95, count(*) AS n
        FROM iceberg.silver.trades
        WHERE event_ts >= {_ts(end - timedelta(minutes=5))} AND event_ts < {_ts(end)}
        GROUP BY 1, 2""")
    for r in size:
        rows.append(_row("trade_size_p95_5m", {"source": r["source"], "symbol_canonical": r["symbol_canonical"]},
                         end, end - timedelta(minutes=5), end, r["p95"], r["n"]))

    returns = _safe(trino, f"""
        SELECT c.source, c.symbol_std,
               (CAST(c.close AS double) / CAST(p.close AS double) - 1) * 10000 AS bps, c.trade_count AS n
        FROM iceberg.gold.ohlcv_1m c
        JOIN iceberg.gold.ohlcv_1m p
          ON p.source = c.source AND p.symbol_std = c.symbol_std AND p.minute = c.minute - INTERVAL '1' MINUTE
        WHERE c.minute = {_ts(minute)}""")
    for r in returns:
        rows.append(_row("return_1m_bps", {"source": r["source"], "symbol_std": r["symbol_std"]},
                         end, minute, end, r["bps"], r["n"]))

    spreads = _safe(trino, f"""
        SELECT symbol_std, spread_bps, n_sources FROM iceberg.gold.cross_exchange_spread
        WHERE minute = {_ts(minute)}""")
    for r in spreads:
        rows.append(_row("cross_source_spread_bps", {"symbol_std": r["symbol_std"]},
                         end, minute, end, r["spread_bps"], r["n_sources"]))
    return rows


def collect(trino: TrinoClient, now: datetime) -> list[dict]:
    return table_freshness(trino, now) + distribution(trino, now)
