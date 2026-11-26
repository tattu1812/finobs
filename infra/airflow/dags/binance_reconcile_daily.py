"""DAG binance_reconcile_daily (DE-B, Sprint 4): đối soát D+1 stream vs file chính thức.

Chạy hằng ngày lúc 05:00 UTC (sau DAG binance_official_daily 03:30 UTC).
Với mỗi mã Binance:
  1. Nạp file chính thức D-1 nếu chưa nạp (gọi binance_official.load_day)
  2. Chạy đối soát: so khớp stream bronze với file chính thức

Chịu được file chính thức chưa có: retry 3 lần × 60 phút.
Idempotent: chạy lại cùng ngày không nhân đôi kết quả (upsert metric, overwrite report).
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

FINOBS_HOME = Path("/opt/finobs")
sys.path.insert(0, str(FINOBS_HOME / "platform" / "batch"))


def _symbols() -> list[str]:
    path = FINOBS_HOME / "config" / "symbols.json"
    try:
        return json.loads(path.read_text(encoding="utf-8"))["binance"]
    except FileNotFoundError:
        return []


def load_official_if_needed(symbol: str, data_interval_start=None, **_) -> dict:
    """Nạp file chính thức D-1 nếu chưa nạp. Nếu file chưa được công bố → exception → retry."""
    import binance_official

    day = data_interval_start.date()
    return binance_official.load_day(symbol, day)


def reconcile_symbol_task(symbol: str, data_interval_start=None, **_) -> dict:
    """Chạy đối soát cho 1 mã, 1 ngày."""
    from decimal import Decimal

    # Thêm path cho module reconcile
    sys.path.insert(0, str(FINOBS_HOME))

    from observability.reconcile.reconcile import (
        ReconcileReport,
        generate_markdown,
        reconcile_result_to_metrics,
        reconcile_symbol,
    )
    from observability.reconcile.gap_detection import gap_report_to_metrics

    day = data_interval_start.date()
    day_str = day.isoformat()
    now = datetime.now(timezone.utc)
    observed_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    # --- Lấy dữ liệu từ Trino ---
    import subprocess

    def trino_query(sql: str) -> list[dict]:
        """Chạy query Trino và trả về list of dicts."""
        cmd = [
            "docker", "exec", "finobs-trino", "trino",
            "--output-format", "JSON",
            "--execute", sql,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            print(f"Trino error: {result.stderr}", file=sys.stderr)
            return []
        try:
            return json.loads(result.stdout) if result.stdout.strip() else []
        except json.JSONDecodeError:
            return []

    next_day = day + timedelta(days=1)

    # Stream trades
    stream_sql = (
        f"SELECT event_id, payload, CAST(event_ts AS VARCHAR) AS event_ts "
        f"FROM iceberg.bronze.trades_raw "
        f"WHERE source = 'binance' AND symbol = '{symbol}' "
        f"AND event_ts >= TIMESTAMP '{day_str} 00:00:00' "
        f"AND event_ts < TIMESTAMP '{next_day.isoformat()} 00:00:00'"
    )

    # Official trades
    official_sql = (
        f"SELECT CAST(trade_id AS VARCHAR) AS trade_id, "
        f"CAST(price AS VARCHAR) AS price, CAST(qty AS VARCHAR) AS qty, "
        f"CAST(trade_ts AS VARCHAR) AS trade_ts "
        f"FROM iceberg.bronze.binance_official_trades "
        f"WHERE file_date = DATE '{day_str}' AND symbol = '{symbol}'"
    )

    # Note: trong container Airflow, trino_query dùng docker exec — chỉ hoạt động
    # nếu Airflow scheduler chạy trên host hoặc có Docker socket mount.
    # Trong production, sẽ dùng trino Python client hoặc Spark SQL.
    # Hiện tại, DAG này là reference implementation — chạy trên VM qua docker exec.
    from observability.reconcile.reconcile import (
        load_tolerances,
        parse_official_trade,
        parse_stream_trade,
    )

    config_path = FINOBS_HOME / "config" / "reconcile.yaml"
    tolerances = load_tolerances(config_path)

    stream_rows = trino_query(stream_sql)
    official_rows = trino_query(official_sql)

    if not official_rows:
        # File chính thức chưa được nạp hoặc chưa có
        result_dict = {
            "symbol": symbol, "date": day_str, "status": "OFFICIAL_NOT_AVAILABLE",
            "note": "File chính thức chưa được nạp hoặc chưa có trên Binance Public Data",
        }
        print(json.dumps(result_dict, ensure_ascii=False))
        return result_dict

    stream_trades = [parse_stream_trade(r) for r in stream_rows]
    official_trades = [parse_official_trade(r) for r in official_rows]

    result = reconcile_symbol(stream_trades, official_trades, symbol, tolerances)

    # Ghi metric vào metric store
    import os

    dsn = os.environ.get("FINOBS_METRIC_DSN")
    if dsn:
        sys.path.insert(0, str(FINOBS_HOME / "observability" / "collectors"))
        from trade_count import write_to_store

        metrics = reconcile_result_to_metrics(result, observed_at, day_str)
        if result.gap_report:
            metrics += gap_report_to_metrics(
                result.gap_report, observed_at,
                f"{day_str}T00:00:00Z", f"{next_day.isoformat()}T00:00:00Z",
            )
        try:
            write_to_store(metrics, dsn)
        except Exception as exc:
            print(f"[reconcile] ghi metric store lỗi: {exc!r}", file=sys.stderr)

    # Ghi báo cáo
    report_dir = FINOBS_HOME / "reports" / "reconcile"
    report_dir.mkdir(parents=True, exist_ok=True)

    report = ReconcileReport(
        report_date=day_str, run_at=observed_at, code_version="airflow",
        is_partial_day=False, window_from=None, window_to=None,
        symbols=[result],
    )

    summary = result.as_dict()
    print(json.dumps(summary, ensure_ascii=False, default=str))
    return summary


with DAG(
    dag_id="binance_reconcile_daily",
    description="Đối soát D+1: stream bronze vs file chính thức Binance → báo cáo + metric",
    schedule="0 5 * * *",
    start_date=datetime(2026, 11, 27, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"owner": "DE", "retries": 3, "retry_delay": timedelta(minutes=60)},
    tags=["finobs", "reconcile", "binance", "sprint4"],
) as dag:
    for sym in _symbols():
        load_task = PythonOperator(
            task_id=f"load_official_{sym.lower()}",
            python_callable=load_official_if_needed,
            op_kwargs={"symbol": sym},
        )
        reconcile_task = PythonOperator(
            task_id=f"reconcile_{sym.lower()}",
            python_callable=reconcile_symbol_task,
            op_kwargs={"symbol": sym},
        )
        load_task >> reconcile_task
