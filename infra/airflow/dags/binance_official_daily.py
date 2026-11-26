"""
DAG binance_official_daily (FR-05): mỗi ngày nạp file trades chính thức của ngày hôm trước.

Mỗi mã Binance trong config/symbols.json là một task: tải zip + .CHECKSUM,
kiểm SHA-256, chỉ khi khớp mới ghi iceberg bronze.binance_official_trades.
File sai checksum -> task FAILED (không ghi gì), Airflow thử lại theo retries.

Ngày được nạp = data_interval_start (logical date): run lúc 03:30 UTC ngày D+1
nạp file của ngày D. Backfill:  airflow dags backfill -s 2026-11-20 -e 2026-11-24 binance_official_daily

Param simulate_corruption=true làm hỏng file sau khi tải, để demo nhánh sai checksum.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

FINOBS_HOME = Path("/opt/finobs")
sys.path[:0] = [str(FINOBS_HOME / "platform" / "batch"), str(FINOBS_HOME / "platform")]  # platform/ trùng tên module chuẩn


def _symbols() -> list[str]:
    path = FINOBS_HOME / "config" / "symbols.json"
    try:
        return json.loads(path.read_text(encoding="utf-8"))["binance"]
    except FileNotFoundError:
        return []


def load_symbol(symbol: str, data_interval_start=None, params=None, **_) -> dict:
    import binance_official
    from lineage import LineageEmitter

    day = data_interval_start.date()
    lineage = LineageEmitter.from_env()
    run = lineage.start("binance_official_daily", outputs=["bronze.binance_official_trades"])
    try:
        summary = binance_official.load_day(
            symbol, day, tamper=bool((params or {}).get("simulate_corruption", False)))
    except Exception as exc:
        lineage.fail(run, repr(exc))
        raise
    lineage.complete(run)
    print(json.dumps(summary, ensure_ascii=False))
    return summary


with DAG(
    dag_id="binance_official_daily",
    description="Binance Public Data trades D+1 -> kiểm SHA-256 -> bronze.binance_official_trades",
    schedule="30 3 * * *",
    start_date=datetime(2026, 11, 20, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    # Một task mỗi lúc: các task cùng ghi một bảng Iceberg (commit tuần tự),
    # và giữ RAM của scheduler thấp. Bộ nạp vẫn tự thử lại nếu commit bị xung đột.
    max_active_tasks=1,
    default_args={"owner": "DE", "retries": 3, "retry_delay": timedelta(minutes=30)},
    params={"simulate_corruption": Param(False, type="boolean")},
    tags=["finobs", "batch", "binance", "C2"],
) as dag:
    for sym in _symbols():
        PythonOperator(task_id=f"load_{sym.lower()}", python_callable=load_symbol, op_kwargs={"symbol": sym})
