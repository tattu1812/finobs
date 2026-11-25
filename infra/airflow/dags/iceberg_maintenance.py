"""
DAG iceberg_maintenance (Sprint 3, PL-8): mỗi ngày 04:30 UTC gộp file nhỏ, xóa snapshot quá 7 ngày,
xóa file mồ côi cho các bảng bronze/silver/gold qua Trino. Một task mỗi bảng, chạy tuần tự để không
tranh tài nguyên với job streaming. Bảng chưa tồn tại (job chưa chạy lần nào) thì task bỏ qua.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/finobs/platform/batch")


def maintain_table(table: str, **_) -> dict:
    import os

    import iceberg_maintenance
    from trino_client import TrinoClient, TrinoError

    trino = TrinoClient(os.environ.get("TRINO_URL", "http://trino:8080"), timeout=1800)
    try:
        result = iceberg_maintenance.maintain(trino, table)
    except TrinoError as exc:
        if "does not exist" in str(exc):
            raise AirflowSkipException(f"{table} chưa tồn tại") from exc
        raise
    print(json.dumps(result, ensure_ascii=False))
    return result


with DAG(
    dag_id="iceberg_maintenance",
    description="Iceberg: optimize + expire_snapshots(7d) + remove_orphan_files qua Trino",
    schedule="30 4 * * *",
    start_date=datetime(2026, 11, 25, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"owner": "PL", "retries": 2, "retry_delay": timedelta(minutes=15)},
    tags=["finobs", "maintenance", "iceberg"],
) as dag:
    previous = None
    for name in ("bronze.trades_raw", "bronze.binance_official_trades", "silver.trades",
                 "gold.ohlcv_1m", "gold.cross_exchange_spread"):
        task = PythonOperator(task_id=f"maintain_{name.replace('.', '_')}", python_callable=maintain_table,
                              op_kwargs={"table": name}, trigger_rule="all_done")
        if previous is not None:
            previous >> task
        previous = task
