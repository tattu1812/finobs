"""Bảo trì bảng Iceberg qua Trino (Sprint 3, PL-8): gộp file nhỏ, xóa snapshot cũ, xóa file mồ côi.

    TRINO_URL=http://trino:8080 python platform/batch/iceberg_maintenance.py
    python platform/batch/iceberg_maintenance.py --tables bronze.trades_raw --dry-run

Bronze ghi mỗi phút một lần (S3 đã đổi trigger 15 s -> 60 s) nên vẫn sinh vài nghìn file nhỏ mỗi ngày;
silver/gold ghi bằng MERGE còn sinh thêm snapshot. Chạy hằng ngày bởi DAG iceberg_maintenance.
Giữ snapshot 7 ngày (mức tối thiểu mặc định của Trino): đủ cho time travel khi tái lập kịch bản
lỗi, và stream Spark dừng dưới 7 ngày vẫn chạy tiếp được từ checkpoint.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

for candidate in (Path("/opt/finobs/observability/collectors"),
                  Path(__file__).resolve().parents[2] / "observability" / "collectors"):
    if candidate.exists():
        sys.path.insert(0, str(candidate))
        break
from trino_client import TrinoClient  # noqa: E402

TABLES = ["bronze.trades_raw", "bronze.binance_official_trades", "silver.trades",
          "gold.ohlcv_1m", "gold.cross_exchange_spread"]
RETENTION = "7d"
TARGET_FILE_SIZE = "128MB"


def file_stats(trino: TrinoClient, table: str) -> dict:
    namespace, name = table.split(".")
    row = trino.query(f'SELECT count(*) AS files, coalesce(sum(file_size_in_bytes), 0) AS bytes '
                      f'FROM iceberg.{namespace}."{name}$files"')[0]
    snaps = trino.query(f'SELECT count(*) AS n FROM iceberg.{namespace}."{name}$snapshots"')[0]["n"]
    return {"files": row["files"], "mb": round(row["bytes"] / 1048576, 1), "snapshots": snaps}


def maintain(trino: TrinoClient, table: str, dry_run: bool = False) -> dict:
    full = f"iceberg.{table}"
    before = file_stats(trino, table)
    if not dry_run:
        trino.query(f"ALTER TABLE {full} EXECUTE optimize(file_size_threshold => '{TARGET_FILE_SIZE}')")
        trino.query(f"ALTER TABLE {full} EXECUTE expire_snapshots(retention_threshold => '{RETENTION}')")
        trino.query(f"ALTER TABLE {full} EXECUTE remove_orphan_files(retention_threshold => '{RETENTION}')")
    after = file_stats(trino, table)
    return {"table": table, "before": before, "after": after}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tables", default=",".join(TABLES))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    trino = TrinoClient(os.environ.get("TRINO_URL", "http://trino:8080"), timeout=1800)
    for table in args.tables.split(","):
        print(json.dumps(maintain(trino, table.strip(), args.dry_run), ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
