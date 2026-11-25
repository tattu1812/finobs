"""Áp mọi file observability/store/*.sql theo thứ tự tên (idempotent: mọi lệnh đều IF NOT EXISTS
hoặc CREATE OR REPLACE). PostgreSQL chỉ tự chạy initdb ở lần tạo volume đầu tiên; metrics-collector
gọi hàm này lúc khởi động để metric store đã có từ sprint trước cũng nhận bảng mới.

    python observability/store/apply.py          # cần FINOBS_METRIC_DSN
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent


def migration_files() -> list[Path]:
    return sorted(HERE.glob("[0-9][0-9][0-9]_*.sql"))


def apply(dsn: str) -> list[str]:
    import psycopg

    applied = []
    with psycopg.connect(dsn, autocommit=True) as connection:
        for path in migration_files():
            connection.execute(path.read_text(encoding="utf-8"))
            applied.append(path.name)
    return applied


if __name__ == "__main__":
    dsn = os.environ.get("FINOBS_METRIC_DSN")
    if not dsn:
        sys.exit("cần FINOBS_METRIC_DSN")
    print("đã áp:", ", ".join(apply(dsn)))
