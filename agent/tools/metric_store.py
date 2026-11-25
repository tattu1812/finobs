"""
Backend thật cho tool query_metric: đọc bảng metric_observations của metric store
(PostgreSQL, observability/store/*.sql) do metrics-collector ghi mỗi phút.

    from agent.tools.registry import Toolbox
    tb = Toolbox.with_metric_store("postgresql://finobs:...@localhost:5435/finobs_metrics")
    tb.call("query_metric", {"metric": "trade_count_1m", "group": {"source": "binance"},
                             "start": "...", "end": "..."})

Đơn vị lấy từ observability/metric_catalog.yaml. Giá trị null (status khác ok, ví dụ
source_unavailable) được giữ nguyên là null, đúng mô tả trong schema của tool.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

CATALOG = Path(__file__).resolve().parents[2] / "observability" / "metric_catalog.yaml"

QUERY = """
    SELECT dimensions, observed_at, value
    FROM metric_observations
    WHERE metric_name = %s
      AND observed_at >= %s AND observed_at < %s
      AND dimensions @> %s::jsonb
    ORDER BY observed_at
"""


def catalog_units(path: Path = CATALOG) -> dict[str, str]:
    import yaml

    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    return {m["name"]: m.get("unit", "unknown") for m in doc.get("metrics", [])}


class MetricStore:
    def __init__(self, dsn: str | None = None, connect=None, units: dict[str, str] | None = None):
        """connect: hàm trả về connection kiểu DB-API (mặc định psycopg.connect(dsn)); truyền vào trong test."""
        if connect is None:
            if not dsn:
                raise ValueError("MetricStore cần dsn hoặc connect")
            import psycopg

            def connect():
                return psycopg.connect(dsn, connect_timeout=5)

        self._connect = connect
        self.units = units if units is not None else catalog_units()

    def series(self, metric: str, group: dict, start: datetime, end: datetime) -> dict | None:
        """{unit, series: [(group, [(ts, value)])]} giống FakeWorld.metrics[metric]; None nếu không có dòng nào."""
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(QUERY, (metric, start, end, json.dumps(group or {})))
                rows = cursor.fetchall()
        if not rows:
            return None

        by_group: dict[str, tuple[dict, list]] = defaultdict(lambda: ({}, []))
        for dimensions, observed_at, value in rows:
            dims = dimensions if isinstance(dimensions, dict) else json.loads(dimensions)
            key = json.dumps(dims, sort_keys=True)
            if key not in by_group:
                by_group[key] = ({k: str(v) for k, v in dims.items()}, [])
            by_group[key][1].append((observed_at, None if value is None else float(value)))
        return {
            "unit": self.units.get(metric, "unknown"),
            "series": [by_group[k] for k in sorted(by_group)],
        }
