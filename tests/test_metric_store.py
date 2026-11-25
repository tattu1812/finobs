"""query_metric đọc metric store thật (agent/tools/metric_store.py).

Test đơn vị dùng connection giả. Test tích hợp chỉ chạy khi có FINOBS_METRIC_DSN trỏ tới
metric store đang chạy (make up), ví dụ:
    FINOBS_METRIC_DSN=postgresql://finobs:<mật khẩu>@localhost:5435/finobs_metrics \\
        python -m unittest tests.test_metric_store
"""

from __future__ import annotations

import os
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from jsonschema import Draft202012Validator

from agent.tools.metric_store import MetricStore, catalog_units
from agent.tools.registry import Toolbox

T0 = datetime(2026, 11, 25, 5, 18, tzinfo=timezone.utc)
START, END = "2026-11-25T05:00:00.000Z", "2026-11-25T06:00:00.000Z"


class FakeCursor:
    def __init__(self, rows, log):
        self.rows, self.log = rows, log

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params):
        self.log.append(params)

    def fetchall(self):
        return self.rows


class FakeConnection(FakeCursor):
    def cursor(self):
        return FakeCursor(self.rows, self.log)


def store_with(rows):
    log = []
    return MetricStore(connect=lambda: FakeConnection(rows, log), units=catalog_units()), log


class MetricStoreTest(unittest.TestCase):
    def test_units_come_from_catalog(self):
        units = catalog_units()
        self.assertEqual(units["trade_count_1m"], "trades")
        self.assertEqual(units["ingest_delay_p95_ms"], "milliseconds")

    def test_rows_grouped_by_dimensions_and_null_kept(self):
        rows = [
            ({"source": "binance", "symbol_canonical": "BTC-USDT"}, T0, Decimal("1094")),
            ({"source": "coinbase", "symbol_canonical": "BTC-USD"}, T0, None),
            ({"source": "binance", "symbol_canonical": "BTC-USDT"}, T0 + timedelta(minutes=1), Decimal("1020")),
        ]
        store, log = store_with(rows)
        entry = store.series("trade_count_1m", {"symbol_canonical": "BTC-USDT"}, T0, T0 + timedelta(hours=1))
        self.assertEqual(entry["unit"], "trades")
        self.assertEqual(len(entry["series"]), 2)
        binance = next(points for group, points in entry["series"] if group["source"] == "binance")
        self.assertEqual([v for _, v in binance], [1094.0, 1020.0])
        coinbase = next(points for group, points in entry["series"] if group["source"] == "coinbase")
        self.assertEqual(coinbase, [(T0, None)])
        self.assertEqual(log[0][0], "trade_count_1m")
        self.assertEqual(log[0][3], '{"symbol_canonical": "BTC-USDT"}')

    def test_toolbox_output_matches_tool_schema(self):
        store, _ = store_with([({"source": "binance", "symbol_canonical": "BTC-USDT"}, T0, Decimal("7.5"))])
        tb = Toolbox.fake()
        tb.metric_store = store
        result = tb.call("query_metric", {"metric": "ingest_delay_p95_ms", "start": START, "end": END})
        self.assertNotIn("error", result)
        Draft202012Validator(tb.specs["query_metric"]["output_schema"]).validate(result)
        self.assertEqual(result["series"][0]["points"], [{"ts": "2026-11-25T05:18:00.000Z", "value": 7.5}])
        self.assertEqual(result["unit"], "milliseconds")

    def test_empty_result_is_not_found(self):
        store, _ = store_with([])
        tb = Toolbox.fake()
        tb.metric_store = store
        result = tb.call("query_metric", {"metric": "trade_count_1m", "start": START, "end": END})
        self.assertEqual(result["error"]["code"], "not_found")

    def test_database_error_is_backend_unavailable(self):
        def broken():
            raise OSError("connection refused")

        tb = Toolbox.fake()
        tb.metric_store = MetricStore(connect=broken, units={})
        result = tb.call("query_metric", {"metric": "trade_count_1m", "start": START, "end": END})
        self.assertEqual(result["error"]["code"], "backend_unavailable")


@unittest.skipUnless(os.environ.get("FINOBS_METRIC_DSN"), "cần FINOBS_METRIC_DSN trỏ tới metric store đang chạy")
class MetricStoreIntegrationTest(unittest.TestCase):
    def test_live_trade_count(self):
        tb = Toolbox.with_metric_store(os.environ["FINOBS_METRIC_DSN"])
        now = datetime.now(timezone.utc)
        fmt = "%Y-%m-%dT%H:%M:%S.000Z"
        result = tb.call("query_metric", {
            "metric": "trade_count_1m", "group": {"source": "binance"},
            "start": (now - timedelta(minutes=30)).strftime(fmt), "end": now.strftime(fmt),
        })
        self.assertNotIn("error", result, result)
        Draft202012Validator(tb.specs["query_metric"]["output_schema"]).validate(result)
        self.assertTrue(any(s["summary"]["count"] > 0 for s in result["series"]))


if __name__ == "__main__":
    unittest.main()
