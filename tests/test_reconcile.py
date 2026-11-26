"""Unit test: đối soát D+1 stream vs file chính thức Binance (DE-B, Sprint 4)."""

from __future__ import annotations

import json
import sys
import unittest
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from observability.reconcile.reconcile import (
    ReconcileReport,
    SymbolResult,
    generate_markdown,
    reconcile_result_to_metrics,
    reconcile_symbol,
)


def _stream(trade_id, price="100.00000000", qty="1.00000000", ts_ms=1795478400000):
    return {"trade_id": trade_id, "price": Decimal(price), "qty": Decimal(qty), "event_ts_ms": ts_ms}


def _official(trade_id, price="100.00000000", qty="1.00000000", ts_ms=1795478400000):
    return {"trade_id": trade_id, "price": Decimal(price), "qty": Decimal(qty), "trade_ts_ms": ts_ms}


TOLERANCES = {"price_tolerance": Decimal("0"), "qty_tolerance": Decimal("0"), "time_tolerance_ms": 1000}


class TestReconcileSymbol(unittest.TestCase):
    """Test hàm thuần tuý reconcile_symbol."""

    # --- Khớp hoàn toàn ---
    def test_perfect_match(self):
        stream = [_stream(100), _stream(101), _stream(102)]
        official = [_official(100), _official(101), _official(102)]
        result = reconcile_symbol(stream, official, "BTCUSDT", TOLERANCES)
        self.assertEqual(result.status, "OK")
        self.assertEqual(result.matched, 3)
        self.assertEqual(result.match_ratio, 1.0)
        self.assertEqual(len(result.missing_in_stream), 0)
        self.assertEqual(len(result.extra_in_stream), 0)
        self.assertEqual(len(result.value_mismatches), 0)

    # --- Thiếu vài trade trong stream ---
    def test_missing_in_stream(self):
        stream = [_stream(100), _stream(103)]
        official = [_official(100), _official(101), _official(102), _official(103)]
        result = reconcile_symbol(stream, official, "BTCUSDT", TOLERANCES)
        self.assertEqual(result.status, "MISSING_IN_STREAM")
        self.assertEqual(len(result.missing_in_stream), 2)
        self.assertIn(101, result.missing_in_stream)
        self.assertIn(102, result.missing_in_stream)

    # --- Thừa vài trade trong stream ---
    def test_extra_in_stream(self):
        stream = [_stream(100), _stream(101), _stream(102), _stream(103)]
        official = [_official(100), _official(101)]
        result = reconcile_symbol(stream, official, "ETHUSDT", TOLERANCES)
        self.assertEqual(result.status, "EXTRA_IN_STREAM")
        self.assertEqual(len(result.extra_in_stream), 2)
        self.assertIn(102, result.extra_in_stream)
        self.assertIn(103, result.extra_in_stream)

    # --- Sai giá ---
    def test_price_mismatch(self):
        stream = [_stream(100, price="100.00000000"), _stream(101, price="99.00000000")]
        official = [_official(100, price="100.00000000"), _official(101, price="101.00000000")]
        result = reconcile_symbol(stream, official, "SOLUSDT", TOLERANCES)
        self.assertEqual(result.status, "VALUE_MISMATCH")
        self.assertEqual(len(result.value_mismatches), 1)
        self.assertEqual(result.value_mismatches[0].trade_id, 101)
        self.assertEqual(result.value_mismatches[0].field, "price")

    # --- File chính thức vắng mặt ---
    def test_official_not_available(self):
        stream = [_stream(100)]
        result = reconcile_symbol(stream, [], "XRPUSDT", TOLERANCES)
        self.assertEqual(result.status, "OFFICIAL_NOT_AVAILABLE")
        self.assertEqual(result.stream_count, 1)

    # --- Lệch múi giờ ở ranh giới ngày (UTC) ---
    def test_timezone_boundary(self):
        """Trade ở 23:59:59.999Z (day) và 00:00:00.001Z (day+1) khớp theo ID.
        time diff = 2ms, dưới tolerance 1000ms → matched."""
        ts1 = 1795478399999  # 23:59:59.999
        ts2 = 1795478400001  # 00:00:00.001
        stream = [_stream(100, ts_ms=ts1)]
        official = [_official(100, ts_ms=ts2)]
        result = reconcile_symbol(stream, official, "ADAUSDT", TOLERANCES)
        self.assertEqual(result.status, "OK")

    # --- Lệch thời gian vượt tolerance ---
    def test_time_mismatch_exceeds_tolerance(self):
        ts_stream = 1795478400000
        ts_official = ts_stream + 2000  # 2 seconds > 1000ms tolerance
        stream = [_stream(100, ts_ms=ts_stream)]
        official = [_official(100, ts_ms=ts_official)]
        result = reconcile_symbol(stream, official, "DOGEUSDT", TOLERANCES)
        self.assertEqual(result.status, "VALUE_MISMATCH")
        self.assertEqual(result.value_mismatches[0].field, "time_ms")

    # --- Cả stream và official rỗng ---
    def test_both_empty(self):
        result = reconcile_symbol([], [], "CELRUSDT", TOLERANCES)
        self.assertEqual(result.status, "OFFICIAL_NOT_AVAILABLE")

    # --- Detection coverage: gap detection bắt được thiếu ---
    def test_detection_coverage(self):
        stream = [_stream(100), _stream(104)]  # gap 101-103
        official = [_official(100), _official(101), _official(102), _official(103), _official(104)]
        result = reconcile_symbol(stream, official, "BTCUSDT", TOLERANCES)
        self.assertEqual(result.status, "MISSING_IN_STREAM")
        # gap detection phát hiện gap 101-103, trùng với missing → coverage = 1.0
        self.assertEqual(result.detection_coverage, 1.0)

    # --- Duplicate trong stream không tạo extra ---
    def test_stream_duplicates_not_counted_as_extra(self):
        stream = [_stream(100), _stream(100), _stream(101)]
        official = [_official(100), _official(101)]
        result = reconcile_symbol(stream, official, "ALGOUSDT", TOLERANCES)
        self.assertEqual(result.status, "OK")
        self.assertEqual(len(result.extra_in_stream), 0)


class TestReconcileMetrics(unittest.TestCase):
    def test_ok_produces_four_metrics(self):
        result = SymbolResult(symbol="BTCUSDT", status="OK", official_count=100,
                              stream_count=100, matched=100)
        metrics = reconcile_result_to_metrics(result, "2026-11-28T05:00:00Z", "2026-11-27")
        self.assertEqual(len(metrics), 4)
        names = {m["metric_name"] for m in metrics}
        self.assertIn("reconciliation_match_ratio", names)
        self.assertIn("reconciliation_missing_trades", names)

    def test_unavailable_produces_one_metric(self):
        result = SymbolResult(symbol="BTCUSDT", status="OFFICIAL_NOT_AVAILABLE")
        metrics = reconcile_result_to_metrics(result, "2026-11-28T05:00:00Z", "2026-11-27")
        self.assertEqual(len(metrics), 1)
        self.assertEqual(metrics[0]["status"], "no_data")


class TestReportGeneration(unittest.TestCase):
    def test_markdown_contains_table(self):
        report = ReconcileReport(
            report_date="2026-11-27", run_at="2026-11-28T05:00:00Z",
            code_version="abc1234", is_partial_day=False,
            window_from=None, window_to=None,
            symbols=[SymbolResult(symbol="BTCUSDT", status="OK",
                                  official_count=100, stream_count=100, matched=100)],
        )
        md = generate_markdown(report)
        self.assertIn("BTCUSDT", md)
        self.assertIn("100.000%", md)
        self.assertIn("OK", md)

    def test_partial_day_note(self):
        report = ReconcileReport(
            report_date="2026-11-26", run_at="2026-11-27T05:00:00Z",
            code_version="abc1234", is_partial_day=True,
            window_from="08:00", window_to="23:59",
        )
        md = generate_markdown(report)
        self.assertIn("Ngày chưa đầy đủ", md)
        self.assertIn("08:00", md)

    def test_json_serialization(self):
        result = SymbolResult(symbol="ETHUSDT", status="MISSING_IN_STREAM",
                              official_count=1000, stream_count=990, matched=990,
                              missing_in_stream=list(range(10)))
        report = ReconcileReport(
            report_date="2026-11-27", run_at="2026-11-28T05:00:00Z",
            code_version="abc1234", is_partial_day=False,
            window_from=None, window_to=None, symbols=[result],
        )
        d = report.as_dict()
        # Should be JSON-serializable
        text = json.dumps(d, default=str)
        self.assertIn("ETHUSDT", text)


if __name__ == "__main__":
    unittest.main()
