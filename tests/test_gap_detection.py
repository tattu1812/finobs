"""Unit test: phát hiện khoảng hở trade ID (DE-A, Sprint 4)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from observability.reconcile.gap_detection import (
    Gap,
    GapReport,
    analyze_trade_id_sequence,
    gap_report_to_metrics,
)


class TestAnalyzeTradeIdSequence(unittest.TestCase):
    """Test hàm thuần tuý analyze_trade_id_sequence."""

    # --- Chuỗi đầy đủ, 0 lỗi ---
    def test_complete_sequence(self):
        report = analyze_trade_id_sequence([100, 101, 102, 103], "binance", "BTCUSDT")
        self.assertEqual(report.gap_count, 0)
        self.assertEqual(report.gap_missing_total, 0)
        self.assertEqual(report.duplicate_count, 0)
        self.assertEqual(report.out_of_order_count, 0)
        self.assertEqual(report.total_ids, 4)
        self.assertEqual(report.distinct_ids, 4)

    # --- 1 khoảng hở ---
    def test_single_gap(self):
        report = analyze_trade_id_sequence([100, 101, 105, 106], "binance", "ETHUSDT")
        self.assertEqual(report.gap_count, 1)
        self.assertEqual(report.gap_missing_total, 3)  # 102, 103, 104
        self.assertEqual(report.gaps[0].start, 102)
        self.assertEqual(report.gaps[0].end, 104)

    # --- Nhiều khoảng hở ---
    def test_multiple_gaps(self):
        report = analyze_trade_id_sequence([100, 103, 110], "binance", "SOLUSDT")
        self.assertEqual(report.gap_count, 2)
        # gap 1: 101, 102 → 2 missing; gap 2: 104–109 → 6 missing
        self.assertEqual(report.gap_missing_total, 8)

    # --- Trùng ID ---
    def test_duplicate_ids(self):
        report = analyze_trade_id_sequence([100, 101, 101, 102], "coinbase", "BTC-USD")
        self.assertEqual(report.duplicate_count, 1)
        self.assertIn(101, report.duplicates)
        self.assertEqual(report.gap_count, 0)

    # --- Đảo thứ tự ---
    def test_out_of_order(self):
        # Theo thứ tự nhận: 100, 102, 101, 103 → tại vị trí 2 (101 < 102)
        report = analyze_trade_id_sequence([100, 102, 101, 103], "coinbase", "ETH-USD")
        self.assertEqual(report.out_of_order_count, 1)
        self.assertEqual(report.gap_count, 0)  # không thiếu ID nào

    # --- Chuỗi rỗng ---
    def test_empty_sequence(self):
        report = analyze_trade_id_sequence([], "binance", "XRPUSDT")
        self.assertEqual(report.gap_count, 0)
        self.assertEqual(report.duplicate_count, 0)
        self.assertEqual(report.out_of_order_count, 0)
        self.assertEqual(report.total_ids, 0)
        self.assertEqual(report.distinct_ids, 0)

    # --- 1 phần tử ---
    def test_single_element(self):
        report = analyze_trade_id_sequence([42], "binance", "ADAUSDT")
        self.assertEqual(report.gap_count, 0)
        self.assertEqual(report.duplicate_count, 0)
        self.assertEqual(report.out_of_order_count, 0)
        self.assertEqual(report.total_ids, 1)
        self.assertEqual(report.distinct_ids, 1)

    # --- ID đầu cửa sổ: gap ngay đầu ---
    def test_gap_at_start_of_window(self):
        """Dãy [103, 104, 105] có 2 gap từ 101–102 nếu ID bắt đầu từ 100.
        Nhưng ta không biết ID đầu tiên 'nên' là bao nhiêu — ta chỉ kiểm
        gap giữa các ID thu được. Nên dãy [103, 104, 105] = 0 gap."""
        report = analyze_trade_id_sequence([103, 104, 105], "binance", "DOGEUSDT")
        self.assertEqual(report.gap_count, 0)

    # --- Coinbase batch ngược ---
    def test_coinbase_batch_reversed_order(self):
        """Coinbase gửi batch 750, 749, 748 — 3 out-of-order nhưng 0 gap."""
        report = analyze_trade_id_sequence([750, 749, 748, 751], "coinbase", "ETH-USD")
        self.assertEqual(report.gap_count, 0)
        self.assertEqual(report.out_of_order_count, 2)  # 749<750, 748<749
        self.assertEqual(report.duplicate_count, 0)

    # --- Kết hợp: trùng + gap + đảo ---
    def test_combined_issues(self):
        ids = [100, 102, 101, 101, 106]  # gap: 103-105 (3 missing), dup: 101, ooo: tại 101<102
        report = analyze_trade_id_sequence(ids, "binance", "ZILUSDT")
        self.assertEqual(report.gap_count, 1)
        self.assertEqual(report.gap_missing_total, 3)
        self.assertEqual(report.duplicate_count, 1)
        self.assertGreater(report.out_of_order_count, 0)


class TestGapReportSerialization(unittest.TestCase):
    def test_as_dict(self):
        report = analyze_trade_id_sequence([100, 103], "binance", "BTCUSDT")
        d = report.as_dict()
        self.assertEqual(d["gap_count"], 1)
        self.assertEqual(d["gaps"][0]["missing_count"], 2)

    def test_as_dict_truncation(self):
        report = GapReport("binance", "BTCUSDT", 0, 0,
                           gaps=[Gap(i * 10, i * 10 + 1) for i in range(200)])
        d = report.as_dict(max_gaps=5)
        self.assertEqual(len(d["gaps"]), 6)  # 5 + truncation marker
        self.assertTrue(d["gaps"][-1].get("truncated"))


class TestGapReportToMetrics(unittest.TestCase):
    def test_produces_four_metrics(self):
        report = analyze_trade_id_sequence([100, 103], "binance", "BTCUSDT")
        metrics = gap_report_to_metrics(report, "2026-11-27T05:00:00Z",
                                        "2026-11-27T00:00:00Z", "2026-11-28T00:00:00Z")
        self.assertEqual(len(metrics), 4)
        names = {m["metric_name"] for m in metrics}
        self.assertEqual(names, {
            "trade_id_gap_count", "trade_id_gap_missing_total",
            "trade_id_duplicate_count", "trade_id_out_of_order_count",
        })
        gap_count_m = next(m for m in metrics if m["metric_name"] == "trade_id_gap_count")
        self.assertEqual(gap_count_m["value"], 1)
        self.assertEqual(gap_count_m["dimensions"]["source"], "binance")


if __name__ == "__main__":
    unittest.main()
