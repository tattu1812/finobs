"""Unit test: Injector v2 (8 loại lỗi, seed cố định, dry-run) & MTTD harness (EV-A, EV-B)."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from evaluation.bench.mttd_harness import evaluate_mttd, write_bench_reports
from evaluation.injector.injector_v2 import SUPPORTED_FAULTS, inject_fault


def _sample_records(n: int = 10):
    return [
        {
            "source": "binance",
            "symbol_native": "BTCUSDT",
            "sequence": 1000 + i,
            "ingest_time": f"2026-11-26T10:00:{i:02d}.000Z",
            "payload": {"p": "90000.00000000", "q": "0.10000000"},
        }
        for i in range(n)
    ]


class TestInjectorV2(unittest.TestCase):
    def test_at_least_8_faults_and_deterministic(self):
        self.assertGreaterEqual(len(SUPPORTED_FAULTS), 8)
        recs = _sample_records(10)
        for ft in SUPPORTED_FAULTS:
            out1 = inject_fault(recs, fault_type=ft, source="binance", symbol="BTCUSDT", seed=42)
            out2 = inject_fault(recs, fault_type=ft, source="binance", symbol="BTCUSDT", seed=42)
            self.assertEqual(out1.records, out2.records)
            self.assertTrue(out1.dry_run)
            self.assertTrue(out1.target_topic.startswith("test."))
            self.assertEqual(out1.ground_truth["fault_type"], ft)


class TestMTTDHarness(unittest.TestCase):
    def test_exact_match_mttd(self):
        gt = [{"fault_type": "FT02_volume_loss", "source": "binance",
               "affected_asset": "finobs:raw.binance.trade",
               "start_ts": "2026-11-26T10:00:00Z", "end_ts": "2026-11-26T10:05:00Z"}]
        alerts = [{"alert_id": "A1", "source": "binance",
                   "asset": "finobs:raw.binance.trade", "detected_at": "2026-11-26T10:00:45Z"}]
        res = evaluate_mttd(scenario_id="S004", ground_truths=gt, alerts=alerts)
        self.assertTrue(res.detected)
        self.assertEqual(res.mttd_seconds, 45.0)
        self.assertEqual(res.false_positives, 0)
        self.assertEqual(res.misses, 0)

    def test_miss_and_false_positive_and_early(self):
        gt = [{"fault_type": "FT01_stale_feed", "source": "binance",
               "affected_asset": "finobs:raw.binance.trade",
               "start_ts": "2026-11-26T10:05:00Z", "end_ts": "2026-11-26T10:10:00Z"}]
        alerts = [
            {"alert_id": "EARLY", "source": "binance", "asset": "finobs:raw.binance.trade",
             "detected_at": "2026-11-26T10:01:00Z"},
            {"alert_id": "FP", "source": "coinbase", "asset": "finobs:raw.coinbase.trade",
             "detected_at": "2026-11-26T10:06:00Z"},
        ]
        res = evaluate_mttd(scenario_id="S001", ground_truths=gt, alerts=alerts)
        self.assertFalse(res.detected)
        self.assertEqual(res.misses, 1)
        self.assertEqual(res.early_alerts, 1)
        self.assertEqual(res.false_positives, 2)

    def test_overlapping_faults_and_reports(self):
        gts = [
            {"fault_type": "FT02_volume_loss", "source": "binance",
             "affected_asset": "finobs:raw.binance.trade",
             "start_ts": "2026-11-26T10:00:00Z", "end_ts": "2026-11-26T10:05:00Z"},
            {"fault_type": "FT09_value_anomaly", "source": "coinbase",
             "affected_asset": "finobs:raw.coinbase.trade",
             "start_ts": "2026-11-26T10:02:00Z", "end_ts": "2026-11-26T10:06:00Z"},
        ]
        alerts = [
            {"alert_id": "A1", "source": "binance", "asset": "finobs:raw.binance.trade",
             "detected_at": "2026-11-26T10:00:30Z"},
            {"alert_id": "A2", "source": "coinbase", "asset": "finobs:raw.coinbase.trade",
             "detected_at": "2026-11-26T10:02:20Z"},
        ]
        res = evaluate_mttd(scenario_id="S_OVERLAP", ground_truths=gts, alerts=alerts)
        self.assertTrue(res.detected)
        self.assertEqual(res.mttd_seconds, 25.0)
        with tempfile.TemporaryDirectory() as tmp:
            j, c, m = write_bench_reports([res], Path(tmp))
            self.assertTrue(j.is_file() and c.is_file() and m.is_file())


if __name__ == "__main__":
    unittest.main()
