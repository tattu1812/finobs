"""Unit test: KLLSketch, HyperLogLog, profiling và đo overhead (OB-A, Sprint 4)."""

from __future__ import annotations

import random
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from observability.collectors.sketch_profiler import (  # noqa: E402
    HyperLogLog,
    KLLSketch,
    compare_sketch_vs_exact,
    measure_overhead,
    profile_records,
)


class TestKLLSketch(unittest.TestCase):
    def test_empty_returns_none(self):
        self.assertIsNone(KLLSketch().quantile(0.5))

    def test_accuracy_and_merge(self):
        rng = random.Random(42)
        vals1 = [rng.uniform(100.0, 200.0) for _ in range(2000)]
        vals2 = [rng.uniform(100.0, 200.0) for _ in range(2000)]

        s1 = KLLSketch(k=200)
        s2 = KLLSketch(k=200)
        for v in vals1:
            s1.update(v)
        for v in vals2:
            s2.update(v)

        merged = s1.merge(s2)
        self.assertEqual(merged.n, 4000)

        combined_sorted = sorted(vals1 + vals2)
        exact_p95 = combined_sorted[int(0.95 * 4000) - 1]
        est_p95 = merged.quantile(0.95)
        self.assertIsNotNone(est_p95)
        # Sai số tương đối < 5%
        self.assertLess(abs(est_p95 - exact_p95) / exact_p95, 0.05)

    def test_compare_vs_exact(self):
        vals = [float(i) for i in range(1, 1001)]
        res = compare_sketch_vs_exact(vals, q=0.95)
        self.assertLess(res["rel_error"], 0.05)


class TestHyperLogLog(unittest.TestCase):
    def test_distinct_and_merge(self):
        h1 = HyperLogLog(p=10)
        h2 = HyperLogLog(p=10)
        for i in range(0, 1500):
            h1.update(i)
        for i in range(1000, 2500):  # giao 500 phần tử
            h2.update(i)
        merged = h1.merge(h2)
        est = merged.estimate()
        # Thực tế có 2500 phần tử duy nhất; HLL p=10 sai số chuẩn ~3.25% -> kiểm tra trong +-15%
        self.assertGreater(est, 2100)
        self.assertLess(est, 2900)


class TestProfileRecordsAndOverhead(unittest.TestCase):
    def test_profile_records_windows_and_snapshot_filter(self):
        recs = [
            {
                "source": "binance",
                "symbol_canonical": "BTC-USDT",
                "ingest_time": "2026-11-26T10:00:10.000Z",
                "sequence": 1,
                "transport": {"is_snapshot": False},
                "payload": {"p": "90000.0", "q": "0.1"},
            },
            {
                "source": "binance",
                "symbol_canonical": "BTC-USDT",
                "ingest_time": "2026-11-26T10:00:20.000Z",
                "sequence": 2,
                "transport": {"is_snapshot": False},
                "payload": {"p": "90100.0", "q": "0.2"},
            },
            {
                "source": "coinbase",
                "symbol_canonical": "BTC-USD",
                "ingest_time": "2026-11-26T10:00:30.000Z",
                "sequence": 99,
                "transport": {"is_snapshot": True},  # phải bị bỏ qua
                "payload": {"price": "90000.0", "size": "0.1"},
            },
        ]
        windows = profile_records(recs, window_minutes=1)
        self.assertEqual(len(windows), 1)
        self.assertEqual(windows[0].count, 2)
        metrics = windows[0].to_metrics("2026-11-26T10:01:00Z", "1m")
        self.assertEqual(len(metrics), 4)

    def test_measure_overhead(self):
        stats = measure_overhead(n_items=2000)
        self.assertEqual(stats["n_items"], 2000)
        self.assertGreater(stats["throughput_eps"], 0)


if __name__ == "__main__":
    unittest.main()
