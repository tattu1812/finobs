"""Unit test cho công cụ phát lại tốc độ cao (PL-A, Sprint 5)."""

from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from infra.replay.replayer import (
    DryRunSink,
    ReplayConfig,
    ReplayNamespaceError,
    load_official_csv_records,
    load_raw_records,
    replay_records,
    validate_namespace,
)

RAW_FIXTURE = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "raw"


class ReplayerTest(unittest.TestCase):
    def test_rejects_production_namespaces(self):
        for bad in ["raw", "bronze", "silver", "gold", "live", "prod", "finobs", "raw.binance", "bronze_test"]:
            with self.assertRaises(ReplayNamespaceError):
                validate_namespace(bad)
            cfg = ReplayConfig(namespace=bad, speed=0)
            with self.assertRaises(ReplayNamespaceError):
                replay_records([], cfg)

        self.assertEqual(validate_namespace("replay"), "replay")
        self.assertEqual(validate_namespace("replay_s5"), "replay_s5")

    def test_preserves_event_time_and_sequence_order(self):
        records = load_raw_records(RAW_FIXTURE)
        sink = DryRunSink(capture=True)
        res = replay_records(records, ReplayConfig(namespace="replay", speed=0), sink=sink)
        self.assertEqual(res.metrics.emitted_events, len(records))
        keys = [(r["event_time"], int(r["sequence"])) for r in sink.records]
        self.assertEqual(keys, sorted(keys))
        self.assertTrue(all(t.startswith("replay.") for t in res.target_topics))

    def test_speed_control_within_tolerance(self):
        base = load_raw_records(RAW_FIXTURE)[:4]
        # Tạo 2 event cách nhau 1.0s
        r1 = dict(base[0], event_time="2026-10-27T02:48:00.000Z", sequence=1)
        r2 = dict(base[0], event_time="2026-10-27T02:48:01.000Z", sequence=2)
        slept: list[float] = []
        replay_records([r1, r2], ReplayConfig(namespace="replay", speed=10.0), sleep_fn=slept.append)
        self.assertEqual(len(slept), 1)
        self.assertAlmostEqual(slept[0], 0.1, places=4)

    def test_pause_and_resume_deterministic(self):
        records = load_raw_records(RAW_FIXTURE)[:10]
        with tempfile.TemporaryDirectory() as tmp:
            ckpt = Path(tmp) / "replay.ckpt"
            sink1 = DryRunSink(capture=True)
            res1 = replay_records(
                records,
                ReplayConfig(namespace="replay", speed=0, checkpoint_path=ckpt, seed=7),
                sink=sink1,
                stop_after=4,
            )
            self.assertTrue(res1.metrics.paused)
            self.assertEqual(res1.metrics.emitted_events, 4)

            sink2 = DryRunSink(capture=True)
            res2 = replay_records(
                records,
                ReplayConfig(namespace="replay", speed=0, checkpoint_path=ckpt, resume=True, seed=7),
                sink=sink2,
            )
            self.assertFalse(res2.metrics.paused)
            self.assertEqual(res2.metrics.emitted_events, 6)
            combined_ids = [r["event_id"] for r in sink1.records + sink2.records]

            sink_full = DryRunSink(capture=True)
            replay_records(records, ReplayConfig(namespace="replay", speed=0, seed=7), sink=sink_full)
            self.assertEqual(combined_ids, [r["event_id"] for r in sink_full.records])

    def test_synthetic_symbol_scaling_and_official_csv(self):
        records = load_raw_records(RAW_FIXTURE)
        sink = DryRunSink(capture=True)
        res = replay_records(records, ReplayConfig(namespace="replay", speed=0, num_symbols=30, seed=99), sink=sink)
        self.assertGreater(res.metrics.synthetic_symbols, 0)
        distinct_syms = {r["symbol_native"] for r in sink.records}
        self.assertEqual(len(distinct_syms), 30)
        self.assertTrue(any(r.get("transport", {}).get("is_synthetic") for r in sink.records))

        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "BTCUSDT-trades-2026-11-27.csv"
            with csv_path.open("w", newline="", encoding="utf-8") as fh:
                w = csv.writer(fh)
                w.writerow(["1001", "95000.00", "0.01", "950.00", "1795737600000", "true", "true"])
            loaded = load_official_csv_records(csv_path, symbol="BTCUSDT")
            self.assertEqual(len(loaded), 1)
            self.assertEqual(loaded[0]["symbol_canonical"], "BTC-USDT")
            self.assertEqual(loaded[0]["sequence"], 1001)


if __name__ == "__main__":
    unittest.main()
