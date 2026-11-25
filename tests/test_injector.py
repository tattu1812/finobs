"""Injector v1 (EV-4): 5 loại lỗi, tất định theo seed, không đụng bản ghi ngoài vùng tiêm."""

from __future__ import annotations

import copy
import json
import random
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "evaluation" / "injector"))

import inject  # noqa: E402


def records() -> list[dict]:
    rows = []
    for source in ("binance", "coinbase"):
        path = ROOT / "tests" / "fixtures" / "raw" / source / "sample.jsonl"
        rows += [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    rows.sort(key=lambda r: r["ingest_time"])
    return rows


def scenario(injector: str, params: dict, source="binance", layer="source", offset=0, duration=10**6) -> dict:
    rows = records()
    return {
        "input": {"window": {"start": rows[0]["ingest_time"], "end": "2099-01-01T00:00:00.000Z"}, "seed": 7},
        "injection": {"injector": injector, "target": {"layer": layer, "source": source},
                      "start_offset_s": offset, "duration_s": duration, "params": params},
    }


class InjectorTest(unittest.TestCase):
    def run_injection(self, sc):
        return inject.apply_injection(records(), sc, random.Random(sc["input"]["seed"]))

    def test_drop_all_target_records(self):
        out, touched = self.run_injection(scenario("drop_records", {"fraction": 1.0}))
        self.assertFalse(any(r["source"] == "binance" for r in out))
        self.assertEqual(len(out) + len(touched), len(records()))

    def test_duplicate_keeps_event_id(self):
        out, touched = self.run_injection(scenario("duplicate_records", {"fraction": 1.0, "max_delay_ms": 50}))
        binance = [r for r in records() if r["source"] == "binance"]
        self.assertEqual(len(out), len(records()) + len(binance))
        ids = [r["event_id"] for r in out if r["source"] == "binance"]
        self.assertEqual(len(set(ids)), len(binance))

    def test_shift_ingest_time(self):
        out, _ = self.run_injection(scenario("shift_ingest_time", {"offset_ms": -500}, layer="host"))
        before = {r["event_id"]: r["ingest_time"] for r in records()}
        some = out[0]
        delta = inject.parse_utc(before[some["event_id"]]) - inject.parse_utc(some["ingest_time"])
        self.assertAlmostEqual(delta.total_seconds(), 0.5, places=3)

    def test_mutate_payload_breaks_contract_shape(self):
        out, _ = self.run_injection(scenario("mutate_payload", {"rename_fields": {"p": "price"},
                                                                "cast_fields": {"t": "string"}}))
        rec = next(r for r in out if r["source"] == "binance")
        self.assertIn("price", rec["payload"])
        self.assertNotIn("p", rec["payload"])
        self.assertIsInstance(rec["payload"]["t"], str)

    def test_scale_price_exact_decimal(self):
        out, _ = self.run_injection(scenario("scale_price", {"factor": 1000}, source="coinbase"))
        original = {r["event_id"]: r for r in records()}
        rec = next(r for r in out if r["source"] == "coinbase")
        from decimal import Decimal
        self.assertEqual(Decimal(rec["payload"]["price"]), Decimal(original[rec["event_id"]]["payload"]["price"]) * 1000)

    def test_outside_window_untouched_and_deterministic(self):
        sc = scenario("drop_records", {"fraction": 0.5}, offset=10**7)  # vùng tiêm sau mọi bản ghi
        out, touched = self.run_injection(sc)
        self.assertEqual(touched, [])
        self.assertEqual(len(out), len(records()))
        sc2 = scenario("drop_records", {"fraction": 0.5})
        a = inject.lines_of(self.run_injection(copy.deepcopy(sc2))[0])
        b = inject.lines_of(self.run_injection(copy.deepcopy(sc2))[0])
        self.assertEqual(a, b)


if __name__ == "__main__":
    unittest.main()
