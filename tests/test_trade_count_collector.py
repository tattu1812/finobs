"""Behavior checks for the first executable observability metric."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from observability.collectors.trade_count import first_valid_trades, observations

ROOT = Path(__file__).resolve().parents[1]


def sample(source: str, line_number: int = 0) -> dict:
    path = ROOT / "tests" / "fixtures" / "raw" / source / "sample.jsonl"
    return json.loads(path.read_text(encoding="utf-8").splitlines()[line_number])


class TradeCountCollectorTests(unittest.TestCase):
    def test_snapshot_and_repeated_trade_do_not_add_volume(self) -> None:
        old_trade = sample("coinbase")
        old_trade["transport"]["is_snapshot"] = True
        replay = json.loads(json.dumps(old_trade))
        replay["transport"]["is_snapshot"] = False
        replay["ingest_time"] = "2026-10-27T02:49:10.000Z"

        new_trade = sample("coinbase", 1)
        new_trade["ingest_time"] = "2026-10-27T02:49:20.000Z"

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "coinbase.jsonl"
            path.write_text(
                "\n".join(json.dumps(row) for row in (old_trade, replay, new_trade))
                + "\n{bad json\n",
                encoding="utf-8",
            )
            first, stats = first_valid_trades(path)

        rows = observations(first)
        self.assertEqual(stats["invalid_records"], 1)
        self.assertEqual(stats["duplicate_records"], 1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["value"], 1)
        self.assertEqual(rows[0]["observed_at"], "2026-10-27T02:50:00Z")

    def test_minute_boundary_uses_ingest_time(self) -> None:
        before = sample("binance")
        after = sample("binance", 1)
        before["ingest_time"] = "2026-10-27T02:48:59.999Z"
        after["ingest_time"] = "2026-10-27T02:49:00.000Z"

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "binance.jsonl"
            path.write_text(
                "\n".join(json.dumps(row) for row in (before, after)) + "\n",
                encoding="utf-8",
            )
            first, _ = first_valid_trades(path)

        rows = observations(first)
        self.assertEqual([row["value"] for row in rows], [1, 1])
        self.assertEqual(
            [row["window_start"] for row in rows],
            ["2026-10-27T02:48:00Z", "2026-10-27T02:49:00Z"],
        )


if __name__ == "__main__":
    unittest.main()
