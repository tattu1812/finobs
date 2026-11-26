"""Unit tests cho EV Sprint 5: chạy baseline runner trên tập tuning/eval và kiểm tra output."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from evaluation.bench.run_sprint5_baseline import run_baseline_suite


class Sprint5BaselineTest(unittest.TestCase):
    def test_baseline_suite_runs_and_separates_splits(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            summary = run_baseline_suite(out_dir=out)
            self.assertTrue(summary["is_preliminary"])
            self.assertGreaterEqual(summary["total_scenarios"], 20)
            self.assertGreaterEqual(summary["tuning_count"], 5)
            self.assertGreaterEqual(summary["eval_count"], 5)
            self.assertTrue((out / "sprint5_baseline.json").is_file())
            self.assertTrue((out / "sprint5_baseline.csv").is_file())
            self.assertTrue((out / "sprint5_baseline.md").is_file())

            data = json.loads((out / "sprint5_baseline.json").read_text(encoding="utf-8"))
            self.assertEqual(data["data_source"], "simulated_fixtures")


if __name__ == "__main__":
    unittest.main()
