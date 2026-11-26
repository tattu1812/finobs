"""Unit test cho DE Sprint 5: backfill nhiều ngày, đo M3 và kiểm tra dashboard JSON."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from observability.reconcile.backfill import run_backfill
from observability.reconcile.measure_m3 import measure_m3_on_fixtures

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures" / "reconcile"
DASHBOARD = ROOT / "dashboards" / "finobs-stream-completeness.json"


class ReconcileBackfillAndDashboardTest(unittest.TestCase):
    def test_backfill_idempotent_and_partial_day(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            s1 = run_backfill(
                from_date="2026-11-26",
                to_date="2026-11-28",
                fixture_dir=FIXTURES,
                out_dir=out,
                partial_dates={"2026-11-26"},
            )
            s2 = run_backfill(
                from_date="2026-11-26",
                to_date="2026-11-28",
                fixture_dir=FIXTURES,
                out_dir=out,
                partial_dates={"2026-11-26"},
            )
            self.assertEqual(s1["days_total"], 3)
            self.assertEqual(s1["report_rate"], 1.0)
            self.assertEqual(s1["daily"][0]["overall_status"], "INCOMPLETE_DAY")
            self.assertEqual(s1["overall_detection_coverage"], s2["overall_detection_coverage"])
            self.assertTrue((out / "2026-11-26.json").is_file())
            self.assertTrue((out / "2026-11-27.md").is_file())
            self.assertTrue((out / "coverage_summary.md").is_file())

    def test_backfill_handles_missing_official_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            s = run_backfill(
                from_date="2026-11-27",
                to_date="2026-11-27",
                fixture_dir=None,
                out_dir=Path(tmp),
                dry_run=True,
            )
            self.assertEqual(s["daily"][0]["overall_status"], "OFFICIAL_NOT_AVAILABLE")

    def test_measure_m3_passes_on_fixtures(self):
        res = measure_m3_on_fixtures(FIXTURES)
        self.assertTrue(res["m3_latency_pass"])
        self.assertTrue(res["m3_report_pass"])
        self.assertTrue(res["is_preliminary"])

    def test_grafana_dashboard_json_valid(self):
        data = json.loads(DASHBOARD.read_text(encoding="utf-8"))
        self.assertEqual(data["uid"], "finobs-stream-completeness")
        self.assertGreaterEqual(len(data["panels"]), 6)
        var_names = {v["name"] for v in data["templating"]["list"]}
        self.assertEqual(var_names, {"source", "symbol"})
        for p in data["panels"]:
            self.assertIn("title", p)
            self.assertIn("targets", p)


if __name__ == "__main__":
    unittest.main()
