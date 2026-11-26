"""Unit test: SLO YAML validation, error budget, burn rate, trading hours (OB-B, Sprint 4)."""

from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from observability.slo.slo_engine import (
    DEFAULT_SLOS_PATH,
    evaluate_slo,
    generate_prometheus_burn_rate_rules,
    is_in_trading_hours,
    validate_slo_file,
    write_prometheus_rules,
)


class TestSLOEngine(unittest.TestCase):
    def test_default_slos_yaml_is_valid(self):
        doc = validate_slo_file(DEFAULT_SLOS_PATH)
        self.assertEqual(doc["schema_version"], 1)
        self.assertGreaterEqual(len(doc["slos"]), 5)

    def test_slo_meets_target(self):
        slo = {
            "name": "test_freshness",
            "pillar": "freshness",
            "target": 0.99,
            "trading_schedule": "24x7",
            "sli": {"metric_name": "last_live_trade_age_seconds", "comparator": "lte", "threshold": 30},
        }
        obs = [{"metric_name": "last_live_trade_age_seconds", "status": "ok", "value": 10.0} for _ in range(100)]
        res = evaluate_slo(slo, obs)
        self.assertEqual(res.sli_current, 1.0)
        self.assertEqual(res.burn_rate, 0.0)
        self.assertEqual(res.error_budget_remaining, 1.0)

    def test_short_and_sustained_breach(self):
        slo = {
            "name": "test_freshness",
            "pillar": "freshness",
            "target": 0.99,  # error budget = 0.01
            "trading_schedule": "24x7",
            "sli": {"metric_name": "last_live_trade_age_seconds", "comparator": "lte", "threshold": 30},
        }
        # Vi phạm 2/100 -> bad_rate = 0.02 -> burn_rate = 2.0, remaining = 0
        obs = [{"metric_name": "last_live_trade_age_seconds", "status": "ok", "value": 10.0} for _ in range(98)]
        obs += [{"metric_name": "last_live_trade_age_seconds", "status": "ok", "value": 45.0} for _ in range(2)]
        res = evaluate_slo(slo, obs)
        self.assertAlmostEqual(res.sli_current, 0.98)
        self.assertAlmostEqual(res.burn_rate, 2.0)
        self.assertEqual(res.error_budget_remaining, 0.0)

    def test_missing_data_never_counted_as_good(self):
        slo = {
            "name": "test_volume",
            "pillar": "volume",
            "target": 0.99,
            "trading_schedule": "24x7",
            "sli": {"metric_name": "trade_count_1m", "comparator": "gte", "threshold": 1},
        }
        obs = [
            {"metric_name": "trade_count_1m", "status": "ok", "value": 10},
            {"metric_name": "trade_count_1m", "status": "source_unavailable", "value": None},
        ]
        res = evaluate_slo(slo, obs)
        self.assertEqual(res.total_samples, 2)
        self.assertEqual(res.good_samples, 1)
        self.assertAlmostEqual(res.sli_current, 0.5)

    def test_trading_hours_hose(self):
        # Thứ 5, 03:00 UTC (10:00 VN) -> trong giờ
        in_session = datetime(2026, 11, 26, 3, 0, tzinfo=timezone.utc)
        # Thứ 5, 15:00 UTC (22:00 VN) -> ngoài giờ
        off_session = datetime(2026, 11, 26, 15, 0, tzinfo=timezone.utc)
        self.assertTrue(is_in_trading_hours(in_session, "hose_vn30"))
        self.assertFalse(is_in_trading_hours(off_session, "hose_vn30"))
        self.assertTrue(is_in_trading_hours(off_session, "24x7"))

    def test_generate_prometheus_rules(self):
        doc = validate_slo_file(DEFAULT_SLOS_PATH)
        rules = generate_prometheus_burn_rate_rules(doc)
        self.assertIn("groups", rules)
        self.assertGreater(len(rules["groups"][0]["rules"]), 0)
        with tempfile.TemporaryDirectory() as tmp:
            out = write_prometheus_rules(Path(tmp) / "slo.yml")
            self.assertTrue(out.is_file())


if __name__ == "__main__":
    unittest.main()
