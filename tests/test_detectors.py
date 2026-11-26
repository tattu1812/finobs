"""Unit tests cho OB Sprint 5: BaseDetector, ZScoreSingleSourceDetector, HistoricalBoundsDetector."""

from __future__ import annotations

import unittest

from observability.detectors.historical_bounds import HistoricalBoundsDetector
from observability.detectors.run_baseline import run_detectors
from observability.detectors.zscore_single import ZScoreSingleSourceDetector


def _make_series(
    values: list[float | None],
    *,
    source: str = "binance",
    symbol: str = "BTCUSDT",
    metric: str = "trade_count_1m",
) -> list[dict]:
    out = []
    for i, v in enumerate(values):
        mm = i % 60
        hh = 3 + i // 60
        out.append({
            "ts": f"2026-10-27T{hh:02d}:{mm:02d}:00.000Z",
            "source": source,
            "symbol": symbol,
            "metric": metric,
            "value": v,
            "asset": f"finobs:raw.{source}.trade",
        })
    return out


class DetectorsBaselineTest(unittest.TestCase):
    def test_normal_series_produces_zero_alerts(self):
        vals = [100.0 + (i % 5) - 2.0 for i in range(40)]
        obs = _make_series(vals)
        det = ZScoreSingleSourceDetector(window=20, threshold=3.5, min_samples=10, robust_mad=True)
        alerts = det.detect(obs)
        self.assertEqual(alerts, [])

    def test_shock_triggers_alert_and_respects_warmup(self):
        # 5 điểm đầu là warm-up (< min_samples=10), dù có số lạ ở idx=3 cũng không được báo
        vals = [100.0, 101.0, 99.0, 500.0, 100.0] + [100.0 + (i % 3) for i in range(15)] + [900.0]
        obs = _make_series(vals)
        det = ZScoreSingleSourceDetector(window=20, threshold=3.5, min_samples=10, robust_mad=True)
        alerts = det.detect(obs)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].evidence["value"], 900.0)
        self.assertEqual(alerts[0].source, "binance")
        self.assertIn("source", alerts[0].as_dict()["labels"])

    def test_missing_data_and_zero_std_handled_safely(self):
        # Chuỗi hằng số 50.0 xen kẽ None, sau đó có 1 điểm bằng 50.0 (không báo) và 1 điểm 80.0 (có báo)
        vals: list[float | None] = [50.0] * 12 + [None, None, 50.0, 80.0]
        obs = _make_series(vals)
        det = ZScoreSingleSourceDetector(window=15, threshold=3.0, min_samples=10, robust_mad=True)
        alerts = det.detect(obs)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].evidence["value"], 80.0)

    def test_volatile_but_valid_day_fewer_false_positives_with_mad_and_margin(self):
        # Ngày biến động mạnh hợp lệ: dao động rộng quanh 100..160
        train_vals = [100.0 + (i % 7) * 10.0 for i in range(40)]
        eval_vals = [105.0 + (i % 7) * 10.0 for i in range(25)]
        train_obs = _make_series(train_vals)
        eval_obs = _make_series(eval_vals)

        hb = HistoricalBoundsDetector(lower_quantile=0.05, upper_quantile=0.95, margin_factor=0.3)
        hb.fit(train_obs)
        self.assertEqual(hb.detect(eval_obs), [])

    def test_historical_bounds_train_eval_separation_and_alert(self):
        train_obs = _make_series([100.0 + (i % 4) for i in range(30)])
        eval_obs = _make_series([101.0, 102.0, 250.0])  # 250 vượt xa ngưỡng [99, 104]
        hb = HistoricalBoundsDetector(lower_quantile=0.01, upper_quantile=0.99, margin_factor=0.2)
        hb.fit(train_obs)
        bounds = hb.get_bounds("binance", "BTCUSDT", "trade_count_1m")
        self.assertIsNotNone(bounds)
        alerts = hb.detect(eval_obs)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].detector, "historical_bounds")

    def test_cli_runner_both_detectors(self):
        train_obs = _make_series([100.0 + (i % 3) for i in range(25)])
        eval_obs = _make_series([100.0 + (i % 3) for i in range(15)] + [500.0])
        alerts = run_detectors(eval_obs, detector_name="both", train_observations=train_obs)
        detectors_hit = {a["detector"] for a in alerts}
        self.assertIn("zscore_single", detectors_hit)
        self.assertIn("historical_bounds", detectors_hit)


if __name__ == "__main__":
    unittest.main()
