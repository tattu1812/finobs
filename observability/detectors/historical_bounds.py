"""Baseline 2: Historical Quantile Bounds dùng KLLSketch (OB, Sprint 5).

Tách biệt hoàn toàn pha ``fit()`` (trên tập lịch sử/train) và pha ``detect()``
(trên tập kiểm thử/eval) để không bị rò rỉ dữ liệu tương lai.
"""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Any

from observability.collectors.sketch_profiler import KLLSketch
from observability.detectors.base import BaseDetector, DetectorAlert


class HistoricalBoundsDetector(BaseDetector):
    """Phát hiện giá trị vượt ngưỡng phân vị lịch sử [q_low, q_high] có nới biên."""

    name = "historical_bounds"

    def __init__(
        self,
        *,
        lower_quantile: float = 0.01,
        upper_quantile: float = 0.99,
        margin_factor: float = 0.25,
        min_fit_samples: int = 10,
        kll_k: int = 200,
    ) -> None:
        if not 0.0 <= lower_quantile < upper_quantile <= 1.0:
            raise ValueError("Cần 0 <= lower_quantile < upper_quantile <= 1")
        if margin_factor < 0.0:
            raise ValueError("margin_factor phải >= 0")
        self.lower_quantile = lower_quantile
        self.upper_quantile = upper_quantile
        self.margin_factor = margin_factor
        self.min_fit_samples = min_fit_samples
        self.kll_k = kll_k
        self._bounds: dict[tuple[str, str, str], tuple[float, float, int]] = {}

    def fit(self, train_observations: list[dict[str, Any]]) -> HistoricalBoundsDetector:
        """Học ngưỡng phân vị từ dữ liệu lịch sử bằng KLLSketch."""
        sketches: dict[tuple[str, str, str], KLLSketch] = defaultdict(
            lambda: KLLSketch(k=self.kll_k)
        )
        for obs in train_observations:
            raw_val = obs.get("value")
            if raw_val is None:
                continue
            try:
                val = float(raw_val)
            except (TypeError, ValueError):
                continue
            if math.isnan(val) or math.isinf(val):
                continue
            key = (
                str(obs.get("source", "unknown")),
                str(obs.get("symbol", "unknown")),
                str(obs.get("metric", "value")),
            )
            sketches[key].update(val)

        self._bounds.clear()
        for key, sk in sketches.items():
            if sk.n < self.min_fit_samples:
                continue
            q_low = sk.quantile(self.lower_quantile)
            q_high = sk.quantile(self.upper_quantile)
            if q_low is None or q_high is None:
                continue
            spread = max(q_high - q_low, abs(q_high) * 0.01, 1e-9)
            low_bound = q_low - self.margin_factor * spread
            high_bound = q_high + self.margin_factor * spread
            self._bounds[key] = (low_bound, high_bound, sk.n)
        return self

    def get_bounds(self, source: str, symbol: str, metric: str) -> tuple[float, float] | None:
        entry = self._bounds.get((source, symbol, metric))
        return (entry[0], entry[1]) if entry else None

    def detect(self, observations: list[dict[str, Any]]) -> list[DetectorAlert]:
        """Kiểm tra từng observation theo ngưỡng đã fit."""
        alerts: list[DetectorAlert] = []
        ordered = sorted(observations, key=lambda o: str(o.get("ts", "")))

        for obs in ordered:
            raw_val = obs.get("value")
            if raw_val is None:
                continue
            try:
                val = float(raw_val)
            except (TypeError, ValueError):
                continue
            if math.isnan(val) or math.isinf(val):
                continue

            source = str(obs.get("source", "unknown"))
            symbol = str(obs.get("symbol", "unknown"))
            metric = str(obs.get("metric", "value"))
            ts = str(obs.get("ts", ""))
            asset = str(obs.get("asset") or f"finobs:raw.{source}.trade")

            bound = self._bounds.get((source, symbol, metric))
            if bound is None:
                continue
            low_b, high_b, n_fit = bound
            if low_b <= val <= high_b:
                continue

            width = max(high_b - low_b, 1e-6)
            dist = (low_b - val) if val < low_b else (val - high_b)
            score = round(1.0 + dist / width, 4)
            severity = "critical" if score >= 3.0 else ("high" if score >= 2.0 else "medium")
            alerts.append(
                DetectorAlert(
                    detector=self.name,
                    source=source,
                    symbol=symbol,
                    ts=ts,
                    detected_at=ts,
                    asset=asset,
                    score=score,
                    severity=severity,
                    metric=metric,
                    evidence={
                        "value": val,
                        "lower_bound": round(low_b, 6),
                        "upper_bound": round(high_b, 6),
                        "fit_samples": n_fit,
                    },
                )
            )
        return alerts
