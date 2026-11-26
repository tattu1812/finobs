"""Baseline 1: Rolling Z-Score đơn nguồn (OB, Sprint 5).

Hỗ trợ cả z-score chuẩn (mean/std) và robust z-score dựa trên median/MAD
để giảm cảnh báo giả trong các ngày thị trường biến động hợp lệ.
"""

from __future__ import annotations

import math
from collections import defaultdict, deque
from statistics import mean, median, stdev
from typing import Any

from observability.detectors.base import BaseDetector, DetectorAlert


def _severity_from_score(score: float, threshold: float) -> str:
    ratio = score / max(threshold, 1e-6)
    if ratio >= 2.5:
        return "critical"
    if ratio >= 1.75:
        return "high"
    if ratio >= 1.25:
        return "medium"
    return "low"


class ZScoreSingleSourceDetector(BaseDetector):
    """Phát hiện bất thường đơn nguồn theo cửa sổ trượt (rolling window)."""

    name = "zscore_single"

    def __init__(
        self,
        *,
        window: int = 30,
        threshold: float = 3.0,
        min_samples: int = 10,
        robust_mad: bool = True,
        min_scale: float = 1e-6,
    ) -> None:
        if window < 2:
            raise ValueError("window phải >= 2")
        if min_samples < 2:
            raise ValueError("min_samples phải >= 2")
        if threshold <= 0:
            raise ValueError("threshold phải > 0")
        self.window = window
        self.threshold = threshold
        self.min_samples = min_samples
        self.robust_mad = robust_mad
        self.min_scale = min_scale

    def _compute_score(self, hist: list[float], value: float) -> tuple[float, float, float]:
        """Trả về (abs_z_score, center, scale)."""
        if self.robust_mad:
            center = median(hist)
            deviations = [abs(x - center) for x in hist]
            mad = median(deviations)
            # Hệ số chuẩn hoá MAD -> độ lệch chuẩn phân phối chuẩn (1.4826)
            scale = 1.4826 * mad
            if scale < self.min_scale:
                # Fallback sang std nếu MAD = 0 nhưng vẫn có biến thiên nhẹ
                s = stdev(hist) if len(hist) >= 2 else 0.0
                scale = s
        else:
            center = mean(hist)
            scale = stdev(hist) if len(hist) >= 2 else 0.0

        if scale < self.min_scale or math.isclose(scale, 0.0, abs_tol=self.min_scale):
            # Chuỗi lịch sử hằng số (std == 0): chỉ báo nếu lệch rõ rệt khỏi hằng số
            if math.isclose(value, center, abs_tol=self.min_scale):
                return 0.0, center, 0.0
            return float("inf"), center, 0.0

        return abs(value - center) / scale, center, scale

    def detect(self, observations: list[dict[str, Any]]) -> list[DetectorAlert]:
        """Chạy rolling z-score độc lập cho từng (source, symbol, metric)."""
        histories: dict[tuple[str, str, str], deque[float]] = defaultdict(
            lambda: deque(maxlen=self.window)
        )
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

            key = (source, symbol, metric)
            hist_buf = histories[key]

            if len(hist_buf) >= self.min_samples:
                score, center, scale = self._compute_score(list(hist_buf), val)
                if score > self.threshold:
                    capped_score = 999.0 if math.isinf(score) else round(score, 4)
                    alerts.append(
                        DetectorAlert(
                            detector=self.name,
                            source=source,
                            symbol=symbol,
                            ts=ts,
                            detected_at=ts,
                            asset=asset,
                            score=capped_score,
                            severity=_severity_from_score(capped_score, self.threshold),
                            metric=metric,
                            evidence={
                                "value": val,
                                "center": round(center, 6),
                                "scale": round(scale, 6),
                                "threshold": self.threshold,
                                "window_samples": len(hist_buf),
                                "robust_mad": self.robust_mad,
                            },
                        )
                    )
                    # Không đưa điểm shock vào cửa sổ để tránh làm ô nhiễm baseline
                    continue

            hist_buf.append(val)

        return alerts
