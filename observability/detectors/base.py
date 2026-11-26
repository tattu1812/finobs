"""Giao diện chung cho mọi detector (OB, Sprint 5).

Mọi detector nhận chuỗi observations chuẩn hoá:
    {"ts": "...", "source": "binance", "symbol": "BTCUSDT", "metric": "...", "value": float | None, "asset": "..."}
và trả về danh sách ``DetectorAlert`` khớp với định dạng cảnh báo của PL/AI/EV.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class DetectorAlert:
    detector: str
    source: str
    symbol: str
    ts: str
    detected_at: str
    asset: str
    score: float
    severity: str  # low | medium | high | critical
    evidence: dict[str, Any] = field(default_factory=dict)
    metric: str = ""

    def as_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["labels"] = {
            "source": self.source,
            "symbol": self.symbol,
            "detector": self.detector,
            "metric": self.metric,
        }
        return d


class BaseDetector(ABC):
    """Lớp cơ sở trừu tượng cho mọi detector."""

    name: str = "base"

    @abstractmethod
    def detect(self, observations: list[dict[str, Any]]) -> list[DetectorAlert]:
        """Phát hiện bất thường trên danh sách observation đã sắp xếp theo thời gian."""
        raise NotImplementedError
