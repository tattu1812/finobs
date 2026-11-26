"""Phát hiện khoảng hở trade ID (DE-A, Sprint 4).

Pure functions: nhận danh sách trade ID (theo thứ tự nhận), trả về kết quả phân tích.
Không cần Docker, Trino, hay Kafka — test được trực tiếp.

Quy tắc trade ID:
  - Binance: trường ``payload.t``, kiểu int, tăng liên tục *theo từng mã*.
  - Coinbase: ``int(trade["trade_id"])``, kiểu int, tăng liên tục *theo từng product_id*.
  Cả hai sàn dùng chung logic phát hiện.

Coinbase gửi trade trong cùng batch WS có thể đảo thứ tự (vd 750, 749, 748).
Khi tính *out-of-order*, chỉ xét thứ tự **nhận** (theo ingest_ts / bronze_ts),
không phải thứ tự bên trong cùng 1 batch.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Gap:
    """Khoảng hở: ID từ *start* đến *end* (inclusive) bị thiếu."""

    start: int
    end: int

    @property
    def missing_count(self) -> int:
        return self.end - self.start + 1

    def as_dict(self) -> dict:
        return {"start": self.start, "end": self.end, "missing_count": self.missing_count}


@dataclass
class GapReport:
    source: str
    symbol: str
    total_ids: int
    distinct_ids: int
    gaps: list[Gap] = field(default_factory=list)
    duplicates: list[int] = field(default_factory=list)
    out_of_order_count: int = 0

    # --- derived ----------------------------------------------------------

    @property
    def gap_count(self) -> int:
        return len(self.gaps)

    @property
    def gap_missing_total(self) -> int:
        return sum(g.missing_count for g in self.gaps)

    @property
    def duplicate_count(self) -> int:
        return len(self.duplicates)

    def as_dict(self, max_gaps: int = 100, max_dups: int = 100) -> dict:
        gaps_list = [g.as_dict() for g in self.gaps[:max_gaps]]
        if len(self.gaps) > max_gaps:
            gaps_list.append({"truncated": True, "total": len(self.gaps)})
        dups_list = self.duplicates[:max_dups]
        if len(self.duplicates) > max_dups:
            dups_list.append(-1)  # sentinel meaning truncated
        return {
            "source": self.source,
            "symbol": self.symbol,
            "total_ids": self.total_ids,
            "distinct_ids": self.distinct_ids,
            "gap_count": self.gap_count,
            "gap_missing_total": self.gap_missing_total,
            "gaps": gaps_list,
            "duplicate_count": self.duplicate_count,
            "duplicates": dups_list,
            "out_of_order_count": self.out_of_order_count,
        }


def analyze_trade_id_sequence(
    trade_ids: list[int],
    source: str = "",
    symbol: str = "",
) -> GapReport:
    """Phân tích chuỗi trade ID.

    Parameters
    ----------
    trade_ids:
        Danh sách trade ID theo **thứ tự nhận** (ingest_ts / bronze_ts).
        Có thể trùng, có thể đảo thứ tự.
    source:
        Nhãn nguồn ("binance" / "coinbase").
    symbol:
        Mã gốc (symbol_native).

    Returns
    -------
    GapReport
        Chứa gaps, duplicates, out_of_order_count.
    """
    total = len(trade_ids)
    if total == 0:
        return GapReport(source=source, symbol=symbol, total_ids=0, distinct_ids=0)

    # --- out-of-order: đếm theo thứ tự nhận --------------------------------
    ooo = 0
    for i in range(1, total):
        if trade_ids[i] < trade_ids[i - 1]:
            ooo += 1

    # --- duplicates ---------------------------------------------------------
    from collections import Counter

    counts = Counter(trade_ids)
    duplicates = sorted(tid for tid, cnt in counts.items() if cnt > 1)
    distinct = len(counts)

    # --- gaps: sắp xếp unique IDs rồi tìm khoảng hở ----------------------
    sorted_ids = sorted(counts.keys())
    gaps: list[Gap] = []
    for i in range(1, len(sorted_ids)):
        prev, curr = sorted_ids[i - 1], sorted_ids[i]
        if curr - prev > 1:
            gaps.append(Gap(start=prev + 1, end=curr - 1))

    return GapReport(
        source=source,
        symbol=symbol,
        total_ids=total,
        distinct_ids=distinct,
        gaps=gaps,
        duplicates=duplicates,
        out_of_order_count=ooo,
    )


# ---------------------------------------------------------------------------
# Metric store helpers
# ---------------------------------------------------------------------------

def gap_report_to_metrics(report: GapReport, observed_at: str,
                          window_start: str, window_end: str) -> list[dict]:
    """Chuyển GapReport thành danh sách metric observations cho metric store.

    Metric names theo quy ước repo (snake_case, có hậu tố đơn vị hoặc window):
      trade_id_gap_count, trade_id_gap_missing_total,
      trade_id_duplicate_count, trade_id_out_of_order_count
    """
    dims = {"source": report.source, "symbol": report.symbol}
    base = {
        "dimensions": dims,
        "observed_at": observed_at,
        "window_start": window_start,
        "window_end": window_end,
        "status": "ok",
    }
    return [
        {**base, "metric_name": "trade_id_gap_count",
         "value": report.gap_count, "sample_count": report.distinct_ids},
        {**base, "metric_name": "trade_id_gap_missing_total",
         "value": report.gap_missing_total, "sample_count": report.distinct_ids},
        {**base, "metric_name": "trade_id_duplicate_count",
         "value": report.duplicate_count, "sample_count": report.total_ids},
        {**base, "metric_name": "trade_id_out_of_order_count",
         "value": report.out_of_order_count, "sample_count": report.total_ids},
    ]
