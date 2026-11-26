"""Profiling dữ liệu stream theo cửa sổ bằng sketch hợp nhất được (OB-A, Sprint 4).

Triển khai thuần Python (không phụ thuộc C++ compiler như datasketches wheel trên Windows/Alpine):
  - KLLSketch: ước lượng phân vị (p50, p95, p99) của giá và kích thước trade.
  - HyperLogLog: ước lượng số giá trị phân biệt (distinct trade_id, active symbols).
Cả hai đều hỗ trợ ``merge()`` để gộp các cửa sổ 1 phút thành cửa sổ 5 phút mà không
cần giữ lại toàn bộ dữ liệu gốc.
"""

from __future__ import annotations

import hashlib
import math
import time
import tracemalloc
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Iterable


class KLLSketch:
    """Simplified mergeable KLL-style quantile sketch (pure Python)."""

    def __init__(self, k: int = 200):
        if k < 8:
            raise ValueError("k phải >= 8")
        self.k = k
        self.compactors: list[list[float]] = [[]]
        self.n = 0

    def update(self, value: float) -> None:
        self.compactors[0].append(float(value))
        self.n += 1
        self._compress_if_needed()

    def _capacity(self, level: int) -> int:
        depth = len(self.compactors) - level - 1
        return max(4, int(self.k * ((2 / 3) ** depth)))

    def _compress_if_needed(self) -> None:
        for level in range(len(self.compactors)):
            if len(self.compactors[level]) > self._capacity(level):
                buf = sorted(self.compactors[level])
                # Giữ các phần tử ở vị trí chẵn để đẩy lên tầng trên (giảm một nửa)
                promoted = buf[::2]
                self.compactors[level] = []
                if level + 1 >= len(self.compactors):
                    self.compactors.append([])
                self.compactors[level + 1].extend(promoted)

    def merge(self, other: KLLSketch) -> KLLSketch:
        out = KLLSketch(k=max(self.k, other.k))
        max_levels = max(len(self.compactors), len(other.compactors))
        out.compactors = [[] for _ in range(max_levels)]
        for lvl in range(len(self.compactors)):
            out.compactors[lvl].extend(self.compactors[lvl])
        for lvl in range(len(other.compactors)):
            out.compactors[lvl].extend(other.compactors[lvl])
        out.n = self.n + other.n
        out._compress_if_needed()
        return out

    def quantile(self, q: float) -> float | None:
        if self.n == 0:
            return None
        if not 0.0 <= q <= 1.0:
            raise ValueError("q phải nằm trong [0, 1]")
        weighted: list[tuple[float, int]] = []
        for lvl, items in enumerate(self.compactors):
            w = 1 << lvl
            for val in items:
                weighted.append((val, w))
        if not weighted:
            return None
        weighted.sort(key=lambda x: x[0])
        total_w = sum(w for _, w in weighted)
        target = q * total_w
        acc = 0
        for val, w in weighted:
            acc += w
            if acc >= target:
                return val
        return weighted[-1][0]


class HyperLogLog:
    """HyperLogLog cardinality estimator (p=10 -> m=1024 registers, ~1 KB RAM)."""

    def __init__(self, p: int = 10):
        if not 4 <= p <= 16:
            raise ValueError("p phải trong [4, 16]")
        self.p = p
        self.m = 1 << p
        self.registers = [0] * self.m
        if self.m == 16:
            self._alpha = 0.673
        elif self.m == 32:
            self._alpha = 0.697
        elif self.m == 64:
            self._alpha = 0.709
        else:
            self._alpha = 0.7213 / (1.0 + 1.079 / self.m)

    def _hash64(self, value: str) -> int:
        digest = hashlib.blake2b(value.encode("utf-8"), digest_size=8).digest()
        return int.from_bytes(digest, "big")

    def update(self, value: str | int) -> None:
        x = self._hash64(str(value))
        idx = x >> (64 - self.p)
        w = (x << self.p) & ((1 << 64) - 1)
        # Đếm số bit 0 ở đầu + 1
        rho = 1
        if w == 0:
            rho = 64 - self.p + 1
        else:
            rho = (64 - self.p) - w.bit_length() + 1 + 1
            # Chuẩn hoá: đếm leading zeros trong (64 - p) bit
            rho = (64 - w.bit_length()) + 1
        if rho > self.registers[idx]:
            self.registers[idx] = rho

    def merge(self, other: HyperLogLog) -> HyperLogLog:
        if self.p != other.p:
            raise ValueError("Không thể merge hai HLL khác precision p")
        out = HyperLogLog(p=self.p)
        out.registers = [max(a, b) for a, b in zip(self.registers, other.registers)]
        return out

    def estimate(self) -> int:
        z = sum(2.0 ** (-r) for r in self.registers)
        raw = self._alpha * (self.m ** 2) / z
        if raw <= 2.5 * self.m:
            zeros = self.registers.count(0)
            if zeros > 0:
                return round(self.m * math.log(self.m / zeros))
        return round(raw)


@dataclass
class WindowSketch:
    source: str
    symbol: str
    window_start: str
    window_end: str
    price_kll: KLLSketch = field(default_factory=KLLSketch)
    size_kll: KLLSketch = field(default_factory=KLLSketch)
    trade_id_hll: HyperLogLog = field(default_factory=HyperLogLog)
    count: int = 0

    def add(self, trade_id: int | str, price: float | Decimal, size: float | Decimal) -> None:
        self.price_kll.update(float(price))
        self.size_kll.update(float(size))
        self.trade_id_hll.update(trade_id)
        self.count += 1

    def merge(self, other: WindowSketch, window_start: str, window_end: str) -> WindowSketch:
        return WindowSketch(
            source=self.source,
            symbol=self.symbol,
            window_start=window_start,
            window_end=window_end,
            price_kll=self.price_kll.merge(other.price_kll),
            size_kll=self.size_kll.merge(other.size_kll),
            trade_id_hll=self.trade_id_hll.merge(other.trade_id_hll),
            count=self.count + other.count,
        )

    def to_metrics(self, observed_at: str, window_label: str = "1m") -> list[dict]:
        dims = {"source": self.source, "symbol_canonical": self.symbol}
        base = {
            "dimensions": dims,
            "observed_at": observed_at,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "status": "ok" if self.count > 0 else "no_data",
            "sample_count": self.count,
        }
        if self.count == 0:
            return [{**base, "metric_name": f"price_sketch_p50_{window_label}", "value": None}]
        return [
            {**base, "metric_name": f"price_sketch_p50_{window_label}",
             "value": round(self.price_kll.quantile(0.50) or 0.0, 8)},
            {**base, "metric_name": f"price_sketch_p95_{window_label}",
             "value": round(self.price_kll.quantile(0.95) or 0.0, 8)},
            {**base, "metric_name": f"size_sketch_p95_{window_label}",
             "value": round(self.size_kll.quantile(0.95) or 0.0, 8)},
            {**base, "metric_name": f"trade_id_hll_distinct_{window_label}",
             "value": self.trade_id_hll.estimate()},
        ]


def profile_records(records: Iterable[dict], window_minutes: int = 1) -> list[WindowSketch]:
    """Nhóm bản ghi raw theo (source, symbol_canonical, window) và dựng sketch."""
    buckets: dict[tuple[str, str, datetime], WindowSketch] = {}
    for rec in records:
        if rec.get("transport", {}).get("is_snapshot"):
            continue
        ts_raw = rec.get("ingest_time") or rec.get("event_time")
        if not ts_raw:
            continue
        dt = datetime.fromisoformat(ts_raw.replace("Z", "+00:00")).astimezone(timezone.utc)
        minute_floor = (dt.minute // window_minutes) * window_minutes
        w_start = dt.replace(minute=minute_floor, second=0, microsecond=0)
        w_end = w_start + timedelta(minutes=window_minutes)
        src = rec["source"]
        sym = rec["symbol_canonical"]
        key = (src, sym, w_start)
        if key not in buckets:
            buckets[key] = WindowSketch(
                source=src,
                symbol=sym,
                window_start=w_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                window_end=w_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            )
        payload = rec.get("payload", {})
        price = payload.get("p") if src == "binance" else payload.get("price", 0)
        size = payload.get("q") if src == "binance" else payload.get("size", 0)
        buckets[key].add(rec["sequence"], Decimal(str(price)), Decimal(str(size)))
    return [buckets[k] for k in sorted(buckets)]


def compare_sketch_vs_exact(values: list[float], q: float = 0.95, k: int = 200) -> dict:
    """So sánh sai số giữa KLLSketch và tính chính xác."""
    if not values:
        return {"exact": None, "sketch": None, "abs_error": 0.0, "rel_error": 0.0}
    sorted_vals = sorted(values)
    idx = min(len(sorted_vals) - 1, max(0, math.ceil(q * len(sorted_vals)) - 1))
    exact = sorted_vals[idx]
    sketch = KLLSketch(k=k)
    for v in values:
        sketch.update(v)
    est = sketch.quantile(q) or 0.0
    abs_err = abs(est - exact)
    rel_err = abs_err / abs(exact) if exact != 0 else 0.0
    return {"exact": exact, "sketch": est, "abs_error": abs_err, "rel_error": rel_err}


def measure_overhead(n_items: int = 50_000) -> dict:
    """Đo chi phí CPU, bộ nhớ và tốc độ xử lý của KLL + HLL trên n_items bản ghi giả."""
    tracemalloc.start()
    t0 = time.perf_counter()
    kll = KLLSketch(k=200)
    hll = HyperLogLog(p=10)
    for i in range(n_items):
        kll.update(100.0 + (i % 1000) * 0.01)
        hll.update(i)
    _ = kll.quantile(0.95)
    _ = hll.estimate()
    elapsed_s = time.perf_counter() - t0
    _, peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return {
        "n_items": n_items,
        "elapsed_seconds": round(elapsed_s, 4),
        "throughput_eps": round(n_items / max(elapsed_s, 1e-6), 1),
        "peak_memory_kb": round(peak_bytes / 1024, 2),
    }
