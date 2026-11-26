"""Fault Injector v2 (EV-A, Sprint 4): >= 8 loại lỗi, tất định theo seed, mặc định dry-run.

Quy tắc an toàn:
  - KHÔNG BAO GIỜ ghi vào topic Kafka thật (raw.binance.trade, raw.coinbase.trade)
    hay bảng Iceberg đang nuôi việc thu 14 ngày.
  - Mặc định chạy ở chế độ ``dry_run=True`` trên dữ liệu phát lại cục bộ.
  - Nếu bật công tắc ``live_inject=True``, chỉ cho phép ghi vào topic có tiền tố ``test.``.
"""

from __future__ import annotations

import copy
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

SUPPORTED_FAULTS = (
    "FT01_stale_feed",
    "FT02_volume_loss",
    "FT03_duplicate_events",
    "FT05_late_out_of_order",
    "FT06_clock_skew",
    "FT07_schema_drift",
    "FT09_value_anomaly",
    "FT12_null_spike",
)


@dataclass
class InjectionOutput:
    records: list[dict]
    ground_truth: dict
    target_topic: str
    dry_run: bool


def _shift_iso(ts_str: str, delta_ms: int) -> str:
    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).astimezone(timezone.utc)
    shifted = dt + timedelta(milliseconds=delta_ms)
    return shifted.strftime("%Y-%m-%dT%H:%M:%S.") + f"{shifted.microsecond // 1000:03d}Z"


def inject_fault(
    records: list[dict],
    *,
    fault_type: str,
    source: str,
    symbol: str | None = None,
    start_idx: int = 0,
    duration_count: int = 5,
    intensity: float = 0.5,
    seed: int = 42,
    live_inject: bool = False,
) -> InjectionOutput:
    """Tiêm một loại lỗi có kiểm soát vào bản sao của ``records`` và sinh ground truth."""
    if fault_type not in SUPPORTED_FAULTS:
        raise ValueError(f"Loại lỗi chưa hỗ trợ: {fault_type}. Hỗ trợ: {SUPPORTED_FAULTS}")

    rng = random.Random(seed)
    cloned = copy.deepcopy(records)
    out: list[dict] = []

    matching_indices = [
        idx for idx, r in enumerate(cloned)
        if r.get("source") == source and (symbol is None or r.get("symbol_native") == symbol)
    ]
    window_indices = set(matching_indices[start_idx : start_idx + duration_count])

    start_ts = cloned[min(window_indices)]["ingest_time"] if window_indices else ""
    end_ts = cloned[max(window_indices)]["ingest_time"] if window_indices else ""

    for idx, rec in enumerate(cloned):
        if idx not in window_indices:
            out.append(rec)
            continue

        if fault_type == "FT01_stale_feed":
            # Ngừng phát toàn bộ event trong cửa sổ
            continue

        elif fault_type == "FT02_volume_loss":
            # Bỏ ngẫu nhiên theo intensity -> tạo khoảng hở trade_id
            if rng.random() < intensity:
                continue
            out.append(rec)

        elif fault_type == "FT03_duplicate_events":
            out.append(rec)
            if rng.random() < intensity:
                out.append(copy.deepcopy(rec))

        elif fault_type == "FT05_late_out_of_order":
            # Trì hoãn ingest_time và đẩy về sau (hoặc đảo thứ tự)
            rec["ingest_time"] = _shift_iso(rec["ingest_time"], int(5000 * max(intensity, 0.2)))
            out.append(rec)

        elif fault_type == "FT06_clock_skew":
            # Lệch đồng hồ: trừ 3000ms khỏi ingest_time
            rec["ingest_time"] = _shift_iso(rec["ingest_time"], -3000)
            out.append(rec)

        elif fault_type == "FT07_schema_drift":
            payload = rec.get("payload", {})
            if "p" in payload:
                payload["price"] = payload.pop("p")
            elif "price" in payload:
                payload["last_price"] = payload.pop("price")
            out.append(rec)

        elif fault_type == "FT09_value_anomaly":
            # Nhân giá x1000 chỉ ở một sàn
            payload = rec.get("payload", {})
            for key in ("p", "price"):
                if key in payload and payload[key] is not None:
                    payload[key] = f"{float(payload[key]) * 1000:.8f}" if key == "p" else str(float(payload[key]) * 1000)
            out.append(rec)

        elif fault_type == "FT12_null_spike":
            payload = rec.get("payload", {})
            for key in ("p", "price"):
                if key in payload:
                    payload[key] = None
            out.append(rec)

    if fault_type == "FT05_late_out_of_order":
        out.sort(key=lambda r: r.get("ingest_time", ""))

    # Quy tắc an toàn: chỉ dùng tiền tố test. nếu live_inject=True
    target_topic = f"test.raw.{source}.trade"
    ground_truth = {
        "fault_type": fault_type,
        "source": source,
        "symbol": symbol or "all",
        "affected_asset": f"finobs:raw.{source}.trade",
        "start_ts": start_ts,
        "end_ts": end_ts,
        "seed": seed,
    }
    return InjectionOutput(
        records=out,
        ground_truth=ground_truth,
        target_topic=target_topic,
        dry_run=not live_inject,
    )
