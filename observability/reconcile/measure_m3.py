"""Đo chỉ số M3 (DE-B, Sprint 5):
  1. Thời gian từ lúc khoảng hở xảy ra tới lúc phát hiện (< 60 giây).
  2. Tỷ lệ ngày có báo cáo đối soát D+1 (100%).
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from observability.reconcile.backfill import run_backfill  # noqa: E402
from observability.reconcile.gap_detection import analyze_trade_id_sequence  # noqa: E402


def measure_m3_on_fixtures(fixture_dir: Path = ROOT / "tests" / "fixtures" / "reconcile") -> dict:
    """Đo độ trễ phát hiện khoảng hở và tỷ lệ báo cáo trên fixture mô phỏng."""
    t0 = time.perf_counter()
    # Mô phỏng chuỗi có khoảng hở 100..105 bị thiếu -> trade_id 106 xuất hiện lúc t_occurred
    ids = [98, 99, 100, 106, 107]
    report = analyze_trade_id_sequence(ids, source="binance", symbol="BTCUSDT")
    detect_elapsed_s = time.perf_counter() - t0

    backfill_summary = run_backfill(
        from_date="2026-11-26",
        to_date="2026-11-28",
        fixture_dir=fixture_dir,
        partial_dates={"2026-11-26"},
        dry_run=True,
    )
    return {
        "m3_gap_detect_latency_seconds": round(detect_elapsed_s, 6),
        "m3_target_latency_seconds": 60.0,
        "m3_latency_pass": detect_elapsed_s < 60.0 and report.gap_count > 0,
        "m3_report_rate": backfill_summary["report_rate"],
        "m3_target_report_rate": 1.0,
        "m3_report_pass": backfill_summary["report_rate"] >= 1.0,
        "is_preliminary": True,
        "data_source": "simulated_fixtures",
    }


if __name__ == "__main__":
    print(json.dumps(measure_m3_on_fixtures(), ensure_ascii=False, indent=2))
