"""Harness đo MTTD (Mean Time To Detect) cho Sprint 4 (EV-B).

Định nghĩa chính xác:
  - ``MTTD = first_valid_alert_ts - ground_truth.start_ts`` (giây, theo UTC).
  - Cảnh báo khớp nếu đúng ``asset`` (hoặc ``source``) và nằm trong cửa sổ
    ``[start_ts, end_ts + detect_grace_s]``.
  - Cảnh báo đến trước ``start_ts`` (trừ khi nằm trong ``early_tolerance_s``) bị tính là
    ``early_alert`` / ``false_positive`` (không tính là phát hiện hợp lệ).
  - Cảnh báo không khớp bất kỳ ground truth nào là ``false_positive``.
  - Ground truth không có cảnh báo khớp là ``miss`` (false negative).
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


def _parse_ts(ts_str: str) -> datetime:
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00")).astimezone(timezone.utc)


@dataclass
class ScenarioEvalResult:
    scenario_id: str
    fault_type: str
    detected: bool
    mttd_seconds: float | None
    false_positives: int
    early_alerts: int
    misses: int

    def as_dict(self) -> dict:
        return {
            "scenario_id": self.scenario_id,
            "fault_type": self.fault_type,
            "detected": self.detected,
            "mttd_seconds": self.mttd_seconds,
            "false_positives": self.false_positives,
            "early_alerts": self.early_alerts,
            "misses": self.misses,
        }


def evaluate_mttd(
    *,
    scenario_id: str,
    ground_truths: list[dict],
    alerts: list[dict],
    detect_grace_s: float = 300.0,
    early_tolerance_s: float = 0.0,
) -> ScenarioEvalResult:
    """Khớp cảnh báo với danh sách ground_truth và tính MTTD, misses, false_positives."""
    matched_alert_ids: set[int] = set()
    early_count = 0
    misses = 0
    mttds: list[float] = []

    for gt in ground_truths:
        gt_start = _parse_ts(gt["start_ts"])
        gt_end = _parse_ts(gt["end_ts"])
        gt_asset = gt.get("affected_asset", "")
        gt_source = gt.get("source", "")

        candidates: list[tuple[datetime, int]] = []
        for idx, al in enumerate(alerts):
            al_asset = al.get("asset", "")
            al_source = al.get("source") or al.get("labels", {}).get("source", "")
            if gt_asset and al_asset and al_asset != gt_asset:
                continue
            if gt_source and al_source and al_source != gt_source:
                continue

            al_ts = _parse_ts(al["detected_at"])
            delta_s = (al_ts - gt_start).total_seconds()
            if delta_s < -early_tolerance_s:
                early_count += 1
                continue
            if al_ts.timestamp() <= gt_end.timestamp() + detect_grace_s:
                candidates.append((al_ts, idx))

        if not candidates:
            misses += 1
        else:
            candidates.sort(key=lambda x: x[0])
            first_ts, first_idx = candidates[0]
            matched_alert_ids.add(first_idx)
            mttd = max(0.0, (first_ts - gt_start).total_seconds())
            mttds.append(round(mttd, 3))
            # Đánh dấu tất cả candidate của cùng sự cố để không bị tính nhầm là FP
            for _, cidx in candidates:
                matched_alert_ids.add(cidx)

    false_positives = sum(1 for i in range(len(alerts)) if i not in matched_alert_ids)
    fault_label = ground_truths[0]["fault_type"] if ground_truths else "control"

    return ScenarioEvalResult(
        scenario_id=scenario_id,
        fault_type=fault_label,
        detected=(misses == 0 and len(mttds) > 0),
        mttd_seconds=round(sum(mttds) / len(mttds), 3) if mttds else None,
        false_positives=false_positives,
        early_alerts=early_count,
        misses=misses,
    )


def write_bench_reports(results: list[ScenarioEvalResult], out_dir: Path) -> tuple[Path, Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "mttd_summary.json"
    csv_path = out_dir / "mttd_summary.csv"
    md_path = out_dir / "mttd_summary.md"

    rows = [r.as_dict() for r in results]
    json_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")

    fieldnames = ["scenario_id", "fault_type", "detected", "mttd_seconds", "false_positives", "early_alerts", "misses"]
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    lines = [
        "# Kết quả Benchmark MTTD (Sprint 4)",
        "",
        "| Scenario | Fault Type | Detected | MTTD (s) | FP | Early | Miss |",
        "|---|---|---|---|---|---|---|",
    ]
    for r in results:
        mttd_str = f"{r.mttd_seconds:.3f}" if r.mttd_seconds is not None else "—"
        lines.append(
            f"| {r.scenario_id} | `{r.fault_type}` | {r.detected} | {mttd_str} | "
            f"{r.false_positives} | {r.early_alerts} | {r.misses} |"
        )
    lines.append("")
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return json_path, csv_path, md_path
