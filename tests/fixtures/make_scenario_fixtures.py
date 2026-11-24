"""
Sinh các kịch bản lỗi CỐ Ý vào tests/fixtures/scenarios_invalid/ để chứng minh
evaluation/scenarios/validate.py thật sự bắt được lỗi.

    python tests/fixtures/make_scenario_fixtures.py

Mỗi ca lấy S001 làm gốc và làm hỏng đúng một chỗ.
"""

from __future__ import annotations

import copy
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / "evaluation" / "scenarios" / "S001_coinbase_duplicate_burst.yaml"
OUT = Path(__file__).resolve().parent / "scenarios_invalid"


def case_unknown_fault_type(d):
    d["fault_type"] = d["ground_truth"]["fault_type"] = "FT99_unknown"


def case_control_with_injection(d):
    d["ground_truth"] = {"is_incident": False}


def case_injection_outside_window(d):
    d["injection"]["start_offset_s"] = 1790  # window dài 1800 s, tiêm 120 s


def case_truth_before_injection(d):
    d["ground_truth"]["start_offset_s"] = 300


def case_fault_type_mismatch(d):
    d["ground_truth"]["fault_type"] = "FT02_volume_loss"


def case_missing_root_cause(d):
    del d["ground_truth"]["root_cause"]


def case_timestamp_not_ms(d):
    d["input"]["window"]["start"] = "2026-10-27T03:00:00Z"


def case_bad_asset_name(d):
    d["injection"]["target"]["asset"] = "raw.coinbase_trades"  # thiếu namespace


CASES = {
    "S901_unknown_fault_type": case_unknown_fault_type,
    "S902_control_with_injection": case_control_with_injection,
    "S903_injection_outside_window": case_injection_outside_window,
    "S904_truth_before_injection": case_truth_before_injection,
    "S905_fault_type_mismatch": case_fault_type_mismatch,
    "S906_missing_root_cause": case_missing_root_cause,
    "S907_timestamp_not_ms": case_timestamp_not_ms,
    "S908_bad_asset_name": case_bad_asset_name,
}


def main() -> None:
    base = yaml.safe_load(BASE.read_text(encoding="utf-8"))
    OUT.mkdir(exist_ok=True)
    for old in OUT.glob("*.yaml"):
        old.unlink()
    for name, mutate in CASES.items():
        doc = copy.deepcopy(base)
        doc["id"] = name
        mutate(doc)
        text = yaml.safe_dump(doc, allow_unicode=True, sort_keys=False)
        (OUT / f"{name}.yaml").write_text(f"# Ca lỗi cố ý: {mutate.__name__}\n{text}", encoding="utf-8")
    print(f"Đã ghi {len(CASES)} ca vào {OUT}")


if __name__ == "__main__":
    main()
