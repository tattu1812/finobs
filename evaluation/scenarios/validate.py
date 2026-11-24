"""
Kiểm tra các file kịch bản lỗi (*.yaml) theo scenario.schema.json.

    python evaluation/scenarios/validate.py                       # mọi kịch bản trong thư mục này
    python evaluation/scenarios/validate.py --in tests/fixtures/scenarios_invalid --expect-invalid

Ngoài schema còn các kiểm tra liên trường mà JSON Schema không diễn đạt được:
  - id == tên file (không đuôi), nên id không thể trùng giữa các file
  - window.end > window.start
  - đoạn tiêm lỗi và đoạn ground truth nằm trong window
  - ground_truth.fault_type == fault_type
  - is_incident = false  <=>  injection = null
  - ground truth không bắt đầu trước khi tiêm lỗi
Và một kiểm tra đồng bộ: mã FTxx trong evaluation/fault_taxonomy.md khớp đúng enum của schema.

Exit 0 khi mọi file hợp lệ. Với --expect-invalid: exit 0 chỉ khi MỌI file đều bị bắt lỗi.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator

HERE = Path(__file__).resolve().parent
SCHEMA_PATH = HERE / "scenario.schema.json"
TAXONOMY_PATH = HERE.parent / "fault_taxonomy.md"
FAULT_RE = re.compile(r"`(FT[0-9]{2}_[a-z_]+)`")


def parse_utc(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def semantic_checks(doc: dict, path: Path) -> list[str]:
    errors = []
    if doc.get("id") != path.stem:
        errors.append(f"id '{doc.get('id')}' khác tên file '{path.stem}'")

    window = doc["input"]["window"]
    span = (parse_utc(window["end"]) - parse_utc(window["start"])).total_seconds()
    if span <= 0:
        errors.append("input.window.end phải sau input.window.start")

    inj = doc["injection"]
    gt = doc["ground_truth"]

    if gt["is_incident"] and inj is None:
        errors.append("is_incident = true nhưng injection = null")
    if not gt["is_incident"] and inj is not None:
        errors.append("kịch bản đối chứng (is_incident = false) không được có injection")

    if inj is not None and inj["start_offset_s"] + inj["duration_s"] > span:
        errors.append(f"đoạn tiêm lỗi vượt quá window ({span:.0f} s)")

    if gt["is_incident"]:
        if gt["fault_type"] != doc["fault_type"]:
            errors.append("ground_truth.fault_type khác fault_type")
        if gt["end_offset_s"] <= gt["start_offset_s"]:
            errors.append("ground_truth.end_offset_s phải lớn hơn start_offset_s")
        if gt["end_offset_s"] > span:
            errors.append(f"ground_truth vượt quá window ({span:.0f} s)")
        if inj is not None and gt["start_offset_s"] < inj["start_offset_s"]:
            errors.append("ground truth bắt đầu trước khi tiêm lỗi")
    return errors


def taxonomy_errors(schema: dict) -> list[str]:
    in_schema = set(schema["$defs"]["fault_type"]["enum"])
    in_doc = set(FAULT_RE.findall(TAXONOMY_PATH.read_text(encoding="utf-8")))
    errors = [f"{code} có trong schema nhưng thiếu trong fault_taxonomy.md" for code in sorted(in_schema - in_doc)]
    errors += [f"{code} có trong fault_taxonomy.md nhưng thiếu trong schema" for code in sorted(in_doc - in_schema)]
    return errors


def check_file(path: Path, validator: Draft202012Validator) -> list[str]:
    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        return [f"YAML lỗi: {exc}"]
    errors = [
        f"{'/'.join(map(str, e.absolute_path)) or '<gốc>'}: {e.message}"
        for e in sorted(validator.iter_errors(doc), key=lambda e: list(map(str, e.absolute_path)))
    ]
    # Kiểm tra liên trường chỉ có nghĩa khi cấu trúc đã đúng.
    return errors or semantic_checks(doc, path)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--in", dest="src", type=Path, default=HERE, help="thư mục chứa *.yaml")
    ap.add_argument("--expect-invalid", action="store_true", help="mọi file đều phải bị bắt lỗi")
    args = ap.parse_args()

    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema)

    status = 0
    sync = taxonomy_errors(schema)
    for msg in sync:
        print(f"[taxonomy] {msg}")
    if sync:
        status = 1

    files = sorted(args.src.glob("*.yaml"))
    if not files:
        print(f"Không có file *.yaml trong {args.src}")
        return 1

    bad = 0
    for path in files:
        errors = check_file(path, validator)
        if errors:
            bad += 1
            mark = "OK (bị bắt đúng)" if args.expect_invalid else "LỖI"
            print(f"{mark}  {path.name}")
            for msg in errors:
                print(f"      - {msg}")
        else:
            print(f"{'LỌT' if args.expect_invalid else 'OK'}  {path.name}")

    if args.expect_invalid:
        caught = bad == len(files)
        print(f"\n{bad}/{len(files)} file lỗi bị bắt.")
        return status or (0 if caught else 1)
    print(f"\n{len(files) - bad}/{len(files)} kịch bản hợp lệ.")
    return status or (1 if bad else 0)


if __name__ == "__main__":
    sys.exit(main())
