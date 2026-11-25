"""
Validate toàn bộ file raw đã thu với contract trong thư mục này.

    python platform/contracts/validate.py --in data/raw
    python platform/contracts/validate.py --in tests/fixtures/invalid --expect-invalid

Chọn schema theo trường "source": binance -> binance_trade, coinbase ->
coinbase_trade, nguồn khác -> chỉ kiểm vỏ (raw_event) và in rõ điều đó.

Ngoài schema còn 3 kiểm tra chéo trường mà JSON Schema không diễn đạt được:
  - event_id == "<source>:<symbol_native>:<sequence>"
  - id trong payload == sequence
  - mã trong payload == symbol_native

Exit 0 khi mọi bản ghi hợp lệ. Với --expect-invalid thì ngược lại: exit 0
chỉ khi MỌI bản ghi bị bắt lỗi — dùng để chứng minh schema thật sự chặn được.
"""

from __future__ import annotations

import argparse
import gzip
import json
from collections import Counter, defaultdict
from pathlib import Path

from jsonschema import Draft202012Validator
from referencing import Registry, Resource

HERE = Path(__file__).resolve().parent
SCHEMA_BY_SOURCE = {"binance": "binance_trade", "coinbase": "coinbase_trade"}
FALLBACK = "raw_event"

# (khoá id trong payload, khoá mã trong payload)
PAYLOAD_KEYS = {"binance": ("t", "s"), "coinbase": ("trade_id", "product_id"), "vnstock": ("id", "symbol")}


def load_validators() -> dict:
    schemas = {
        path.name.removesuffix(".schema.json"): json.loads(path.read_text(encoding="utf-8"))
        for path in sorted(HERE.glob("*.schema.json"))
    }
    registry = Registry().with_resources(
        (schema["$id"], Resource.from_contents(schema)) for schema in schemas.values()
    )
    validators = {}
    for name, schema in schemas.items():
        Draft202012Validator.check_schema(schema)
        validators[name] = Draft202012Validator(schema, registry=registry)
    return validators


def cross_checks(rec: dict) -> list[str]:
    errors = []
    source, native, seq = rec.get("source"), rec.get("symbol_native"), rec.get("sequence")
    if rec.get("event_id") != f"{source}:{native}:{seq}":
        errors.append("event_id khác <source>:<symbol_native>:<sequence>")
    keys = PAYLOAD_KEYS.get(source)
    payload = rec.get("payload")
    if keys and isinstance(payload, dict):
        id_key, sym_key = keys
        if str(payload.get(id_key)) != str(seq):
            errors.append(f"payload.{id_key} khác sequence")
        if payload.get(sym_key) != native:
            errors.append(f"payload.{sym_key} khác symbol_native")
    return errors


def iter_records(root: Path):
    files = sorted(root.rglob("*.jsonl.gz")) + sorted(root.rglob("*.jsonl"))
    files = [p for p in files if "_ops" not in p.parts]
    if not files:
        raise SystemExit(f"Không tìm thấy file .jsonl(.gz) nào trong {root}")
    for path in files:
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, 1):
                if line.strip():
                    yield f"{path}:{lineno}", line


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in", dest="indir", default="data/raw")
    parser.add_argument("--expect-invalid", action="store_true")
    parser.add_argument("--show", type=int, default=3, help="số ví dụ in ra cho mỗi loại lỗi")
    args = parser.parse_args()

    validators = load_validators()
    total, valid = Counter(), Counter()
    used_schema = {}
    error_kinds: dict = defaultdict(list)  # (source, lỗi) -> [vị trí]

    for where, line in iter_records(Path(args.indir)):
        try:
            rec = json.loads(line)
        except json.JSONDecodeError as exc:
            total["?"] += 1
            error_kinds[("?", f"JSON hỏng: {exc.msg}")].append(where)
            continue
        source = rec.get("source", "?") if isinstance(rec, dict) else "?"
        name = SCHEMA_BY_SOURCE.get(source, FALLBACK)
        used_schema[source] = name
        total[source] += 1

        problems = [
            f"{'/'.join(map(str, err.absolute_path)) or '(gốc)'}: {err.message}"
            for err in validators[name].iter_errors(rec)
        ]
        if isinstance(rec, dict):
            problems += cross_checks(rec)
        if problems:
            for problem in problems:
                error_kinds[(source, problem[:160])].append(where)
        else:
            valid[source] += 1

    print(f"Validate {args.indir}\n")
    print(f"{'nguồn':<10}{'schema':<17}{'bản ghi':>10}{'hợp lệ':>10}{'lỗi':>8}")
    for source in sorted(total):
        schema = used_schema.get(source, "-")
        note = "  (chỉ kiểm vỏ)" if schema == FALLBACK else ""
        print(f"{source:<10}{schema:<17}{total[source]:>10}{valid[source]:>10}"
              f"{total[source] - valid[source]:>8}{note}")
    n_total, n_valid = sum(total.values()), sum(valid.values())
    print(f"{'TỔNG':<27}{n_total:>10}{n_valid:>10}{n_total - n_valid:>8}")

    if error_kinds:
        print(f"\nDanh sách lỗi ({len(error_kinds)} loại):")
        for (source, problem), places in sorted(error_kinds.items(), key=lambda kv: -len(kv[1])):
            print(f"  [{source}] x{len(places)}  {problem}")
            for where in places[: args.show]:
                print(f"      {where}")

    if args.expect_invalid:
        ok = n_total > 0 and n_valid == 0
        print("\nKỲ VỌNG: mọi bản ghi đều bị bắt lỗi ->", "ĐẠT" if ok else "KHÔNG ĐẠT")
    else:
        ok = n_total > 0 and n_valid == n_total
        print("\nKẾT QUẢ:", "ĐẠT" if ok else "KHÔNG ĐẠT")
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
