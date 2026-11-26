"""
Injector v1 của FinObs-Bench (Sprint 3, EV-4 + EV-5): phát lại dữ liệu raw của một scenario,
tiêm lỗi đúng như YAML, ghi kết quả vào namespace riêng và lưu đáp án vào bench.scenarios.

    python evaluation/injector/inject.py evaluation/scenarios/S002_binance_schema_drift.yaml
    python evaluation/injector/inject.py S004.yaml --out bench --kafka localhost:9092 --dsn postgresql://...

Không đụng luồng thật (quyết định Q7): file ra ở <out>/<run_id>/<source>/<ngày>/<giờ>.jsonl.gz,
Kafka (tùy chọn) vào topic bench.<scenario_id>.raw.<source>.trade, không khớp mẫu raw\\.[a-z]+\\.trade
mà job bronze đọc.

5 injector (taxonomy):
  drop_records        FT02 mất tin            params: fraction (mặc định 1.0)
  duplicate_records   FT03 trùng tin          params: fraction, keep_event_id, max_delay_ms
  shift_ingest_time   FT06 lệch đồng hồ       params: offset_ms, drift_ms_per_hour
  mutate_payload      FT07 đổi schema         params: rename_fields {cũ: mới}, cast_fields {trường: string|integer|number}
  scale_price         FT09 sai đơn vị giá     params: factor (vd 1000)

Tất định: cùng file YAML (seed trong input.seed) và cùng dữ liệu vào cho cùng output_sha256.
"""

from __future__ import annotations

import argparse
import copy
import gzip
import hashlib
import json
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
PRICE_FIELD = {"binance": "p", "coinbase": "price", "kraken": "price"}


def parse_utc(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def fmt(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


# ---------------------------------------------------------------- đọc dữ liệu
def load_window(scenario: dict) -> list[dict]:
    inp = scenario["input"]
    start, end = parse_utc(inp["window"]["start"]), parse_utc(inp["window"]["end"])
    sources = set(inp.get("sources") or [])
    symbols = set(inp.get("symbols") or [])
    base = ROOT / inp["dataset"] if not Path(inp["dataset"]).is_absolute() else Path(inp["dataset"])
    files = sorted(base.rglob("*.jsonl.gz")) + sorted(base.rglob("*.jsonl"))
    if not files:
        raise SystemExit(f"Không có dữ liệu phát lại ở {base}")
    records = []
    for path in files:
        if "_ops" in path.parts:
            continue
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8") as fh:
            for line in fh:
                rec = json.loads(line)
                ingest = parse_utc(rec["ingest_time"])
                if not (start <= ingest < end):
                    continue
                if sources and rec["source"] not in sources:
                    continue
                if symbols and rec["symbol_native"] not in symbols and rec["symbol_canonical"] not in symbols:
                    continue
                records.append(rec)
    records.sort(key=lambda r: (r["ingest_time"], r["source"], r["sequence"]))
    return records


# ---------------------------------------------------------------- injector
def _targeted(rec: dict, target: dict, lo: datetime, hi: datetime) -> bool:
    if not (lo <= parse_utc(rec["ingest_time"]) < hi):
        return False
    if target.get("source") and rec["source"] != target["source"]:
        return False
    symbols = target.get("symbols")
    return not symbols or rec["symbol_native"] in symbols or rec["symbol_canonical"] in symbols


def _cast(value, kind: str):
    if kind == "string":
        return str(value)
    if kind == "integer":
        return int(Decimal(str(value)))
    if kind == "number":
        return float(value)
    raise ValueError(f"cast_fields không hỗ trợ kiểu {kind}")


def apply_injection(records: list[dict], scenario: dict, rng: random.Random) -> tuple[list[dict], list[datetime]]:
    """Trả (bản ghi sau tiêm, thời điểm ingest của các bản ghi bị tác động)."""
    inj = scenario.get("injection")
    if not inj:
        return [copy.deepcopy(r) for r in records], []
    start = parse_utc(scenario["input"]["window"]["start"])
    lo = start + timedelta(seconds=inj["start_offset_s"])
    hi = lo + timedelta(seconds=inj["duration_s"])
    target, params, kind = inj.get("target", {}), inj.get("params", {}) or {}, inj["injector"]
    out, touched = [], []

    for original in records:
        rec = copy.deepcopy(original)
        hit = _targeted(rec, target, lo, hi) if target.get("layer") != "host" else \
            lo <= parse_utc(rec["ingest_time"]) < hi
        if not hit:
            out.append(rec)
            continue
        ingest = parse_utc(rec["ingest_time"])

        if kind == "drop_records":
            if rng.random() < float(params.get("fraction", 1.0)):
                touched.append(ingest)
                continue
            out.append(rec)
        elif kind == "duplicate_records":
            out.append(rec)
            if rng.random() < float(params.get("fraction", 0.05)):
                dup = copy.deepcopy(rec)
                delay = rng.uniform(0, float(params.get("max_delay_ms", 50)))
                dup["ingest_time"] = fmt(ingest + timedelta(milliseconds=delay))
                if not params.get("keep_event_id", True):
                    dup["event_id"] = f"{dup['event_id']}-dup"
                out.append(dup)
                touched.append(ingest)
        elif kind == "shift_ingest_time":
            hours = (ingest - lo).total_seconds() / 3600
            offset = float(params.get("offset_ms", 0)) + float(params.get("drift_ms_per_hour", 0)) * hours
            rec["ingest_time"] = fmt(ingest + timedelta(milliseconds=offset))
            out.append(rec)
            touched.append(ingest)
        elif kind == "mutate_payload":
            payload = rec["payload"]
            for field, kind_ in (params.get("cast_fields") or {}).items():
                if field in payload:
                    payload[field] = _cast(payload[field], kind_)
            for old, new in (params.get("rename_fields") or {}).items():
                if old in payload:
                    payload[new] = payload.pop(old)
            out.append(rec)
            touched.append(ingest)
        elif kind == "scale_price":
            field = PRICE_FIELD[rec["source"]]
            factor = Decimal(str(params.get("factor", 1000)))
            rec["payload"][field] = format(Decimal(rec["payload"][field]) * factor, "f")
            out.append(rec)
            touched.append(ingest)
        else:
            raise SystemExit(f"Injector chưa hỗ trợ: {kind}")

    out.sort(key=lambda r: (r["ingest_time"], r["source"], r["sequence"]))
    return out, touched


# ---------------------------------------------------------------- ghi kết quả
def lines_of(records: list[dict]) -> list[str]:
    return [json.dumps(r, separators=(",", ":"), ensure_ascii=False, sort_keys=True) for r in records]


def write_files(lines: list[str], records: list[dict], root: Path) -> None:
    groups: dict[Path, list[str]] = {}
    for line, rec in zip(lines, records):
        ts = parse_utc(rec["ingest_time"])
        path = root / rec["source"] / ts.strftime("%Y-%m-%d") / f"{ts:%H}.jsonl.gz"
        groups.setdefault(path, []).append(line)
    for path, group in groups.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wt", encoding="utf-8") as fh:
            fh.write("\n".join(group) + "\n")


def publish(lines: list[str], records: list[dict], bootstrap: str, scenario_id: str) -> str:
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True})
    prefix = f"bench.{scenario_id}"
    for line, rec in zip(lines, records):
        producer.produce(f"{prefix}.raw.{rec['source']}.trade", value=line.encode(),
                         key=str(rec["symbol_native"]).encode())
        producer.poll(0)
    producer.flush(30)
    return f"kafka:{prefix}.raw.<source>.trade"


def record_ground_truth(dsn: str, row: dict) -> None:
    import psycopg
    from psycopg.types.json import Jsonb

    columns = list(row)
    values = [Jsonb(v) if isinstance(v, (dict, list)) else v for v in row.values()]
    with psycopg.connect(dsn) as connection:
        connection.execute(
            f"INSERT INTO bench.scenarios ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})",
            values)


def run(scenario_path: Path, out: Path, kafka: str | None = None, dsn: str | None = None) -> dict:
    sys.path.insert(0, str(ROOT / "evaluation" / "scenarios"))
    import validate

    errors = validate.check_file(scenario_path, validate.load_validators())
    if errors:
        raise SystemExit(f"{scenario_path.name} không hợp lệ: {errors}")
    scenario = yaml.safe_load(scenario_path.read_text(encoding="utf-8"))

    records = load_window(scenario)
    rng = random.Random(scenario["input"].get("seed", 0))
    injected, touched = apply_injection(records, scenario, rng)
    lines = lines_of(injected)
    digest = hashlib.sha256("\n".join(lines).encode()).hexdigest()

    run_id = f"{scenario['id']}-{digest[:12]}-{uuid.uuid4().hex[:6]}"
    target_dir = out / run_id
    write_files(lines, injected, target_dir)
    target = str(target_dir)
    if kafka:
        target += " + " + publish(lines, injected, kafka, scenario["id"])

    truth, inj = scenario["ground_truth"], scenario.get("injection") or {}
    row = {
        "run_id": run_id, "scenario_id": scenario["id"], "scenario_version": scenario["scenario_version"],
        "fault_type": scenario["fault_type"], "is_incident": truth["is_incident"],
        "injector": inj.get("injector"), "target_node": (inj.get("target") or {}).get("asset"),
        "root_cause": (truth.get("root_cause") or {}).get("asset"),
        "affected_assets": truth.get("affected_assets", []), "params": inj.get("params") or {},
        "seed": scenario["input"].get("seed"),
        "window_start": scenario["input"]["window"]["start"], "window_end": scenario["input"]["window"]["end"],
        "injected_from": fmt(min(touched)) if touched else None,
        "injected_to": fmt(max(touched)) if touched else None,
        "records_in": len(records), "records_out": len(injected), "records_affected": len(touched),
        "output_sha256": digest, "output_target": target,
    }
    if dsn:
        record_ground_truth(dsn, row)
    return row


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("scenarios", nargs="+", type=Path)
    parser.add_argument("--out", type=Path, default=ROOT / "bench")
    parser.add_argument("--kafka", help="bootstrap server; có thì phát vào topic bench.<scenario>.*")
    parser.add_argument("--dsn", help="metric store; có thì ghi đáp án vào bench.scenarios")
    args = parser.parse_args()
    for path in args.scenarios:
        print(json.dumps(run(path, args.out, args.kafka, args.dsn), ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
