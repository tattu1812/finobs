"""
Sinh fixture nhỏ cho CI từ dữ liệu thu thật.

    python tests/fixtures/make_fixtures.py --in data/raw

  raw/<source>/sample.jsonl  — bản ghi thật, phải hợp lệ 100%
  invalid/<lỗi>.jsonl        — mỗi file 1 bản ghi thật bị sửa đúng 1 chỗ, phải bị bắt
"""

from __future__ import annotations

import argparse
import copy
import gzip
import json
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
PER_SYMBOL = 3


def pick(root: Path, source: str) -> list[dict]:
    """PER_SYMBOL bản ghi đầu của mỗi mã + vài bản snapshot và 1 lô nhiều trade."""
    by_symbol: dict = defaultdict(list)
    extra = []
    for path in sorted(root.glob(f"{source}/*/*.jsonl.gz")):
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            for line in fh:
                rec = json.loads(line)
                transport = rec["transport"]
                if transport.get("is_snapshot"):
                    if len(extra) < 2:
                        extra.append(rec)
                    continue
                if transport["batch_size"] > 1 and not any(r["transport"]["batch_size"] > 1 for r in extra):
                    extra.append(rec)
                    continue
                if len(by_symbol[rec["symbol_native"]]) < PER_SYMBOL:
                    by_symbol[rec["symbol_native"]].append(rec)
    return [r for recs in by_symbol.values() for r in recs] + extra


def mutations(binance: dict, coinbase: dict) -> dict:
    def mut(base, fn):
        rec = copy.deepcopy(base)
        fn(rec)
        return rec

    return {
        "price_as_number": mut(binance, lambda r: r["payload"].update(p=float(r["payload"]["p"]))),
        "binance_price_2_decimals": mut(binance, lambda r: r["payload"].update(p="79707.83")),
        "coinbase_size_as_number": mut(coinbase, lambda r: r["payload"].update(size=0.5)),
        "coinbase_price_exponent": mut(coinbase, lambda r: r["payload"].update(price="1e-05")),
        "event_time_no_z": mut(binance, lambda r: r.update(event_time=r["event_time"][:-1])),
        "event_time_offset": mut(binance, lambda r: r.update(event_time=r["event_time"][:-1] + "+07:00")),
        "event_time_microseconds": mut(coinbase, lambda r: r.update(event_time=r["event_time"][:-1] + "123Z")),
        "ingest_time_no_millis": mut(coinbase, lambda r: r.update(ingest_time=r["ingest_time"][:19] + "Z")),
        "canonical_no_dash": mut(binance, lambda r: r.update(symbol_canonical="BTCUSDT")),
        "canonical_lowercase": mut(coinbase, lambda r: r.update(symbol_canonical="btc-usd")),
        "missing_symbol_native": mut(binance, lambda r: r.pop("symbol_native")),
        "missing_payload_field": mut(coinbase, lambda r: r["payload"].pop("side")),
        "unknown_envelope_field": mut(binance, lambda r: r.update(price="1")),
        "unknown_payload_field": mut(binance, lambda r: r["payload"].update(b=123)),
        "unknown_source": mut(binance, lambda r: r.update(source="kraken")),
        "schema_major_2": mut(coinbase, lambda r: r.update(schema_version="2.0.0")),
        "coinbase_trade_id_int": mut(coinbase, lambda r: r["payload"].update(trade_id=int(r["payload"]["trade_id"]))),
        "coinbase_missing_is_snapshot": mut(coinbase, lambda r: r["transport"].pop("is_snapshot")),
        "coinbase_batch_over_100": mut(coinbase, lambda r: r["transport"].update(batch_size=101)),
        "binance_batch_2": mut(binance, lambda r: r["transport"].update(batch_size=2)),
        "event_id_mismatch": mut(binance, lambda r: r.update(event_id="binance:BTCUSDT:1")),
        "payload_id_mismatch": mut(coinbase, lambda r: r["payload"].update(trade_id="1")),
    }


def write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, separators=(",", ":"), ensure_ascii=False) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in", dest="indir", default="data/raw")
    args = parser.parse_args()
    root = Path(args.indir)

    samples = {source: pick(root, source) for source in ("binance", "coinbase")}
    for source, records in samples.items():
        write_jsonl(HERE / "raw" / source / "sample.jsonl", records)
        print(f"raw/{source}/sample.jsonl: {len(records)} bản ghi")

    cases = mutations(samples["binance"][0], samples["coinbase"][0])
    for name, rec in cases.items():
        write_jsonl(HERE / "invalid" / f"{name}.jsonl", [rec])
    print(f"invalid/: {len(cases)} ca lỗi")


if __name__ == "__main__":
    main()
