"""Collector chạy liên tục: mỗi phút ghi 3 metric vào metric store (PostgreSQL).

    python observability/collectors/run_loop.py --raw /data/raw            # FINOBS_METRIC_DSN trong env
    python observability/collectors/run_loop.py --raw data/raw --once --dry-run

Metric: trade_count_1m, ingest_delay_p95_ms (theo phút) và last_live_trade_age_seconds
(tại mốc phút). Đọc file raw producer ghi ra (<raw>/<source>/<ngày>/<giờ>.jsonl.gz).

Đọc tăng dần: nhớ số dòng đã xử lý của từng file, lần sau chỉ parse + kiểm contract
phần mới. Bộ nhớ khử trùng giữ event_id trong --keep-minutes gần nhất (mặc định 65),
nên trade Coinbase phát lại cũ hơn mốc đó vẫn bị tính là mới: giới hạn đã ghi trong README.

Mỗi vòng tính lại --recompute phút gần nhất (mặc định 3) và upsert, để phần dữ liệu
producer flush muộn vẫn được cập nhật. Cặp (nguồn, mã) có trong config/symbols.json
nhưng không có trade:
  - nguồn vẫn có trade khác trong phút -> trade_count_1m = 0 (status ok)
  - cả nguồn không có trade nào         -> status source_unavailable, value null
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from observability.collectors import freshness, trade_count  # noqa: E402

SOURCES = ("binance", "coinbase")


def expected_pairs(symbols_path: Path) -> set[tuple[str, str]]:
    """Cặp (nguồn, mã chuẩn hóa) phải có metric, lấy từ config/symbols.json."""
    sys.path.insert(0, str(ROOT / "platform" / "ingest"))
    from common import canonical_binance, canonical_coinbase

    config = json.loads(symbols_path.read_text(encoding="utf-8"))
    canon = {"binance": canonical_binance, "coinbase": canonical_coinbase}
    return {(src, canon[src](sym)) for src in SOURCES for sym in config.get(src, [])}


class IncrementalReader:
    """Giữ trạng thái giữa các vòng: dòng đã đọc của mỗi file và các trade đã thấy."""

    def __init__(self, raw: Path, keep_minutes: int = 65):
        self.raw = raw
        self.keep = timedelta(minutes=keep_minutes)
        self.offsets: dict[Path, int] = {}
        self.first: dict[str, tuple[datetime, dict]] = {}
        self.stats: Counter = Counter()
        self._tools = trade_count.contract_tools()

    def files(self, now: datetime) -> list[Path]:
        hours = {now - timedelta(hours=h) for h in (0, 1)}
        found = []
        for source in SOURCES:
            for hour in sorted(hours):
                # <HH>.jsonl.gz (record.py) và <HH>.<epoch>.jsonl.gz (mỗi tiến trình producer một file)
                found += sorted((self.raw / source / hour.strftime("%Y-%m-%d")).glob(f"{hour:%H}*.jsonl.gz"))
        return found

    def poll(self, now: datetime) -> None:
        validators, schema_by_source, fallback, cross_checks = self._tools
        for path in self.files(now):
            done = self.offsets.get(path, 0)
            count = 0
            for count, line in enumerate(trade_count.read_lines(path), 1):
                if count <= done:
                    continue
                if not line.endswith("\n"):  # dòng ghi dở: để vòng sau đọc lại
                    count -= 1
                    break
                self._take(line, validators, schema_by_source, fallback, cross_checks)
            self.offsets[path] = max(done, count)

        cutoff = now - self.keep
        self.first = {k: v for k, v in self.first.items() if v[0] >= cutoff}
        self.offsets = {p: n for p, n in self.offsets.items() if p.exists()}

    def _take(self, line, validators, schema_by_source, fallback, cross_checks) -> None:
        self.stats["records_read"] += 1
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            self.stats["invalid_records"] += 1
            return
        if not isinstance(record, dict):
            self.stats["invalid_records"] += 1
            return
        schema = schema_by_source.get(record.get("source"), fallback)
        if not validators[schema].is_valid(record) or cross_checks(record):
            self.stats["invalid_records"] += 1
            return
        received = trade_count.utc_time(record["ingest_time"])
        slim = {  # chỉ giữ trường metric cần, để bộ nhớ khử trùng nhỏ
            "source": record["source"],
            "symbol_canonical": record["symbol_canonical"],
            "event_time": record["event_time"],
            "transport": {"is_snapshot": record["transport"].get("is_snapshot", False)},
        }
        previous = self.first.get(record["event_id"])
        if previous is None or received < previous[0]:
            self.first[record["event_id"]] = (received, slim)
        if previous is not None:
            self.stats["duplicate_records"] += 1


def trade_count_rows(first: dict, window_end: datetime, expected: set[tuple[str, str]]) -> list[dict]:
    rows = trade_count.observations(first, window_end)
    seen = {(r["dimensions"]["source"], r["dimensions"]["symbol_canonical"]) for r in rows}
    active_sources = {source for source, _ in seen}
    start = window_end - timedelta(minutes=1)
    for source, symbol in sorted(expected - seen):
        observed = source in active_sources
        rows.append({
            "metric_name": trade_count.METRIC_NAME,
            "dimensions": {"source": source, "symbol_canonical": symbol},
            "observed_at": trade_count.iso_utc(window_end),
            "window_start": trade_count.iso_utc(start),
            "window_end": trade_count.iso_utc(window_end),
            "value": 0 if observed else None,
            "status": "ok" if observed else "source_unavailable",
            "sample_count": 0 if observed else None,
        })
    return rows


def compute(first: dict, latest_end: datetime, recompute: int, expected: set[tuple[str, str]]) -> list[dict]:
    rows = []
    for k in range(recompute):
        end = latest_end - timedelta(minutes=k)
        rows += trade_count_rows(first, end, expected)
        rows += freshness.ingest_delay_p95(first, end, expected)
        rows += freshness.last_live_trade_age(first, end, expected)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--raw", default="data/raw")
    parser.add_argument("--symbols", default=str(ROOT / "config" / "symbols.json"))
    parser.add_argument("--lag-seconds", type=int, default=20, help="chờ sau mốc phút để producer kịp flush")
    parser.add_argument("--recompute", type=int, default=3)
    parser.add_argument("--keep-minutes", type=int, default=65)
    parser.add_argument("--once", action="store_true", help="chạy một vòng rồi thoát")
    parser.add_argument("--dry-run", action="store_true", help="in ra thay vì ghi DB")
    args = parser.parse_args()

    dsn = os.environ.get("FINOBS_METRIC_DSN")
    if not args.dry_run and not dsn:
        parser.error("cần FINOBS_METRIC_DSN (hoặc --dry-run)")

    reader = IncrementalReader(Path(args.raw), args.keep_minutes)
    expected = expected_pairs(Path(args.symbols))
    print(f"metrics-collector: {args.raw}, {len(expected)} cặp nguồn/mã", flush=True)

    while True:
        now = datetime.now(timezone.utc)
        latest_end = (now - timedelta(seconds=args.lag_seconds)).replace(second=0, microsecond=0)
        reader.poll(now)
        rows = compute(reader.first, latest_end, args.recompute, expected)
        if args.dry_run:
            for row in rows:
                print(json.dumps(row, ensure_ascii=False))
        else:
            try:
                trade_count.write_to_store(rows, dsn)
            except Exception as exc:  # noqa: BLE001 — DB tạm mất thì vòng sau ghi lại
                print(f"[collector] ghi DB lỗi: {exc!r}", flush=True)
        stats = reader.stats
        print(f"[collector] {latest_end:%H:%M}Z rows={len(rows)} read={stats['records_read']} "
              f"invalid={stats['invalid_records']} dup={stats['duplicate_records']} "
              f"tracked={len(reader.first)}", flush=True)
        if args.once:
            return
        next_tick = latest_end + timedelta(minutes=1, seconds=args.lag_seconds)
        time.sleep(max(5.0, (next_tick - datetime.now(timezone.utc)).total_seconds()))


if __name__ == "__main__":
    main()
