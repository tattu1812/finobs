"""Collector chạy liên tục: mỗi phút ghi 3 metric vào metric store (PostgreSQL).

    python observability/collectors/run_loop.py --raw /data/raw            # FINOBS_METRIC_DSN trong env
    python observability/collectors/run_loop.py --raw data/raw --once --dry-run

Metric từ file raw producer ghi ra (<raw>/<source>/<ngày>/<giờ>.jsonl.gz):
  trade_count_1m, ingest_delay_p95_ms, last_live_trade_age_seconds        (volume, freshness)
  contract_violation_count_1m, contract_violation_rate_1m               (schema)
  trading_freshness_seconds: freshness cổ phiếu VN tính theo giờ giao dịch (lịch HOSE)
Có TRINO_URL thì thêm metric cấp bảng (table_metrics.py): table_freshness_seconds,
trade_size_p95_5m, return_1m_bps, cross_source_spread_bps (lineage, distribution).

Đọc tăng dần: nhớ số dòng đã xử lý của từng file, lần sau chỉ parse + kiểm contract
phần mới. Bộ nhớ khử trùng giữ event_id trong --keep-minutes gần nhất (mặc định 15; S3 giảm từ 65 vì 3 sàn x 65 phút tốn ~400 MB),
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

from observability.collectors import freshness, table_metrics, trade_count  # noqa: E402
from observability.collectors.trino_client import TrinoClient  # noqa: E402
from observability.slo.trading_calendar import trading_freshness_rows  # noqa: E402

SOURCES = ("binance", "coinbase", "kraken", "vnstock")  # thư mục file raw cần đọc
SAMPLES_PER_MINUTE = 20
CRYPTO = ("binance", "coinbase", "kraken")  # nguồn chạy 24/7: thiếu trade = 0 / source_unavailable


def expected_pairs(symbols_path: Path) -> set[tuple[str, str]]:
    """Cặp (nguồn, mã chuẩn hóa) phải có metric, lấy từ config/symbols.json."""
    sys.path.insert(0, str(ROOT / "platform" / "ingest"))
    from common import canonical_binance, canonical_coinbase, canonical_kraken

    config = json.loads(symbols_path.read_text(encoding="utf-8"))
    canon = {"binance": canonical_binance, "coinbase": canonical_coinbase, "kraken": canonical_kraken}
    return {(src, canon[src](sym)) for src in CRYPTO for sym in config.get(src, [])}


class IncrementalReader:
    """Giữ trạng thái giữa các vòng: dòng đã đọc của mỗi file và các trade đã thấy."""

    def __init__(self, raw: Path, keep_minutes: int = 15):
        self.raw = raw
        self.keep = timedelta(minutes=keep_minutes)
        self.offsets: dict[Path, int] = {}
        self.first: dict[str, tuple[datetime, dict]] = {}
        self.stats: Counter = Counter()
        # (nguồn, phút ingest) -> số bản ghi đã kiểm / vi phạm contract (trụ cột schema)
        self.checked: Counter = Counter()
        self.violations: Counter = Counter()
        # (nguồn, mã) -> ingest_time của trade live mới nhất; không dọn theo keep_minutes
        # (nghỉ trưa HOSE dài 90 phút) - dùng cho freshness theo lịch giao dịch
        self.last_live: dict[tuple[str, str], datetime] = {}
        # Mẫu vi phạm chờ ghi vào contract_violation_samples (tối đa SAMPLES_PER_MINUTE mỗi nguồn/phút)
        self.samples: list[dict] = []
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
        for counter in (self.checked, self.violations):
            for key in [k for k in counter if k[1] < cutoff]:
                del counter[key]
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
        source = str(record.get("source", "?"))
        try:
            minute = trade_count.utc_time(record["ingest_time"]).replace(second=0, microsecond=0)
        except (KeyError, TypeError, ValueError):
            minute = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        self.checked[(source, minute)] += 1
        schema = schema_by_source.get(record.get("source"), fallback)
        problems = [] if validators[schema].is_valid(record) else [
            f"{'/'.join(map(str, e.absolute_path)) or '(gốc)'}: {e.message}"[:200]
            for e in validators[schema].iter_errors(record)]
        problems += cross_checks(record)
        if problems:
            self.stats["invalid_records"] += 1
            self.violations[(source, minute)] += 1
            if self.violations[(source, minute)] <= SAMPLES_PER_MINUTE:
                self.samples.append({"source": source, "symbol_native": record.get("symbol_native"),
                                     "event_id": record.get("event_id"), "ingest_minute": minute,
                                     "errors": problems})
            return
        received = trade_count.utc_time(record["ingest_time"])
        slim = {  # chỉ giữ trường metric cần, để bộ nhớ khử trùng nhỏ
            "source": record["source"],
            "symbol_canonical": record["symbol_canonical"],
            "event_time": record["event_time"],
            "transport": {"is_snapshot": record["transport"].get("is_snapshot", False)},
        }
        if not slim["transport"]["is_snapshot"]:
            key = (slim["source"], slim["symbol_canonical"])
            if key not in self.last_live or received > self.last_live[key]:
                self.last_live[key] = received
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


def write_samples(samples: list[dict], dsn: str) -> None:
    if not samples:
        return
    import psycopg
    from psycopg.types.json import Jsonb

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            for row in samples:
                cursor.execute(
                    "INSERT INTO contract_violation_samples (source, symbol_native, event_id, ingest_minute, errors) "
                    "VALUES (%s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT unique_violation_sample DO NOTHING",
                    (row["source"], row["symbol_native"], row["event_id"], row["ingest_minute"], Jsonb(row["errors"])))


def contract_rows(checked: Counter, violations: Counter, window_end: datetime) -> list[dict]:
    """contract_violation_count_1m và contract_violation_rate_1m theo nguồn (trụ cột schema).
    Nguồn crypto không có bản ghi nào trong phút -> source_unavailable."""
    start = window_end - timedelta(minutes=1)
    rows = []
    sources = sorted(set(CRYPTO) | {s for s, m in checked if m == start})
    for source in sources:
        total, bad = checked.get((source, start), 0), violations.get((source, start), 0)
        for metric, value in (("contract_violation_count_1m", bad),
                              ("contract_violation_rate_1m", round(bad / total, 6) if total else None)):
            rows.append({
                "metric_name": metric, "dimensions": {"source": source},
                "observed_at": trade_count.iso_utc(window_end),
                "window_start": trade_count.iso_utc(start), "window_end": trade_count.iso_utc(window_end),
                "value": value if total else None,
                "status": "ok" if total else "source_unavailable",
                "sample_count": total if total else None,
            })
    return rows


def compute(first: dict, latest_end: datetime, recompute: int, expected: set[tuple[str, str]],
            checked: Counter | None = None, violations: Counter | None = None) -> list[dict]:
    rows = []
    for k in range(recompute):
        end = latest_end - timedelta(minutes=k)
        rows += trade_count_rows(first, end, expected)
        rows += freshness.ingest_delay_p95(first, end, expected)
        rows += freshness.last_live_trade_age(first, end, expected)
        if checked is not None:
            rows += contract_rows(checked, violations or Counter(), end)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--raw", default="data/raw")
    parser.add_argument("--symbols", default=str(ROOT / "config" / "symbols.json"))
    parser.add_argument("--lag-seconds", type=int, default=20, help="chờ sau mốc phút để producer kịp flush")
    parser.add_argument("--recompute", type=int, default=3)
    parser.add_argument("--keep-minutes", type=int, default=15)
    parser.add_argument("--once", action="store_true", help="chạy một vòng rồi thoát")
    parser.add_argument("--dry-run", action="store_true", help="in ra thay vì ghi DB")
    args = parser.parse_args()

    dsn = os.environ.get("FINOBS_METRIC_DSN")
    if not args.dry_run and not dsn:
        parser.error("cần FINOBS_METRIC_DSN (hoặc --dry-run)")

    if dsn and not args.dry_run:
        from observability.store.apply import apply

        for attempt in range(30):  # metric store có thể chưa sẵn sàng ngay lúc khởi động
            try:
                print(f"[collector] migration: {', '.join(apply(dsn))}", flush=True)
                break
            except Exception as exc:  # noqa: BLE001
                print(f"[collector] chờ metric store ({exc!r})", flush=True)
                time.sleep(5)
    reader = IncrementalReader(Path(args.raw), args.keep_minutes)
    trino_url = os.environ.get("TRINO_URL")
    trino = TrinoClient(trino_url) if trino_url else None
    expected = expected_pairs(Path(args.symbols))
    print(f"metrics-collector: {args.raw}, {len(expected)} cặp nguồn/mã", flush=True)

    while True:
        now = datetime.now(timezone.utc)
        latest_end = (now - timedelta(seconds=args.lag_seconds)).replace(second=0, microsecond=0)
        reader.poll(now)
        rows = compute(reader.first, latest_end, args.recompute, expected, reader.checked, reader.violations)
        if trino is not None:
            try:
                rows += table_metrics.collect(trino, now)
            except Exception as exc:  # noqa: BLE001 — Trino chưa sẵn sàng thì vòng sau thử lại
                print(f"[collector] metric bảng lỗi: {exc!r}", flush=True)
        rows += trading_freshness_rows(reader.last_live, latest_end)
        if args.dry_run:
            for row in rows:
                print(json.dumps(row, ensure_ascii=False))
        else:
            try:
                trade_count.write_to_store(rows, dsn)
                write_samples(reader.samples, dsn)
                reader.samples.clear()
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
