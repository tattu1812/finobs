"""Batch runner cho gap detection (DE-A, Sprint 4).

    python observability/reconcile/run_gap_detection.py --source binance --symbol BTCUSDT --from 2026-11-27T00:00:00Z --to 2026-11-28T00:00:00Z --dry-run
    python observability/reconcile/run_gap_detection.py --in tests/fixtures/raw --dry-run
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from observability.reconcile.gap_detection import (  # noqa: E402
    GapReport,
    analyze_trade_id_sequence,
    gap_report_to_metrics,
)


def _iter_raw_files(root: Path):
    if root.is_file():
        return [root]
    files = sorted(root.rglob("*.jsonl")) + sorted(root.rglob("*.jsonl.gz"))
    return [p for p in files if "_ops" not in p.parts]


def _load_ids_from_raw(
    raw_dir: Path,
    source_filter: str | None = None,
    symbol_filter: str | None = None,
    from_ts: str | None = None,
    to_ts: str | None = None,
) -> dict[tuple[str, str], list[int]]:
    grouped: dict[tuple[str, str], list[tuple[str, int]]] = defaultdict(list)
    for path in _iter_raw_files(raw_dir):
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(rec, dict):
                    continue
                src = rec.get("source")
                sym = rec.get("symbol_native")
                seq = rec.get("sequence")
                ingest_ts = rec.get("ingest_time", "")
                if not src or not sym or seq is None:
                    continue
                if rec.get("transport", {}).get("is_snapshot"):
                    continue
                if source_filter and src != source_filter:
                    continue
                if symbol_filter and sym != symbol_filter:
                    continue
                if from_ts and ingest_ts < from_ts:
                    continue
                if to_ts and ingest_ts >= to_ts:
                    continue
                grouped[(src, sym)].append((ingest_ts, int(seq)))

    # Giữ thứ tự nhận theo ingest_ts (ổn định theo thứ tự xuất hiện nếu trùng ms)
    return {
        key: [seq for _, seq in sorted(items, key=lambda x: x[0])]
        for key, items in grouped.items()
    }


def _load_ids_from_trino(
    source: str,
    symbol: str,
    from_ts: str,
    to_ts: str,
    container: str = "finobs-trino",
) -> list[int]:
    sql = (
        f"SELECT event_id FROM iceberg.bronze.trades_raw "
        f"WHERE source = '{source}' AND symbol = '{symbol}' "
        f"AND ingest_ts >= TIMESTAMP '{from_ts.replace('T', ' ').rstrip('Z')}' "
        f"AND ingest_ts < TIMESTAMP '{to_ts.replace('T', ' ').rstrip('Z')}' "
        f"ORDER BY ingest_ts ASC, bronze_ts ASC"
    )
    cmd = [
        "docker", "exec", container, "trino",
        "--output-format", "JSON", "--execute", sql,
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if res.returncode != 0:
        raise RuntimeError(f"Trino query failed: {res.stderr.strip()}")
    ids: list[int] = []
    for line in res.stdout.splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        eid = row.get("event_id") if isinstance(row, dict) else row[0]
        if eid:
            ids.append(int(str(eid).rsplit(":", 1)[-1]))
    return ids


def run_batch(
    *,
    raw_dir: Path | None = None,
    source: str | None = None,
    symbol: str | None = None,
    from_ts: str = "",
    to_ts: str = "",
    write_db: bool = False,
    dsn: str | None = None,
) -> list[GapReport]:
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    w_start = from_ts or now_iso
    w_end = to_ts or now_iso

    reports: list[GapReport] = []
    if raw_dir is not None:
        grouped = _load_ids_from_raw(raw_dir, source, symbol, from_ts or None, to_ts or None)
        for (src, sym), ids in sorted(grouped.items()):
            reports.append(analyze_trade_id_sequence(ids, source=src, symbol=sym))
    else:
        if not source or not symbol or not from_ts or not to_ts:
            raise ValueError("Chế độ Trino yêu cầu --source, --symbol, --from, --to")
        ids = _load_ids_from_trino(source, symbol, from_ts, to_ts)
        reports.append(analyze_trade_id_sequence(ids, source=source, symbol=symbol))

    if write_db:
        target_dsn = dsn or os.environ.get("FINOBS_METRIC_DSN")
        if not target_dsn:
            raise ValueError("--write-db yêu cầu FINOBS_METRIC_DSN")
        sys.path.insert(0, str(ROOT / "observability" / "collectors"))
        from trade_count import write_to_store

        metrics: list[dict] = []
        for rep in reports:
            metrics.extend(gap_report_to_metrics(rep, now_iso, w_start, w_end))
        write_to_store(metrics, target_dsn)

    return reports


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--in", dest="raw_dir", default="", help="đọc từ file raw (.jsonl / .jsonl.gz) thay vì Trino")
    parser.add_argument("--source", default="", help="binance hoặc coinbase")
    parser.add_argument("--symbol", default="", help="mã gốc (symbol_native)")
    parser.add_argument("--from", dest="from_ts", default="", help="UTC ISO8601 bắt đầu (vd 2026-11-27T00:00:00Z)")
    parser.add_argument("--to", dest="to_ts", default="", help="UTC ISO8601 kết thúc")
    parser.add_argument("--write-db", action="store_true", help="ghi metric vào PostgreSQL metric store")
    parser.add_argument("--dry-run", action="store_true", help="in JSON ra stdout")
    args = parser.parse_args()

    reports = run_batch(
        raw_dir=Path(args.raw_dir) if args.raw_dir else None,
        source=args.source or None,
        symbol=args.symbol or None,
        from_ts=args.from_ts,
        to_ts=args.to_ts,
        write_db=args.write_db and not args.dry_run,
    )
    payload = [r.as_dict() for r in reports]
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
