"""Collect trade_count_1m from FinObs raw trade files.

The default mode prints observations as JSON Lines. Use --write-db to persist
them through the metric store's upsert_metric_observation SQL function.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
import zlib
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
METRIC_NAME = "trade_count_1m"


def utc_time(value: str) -> datetime:
    """Parse a timestamp and require an explicit timezone."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"Timestamp has no timezone: {value}")
    return parsed.astimezone(timezone.utc)


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def raw_files(root: Path) -> list[Path]:
    if root.is_file():
        files = [root]
    elif root.is_dir():
        files = sorted(root.rglob("*.jsonl")) + sorted(root.rglob("*.jsonl.gz"))
    else:
        raise FileNotFoundError(f"Input does not exist: {root}")
    files = [path for path in files if "_ops" not in path.parts]
    if not files:
        raise ValueError(f"No raw JSONL files found under {root}")
    return files


def contract_tools():
    """Reuse Trung's source-specific schemas and cross-field checks."""
    contracts_dir = ROOT / "platform" / "contracts"
    sys.path.insert(0, str(contracts_dir))
    from validate import FALLBACK, SCHEMA_BY_SOURCE, cross_checks, load_validators

    return load_validators(), SCHEMA_BY_SOURCE, FALLBACK, cross_checks


def read_lines(path: Path):
    """Đọc từng dòng; file .gz producer đang ghi dở (chưa có trailer) thì dừng ở phần đã flush.
    File hỏng giữa chừng (tiến trình ghi bị giết) cũng chỉ đọc tới chỗ hỏng, không làm crash."""
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as stream:
        try:
            yield from stream
        except EOFError:
            return
        except (gzip.BadGzipFile, zlib.error, UnicodeDecodeError) as exc:
            print(f"[collector] {path}: dừng đọc ở đoạn hỏng ({exc})", file=sys.stderr)
            return


def first_valid_trades(root: Path) -> tuple[dict[str, tuple[datetime, dict]], Counter]:
    return first_valid_trades_from(raw_files(root))


def first_valid_trades_from(files) -> tuple[dict[str, tuple[datetime, dict]], Counter]:
    validators, schema_by_source, fallback, cross_checks = contract_tools()
    first: dict[str, tuple[datetime, dict]] = {}
    stats: Counter = Counter()

    for path in files:
        for line_number, line in enumerate(read_lines(path), 1):
            if not line.strip():
                continue
            stats["records_read"] += 1
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                stats["invalid_records"] += 1
                continue

            if not isinstance(record, dict):
                stats["invalid_records"] += 1
                continue
            schema = schema_by_source.get(record.get("source"), fallback)
            if not validators[schema].is_valid(record) or cross_checks(record):
                stats["invalid_records"] += 1
                continue

            try:
                received = utc_time(record["ingest_time"])
            except ValueError as exc:
                raise ValueError(f"{path}:{line_number}: {exc}") from exc

            stats["valid_records"] += 1
            event_id = record["event_id"]
            previous = first.get(event_id)
            if previous is None or received < previous[0]:
                first[event_id] = (received, record)

    stats["duplicate_records"] = stats["valid_records"] - len(first)
    return first, stats


def observations(
    first: dict[str, tuple[datetime, dict]],
    window_end: datetime | None = None,
) -> list[dict]:
    counts: Counter = Counter()
    for received, record in first.values():
        if record["transport"].get("is_snapshot", False):
            continue
        start = received.replace(second=0, microsecond=0)
        end = start + timedelta(minutes=1)
        if window_end is not None and end != window_end:
            continue
        counts[(record["source"], record["symbol_canonical"], start)] += 1

    result = []
    for (source, symbol, start), count in sorted(counts.items()):
        end = start + timedelta(minutes=1)
        result.append(
            {
                "metric_name": METRIC_NAME,
                "dimensions": {"source": source, "symbol_canonical": symbol},
                "observed_at": iso_utc(end),
                "window_start": iso_utc(start),
                "window_end": iso_utc(end),
                "value": count,
                "status": "ok",
                "sample_count": count,
            }
        )
    return result


def write_to_store(rows: list[dict], dsn: str) -> None:
    try:
        import psycopg
        from psycopg.types.json import Jsonb
    except ImportError as exc:
        raise SystemExit(
            "PostgreSQL output requires psycopg: install requirements-observability.txt"
        ) from exc

    # Ép kiểu tường minh: psycopg gửi float là double precision, mà PostgreSQL không
    # tự ép double -> numeric khi chọn hàm, nên metric có giá trị thực sẽ không ghi được.
    statement = """
        SELECT upsert_metric_observation(
            %s::text, %s::jsonb, %s::timestamptz, %s::timestamptz, %s::timestamptz,
            %s::numeric, %s::text, %s::bigint
        )
    """
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            for row in rows:
                cursor.execute(
                    statement,
                    (
                        row["metric_name"],
                        Jsonb(row["dimensions"]),
                        utc_time(row["observed_at"]),
                        utc_time(row["window_start"]) if row.get("window_start") else None,
                        utc_time(row["window_end"]) if row.get("window_end") else None,
                        row["value"],
                        row["status"],
                        row["sample_count"],
                    ),
                )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--in", dest="input_path", default="data/raw")
    parser.add_argument(
        "--window-end",
        help="Only emit the UTC minute ending at this ISO timestamp",
    )
    parser.add_argument(
        "--write-db",
        action="store_true",
        help="Write observations to PostgreSQL using FINOBS_METRIC_DSN",
    )
    args = parser.parse_args()

    end = utc_time(args.window_end) if args.window_end else None
    if end is not None and (end.second or end.microsecond):
        parser.error("--window-end must be on an exact minute boundary")

    first, stats = first_valid_trades(Path(args.input_path))
    rows = observations(first, end)
    if args.write_db:
        dsn = os.environ.get("FINOBS_METRIC_DSN")
        if not dsn:
            parser.error("--write-db requires FINOBS_METRIC_DSN")
        write_to_store(rows, dsn)
        print(f"Stored {len(rows)} observations", file=sys.stderr)
    else:
        for row in rows:
            print(json.dumps(row, ensure_ascii=False))

    print(
        f"read={stats['records_read']} valid={stats['valid_records']} "
        f"invalid={stats['invalid_records']} "
        f"duplicates={stats['duplicate_records']} observations={len(rows)}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
