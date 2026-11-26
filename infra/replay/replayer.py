"""Công cụ phát lại dữ liệu lịch sử tốc độ cao (PL-A, Sprint 5, FR-23).

Đọc dữ liệu từ file raw (`make record` / `tests/fixtures/raw`) hoặc CSV chính thức
Binance Public Data, rồi phát lại theo thứ tự `(event_time, sequence)` ở tốc độ
1x, 10x, 100x hoặc `max`.

An toàn cách ly:
  - Mặc định `dry_run=True` và `namespace="replay"`.
  - TỪ CHỐI chạy nếu namespace đích trùng hoặc bắt đầu bằng namespace thật
    (`raw`, `bronze`, `silver`, `gold`, `live`, `prod`, `finobs`).
"""

from __future__ import annotations

import argparse
import copy
import csv
import gzip
import io
import json
import os
import random
import sys
import time
import tracemalloc
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Protocol

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

FORBIDDEN_NAMESPACES = ("raw", "bronze", "silver", "gold", "live", "prod", "finobs")
MICROS_THRESHOLD = 10**14


class ReplayNamespaceError(ValueError):
    """Ném khi namespace đích trùng với namespace thu dữ liệu thật."""


def validate_namespace(namespace: str) -> str:
    ns = (namespace or "").strip().lower().rstrip(".")
    if not ns:
        raise ReplayNamespaceError("Namespace không được để trống.")
    for bad in FORBIDDEN_NAMESPACES:
        if ns == bad or ns.startswith(f"{bad}.") or ns.startswith(f"{bad}_"):
            raise ReplayNamespaceError(
                f"Từ chối chạy: namespace '{namespace}' trùng hoặc chạm tới namespace thật '{bad}'."
            )
    return ns


def topic_for_replay(namespace: str, source: str) -> str:
    ns = validate_namespace(namespace)
    return f"{ns}.{source}.trade"


def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


def _iso_ms(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


class ReplaySink(Protocol):
    def write(self, record: dict, topic: str) -> None: ...
    def close(self) -> None: ...


class DryRunSink:
    """Sink mặc định: đếm số sự kiện và ghi nhận topic đích, không gửi ra mạng."""

    def __init__(self, capture: bool = False):
        self.count = 0
        self.topics: set[str] = set()
        self.records: list[dict] = [] if capture else []
        self._capture = capture

    def write(self, record: dict, topic: str) -> None:
        self.count += 1
        self.topics.add(topic)
        if self._capture:
            self.records.append(record)

    def close(self) -> None:
        pass


class JsonlSink:
    """Ghi sự kiện phát lại ra file JSONL cục bộ để OB/EV đọc trực tiếp."""

    def __init__(self, out_path: Path):
        self.out_path = Path(out_path)
        self.out_path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.out_path.open("w", encoding="utf-8")
        self.count = 0

    def write(self, record: dict, topic: str) -> None:
        self._fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        self.count += 1

    def close(self) -> None:
        self._fh.close()


@dataclass
class ReplayConfig:
    namespace: str = "replay"
    speed: float = 1.0  # 1.0, 10.0, 100.0; <= 0 nghĩa là 'max'
    from_ts: str | None = None
    to_ts: str | None = None
    symbols: list[str] | None = None
    num_symbols: int | None = None
    seed: int = 42
    dry_run: bool = True
    checkpoint_path: Path | None = None
    resume: bool = False


@dataclass
class ReplayMetrics:
    emitted_events: int
    total_events: int
    elapsed_seconds: float
    throughput_eps: float
    avg_detect_latency_ms: float
    p95_detect_latency_ms: float
    cpu_seconds: float
    peak_ram_mb: float
    progress_pct: float
    synthetic_symbols: int = 0
    paused: bool = False

    def as_dict(self) -> dict:
        return {
            "emitted_events": self.emitted_events,
            "total_events": self.total_events,
            "elapsed_seconds": round(self.elapsed_seconds, 4),
            "throughput_eps": round(self.throughput_eps, 2),
            "avg_detect_latency_ms": round(self.avg_detect_latency_ms, 3),
            "p95_detect_latency_ms": round(self.p95_detect_latency_ms, 3),
            "cpu_seconds": round(self.cpu_seconds, 4),
            "peak_ram_mb": round(self.peak_ram_mb, 3),
            "progress_pct": round(self.progress_pct, 2),
            "synthetic_symbols": self.synthetic_symbols,
            "paused": self.paused,
        }


@dataclass
class ReplayResult:
    namespace: str
    target_topics: list[str]
    dry_run: bool
    seed: int
    metrics: ReplayMetrics


def load_raw_records(input_path: Path) -> list[dict]:
    """Đọc mọi file .jsonl / .jsonl.gz trong input_path, giữ nguyên định dạng raw_event."""
    path = Path(input_path)
    if path.is_file():
        files = [path]
    else:
        files = sorted(path.rglob("*.jsonl")) + sorted(path.rglob("*.jsonl.gz"))
        files = [p for p in files if "_ops" not in p.parts]

    records: list[dict] = []
    for f in files:
        opener = gzip.open if f.suffix == ".gz" else open
        with opener(f, "rt", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                if isinstance(rec, dict) and "event_time" in rec:
                    records.append(rec)
    return records


def load_official_csv_records(csv_or_zip_path: Path, symbol: str = "BTCUSDT") -> list[dict]:
    """Chuyển đổi CSV/ZIP chính thức của Binance sang envelope `raw_event` 1.0.0."""
    path = Path(csv_or_zip_path)
    rows: list[list[str]] = []
    if path.suffix == ".zip":
        with zipfile.ZipFile(path, "r") as zf:
            csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
            for name in csv_names:
                with zf.open(name) as raw_f:
                    reader = csv.reader(io.TextIOWrapper(raw_f, encoding="utf-8"))
                    rows.extend(reader)
    else:
        with path.open("rt", encoding="utf-8") as fh:
            rows.extend(csv.reader(fh))

    sym_upper = symbol.upper()
    canonical = f"{sym_upper[:-4]}-{sym_upper[-4:]}" if sym_upper.endswith("USDT") else sym_upper
    records: list[dict] = []
    for row in rows:
        if not row or not row[0].isdigit():
            continue
        trade_id = int(row[0])
        price = row[1]
        qty = row[2]
        raw_time = int(row[4])
        epoch_ms = raw_time // 1000 if raw_time >= MICROS_THRESHOLD else raw_time
        dt = datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)
        ts_iso = _iso_ms(dt)
        is_buyer_maker = row[5].strip().lower() == "true"
        is_best_match = row[6].strip().lower() == "true" if len(row) > 6 else True
        records.append({
            "schema_version": "1.0.0",
            "event_id": f"binance:{sym_upper}:{trade_id}",
            "source": "binance",
            "symbol_native": sym_upper,
            "symbol_canonical": canonical,
            "event_type": "trade",
            "event_time": ts_iso,
            "ingest_time": ts_iso,
            "sequence": trade_id,
            "transport": {"batch_size": 1, "raw_bytes": 160, "msg_seq": None, "is_snapshot": False},
            "payload": {
                "e": "trade",
                "E": epoch_ms,
                "s": sym_upper,
                "t": trade_id,
                "p": price,
                "q": qty,
                "T": epoch_ms,
                "m": is_buyer_maker,
                "M": is_best_match,
            },
        })
    return records


def _expand_symbols(
    records: list[dict],
    num_symbols: int | None,
    seed: int,
) -> tuple[list[dict], int]:
    """Nhân bản thêm mã tổng hợp có tên rõ ràng (`SYNTH001-USDT`) nếu cần scale 10/100/500 mã."""
    if not num_symbols or not records:
        return records, 0

    existing = sorted({r["symbol_native"] for r in records})
    if len(existing) >= num_symbols:
        keep = set(existing[:num_symbols])
        return [r for r in records if r["symbol_native"] in keep], 0

    rng = random.Random(seed)
    expanded = list(records)
    synth_count = num_symbols - len(existing)
    for i in range(1, synth_count + 1):
        template = copy.deepcopy(records[rng.randrange(len(records))])
        src = template["source"]
        synth_native = f"SYNTH{i:03d}USDT" if src == "binance" else f"SYNTH{i:03d}-USD"
        synth_canon = f"SYNTH{i:03d}-USDT" if src == "binance" else f"SYNTH{i:03d}-USD"
        # Nhân bản các bản ghi của cùng mã mẫu theo seed cố định
        base_sym = existing[(i - 1) % len(existing)]
        for orig in records:
            if orig["symbol_native"] != base_sym:
                continue
            clone = copy.deepcopy(orig)
            clone["symbol_native"] = synth_native
            clone["symbol_canonical"] = synth_canon
            clone["event_id"] = f"{src}:{synth_native}:{clone['sequence']}"
            clone.setdefault("transport", {})["is_synthetic"] = True
            expanded.append(clone)
    return expanded, synth_count


def _load_checkpoint(path: Path | None) -> int:
    if path and path.is_file():
        data = json.loads(path.read_text(encoding="utf-8"))
        return int(data.get("last_index", 0))
    return 0


def _save_checkpoint(path: Path | None, last_index: int, seed: int) -> None:
    if path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"last_index": last_index, "seed": seed}), encoding="utf-8")


def replay_records(
    records: Iterable[dict],
    config: ReplayConfig,
    sink: ReplaySink | None = None,
    *,
    stop_after: int | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> ReplayResult:
    """Phát lại danh sách bản ghi theo thứ tự `(event_time, sequence)` vào `sink`."""
    ns = validate_namespace(config.namespace)
    active_sink = sink if sink is not None else DryRunSink()

    # Lọc thời gian và mã
    filtered: list[dict] = []
    sym_filter = set(config.symbols) if config.symbols else None
    for rec in records:
        ts = rec.get("event_time", "")
        if config.from_ts and ts < config.from_ts:
            continue
        if config.to_ts and ts > config.to_ts:
            continue
        if sym_filter and rec.get("symbol_native") not in sym_filter and rec.get("symbol_canonical") not in sym_filter:
            continue
        filtered.append(rec)

    expanded, synth_count = _expand_symbols(filtered, config.num_symbols, config.seed)
    ordered = sorted(expanded, key=lambda r: (r["event_time"], int(r.get("sequence", 0))))

    total = len(ordered)
    start_idx = _load_checkpoint(config.checkpoint_path) if config.resume else 0
    start_idx = max(0, min(start_idx, total))

    tracemalloc.start()
    t0 = time.perf_counter()
    cpu0 = time.process_time()

    latencies_ms: list[float] = []
    topics: set[str] = set()
    emitted = 0
    paused = False
    prev_event_dt: datetime | None = None

    for idx in range(start_idx, total):
        if stop_after is not None and emitted >= stop_after:
            paused = True
            _save_checkpoint(config.checkpoint_path, idx, config.seed)
            break

        rec = copy.deepcopy(ordered[idx])
        cur_dt = _parse_iso(rec["event_time"])

        if prev_event_dt is not None and config.speed > 0:
            delta_s = (cur_dt - prev_event_dt).total_seconds()
            if delta_s > 0:
                sleep_fn(delta_s / config.speed)
        prev_event_dt = cur_dt

        emit_start = time.perf_counter()
        topic = topic_for_replay(ns, rec["source"])
        topics.add(topic)
        active_sink.write(rec, topic)
        emit_ms = (time.perf_counter() - emit_start) * 1000.0
        latencies_ms.append(emit_ms)
        emitted += 1

    if not paused and config.checkpoint_path:
        _save_checkpoint(config.checkpoint_path, total, config.seed)

    elapsed = max(1e-9, time.perf_counter() - t0)
    cpu_used = max(0.0, time.process_time() - cpu0)
    _, peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    latencies_sorted = sorted(latencies_ms)
    p95_lat = latencies_sorted[int(0.95 * (len(latencies_sorted) - 1))] if latencies_sorted else 0.0
    avg_lat = sum(latencies_sorted) / len(latencies_sorted) if latencies_sorted else 0.0
    progress = ((start_idx + emitted) / total * 100.0) if total > 0 else 100.0

    metrics = ReplayMetrics(
        emitted_events=emitted,
        total_events=total,
        elapsed_seconds=elapsed,
        throughput_eps=emitted / elapsed,
        avg_detect_latency_ms=avg_lat,
        p95_detect_latency_ms=p95_lat,
        cpu_seconds=cpu_used,
        peak_ram_mb=peak_bytes / (1024 * 1024),
        progress_pct=progress,
        synthetic_symbols=synth_count,
        paused=paused,
    )
    return ReplayResult(
        namespace=ns,
        target_topics=sorted(topics),
        dry_run=config.dry_run,
        seed=config.seed,
        metrics=metrics,
    )


def _parse_speed(raw: str) -> float:
    val = raw.strip().lower().rstrip("x")
    if val == "max":
        return 0.0
    return float(val)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FinObs High-Speed Historical Replayer (PL-A)")
    parser.add_argument("--in", dest="input_path", default="tests/fixtures/raw", help="Thư mục/file raw hoặc CSV/ZIP")
    parser.add_argument("--official-csv", action="store_true", help="Đọc đầu vào dạng Binance official CSV/ZIP")
    parser.add_argument("--symbol-for-csv", default="BTCUSDT", help="Mã dùng khi đọc official CSV")
    parser.add_argument("--namespace", default="replay", help="Tiền tố namespace đích (mặc định: replay)")
    parser.add_argument("--speed", default="max", help="Tốc độ phát lại: 1x, 10x, 100x, max")
    parser.add_argument("--from", dest="from_ts", default=None, help="Mốc bắt đầu ISO UTC")
    parser.add_argument("--to", dest="to_ts", default=None, help="Mốc kết thúc ISO UTC")
    parser.add_argument("--symbols", default=None, help="Danh sách mã cách nhau bằng dấu phẩy")
    parser.add_argument("--num-symbols", type=int, default=None, help="Mở rộng/giới hạn số mã (10, 100, 500)")
    parser.add_argument("--seed", type=int, default=42, help="Seed tái lập")
    parser.add_argument("--out", default=None, help="Ghi ra file JSONL cục bộ thay vì dry-run")
    parser.add_argument("--checkpoint", default=None, help="File checkpoint để pause/resume")
    parser.add_argument("--resume", action="store_true", help="Tiếp tục từ checkpoint")
    parser.add_argument("--stop-after", type=int, default=None, help="Dừng sau N sự kiện (test pause)")
    args = parser.parse_args(argv)

    inp = Path(args.input_path)
    records = (
        load_official_csv_records(inp, symbol=args.symbol_for_csv)
        if args.official_csv
        else load_raw_records(inp)
    )

    cfg = ReplayConfig(
        namespace=args.namespace,
        speed=_parse_speed(args.speed),
        from_ts=args.from_ts,
        to_ts=args.to_ts,
        symbols=[s.strip() for s in args.symbols.split(",")] if args.symbols else None,
        num_symbols=args.num_symbols,
        seed=args.seed,
        dry_run=(args.out is None),
        checkpoint_path=Path(args.checkpoint) if args.checkpoint else None,
        resume=args.resume,
    )
    sink: ReplaySink = JsonlSink(Path(args.out)) if args.out else DryRunSink()
    try:
        res = replay_records(records, cfg, sink=sink, stop_after=args.stop_after)
    finally:
        sink.close()

    print(json.dumps({
        "namespace": res.namespace,
        "target_topics": res.target_topics,
        "dry_run": res.dry_run,
        "seed": res.seed,
        "metrics": res.metrics.as_dict(),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
