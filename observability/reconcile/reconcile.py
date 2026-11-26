"""Đối soát D+1 giữa stream bronze và file chính thức Binance (DE-B, Sprint 4).

    python observability/reconcile/reconcile.py --date 2026-11-27
    python observability/reconcile/reconcile.py --date 2026-11-26 --from 08:00 --to 23:59
    python observability/reconcile/reconcile.py --date 2026-11-27 --symbols BTCUSDT,ETHUSDT --dry-run

Với mỗi mã Binance trong config/symbols.json (hoặc --symbols):
  1. Lấy trade từ bronze.trades_raw (stream) cho ngày đó
  2. Lấy trade từ bronze.binance_official_trades (file chính thức) cho ngày đó
  3. So khớp theo trade_id: missing, extra, value_mismatch
  4. Kết hợp với gap detection (DE-A) để tính detection_coverage
  5. Xuất báo cáo Markdown + JSON vào reports/reconcile/

Dung sai (tolerance) cấu hình qua config/reconcile.yaml, không hardcode.

Lưu ý: Coinbase không có file chính thức D+1 — Coinbase chuyển dịch sang
đối soát thời gian thực (real-time reconciliation) và kiểm soát chất lượng
ngay tại nguồn bằng Data Contract. Gap detection (DE-A) vẫn áp dụng cho cả hai sàn.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from observability.reconcile.gap_detection import (  # noqa: E402
    GapReport,
    analyze_trade_id_sequence,
    gap_report_to_metrics,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DEFAULT_TOLERANCES = {
    "price_tolerance": Decimal("0"),
    "qty_tolerance": Decimal("0"),
    "time_tolerance_ms": 1000,
}


def load_tolerances(path: Path | None = None) -> dict:
    if path and path.is_file():
        import yaml

        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        return {
            "price_tolerance": Decimal(str(raw.get("price_tolerance", 0))),
            "qty_tolerance": Decimal(str(raw.get("qty_tolerance", 0))),
            "time_tolerance_ms": int(raw.get("time_tolerance_ms", 1000)),
        }
    return dict(DEFAULT_TOLERANCES)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ValueMismatch:
    trade_id: int
    field: str
    stream_value: str
    official_value: str

    def as_dict(self) -> dict:
        return {
            "trade_id": self.trade_id,
            "field": self.field,
            "stream_value": self.stream_value,
            "official_value": self.official_value,
        }


@dataclass
class SymbolResult:
    symbol: str
    status: str  # OK, MISSING_IN_STREAM, EXTRA_IN_STREAM, VALUE_MISMATCH, OFFICIAL_NOT_AVAILABLE
    official_count: int = 0
    stream_count: int = 0
    matched: int = 0
    missing_in_stream: list[int] = field(default_factory=list)
    extra_in_stream: list[int] = field(default_factory=list)
    value_mismatches: list[ValueMismatch] = field(default_factory=list)
    gap_report: GapReport | None = None
    detection_coverage: float | None = None  # % missing đã phát hiện bởi gap detection

    @property
    def match_ratio(self) -> float:
        if self.official_count == 0:
            return 1.0 if self.stream_count == 0 else 0.0
        return self.matched / self.official_count

    def as_dict(self, max_items: int = 100) -> dict:
        missing = self.missing_in_stream[:max_items]
        if len(self.missing_in_stream) > max_items:
            missing.append(-1)
        extra = self.extra_in_stream[:max_items]
        if len(self.extra_in_stream) > max_items:
            extra.append(-1)
        mismatches = [m.as_dict() for m in self.value_mismatches[:max_items]]
        return {
            "symbol": self.symbol,
            "status": self.status,
            "official_count": self.official_count,
            "stream_count": self.stream_count,
            "matched": self.matched,
            "match_ratio": round(self.match_ratio, 6),
            "missing_in_stream_count": len(self.missing_in_stream),
            "missing_in_stream": missing,
            "extra_in_stream_count": len(self.extra_in_stream),
            "extra_in_stream": extra,
            "value_mismatch_count": len(self.value_mismatches),
            "value_mismatches": mismatches,
            "detection_coverage": self.detection_coverage,
            "gap_report": self.gap_report.as_dict() if self.gap_report else None,
        }


@dataclass
class ReconcileReport:
    report_date: str  # YYYY-MM-DD
    run_at: str
    code_version: str
    is_partial_day: bool
    window_from: str | None
    window_to: str | None
    symbols: list[SymbolResult] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "report_date": self.report_date,
            "run_at": self.run_at,
            "code_version": self.code_version,
            "is_partial_day": self.is_partial_day,
            "window_from": self.window_from,
            "window_to": self.window_to,
            "symbols": [s.as_dict() for s in self.symbols],
        }


# ---------------------------------------------------------------------------
# Core reconciliation logic (pure, testable)
# ---------------------------------------------------------------------------

def reconcile_symbol(
    stream_trades: list[dict],
    official_trades: list[dict],
    symbol: str,
    tolerances: dict | None = None,
) -> SymbolResult:
    """So khớp dữ liệu stream với file chính thức cho một mã.

    Parameters
    ----------
    stream_trades:
        Mỗi phần tử là dict với ít nhất: trade_id (int), price (Decimal),
        qty (Decimal), event_ts_ms (int, epoch ms).
    official_trades:
        Mỗi phần tử: trade_id (int), price (Decimal), qty (Decimal),
        trade_ts_ms (int, epoch ms).
    symbol:
        Mã gốc (symbol_native).
    tolerances:
        price_tolerance, qty_tolerance (Decimal), time_tolerance_ms (int).

    Returns
    -------
    SymbolResult
    """
    if not official_trades:
        return SymbolResult(
            symbol=symbol,
            status="OFFICIAL_NOT_AVAILABLE",
            stream_count=len(stream_trades),
        )

    tol = tolerances or DEFAULT_TOLERANCES

    stream_by_id: dict[int, dict] = {}
    for t in stream_trades:
        tid = t["trade_id"]
        if tid not in stream_by_id:
            stream_by_id[tid] = t

    official_by_id: dict[int, dict] = {}
    for t in official_trades:
        tid = t["trade_id"]
        if tid not in official_by_id:
            official_by_id[tid] = t

    stream_ids = set(stream_by_id.keys())
    official_ids = set(official_by_id.keys())

    missing_in_stream = sorted(official_ids - stream_ids)
    extra_in_stream = sorted(stream_ids - official_ids)
    common_ids = stream_ids & official_ids

    mismatches: list[ValueMismatch] = []
    matched = 0
    for tid in sorted(common_ids):
        st = stream_by_id[tid]
        of = official_by_id[tid]

        has_mismatch = False

        if abs(st["price"] - of["price"]) > tol["price_tolerance"]:
            mismatches.append(ValueMismatch(tid, "price", str(st["price"]), str(of["price"])))
            has_mismatch = True

        if abs(st["qty"] - of["qty"]) > tol["qty_tolerance"]:
            mismatches.append(ValueMismatch(tid, "qty", str(st["qty"]), str(of["qty"])))
            has_mismatch = True

        time_diff = abs(st["event_ts_ms"] - of["trade_ts_ms"])
        if time_diff > tol["time_tolerance_ms"]:
            mismatches.append(ValueMismatch(tid, "time_ms", str(st["event_ts_ms"]), str(of["trade_ts_ms"])))
            has_mismatch = True

        if not has_mismatch:
            matched += 1

    # Determine status
    if missing_in_stream and mismatches:
        status = "MISSING_IN_STREAM"  # primary issue
    elif missing_in_stream:
        status = "MISSING_IN_STREAM"
    elif extra_in_stream:
        status = "EXTRA_IN_STREAM"
    elif mismatches:
        status = "VALUE_MISMATCH"
    else:
        status = "OK"

    # Gap detection on stream trades
    stream_id_list = [t["trade_id"] for t in stream_trades]
    gap_report = analyze_trade_id_sequence(stream_id_list, source="binance", symbol=symbol)

    # Detection coverage: how many missing trades were caught by gap detection?
    if missing_in_stream and gap_report.gap_missing_total > 0:
        gap_id_set: set[int] = set()
        for gap in gap_report.gaps:
            gap_id_set.update(range(gap.start, gap.end + 1))
        caught = len(set(missing_in_stream) & gap_id_set)
        coverage = caught / len(missing_in_stream)
    elif not missing_in_stream:
        coverage = 1.0  # no missing = perfect coverage
    else:
        coverage = 0.0

    return SymbolResult(
        symbol=symbol,
        status=status,
        official_count=len(official_by_id),
        stream_count=len(stream_by_id),
        matched=matched,
        missing_in_stream=missing_in_stream,
        extra_in_stream=extra_in_stream,
        value_mismatches=mismatches,
        gap_report=gap_report,
        detection_coverage=round(coverage, 4),
    )


# ---------------------------------------------------------------------------
# Metric helpers
# ---------------------------------------------------------------------------

def reconcile_result_to_metrics(result: SymbolResult, observed_at: str,
                                report_date: str) -> list[dict]:
    dims = {"source": "binance", "symbol": result.symbol, "date": report_date}
    if result.status == "OFFICIAL_NOT_AVAILABLE":
        return [{
            "metric_name": "reconciliation_match_ratio",
            "dimensions": dims, "observed_at": observed_at,
            "window_start": None, "window_end": None,
            "value": None, "status": "no_data", "sample_count": None,
        }]
    base = {
        "dimensions": dims, "observed_at": observed_at,
        "window_start": None, "window_end": None, "status": "ok",
    }
    return [
        {**base, "metric_name": "reconciliation_match_ratio",
         "value": round(result.match_ratio, 6), "sample_count": result.official_count},
        {**base, "metric_name": "reconciliation_missing_trades",
         "value": len(result.missing_in_stream), "sample_count": result.official_count},
        {**base, "metric_name": "reconciliation_extra_trades",
         "value": len(result.extra_in_stream), "sample_count": result.stream_count},
        {**base, "metric_name": "reconciliation_value_mismatch",
         "value": len(result.value_mismatches), "sample_count": result.matched + len(result.value_mismatches)},
    ]


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def generate_markdown(report: ReconcileReport) -> str:
    lines = [
        f"# Đối soát {report.report_date}",
        "",
    ]
    if report.is_partial_day:
        lines.append(f"> **Ngày chưa đầy đủ** — cửa sổ: {report.window_from} → {report.window_to}")
        lines.append("")

    lines.append("| Mã | Official | Stream | Missing | Extra | Mismatch | Match% | Status |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for s in report.symbols:
        pct = f"{s.match_ratio * 100:.3f}%" if s.status != "OFFICIAL_NOT_AVAILABLE" else "—"
        lines.append(
            f"| {s.symbol} | {s.official_count} | {s.stream_count} | "
            f"{len(s.missing_in_stream)} | {len(s.extra_in_stream)} | "
            f"{len(s.value_mismatches)} | {pct} | {s.status} |"
        )

    lines += [
        "",
        "## Thông tin chạy",
        "",
        f"- **Ngày báo cáo**: {report.report_date}",
        f"- **Chạy lúc**: {report.run_at}",
        f"- **Phiên bản code**: {report.code_version}",
        f"- **Ngày đầy đủ**: {'không' if report.is_partial_day else 'có'}",
    ]

    # Gap summary
    for s in report.symbols:
        if s.gap_report and s.gap_report.gap_count > 0:
            lines += [
                "",
                f"### Gap detection — {s.symbol}",
                "",
                f"- Khoảng hở: {s.gap_report.gap_count}",
                f"- Tổng ID thiếu: {s.gap_report.gap_missing_total}",
                f"- Trùng: {s.gap_report.duplicate_count}",
                f"- Đảo thứ tự: {s.gap_report.out_of_order_count}",
                f"- Độ phủ phát hiện: {s.detection_coverage:.1%}" if s.detection_coverage is not None else "",
            ]

    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Trino helpers (for VM / Docker usage)
# ---------------------------------------------------------------------------

def _parse_event_id_sequence(event_id: str) -> int:
    """Parse ``<source>:<symbol>:<sequence>`` → sequence (int)."""
    return int(event_id.rsplit(":", 1)[-1])


def _epoch_ms_from_iso(ts_str: str) -> int:
    """ISO timestamp → epoch milliseconds."""
    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


def parse_stream_trade(row: dict) -> dict:
    """Chuyển dòng từ bronze.trades_raw sang format reconcile cần.

    Nhận dict với các cột: event_id, payload (JSON string), event_ts (ISO string).
    Parse payload để lấy price, qty. Parse event_id để lấy trade_id.
    """
    trade_id = _parse_event_id_sequence(row["event_id"])
    # payload là JSON string chứa envelope → bên trong có payload.p, payload.q (Binance)
    payload = json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
    # payload trong bronze.trades_raw là envelope JSON nguyên bản producer gửi
    inner = payload.get("payload", payload)
    return {
        "trade_id": trade_id,
        "price": Decimal(str(inner.get("p", inner.get("price", "0")))),
        "qty": Decimal(str(inner.get("q", inner.get("qty", "0")))),
        "event_ts_ms": _epoch_ms_from_iso(row.get("event_ts", payload.get("event_time", "1970-01-01T00:00:00Z"))),
    }


def parse_official_trade(row: dict) -> dict:
    """Chuyển dòng từ bronze.binance_official_trades sang format reconcile cần."""
    return {
        "trade_id": int(row["trade_id"]),
        "price": Decimal(str(row["price"])),
        "qty": Decimal(str(row["qty"])),
        "trade_ts_ms": _epoch_ms_from_iso(row["trade_ts"]) if isinstance(row["trade_ts"], str) else int(row["trade_ts"]),
    }


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def _get_code_version() -> str:
    """Lấy commit hash ngắn, nếu không được thì trả 'unknown'."""
    import subprocess

    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(ROOT), stderr=subprocess.DEVNULL, text=True,
        ).strip()
    except Exception:
        return "unknown"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--date", required=True, help="ngày đối soát, YYYY-MM-DD")
    parser.add_argument("--symbols", default="",
                        help="danh sách mã cách phẩy; bỏ trống = lấy từ config/symbols.json")
    parser.add_argument("--from", dest="from_time", default=None, help="HH:MM — giới hạn cửa sổ bắt đầu")
    parser.add_argument("--to", dest="to_time", default=None, help="HH:MM — giới hạn cửa sổ kết thúc")
    parser.add_argument("--config", default=str(ROOT / "config" / "reconcile.yaml"))
    parser.add_argument("--out", default=str(ROOT / "reports" / "reconcile"))
    parser.add_argument("--dry-run", action="store_true", help="in JSON ra stdout, không ghi file")
    parser.add_argument("--fixture-dir", default="", help="đọc dữ liệu giả từ thư mục thay vì Trino")
    args = parser.parse_args()

    report_date = date.fromisoformat(args.date)
    tolerances = load_tolerances(Path(args.config) if Path(args.config).is_file() else None)

    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    else:
        sym_path = ROOT / "config" / "symbols.json"
        symbols = json.loads(sym_path.read_text(encoding="utf-8"))["binance"]

    is_partial = args.from_time is not None or args.to_time is not None
    now = datetime.now(timezone.utc)

    report = ReconcileReport(
        report_date=report_date.isoformat(),
        run_at=now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        code_version=_get_code_version(),
        is_partial_day=is_partial,
        window_from=args.from_time,
        window_to=args.to_time,
    )

    if args.fixture_dir:
        # Test mode: đọc từ file JSON trong fixture_dir
        fixture = Path(args.fixture_dir)
        for symbol in symbols:
            stream_file = fixture / f"stream_{symbol.lower()}.json"
            official_file = fixture / f"official_{symbol.lower()}.json"
            stream_trades = json.loads(stream_file.read_text(encoding="utf-8")) if stream_file.is_file() else []
            official_trades = json.loads(official_file.read_text(encoding="utf-8")) if official_file.is_file() else []
            # Convert Decimal strings
            for t in stream_trades:
                t["price"] = Decimal(str(t["price"]))
                t["qty"] = Decimal(str(t["qty"]))
            for t in official_trades:
                t["price"] = Decimal(str(t["price"]))
                t["qty"] = Decimal(str(t["qty"]))
            result = reconcile_symbol(stream_trades, official_trades, symbol, tolerances)
            report.symbols.append(result)
    else:
        print("Chế độ Trino: chưa hỗ trợ trong CLI trực tiếp — dùng --fixture-dir hoặc DAG Airflow.",
              file=sys.stderr)
        raise SystemExit(1)

    if args.dry_run:
        print(json.dumps(report.as_dict(), ensure_ascii=False, indent=2, default=str))
        return

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    json_path = out_dir / f"{report_date.isoformat()}.json"
    json_path.write_text(json.dumps(report.as_dict(), ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    md_path = out_dir / f"{report_date.isoformat()}.md"
    md_path.write_text(generate_markdown(report), encoding="utf-8")

    print(f"Báo cáo: {md_path}  |  {json_path}")
    for s in report.symbols:
        print(f"  {s.symbol}: {s.status} match={s.match_ratio:.4%} "
              f"missing={len(s.missing_in_stream)} extra={len(s.extra_in_stream)} mismatch={len(s.value_mismatches)}")


if __name__ == "__main__":
    main()
