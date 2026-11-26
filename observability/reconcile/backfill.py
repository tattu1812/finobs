"""Chạy bù (backfill) đối soát D+1 nhiều ngày, idempotent (DE-B, Sprint 5).

    python observability/reconcile/backfill.py --from-date 2026-11-27 --to-date 2026-11-29 --fixture-dir tests/fixtures/reconcile
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from observability.reconcile.reconcile import (  # noqa: E402
    ReconcileReport,
    SymbolResult,
    generate_markdown,
    load_tolerances,
    reconcile_symbol,
)
from decimal import Decimal


def _load_from_fixtures(fixture_dir: Path, symbol: str) -> tuple[list[dict], list[dict]]:
    """Load pre-parsed fixture JSON files. Fixtures already have trade_id, price, qty, event_ts_ms/trade_ts_ms."""
    sym_low = symbol.lower()
    stream_path = fixture_dir / f"stream_{sym_low}.json"
    official_path = fixture_dir / f"official_{sym_low}.json"

    def _convert(trades: list[dict]) -> list[dict]:
        for t in trades:
            t["price"] = Decimal(str(t["price"]))
            t["qty"] = Decimal(str(t["qty"]))
        return trades

    stream_trades = _convert(json.loads(stream_path.read_text(encoding="utf-8"))) if stream_path.is_file() else []
    official_trades = _convert(json.loads(official_path.read_text(encoding="utf-8"))) if official_path.is_file() else []
    return stream_trades, official_trades


def _write_reports(report: ReconcileReport, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"{report.report_date}.json").write_text(
        json.dumps(report.as_dict(), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (out_dir / f"{report.report_date}.md").write_text(generate_markdown(report), encoding="utf-8")


def _iter_dates(start_d: date, end_d: date) -> list[date]:
    if end_d < start_d:
        raise ValueError("to-date phải >= from-date")
    days: list[date] = []
    cur = start_d
    while cur <= end_d:
        days.append(cur)
        cur += timedelta(days=1)
    return days


def run_backfill(
    *,
    from_date: str,
    to_date: str,
    symbols: list[str] | None = None,
    fixture_dir: Path | None = None,
    out_dir: Path = ROOT / "reports" / "reconcile",
    partial_dates: set[str] | None = None,
    dry_run: bool = False,
) -> dict:
    """Chạy đối soát D+1 cho dải ngày [from_date, to_date], ghi báo cáo từng ngày + bảng độ phủ."""
    start_d = date.fromisoformat(from_date)
    end_d = date.fromisoformat(to_date)
    target_symbols = symbols or ["BTCUSDT", "ETHUSDT"]
    partials = partial_dates or set()
    tol = load_tolerances(ROOT / "config" / "reconcile.yaml")

    daily_summaries: list[dict] = []
    total_missing = 0
    total_caught_early = 0

    for d in _iter_dates(start_d, end_d):
        d_str = d.isoformat()
        is_partial = d_str in partials
        results: list[SymbolResult] = []

        for sym in target_symbols:
            if fixture_dir is not None:
                # Cho phép thư mục theo ngày (fixture_dir/YYYY-MM-DD/) hoặc dùng chung fixture_dir
                day_dir = fixture_dir / d_str
                use_dir = day_dir if day_dir.is_dir() else fixture_dir
                stream_trades, official_trades = _load_from_fixtures(use_dir, sym)
            else:
                stream_trades, official_trades = [], []

            if not official_trades:
                results.append(
                    SymbolResult(
                        symbol=sym,
                        status="OFFICIAL_NOT_AVAILABLE",
                        official_count=0,
                        stream_count=len(stream_trades),
                        matched=0,
                    )
                )
                continue

            res = reconcile_symbol(stream_trades, official_trades, sym, tol)
            if is_partial and res.status == "MISSING_IN_STREAM":
                res.status = "INCOMPLETE_DAY"
            results.append(res)

            missing_n = len(res.missing_in_stream)
            total_missing += missing_n
            if missing_n > 0 and res.detection_coverage is not None:
                total_caught_early += int(round(res.detection_coverage * missing_n))

        report = ReconcileReport(
            report_date=d_str,
            run_at=__import__("datetime").datetime.now(__import__("datetime").timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            code_version="sprint5-de",
            is_partial_day=is_partial,
            window_from="partial" if is_partial else "00:00",
            window_to="23:59",
            symbols=results,
        )
        if not dry_run:
            _write_reports(report, out_dir)

        statuses = [r.status for r in results]
        avg_cov = sum(r.detection_coverage or 0.0 for r in results) / len(results) if results else 1.0
        daily_summaries.append({
            "date": d_str,
            "is_partial_day": is_partial,
            "statuses": {r.symbol: r.status for r in results},
            "avg_detection_coverage": round(avg_cov, 4),
            "has_report": True,
            "overall_status": (
                "INCOMPLETE_DAY"
                if is_partial
                else ("OFFICIAL_NOT_AVAILABLE" if all(s == "OFFICIAL_NOT_AVAILABLE" for s in statuses) else "DONE")
            ),
        })

    overall_cov = (total_caught_early / total_missing) if total_missing > 0 else 1.0
    summary = {
        "from_date": from_date,
        "to_date": to_date,
        "days_total": len(daily_summaries),
        "days_with_report": len(daily_summaries),
        "report_rate": 1.0 if daily_summaries else 0.0,
        "overall_detection_coverage": round(overall_cov, 4),
        "is_preliminary": fixture_dir is not None,
        "daily": daily_summaries,
    }

    if not dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "coverage_summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        md_lines = [
            f"# Tổng hợp Backfill Đối soát C2 ({from_date} → {to_date})",
            "",
            f"> Ghi chú: {'Dữ liệu mô phỏng/fixture (sơ bộ)' if fixture_dir is not None else 'Dữ liệu production'}.",
            f"- Tỷ lệ ngày có báo cáo: **{summary['report_rate'] * 100:.1f}%** ({summary['days_with_report']}/{summary['days_total']})",
            f"- Độ phủ phát hiện sớm (detection coverage): **{summary['overall_detection_coverage'] * 100:.2f}%**",
            "",
            "| Ngày | Ngày dở dang | Trạng thái từng mã | Độ phủ phát hiện |",
            "|---|---|---|---|",
        ]
        for row in daily_summaries:
            st_str = ", ".join(f"{k}:{v}" for k, v in row["statuses"].items())
            md_lines.append(
                f"| {row['date']} | {'Có' if row['is_partial_day'] else 'Không'} | {st_str} | {row['avg_detection_coverage']*100:.1f}% |"
            )
        (out_dir / "coverage_summary.md").write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Backfill đối soát D+1 nhiều ngày (DE-B, Sprint 5)")
    parser.add_argument("--from-date", required=True, help="Ngày bắt đầu YYYY-MM-DD")
    parser.add_argument("--to-date", required=True, help="Ngày kết thúc YYYY-MM-DD")
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT", help="Danh sách mã")
    parser.add_argument("--fixture-dir", default=None, help="Thư mục fixture mô phỏng")
    parser.add_argument("--out-dir", default=str(ROOT / "reports" / "reconcile"))
    parser.add_argument("--partial-dates", default="2026-11-26", help="Các ngày thu dở dang cách nhau dấu phẩy")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    summary = run_backfill(
        from_date=args.from_date,
        to_date=args.to_date,
        symbols=[s.strip() for s in args.symbols.split(",") if s.strip()],
        fixture_dir=Path(args.fixture_dir) if args.fixture_dir else None,
        out_dir=Path(args.out_dir),
        partial_dates={d.strip() for d in args.partial_dates.split(",") if d.strip()},
        dry_run=args.dry_run,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
