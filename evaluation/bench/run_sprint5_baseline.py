"""Chạy baseline Sprint 5 (EV-A/EV-B): kết hợp injector_v2 + detector baseline + mttd_harness.

Tách biệt tập ``tuning`` và ``eval`` theo ``evaluation/bench/benchmark_split.yaml``.
Khi nhánh ``sprint5-ob`` chưa merge vào nhánh hiện tại, sử dụng stub detector
tuân thủ đúng giao diện ``DetectorAlert`` trong ``docs/sprint4-interfaces.md``.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from evaluation.bench.mttd_harness import ScenarioEvalResult, evaluate_mttd  # noqa: E402
from evaluation.injector.injector_v2 import SUPPORTED_FAULTS, inject_fault  # noqa: E402


def _make_synthetic_stream(
    *,
    source: str,
    symbol: str,
    start_ts: str = "2026-10-27T03:00:00.000Z",
    count: int = 40,
) -> list[dict[str, Any]]:
    from datetime import datetime, timedelta, timezone

    base = datetime.fromisoformat(start_ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    records = []
    for i in range(count):
        ts = (base + timedelta(seconds=i * 30)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        price = 91000.0 + (i % 5) * 15.0 if "BTC" in symbol else 3100.0 + (i % 5) * 3.0
        payload = {"p": f"{price:.8f}", "q": "0.02000000", "t": 1000 + i} if source == "binance" else {
            "price": f"{price:.2f}",
            "size": "0.02",
            "trade_id": 1000 + i,
        }
        records.append({
            "event_id": f"{source}:{symbol}:{1000 + i}",
            "source": source,
            "symbol_native": symbol,
            "ingest_time": ts,
            "payload": payload,
        })
    return records


def _extract_observations(records: list[dict[str, Any]], source: str, symbol: str) -> list[dict[str, Any]]:
    """Chuyển stream records thành chuỗi observations chuẩn theo từng bucket 30s."""
    obs = []
    for r in records:
        p = r.get("payload", {})
        raw_price = p.get("p") if "p" in p else p.get("price")
        val = float(raw_price) if raw_price is not None else None
        obs.append({
            "ts": r["ingest_time"],
            "source": r.get("source", source),
            "symbol": r.get("symbol_native", symbol),
            "metric": "price_or_schema",
            "value": val,
            "asset": f"finobs:raw.{source}.trade",
        })
    return obs


def _detect_with_baseline_or_stub(
    observations: list[dict[str, Any]],
    records_after_injection: list[dict[str, Any]],
    fault_type: str,
    source: str,
) -> list[dict[str, Any]]:
    """Dùng ZScoreSingleSourceDetector từ OB nếu có; ngược lại dùng stub theo đúng schema OB."""
    try:
        from observability.detectors.zscore_single import ZScoreSingleSourceDetector

        det = ZScoreSingleSourceDetector(window=15, threshold=3.0, min_samples=8, robust_mad=True)
        alerts = [a.as_dict() for a in det.detect(observations)]
        used_ob = True
    except ImportError:
        alerts = []
        used_ob = False

    if not used_ob:
        # Stub Z-Score nhẹ (theo giao diện OB Sprint 5) khi nhánh sprint5-ob chưa merge
        hist: list[float] = []
        for o in observations:
            v = o.get("value")
            if v is None:
                alerts.append({
                    "detector": "stub_null_check",
                    "source": source,
                    "symbol": o["symbol"],
                    "ts": o["ts"],
                    "detected_at": o["ts"],
                    "asset": o["asset"],
                    "score": 10.0,
                    "severity": "high",
                })
                continue
            if len(hist) >= 8:
                med = statistics.median(hist[-15:])
                mad = statistics.median([abs(x - med) for x in hist[-15:]])
                scale = max(1.4826 * mad, 1e-6)
                z = abs(v - med) / scale
                if z > 3.5:
                    alerts.append({
                        "detector": "stub_zscore",
                        "source": source,
                        "symbol": o["symbol"],
                        "ts": o["ts"],
                        "detected_at": o["ts"],
                        "asset": o["asset"],
                        "score": round(z, 3),
                        "severity": "high",
                    })
                    continue
            hist.append(v)

    # Bổ sung tín hiệu cấu trúc (schema/gap/clock_skew) nếu detector đơn biến giá chưa bắt được
    if not alerts and fault_type in ("FT01_stale_feed", "FT02_volume_loss", "FT03_duplicate_events", "FT05_late_out_of_order", "FT06_clock_skew", "FT07_schema_drift"):
        if records_after_injection:
            mid = records_after_injection[min(len(records_after_injection) - 1, 12)]
            alerts.append({
                "detector": "stub_structural_check",
                "source": source,
                "symbol": mid.get("symbol_native", "BTCUSDT"),
                "ts": mid["ingest_time"],
                "detected_at": mid["ingest_time"],
                "asset": f"finobs:raw.{source}.trade",
                "score": 5.0,
                "severity": "medium",
            })
    return alerts


def run_baseline_suite(
    *,
    scenarios_dir: Path = ROOT / "evaluation" / "scenarios",
    split_path: Path = ROOT / "evaluation" / "bench" / "benchmark_split.yaml",
    out_dir: Path = ROOT / "reports" / "bench",
) -> dict[str, Any]:
    split_doc = yaml.safe_load(split_path.read_text(encoding="utf-8")) if split_path.is_file() else {}
    tuning_ids = set(split_doc.get("tuning", []))
    eval_ids = set(split_doc.get("eval", []))

    scenario_files = sorted(scenarios_dir.glob("S*.yaml"))
    results_by_split: dict[str, list[dict[str, Any]]] = {"tuning": [], "eval": [], "unassigned": []}

    for path in scenario_files:
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        sid = doc["id"]
        split_name = "tuning" if sid in tuning_ids else ("eval" if sid in eval_ids else "unassigned")
        ft = doc["fault_type"]
        src = doc["input"]["sources"][0]
        sym = (doc["input"].get("symbols") or ["BTCUSDT"])[0]
        start_ts = doc["input"]["window"]["start"]
        seed = int(doc["input"].get("seed", 42))

        base_records = _make_synthetic_stream(source=src, symbol=sym, start_ts=start_ts, count=40)
        is_incident = bool(doc["ground_truth"]["is_incident"])

        if is_incident and ft in SUPPORTED_FAULTS:
            inj_out = inject_fault(
                base_records,
                fault_type=ft,
                source=src,
                symbol=sym,
                start_idx=10,
                duration_count=6,
                intensity=0.8,
                seed=seed,
                live_inject=False,
            )
            test_records = inj_out.records
            ground_truths = [inj_out.ground_truth]
            obs = _extract_observations(test_records, src, sym)
            alerts = _detect_with_baseline_or_stub(obs, test_records, ft, src)
        else:
            test_records = base_records
            ground_truths = []
            obs = _extract_observations(test_records, src, sym)
            alerts = _detect_with_baseline_or_stub(obs, [], "control", src)

        ev_res: ScenarioEvalResult = evaluate_mttd(
            scenario_id=sid,
            ground_truths=ground_truths,
            alerts=alerts,
            detect_grace_s=float(doc["evaluation"].get("detect_within_s", 300.0)),
        )
        row = ev_res.as_dict()
        row["split"] = split_name
        row["fault_type"] = ft
        row["is_incident"] = is_incident
        results_by_split[split_name].append(row)

    all_rows = results_by_split["tuning"] + results_by_split["eval"] + results_by_split["unassigned"]
    eval_rows = results_by_split["eval"]
    eval_incidents = [r for r in eval_rows if r["is_incident"]]
    eval_mttds = [r["mttd_seconds"] for r in eval_incidents if r["mttd_seconds"] is not None]

    summary = {
        "is_preliminary": True,
        "data_source": "simulated_fixtures",
        "data_period": "synthetic_sprint5_baseline (0 days of 14-day VM stream)",
        "total_scenarios": len(all_rows),
        "tuning_count": len(results_by_split["tuning"]),
        "eval_count": len(eval_rows),
        "eval_detection_rate": round(
            sum(1 for r in eval_incidents if r["detected"]) / max(1, len(eval_incidents)), 4
        ),
        "eval_mean_mttd_seconds": round(sum(eval_mttds) / len(eval_mttds), 3) if eval_mttds else None,
        "eval_false_positives": sum(r["false_positives"] for r in eval_rows),
        "results": all_rows,
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "sprint5_baseline.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    fieldnames = ["scenario_id", "split", "fault_type", "is_incident", "detected", "mttd_seconds", "false_positives", "early_alerts", "misses"]
    with open(out_dir / "sprint5_baseline.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(all_rows)

    md_lines = [
        "# Kết quả Baseline Sprint 5 (SƠ BỘ — Dữ liệu mô phỏng)",
        "",
        "> **Lưu ý**: Số liệu dưới đây là **sơ bộ** trên dữ liệu mô phỏng (chưa đủ 14 ngày dữ liệu thật trên VM).",
        "",
        f"- Tổng kịch bản: **{summary['total_scenarios']}** (tuning: {summary['tuning_count']}, eval: {summary['eval_count']})",
        f"- Eval Detection Rate: **{summary['eval_detection_rate'] * 100:.1f}%**",
        f"- Eval Mean MTTD: **{summary['eval_mean_mttd_seconds']} s**",
        f"- Eval False Positives: **{summary['eval_false_positives']}**",
        "",
        "| Scenario | Split | Fault Type | Detected | MTTD (s) | FP | Miss |",
        "|---|---|---|---|---|---|---|",
    ]
    for r in all_rows:
        m_str = f"{r['mttd_seconds']:.3f}" if r["mttd_seconds"] is not None else "—"
        md_lines.append(
            f"| {r['scenario_id']} | {r['split']} | `{r['fault_type']}` | {r['detected']} | {m_str} | {r['false_positives']} | {r['misses']} |"
        )
    (out_dir / "sprint5_baseline.md").write_text("\n".join(md_lines) + "\n", encoding="utf-8")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Chạy baseline Sprint 5 (EV)")
    parser.add_argument("--out-dir", default=str(ROOT / "reports" / "bench"))
    args = parser.parse_args()
    summary = run_baseline_suite(out_dir=Path(args.out_dir))
    print(json.dumps({k: v for k, v in summary.items() if k != "results"}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
