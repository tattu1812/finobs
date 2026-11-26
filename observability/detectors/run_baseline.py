"""CLI chạy baseline detectors trên file observations JSON/JSONL (OB, Sprint 5)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from observability.detectors.historical_bounds import HistoricalBoundsDetector  # noqa: E402
from observability.detectors.zscore_single import ZScoreSingleSourceDetector  # noqa: E402


def load_observations(path: Path) -> list[dict[str, Any]]:
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    if text.startswith("["):
        return json.loads(text)
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def load_detector_config(path: Path = ROOT / "config" / "detectors.yaml") -> dict[str, Any]:
    if path.is_file():
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return {}


def run_detectors(
    observations: list[dict[str, Any]],
    *,
    detector_name: str = "both",
    train_observations: list[dict[str, Any]] | None = None,
    config: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    cfg = config or load_detector_config()
    alerts: list[dict[str, Any]] = []

    if detector_name in ("zscore", "zscore_single", "both"):
        z_cfg = cfg.get("zscore_single", {})
        z_det = ZScoreSingleSourceDetector(
            window=int(z_cfg.get("window", 30)),
            threshold=float(z_cfg.get("threshold", 3.0)),
            min_samples=int(z_cfg.get("min_samples", 10)),
            robust_mad=bool(z_cfg.get("robust_mad", True)),
        )
        alerts.extend(a.as_dict() for a in z_det.detect(observations))

    if detector_name in ("historical_bounds", "bounds", "both"):
        b_cfg = cfg.get("historical_bounds", {})
        b_det = HistoricalBoundsDetector(
            lower_quantile=float(b_cfg.get("lower_quantile", 0.01)),
            upper_quantile=float(b_cfg.get("upper_quantile", 0.99)),
            margin_factor=float(b_cfg.get("margin_factor", 0.25)),
            min_fit_samples=int(b_cfg.get("min_fit_samples", 10)),
        )
        train_data = train_observations if train_observations is not None else observations[: max(10, len(observations) // 2)]
        b_det.fit(train_data)
        alerts.extend(a.as_dict() for a in b_det.detect(observations))

    alerts.sort(key=lambda a: a["detected_at"])
    return alerts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Chạy baseline detectors (OB, Sprint 5)")
    parser.add_argument("--input", required=True, help="File JSON hoặc JSONL chứa observations")
    parser.add_argument("--train-input", default=None, help="File train riêng cho historical_bounds")
    parser.add_argument("--detector", default="both", choices=["zscore_single", "historical_bounds", "both"])
    parser.add_argument("--config", default=str(ROOT / "config" / "detectors.yaml"))
    parser.add_argument("--out", default=None, help="File JSONL xuất cảnh báo (mặc định in stdout)")
    args = parser.parse_args(argv)

    obs = load_observations(Path(args.input))
    train_obs = load_observations(Path(args.train_input)) if args.train_input else None
    cfg = load_detector_config(Path(args.config))
    alerts = run_detectors(obs, detector_name=args.detector, train_observations=train_obs, config=cfg)

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            "\n".join(json.dumps(a, ensure_ascii=False) for a in alerts) + ("\n" if alerts else ""),
            encoding="utf-8",
        )
        print(f"Đã ghi {len(alerts)} cảnh báo ra {out_path}")
    else:
        print(json.dumps({"count": len(alerts), "alerts": alerts}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
