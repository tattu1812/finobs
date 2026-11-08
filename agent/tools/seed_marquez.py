"""
Nạp topology lineage giả (fake_world.JOBS) vào một Marquez THẬT bằng sự kiện OpenLineage,
để client đọc lineage được kiểm tra trên API thật.

    make up   # hoặc chỉ marquez-postgres + marquez-api (xem docs/airflow-marquez.md)
    python -m agent.tools.seed_marquez --marquez-url http://localhost:5000
    python -m agent.tools.lineage_graph --marquez-url http://localhost:5000

Gửi lại nhiều lần không sao: Marquez chỉ thêm run mới cho cùng job/dataset.
"""

from __future__ import annotations

import argparse
import sys
import urllib.error

from agent.tools.marquez import DEFAULT_URL, HttpTransport, openlineage_events


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--marquez-url", default=DEFAULT_URL)
    args = ap.parse_args()

    http = HttpTransport(args.marquez_url)
    for event in openlineage_events():
        try:
            status = http.post("/api/v1/lineage", event)
        except (urllib.error.URLError, OSError) as exc:
            print(f"Không gửi được tới {args.marquez_url}: {exc}", file=sys.stderr)
            return 1
        ins = ", ".join(d["name"] for d in event["inputs"]) or "-"
        outs = ", ".join(d["name"] for d in event["outputs"])
        print(f"HTTP {status}  job {event['job']['name']:<18} {ins} -> {outs}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
