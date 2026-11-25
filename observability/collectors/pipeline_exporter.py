"""Pipeline health của Airflow cho Prometheus (Sprint 3, OB-4 bổ sung: trạng thái DAG run).

    AIRFLOW_URL=http://airflow-webserver:8080 AIRFLOW_USER=admin AIRFLOW_PASSWORD=... \\
        python observability/collectors/pipeline_exporter.py --port 8001

Mỗi INTERVAL giây hỏi REST API của Airflow lượt chạy gần nhất của từng DAG đang bật:
  finobs_airflow_up                                   1 nếu API trả lời
  finobs_airflow_dag_last_run_success{dag_id}         1 thành công, 0 thất bại, không có nếu đang chạy
  finobs_airflow_dag_last_run_state{dag_id,state}     1 cho trạng thái hiện tại
  finobs_airflow_dag_last_run_duration_seconds{dag_id}
  finobs_airflow_dag_last_run_end_timestamp_seconds{dag_id}
Lag Kafka và thời gian micro-batch Spark do chính job Spark xuất (platform/streaming/progress_exporter.py).
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import time
import urllib.request
from datetime import datetime

STATES = ("queued", "running", "success", "failed")


class AirflowClient:
    def __init__(self, url: str, user: str, password: str, timeout: float = 10.0):
        self.url = url.rstrip("/")
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        self.headers = {"Authorization": f"Basic {token}", "Accept": "application/json"}
        self.timeout = timeout

    def get(self, path: str) -> dict:
        request = urllib.request.Request(f"{self.url}/api/v1{path}", headers=self.headers)
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            return json.load(response)

    def latest_runs(self) -> dict[str, dict | None]:
        dags = self.get("/dags?only_active=true&limit=100")["dags"]
        result = {}
        for dag in dags:
            if dag.get("is_paused"):
                continue
            runs = self.get(f"/dags/{dag['dag_id']}/dagRuns?order_by=-execution_date&limit=1")["dag_runs"]
            result[dag["dag_id"]] = runs[0] if runs else None
        return result


def _ts(text: str | None) -> float | None:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp() if text else None


def update(gauges: dict, runs: dict[str, dict | None]) -> None:
    for dag_id, run in runs.items():
        if run is None:
            continue
        state = run.get("state")
        for s in STATES:
            gauges["state"].labels(dag_id, s).set(1 if s == state else 0)
        if state in ("success", "failed"):
            gauges["success"].labels(dag_id).set(1 if state == "success" else 0)
        start, end = _ts(run.get("start_date")), _ts(run.get("end_date"))
        if start and end:
            gauges["duration"].labels(dag_id).set(end - start)
            gauges["end"].labels(dag_id).set(end)


def main() -> None:
    from prometheus_client import Gauge, start_http_server

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--interval", type=float, default=60.0)
    args = parser.parse_args()

    client = AirflowClient(os.environ.get("AIRFLOW_URL", "http://airflow-webserver:8080"),
                           os.environ.get("AIRFLOW_USER", "admin"), os.environ["AIRFLOW_PASSWORD"])
    up = Gauge("finobs_airflow_up", "Airflow REST API trả lời được")
    gauges = {
        "state": Gauge("finobs_airflow_dag_last_run_state", "Trạng thái lượt chạy gần nhất", ["dag_id", "state"]),
        "success": Gauge("finobs_airflow_dag_last_run_success", "1 thành công, 0 thất bại", ["dag_id"]),
        "duration": Gauge("finobs_airflow_dag_last_run_duration_seconds", "Thời gian lượt chạy gần nhất", ["dag_id"]),
        "end": Gauge("finobs_airflow_dag_last_run_end_timestamp_seconds", "Lúc lượt gần nhất kết thúc", ["dag_id"]),
    }
    start_http_server(args.port)
    print(f"pipeline-exporter: {client.url} -> :{args.port}", flush=True)
    while True:
        try:
            update(gauges, client.latest_runs())
            up.set(1)
        except Exception as exc:  # noqa: BLE001 — Airflow tạm mất: báo up=0, vòng sau thử lại
            up.set(0)
            print(f"[pipeline-exporter] {exc!r}", flush=True)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
