"""
Phát sự kiện OpenLineage về Marquez theo quy ước tên của nhóm (docs/conventions/lineage-naming.md).

    from lineage import LineageEmitter
    lin = LineageEmitter.from_env()            # MARQUEZ_URL, mặc định http://marquez-api:5000
    run = lin.start("silver_trades", inputs=["bronze.trades_raw"], outputs=["silver.trades"])
    ...
    lin.complete(run)        # hoặc lin.fail(run, "lý do")

Vì sao không dùng openlineage-python hay listener tự động của Spark (quyết định Q5, Sprint 3):
listener tự đặt tên dataset theo đường dẫn S3/warehouse, phá quy ước "tên dataset = tên topic /
bảng thật" mà RCA và scenario đang dùng. File này chỉ dùng thư viện chuẩn để chạy được trong
mọi image (Spark, Airflow, app) mà không cài thêm gì. Định dạng sự kiện theo OpenLineage 2-0-2.

Lineage là thông tin phụ: Marquez không trả lời thì ghi cảnh báo rồi bỏ qua, KHÔNG làm job lỗi.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

NAMESPACE = "finobs"
PRODUCER = "https://github.com/tattu1812/finobs"
SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _datasets(names) -> list[dict]:
    return [{"namespace": NAMESPACE, "name": name, "facets": {}} for name in names or []]


@dataclass
class Run:
    job: str
    inputs: list[str]
    outputs: list[str]
    streaming: bool = False
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))


class LineageEmitter:
    def __init__(self, url: str | None, timeout: float = 3.0, post=None):
        """post(url, body_bytes): thay thế lệnh gửi HTTP trong test."""
        self.url = url.rstrip("/") + "/api/v1/lineage" if url else None
        self.timeout = timeout
        self._post = post or self._http_post
        self.sent: list[dict] = []

    @classmethod
    def from_env(cls) -> LineageEmitter:
        url = os.environ.get("MARQUEZ_URL", "http://marquez-api:5000")
        return cls(None if url.lower() in ("", "off", "none") else url)

    def _http_post(self, url: str, body: bytes) -> None:
        request = urllib.request.Request(url, data=body, method="POST",
                                         headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            response.read()

    def event(self, event_type: str, run: Run, error: str | None = None) -> dict:
        run_facets = {}
        if error:
            run_facets["errorMessage"] = {
                "_producer": PRODUCER,
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ErrorMessageRunFacet.json",
                "message": error[:2000], "programmingLanguage": "python",
            }
        return {
            "eventType": event_type,
            "eventTime": _now(),
            "run": {"runId": run.run_id, "facets": run_facets},
            "job": {"namespace": NAMESPACE, "name": run.job, "facets": {"jobType": {
                "_producer": PRODUCER,
                "_schemaURL": "https://openlineage.io/spec/facets/2-0-2/JobTypeJobFacet.json",
                # Marquez chỉ dựng cạnh lineage khi run COMPLETE, trừ job đánh dấu STREAMING (dựng ngay
                # từ START). Job streaming chạy liên tục, không bao giờ COMPLETE: thiếu facet này thì
                # đồ thị Marquez không có cạnh nào (đã gặp khi chạy thật, Sprint 3).
                "processingType": "STREAMING" if run.streaming else "BATCH",
                "integration": "FINOBS", "jobType": "JOB"}}},
            "inputs": _datasets(run.inputs),
            "outputs": _datasets(run.outputs),
            "producer": PRODUCER,
            "schemaURL": SCHEMA_URL,
        }

    def emit(self, event_type: str, run: Run, error: str | None = None) -> bool:
        payload = self.event(event_type, run, error)
        self.sent.append(payload)
        if not self.url:
            return False
        try:
            self._post(self.url, json.dumps(payload).encode())
            return True
        except (urllib.error.URLError, OSError, ValueError) as exc:
            print(f"[lineage] không gửi được {event_type} {run.job}: {exc}", file=sys.stderr, flush=True)
            return False

    def start(self, job: str, inputs=None, outputs=None, streaming: bool = False) -> Run:
        run = Run(job, list(inputs or []), list(outputs or []), streaming)
        self.emit("START", run)
        return run

    def running(self, run: Run) -> bool:
        return self.emit("RUNNING", run)

    def complete(self, run: Run) -> bool:
        return self.emit("COMPLETE", run)

    def fail(self, run: Run, error: str) -> bool:
        return self.emit("FAIL", run, error)
