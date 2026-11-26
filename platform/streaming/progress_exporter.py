"""
Metric pipeline health của job Spark Structured Streaming cho Prometheus (Sprint 3, OB-4).

    from progress_exporter import ProgressExporter
    exporter = ProgressExporter(port=9108).start()
    spark.streams.addListener(exporter.listener())
    exporter.inc("finobs_silver_late_rows_total", 12, source="coinbase")   # metric riêng của job

Mỗi micro-batch Spark gửi một StreamingQueryProgress; ta lấy:
  finobs_stream_offsets_behind_latest{query,stat}  Kafka consumer lag (Kafka source tự báo
                                                    min/avg/maxOffsetsBehindLatest; Spark không
                                                    commit offset về consumer group nên không đo
                                                    lag kiểu kafka-consumer-groups được)
  finobs_stream_batch_duration_ms{query}            thời gian xử lý micro-batch gần nhất
  finobs_stream_input_rows{query}                   số dòng vào của batch gần nhất
  finobs_stream_input_rows_per_second / processed_rows_per_second
  finobs_stream_last_progress_timestamp_seconds     để alert "job đứng" (không còn tiến độ)
  finobs_stream_batches_total{query}
Chỉ dùng thư viện chuẩn: image Spark không có prometheus_client.
"""

from __future__ import annotations

import json
import threading
import time
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def _labels(labels: dict) -> str:
    if not labels:
        return ""
    inner = ",".join(f'{k}="{str(v).replace(chr(34), "")}"' for k, v in sorted(labels.items()))
    return "{" + inner + "}"


class ProgressExporter:
    def __init__(self, port: int = 9108):
        self.port = port
        self._gauges: dict[tuple[str, str], float] = {}
        self._counters: dict[tuple[str, str], float] = defaultdict(float)
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ ghi
    def set(self, name: str, value, **labels) -> None:
        if value is None:
            return
        with self._lock:
            self._gauges[(name, _labels(labels))] = float(value)

    def inc(self, name: str, value: float = 1.0, **labels) -> None:
        with self._lock:
            self._counters[(name, _labels(labels))] += float(value)

    def record_progress(self, progress: dict) -> None:
        """progress: StreamingQueryProgress dạng dict (json của Spark)."""
        query = progress.get("name") or progress.get("id", "unknown")
        self.set("finobs_stream_last_progress_timestamp_seconds", time.time(), query=query)
        # Spark 3.5 không có khóa batchDuration trong JSON tiến độ: thời gian batch là durationMs.triggerExecution.
        duration = progress.get("batchDuration") or (progress.get("durationMs") or {}).get("triggerExecution")
        self.set("finobs_stream_batch_duration_ms", duration, query=query)
        self.set("finobs_stream_input_rows", progress.get("numInputRows"), query=query)
        self.set("finobs_stream_input_rows_per_second", progress.get("inputRowsPerSecond"), query=query)
        self.set("finobs_stream_processed_rows_per_second", progress.get("processedRowsPerSecond"), query=query)
        self.inc("finobs_stream_batches_total", 1, query=query)
        for source in progress.get("sources", []):
            metrics = source.get("metrics") or {}
            for stat in ("min", "avg", "max"):
                value = metrics.get(f"{stat}OffsetsBehindLatest")
                if value is not None:
                    self.set("finobs_stream_offsets_behind_latest", value, query=query, stat=stat)

    # ------------------------------------------------------------------ đọc
    def render(self) -> str:
        with self._lock:
            lines = []
            for kind, store in (("gauge", self._gauges), ("counter", self._counters)):
                seen = set()
                for (name, labels), value in sorted(store.items()):
                    if name not in seen:
                        lines.append(f"# TYPE {name} {kind}")
                        seen.add(name)
                    lines.append(f"{name}{labels} {value}")
            return "\n".join(lines) + "\n"

    def start(self) -> ProgressExporter:
        exporter = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                body = exporter.render().encode()
                self.send_response(200 if self.path.startswith("/metrics") else 404)
                self.send_header("Content-Type", "text/plain; version=0.0.4")
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("0.0.0.0", self.port), Handler)
        threading.Thread(target=server.serve_forever, daemon=True, name="progress-exporter").start()
        return self

    def listener(self):
        from pyspark.sql.streaming import StreamingQueryListener

        exporter = self

        class Listener(StreamingQueryListener):
            def onQueryStarted(self, event):  # noqa: N802
                pass

            def onQueryProgress(self, event):  # noqa: N802
                exporter.record_progress(json.loads(event.progress.json))

            def onQueryIdle(self, event):  # noqa: N802
                pass

            def onQueryTerminated(self, event):  # noqa: N802
                exporter.inc("finobs_stream_terminations_total", 1, query=str(event.id))

        return Listener()
