"""
Các "writer" dùng cho producer chạy liên tục. Cùng giao diện với RawWriter
(write(record), count, close()), nên binance_ws.run / coinbase_ws.run dùng được
nguyên si, không phải sửa logic WebSocket.

  KafkaWriter  - kiểm contract từng bản ghi, hợp lệ -> topic của nguồn,
                 vi phạm -> topic DLQ (kèm danh sách lỗi trong header)
  TeeWriter    - ghi đồng thời ra nhiều writer (Kafka + file raw cho collector)
  ContractGate - bọc platform/contracts/validate.py để kiểm một bản ghi
  Metrics      - counter/gauge Prometheus của producer

Topic theo đặc tả (SolutionDesign, Bảng 8): raw.<source>.trade, khóa = mã gốc
của sàn (symbol_native) để mọi trade của một mã nằm cùng partition, giữ thứ tự.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DLQ_TOPIC = "raw.dlq"


def topic_for(source: str) -> str:
    return f"raw.{source}.trade"


# --------------------------------------------------------------------------
# Contract
# --------------------------------------------------------------------------

class ContractGate:
    """Kiểm một bản ghi theo đúng schema + kiểm tra chéo mà validate.py dùng cho file."""

    def __init__(self):
        contracts = str(ROOT / "platform" / "contracts")
        if contracts not in sys.path:
            sys.path.insert(0, contracts)
        from validate import FALLBACK, SCHEMA_BY_SOURCE, cross_checks, load_validators

        self._validators = load_validators()
        self._by_source = SCHEMA_BY_SOURCE
        self._fallback = FALLBACK
        self._cross_checks = cross_checks

    def errors(self, record: dict) -> list[str]:
        validator = self._validators[self._by_source.get(record.get("source"), self._fallback)]
        if validator.is_valid(record):
            problems = []
        else:
            problems = [
                f"{'/'.join(map(str, e.absolute_path)) or '(gốc)'}: {e.message}"[:200]
                for e in validator.iter_errors(record)
            ]
        return problems + self._cross_checks(record)


# --------------------------------------------------------------------------
# Metric Prometheus
# --------------------------------------------------------------------------

class Metrics:
    """Gom metric của producer. Truyền registry riêng trong test để không đụng registry chung."""

    def __init__(self, registry=None):
        from prometheus_client import REGISTRY, Counter, Gauge

        reg = registry if registry is not None else REGISTRY
        self.records = Counter(
            "finobs_producer_records_total",
            "Bản ghi producer đã kiểm contract, theo kết quả",
            ["source", "result"], registry=reg,
        )
        self.delivery_errors = Counter(
            "finobs_producer_delivery_errors_total",
            "Bản ghi Kafka báo gửi thất bại", ["source"], registry=reg,
        )
        self.ws_events = Counter(
            "finobs_producer_ws_events_total",
            "Sự kiện kết nối WebSocket (connected, disconnected, idle_timeout)",
            ["source", "kind"], registry=reg,
        )
        self.last_ingest = Gauge(
            "finobs_producer_last_ingest_timestamp_seconds",
            "Thời điểm (epoch giây) nhận bản ghi gần nhất", ["source"], registry=reg,
        )
        self.clock_offset = Gauge(
            "finobs_producer_clock_offset_ms",
            "Lệch đồng hồ máy producer so với NTP (dương = máy chậm)", registry=reg,
        )


# --------------------------------------------------------------------------
# Writer
# --------------------------------------------------------------------------

class KafkaWriter:
    def __init__(self, producer, source: str, gate: ContractGate, metrics: Metrics | None = None,
                 topic: str | None = None, dlq_topic: str = DLQ_TOPIC):
        self.producer = producer
        self.source = source
        self.gate = gate
        self.metrics = metrics
        self.topic = topic or topic_for(source)
        self.dlq_topic = dlq_topic
        self.count = 0
        self.invalid = 0

    def _on_delivery(self, err, _msg) -> None:
        if err is not None and self.metrics is not None:
            self.metrics.delivery_errors.labels(self.source).inc()

    def write(self, record: dict) -> None:
        errors = self.gate.errors(record)
        value = json.dumps(record, separators=(",", ":"), ensure_ascii=False).encode()
        key = str(record.get("symbol_native", "")).encode()
        if errors:
            self.invalid += 1
            headers = [("finobs.errors", json.dumps(errors, ensure_ascii=False).encode())]
            self.producer.produce(self.dlq_topic, value=value, key=key, headers=headers,
                                  on_delivery=self._on_delivery)
        else:
            self.producer.produce(self.topic, value=value, key=key, on_delivery=self._on_delivery)
        self.count += 1
        self.producer.poll(0)  # chạy callback giao hàng, không chặn

        if self.metrics is not None:
            self.metrics.records.labels(self.source, "invalid" if errors else "valid").inc()
            self.metrics.last_ingest.labels(self.source).set_to_current_time()

    def close(self) -> None:
        self.producer.flush(5)


class TeeWriter:
    def __init__(self, *writers):
        self.writers = writers

    @property
    def count(self) -> int:
        return self.writers[0].count

    def write(self, record: dict) -> None:
        for writer in self.writers:
            writer.write(record)

    def close(self) -> None:
        for writer in self.writers:
            writer.close()


def make_kafka_producer(bootstrap: str):
    """Producer idempotent: không tạo bản trùng khi tự gửi lại sau lỗi mạng."""
    from confluent_kafka import Producer

    return Producer({
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 50,
        "compression.type": "lz4",
        "client.id": "finobs-producer",
    })
