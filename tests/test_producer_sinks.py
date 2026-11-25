"""KafkaWriter: hợp lệ -> raw.<source>.trade, vi phạm contract -> raw.dlq; không cần Kafka thật."""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "platform" / "ingest"))

from sinks import DLQ_TOPIC, ContractGate, KafkaWriter, TeeWriter, topic_for  # noqa: E402


def sample(source: str, line: int = 0) -> dict:
    path = ROOT / "tests" / "fixtures" / "raw" / source / "sample.jsonl"
    return json.loads(path.read_text(encoding="utf-8").splitlines()[line])


class FakeProducer:
    def __init__(self):
        self.sent = []
        self.polls = 0
        self.flushed = False

    def produce(self, topic, value, key, headers=None, on_delivery=None):
        self.sent.append({"topic": topic, "value": value, "key": key, "headers": headers})

    def poll(self, _timeout):
        self.polls += 1

    def flush(self, _timeout):
        self.flushed = True


class ListWriter:
    def __init__(self):
        self.rows, self.count, self.closed = [], 0, False

    def write(self, record):
        self.rows.append(record)
        self.count += 1

    def close(self):
        self.closed = True


class KafkaWriterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.gate = ContractGate()

    def test_valid_record_goes_to_source_topic_keyed_by_symbol(self):
        producer = FakeProducer()
        writer = KafkaWriter(producer, "binance", self.gate)
        record = sample("binance")
        writer.write(record)

        self.assertEqual(len(producer.sent), 1)
        sent = producer.sent[0]
        self.assertEqual(sent["topic"], "raw.binance.trade")
        self.assertEqual(sent["key"], record["symbol_native"].encode())
        self.assertEqual(json.loads(sent["value"]), record)
        self.assertIsNone(sent["headers"])
        self.assertEqual((writer.count, writer.invalid), (1, 0))

    def test_contract_violation_goes_to_dlq_with_errors(self):
        producer = FakeProducer()
        writer = KafkaWriter(producer, "coinbase", self.gate)
        record = sample("coinbase")
        record["payload"]["size"] = 1.5  # contract: size phải là chuỗi
        writer.write(record)

        sent = producer.sent[0]
        self.assertEqual(sent["topic"], DLQ_TOPIC)
        errors = json.loads(dict(sent["headers"])["finobs.errors"])
        self.assertTrue(any("size" in e for e in errors), errors)
        self.assertEqual(writer.invalid, 1)

    def test_cross_check_failure_is_a_violation(self):
        record = sample("binance")
        record["event_id"] = "binance:BTCUSDT:0"
        self.assertTrue(self.gate.errors(record))

    def test_every_fixture_record_passes_the_gate(self):
        for source in ("binance", "coinbase"):
            path = ROOT / "tests" / "fixtures" / "raw" / source / "sample.jsonl"
            for line in path.read_text(encoding="utf-8").splitlines():
                self.assertEqual(self.gate.errors(json.loads(line)), [], f"{source}: {line[:80]}")

    def test_close_flushes_producer(self):
        producer = FakeProducer()
        KafkaWriter(producer, "binance", self.gate).close()
        self.assertTrue(producer.flushed)

    def test_topic_naming(self):
        self.assertEqual(topic_for("coinbase"), "raw.coinbase.trade")


class MetricsTest(unittest.TestCase):
    def test_counters_by_result(self):
        try:
            from prometheus_client import CollectorRegistry
        except ImportError:
            self.skipTest("prometheus_client chưa cài")
        from sinks import Metrics

        registry = CollectorRegistry()
        metrics = Metrics(registry)
        writer = KafkaWriter(FakeProducer(), "binance", ContractGate(), metrics)
        good, bad = sample("binance"), sample("binance", 1)
        bad["payload"]["p"] = 1.0
        writer.write(good)
        writer.write(bad)

        def value(result):
            return registry.get_sample_value("finobs_producer_records_total",
                                             {"source": "binance", "result": result})

        self.assertEqual((value("valid"), value("invalid")), (1.0, 1.0))
        self.assertIsNotNone(registry.get_sample_value(
            "finobs_producer_last_ingest_timestamp_seconds", {"source": "binance"}))


class TeeWriterTest(unittest.TestCase):
    def test_writes_to_all_and_counts_first(self):
        a, b = ListWriter(), ListWriter()
        tee = TeeWriter(a, b)
        tee.write({"x": 1})
        tee.close()
        self.assertEqual((a.rows, b.rows, tee.count), ([{"x": 1}], [{"x": 1}], 1))
        self.assertTrue(a.closed and b.closed)


if __name__ == "__main__":
    unittest.main()
