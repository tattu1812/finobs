"""
Producer chạy liên tục (FR-01, FR-02): WebSocket Binance/Coinbase -> kiểm contract -> Kafka.

    python platform/ingest/produce.py --bootstrap localhost:9092
    python platform/ingest/produce.py --bootstrap kafka:29092 --raw-out /data/raw --metrics-port 8000

Khác record.py (thu N phút ra file để đo đạc):
  - chạy tới khi bị dừng (SIGTERM/Ctrl+C), không có --minutes
  - bản ghi hợp lệ -> raw.<source>.trade, vi phạm contract -> raw.dlq
  - --raw-out: ghi thêm file .jsonl.gz như record.py, để collector metric đọc
  - --metrics-port: endpoint Prometheus (records, vi phạm, reconnect, lệch đồng hồ)
  - đo lệch đồng hồ NTP mỗi --clock-every giây, ghi vào ops log và metric

Tự kết nối lại khi WebSocket rớt nằm sẵn trong binance_ws.run / coinbase_ws.run;
mỗi lần rớt được ghi vào <raw-out>/_ops/<source>.ops.jsonl và đếm trong metric.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
import statistics
from pathlib import Path

import binance_ws
import coinbase_ws
from clock_check import NTP_SERVERS, best, ntp_sample
from common import OpsLog, RawWriter
from sinks import ContractGate, KafkaWriter, Metrics, TeeWriter, make_kafka_producer

RUNNERS = {"binance": binance_ws.run, "coinbase": coinbase_ws.run}
HEARTBEAT_SECONDS = 30


class MetricsOpsLog(OpsLog):
    """OpsLog ghi file như cũ, đồng thời đếm sự kiện kết nối vào Prometheus."""

    def __init__(self, root, source: str, metrics: Metrics):
        super().__init__(root, source)
        self.source = source
        self.metrics = metrics

    def event(self, kind: str, **fields) -> None:
        super().event(kind, **fields)
        self.metrics.ws_events.labels(self.source, kind).inc()


def measure_clock_offset_ms() -> float | None:
    offsets = []
    for host in NTP_SERVERS:
        samples = []
        for _ in range(4):
            try:
                samples.append(ntp_sample(host))
            except OSError:
                pass
        if samples:
            offsets.append(best(samples)["offset_ms"])
    return round(statistics.median(offsets), 1) if offsets else None


async def clock_loop(ops: OpsLog, metrics: Metrics, every: float, stop: asyncio.Event) -> None:
    while not stop.is_set():
        offset = await asyncio.to_thread(measure_clock_offset_ms)
        if offset is None:
            ops.event("clock_check_failed")
        else:
            metrics.clock_offset.set(offset)
            ops.event("clock_offset", ntp_offset_ms=offset)
        try:
            await asyncio.wait_for(stop.wait(), timeout=every)
        except asyncio.TimeoutError:
            pass


async def heartbeat(writers: dict, stop: asyncio.Event) -> None:
    previous = {name: 0 for name in writers}
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=HEARTBEAT_SECONDS)
            return
        except asyncio.TimeoutError:
            pass
        parts = []
        for name, writer in writers.items():
            delta = writer.count - previous[name]
            previous[name] = writer.count
            parts.append(f"{name}={writer.count} ({delta / HEARTBEAT_SECONDS:.1f}/s)")
        print("[tick] " + "  ".join(parts), flush=True)


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--sources", default="binance,coinbase")
    parser.add_argument("--symbols", default="config/symbols.json")
    parser.add_argument("--raw-out", default="", help="thư mục ghi thêm file raw; bỏ trống = không ghi")
    parser.add_argument("--ops-dir", default="data/raw", help="nơi ghi ops log khi không có --raw-out")
    parser.add_argument("--metrics-port", type=int, default=0, help="0 = không mở endpoint Prometheus")
    parser.add_argument("--clock-every", type=float, default=600.0, help="giây giữa hai lần đo NTP; 0 = tắt")
    args = parser.parse_args()

    config = json.loads(Path(args.symbols).read_text(encoding="utf-8"))
    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    for name in sources:
        if name not in RUNNERS:
            raise SystemExit(f"Nguồn không hỗ trợ chạy liên tục: {name}")

    metrics = Metrics()
    if args.metrics_port:
        from prometheus_client import start_http_server

        start_http_server(args.metrics_port)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    ops_root = args.raw_out or args.ops_dir
    gate = ContractGate()
    producer = make_kafka_producer(args.bootstrap)
    writers: dict = {}
    tasks = []
    for name in sources:
        kafka = KafkaWriter(producer, name, gate, metrics)
        raw = RawWriter(args.raw_out, name, flush_seconds=5, file_per_process=True) if args.raw_out else None
        writer = TeeWriter(kafka, raw) if raw else kafka
        writers[name] = writer
        ops = MetricsOpsLog(ops_root, name, metrics)
        tasks.append(asyncio.create_task(RUNNERS[name](config[name], writer, ops, stop)))

    tasks.append(asyncio.create_task(heartbeat(writers, stop)))
    if args.clock_every > 0:
        tasks.append(asyncio.create_task(
            clock_loop(OpsLog(ops_root, "producer"), metrics, args.clock_every, stop)))

    print(f"Producer: {', '.join(sources)} -> Kafka {args.bootstrap}"
          + (f", file {args.raw_out}" if args.raw_out else ""), flush=True)
    try:
        await stop.wait()
    except KeyboardInterrupt:
        stop.set()

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    for name, writer in writers.items():
        writer.close()
        print(f"{name}: {writer.count} bản ghi", flush=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
