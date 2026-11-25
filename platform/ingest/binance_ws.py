"""
Recorder Binance — combined stream <symbol>@trade.

Đặc điểm nguồn này:
  - Mỗi message = đúng 1 lệnh khớp, nên batch_size luôn = 1
  - Payload bọc trong {"stream": ..., "data": ...}
  - Mã symbol phải viết thường trong URL
  - Server gửi ping mỗi 20 giây; thư viện websockets tự trả pong
  - Một kết nối chỉ sống tối đa 24 giờ -> bắt buộc phải có reconnect
"""

from __future__ import annotations

import asyncio
import json

import websockets

from common import (
    OpsLog,
    RawWriter,
    canonical_binance,
    make_event,
    ms_to_iso,
    utc_now_iso,
)

SOURCE = "binance"
BASE_URL = "wss://stream.binance.com:9443/stream?streams="

MAX_BACKOFF = 30.0
IDLE_TIMEOUT = 120.0  # không có message nào trong 2 phút -> coi như kết nối chết


def _handle(raw, writer: RawWriter) -> None:
    ingest_time = utc_now_iso()  # lấy NGAY, trước khi parse, để đo trễ chuẩn
    raw_bytes = len(raw) if isinstance(raw, (bytes, bytearray)) else len(raw.encode())

    message = json.loads(raw)
    data = message.get("data", message)
    if data.get("e") != "trade":
        return  # bỏ qua ack subscribe và các message điều khiển

    symbol = data["s"]
    writer.write(
        make_event(
            source=SOURCE,
            symbol_native=symbol,
            symbol_canonical=canonical_binance(symbol),
            event_type="trade",
            event_time=ms_to_iso(data["T"]),
            ingest_time=ingest_time,
            sequence=data["t"],
            payload=data,
            transport={"batch_size": 1, "raw_bytes": raw_bytes, "msg_seq": None},
        )
    )


async def run(symbols, writer: RawWriter, ops: OpsLog, stop: asyncio.Event) -> None:
    url = BASE_URL + "/".join(f"{s.lower()}@trade" for s in symbols)
    backoff = 1.0

    while not stop.is_set():
        try:
            async with websockets.connect(
                url, ping_interval=20, ping_timeout=60, max_queue=4096
            ) as ws:
                ops.event("connected", streams=len(symbols))
                backoff = 1.0
                while not stop.is_set():
                    raw = await asyncio.wait_for(ws.recv(), timeout=IDLE_TIMEOUT)
                    _handle(raw, writer)

        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            ops.event("idle_timeout", seconds=IDLE_TIMEOUT)
        except Exception as exc:  # noqa: BLE001 — mất mạng kiểu gì cũng phải nối lại
            ops.event("disconnected", error=repr(exc))

        if stop.is_set():
            break
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF)