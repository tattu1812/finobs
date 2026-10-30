"""
Recorder Coinbase — Advanced Trade public WebSocket, kênh market_trades.

Khác biệt cốt lõi so với Binance:
  - Coinbase GOM các lệnh khớp theo lô 250 ms, nên 1 message chứa N trade
  - Ta tách ra ghi từng trade một, nhưng lưu batch_size để sau còn phân biệt
    được "message/giây" với "trade/giây". Thiếu chỗ này thì bảng so sánh
    hai sàn sẽ sai hoàn toàn.
  - Phải gửi message subscribe sau khi kết nối
  - Thời gian là chuỗi ISO micro giây, cần chuẩn hoá về mili giây
"""

from __future__ import annotations

import asyncio
import json

import websockets

from common import (
    OpsLog,
    RawWriter,
    canonical_coinbase,
    iso_to_iso,
    make_event,
    utc_now_iso,
)

SOURCE = "coinbase"
WS_URL = "wss://advanced-trade-ws.coinbase.com"

MAX_BACKOFF = 30.0
IDLE_TIMEOUT = 120.0


def _handle(raw, writer: RawWriter) -> None:
    ingest_time = utc_now_iso()
    raw_bytes = len(raw) if isinstance(raw, (bytes, bytearray)) else len(raw.encode())

    message = json.loads(raw)
    if message.get("channel") != "market_trades":
        return  # bỏ qua subscriptions ack và heartbeat

    msg_seq = message.get("sequence_num")
    # events[].type = "snapshot" (~100 trade cũ gửi lúc subscribe) hoặc "update"
    trades = [
        (event.get("type") == "snapshot", trade)
        for event in message.get("events", [])
        for trade in event.get("trades", [])
    ]
    batch_size = len(trades)
    if batch_size == 0:
        return

    for is_snapshot, trade in trades:
        product_id = trade["product_id"]
        writer.write(
            make_event(
                source=SOURCE,
                symbol_native=product_id,
                symbol_canonical=canonical_coinbase(product_id),
                event_type="trade",
                event_time=iso_to_iso(trade["time"]),
                ingest_time=ingest_time,
                sequence=int(trade["trade_id"]),
                payload=trade,
                transport={
                    "batch_size": batch_size,
                    "raw_bytes": raw_bytes,
                    "msg_seq": msg_seq,
                    "is_snapshot": is_snapshot,
                },
            )
        )


async def run(products, writer: RawWriter, ops: OpsLog, stop: asyncio.Event) -> None:
    subscribe = json.dumps(
        {"type": "subscribe", "product_ids": list(products), "channel": "market_trades"}
    )
    backoff = 1.0

    while not stop.is_set():
        try:
            async with websockets.connect(
                WS_URL, ping_interval=20, ping_timeout=60, max_queue=4096
            ) as ws:
                await ws.send(subscribe)
                ops.event("connected", products=len(products))
                backoff = 1.0
                while not stop.is_set():
                    raw = await asyncio.wait_for(ws.recv(), timeout=IDLE_TIMEOUT)
                    _handle(raw, writer)

        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            ops.event("idle_timeout", seconds=IDLE_TIMEOUT)
        except Exception as exc:  # noqa: BLE001
            ops.event("disconnected", error=repr(exc))

        if stop.is_set():
            break
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF)