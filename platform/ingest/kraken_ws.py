"""
Recorder Kraken — WebSocket API v2, kênh trade (nguồn thứ ba cho đối chứng chéo C1).

Đặc điểm nguồn này (quan sát trực tiếp ngày 2026-11-25):
  - Một message chứa 1..N trade (thấy tới 8), giống Coinbase -> ghi từng trade, giữ batch_size
  - price / qty là SỐ JSON, không phải chuỗi. Đọc bằng json.loads thường sẽ thành float, mất số
    0 ở cuối và có thể sai làm tròn. Ta đọc nguyên văn bằng parse_float=str rồi lưu chuỗi đó:
    không đổi giá trị, giữ quy ước "giá là string" của contract
  - trade_id là số nguyên tăng dần theo từng mã -> dùng làm sequence (phát hiện khoảng hở được)
  - Không có số thứ tự message -> msg_seq = null
  - Mã viết BASE/QUOTE (BTC/USD); v2 dùng BTC, DOGE (REST v1 cũ gọi là XBT, XDG)
  - Kraken có lúc gửi trùng nguyên một message: bản trùng giữ nguyên ở raw/bronze,
    silver khử trùng theo (source, trade_id)
  - Có heartbeat mỗi giây và message status khi kết nối: bỏ qua
"""

from __future__ import annotations

import asyncio
import json

import websockets

from common import (
    OpsLog,
    RawWriter,
    canonical_kraken,
    iso_to_iso,
    make_event,
    utc_now_iso,
)

SOURCE = "kraken"
WS_URL = "wss://ws.kraken.com/v2"

MAX_BACKOFF = 30.0
IDLE_TIMEOUT = 120.0
DECIMAL_FIELDS = ("price", "qty")


def parse(raw) -> dict:
    """Số thực giữ nguyên văn dạng chuỗi; số nguyên ở trường giá/khối lượng cũng đổi sang chuỗi."""
    message = json.loads(raw, parse_float=str)
    for trade in message.get("data", []) if isinstance(message.get("data"), list) else []:
        for key in DECIMAL_FIELDS:
            if isinstance(trade.get(key), int):
                trade[key] = str(trade[key])
    return message


def _handle(raw, writer: RawWriter) -> None:
    ingest_time = utc_now_iso()
    raw_bytes = len(raw) if isinstance(raw, (bytes, bytearray)) else len(raw.encode())

    message = parse(raw)
    if message.get("channel") != "trade":
        return  # heartbeat, status, ack subscribe

    is_snapshot = message.get("type") == "snapshot"
    trades = message.get("data", [])
    batch_size = len(trades)
    for trade in trades:
        symbol = trade["symbol"]
        writer.write(
            make_event(
                source=SOURCE,
                symbol_native=symbol,
                symbol_canonical=canonical_kraken(symbol),
                event_type="trade",
                event_time=iso_to_iso(trade["timestamp"]),
                ingest_time=ingest_time,
                sequence=int(trade["trade_id"]),
                payload=trade,
                transport={
                    "batch_size": batch_size,
                    "raw_bytes": raw_bytes,
                    "msg_seq": None,
                    "is_snapshot": is_snapshot,
                },
            )
        )


async def run(symbols, writer: RawWriter, ops: OpsLog, stop: asyncio.Event) -> None:
    subscribe = json.dumps({
        "method": "subscribe",
        # snapshot=False: không xin lịch sử khi subscribe; bản ghi nào có type=snapshot vẫn được đánh dấu
        "params": {"channel": "trade", "symbol": list(symbols), "snapshot": False},
    })
    backoff = 1.0

    while not stop.is_set():
        try:
            async with websockets.connect(
                WS_URL, ping_interval=20, ping_timeout=60, max_queue=4096
            ) as ws:
                await ws.send(subscribe)
                ops.event("connected", symbols=len(symbols))
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
