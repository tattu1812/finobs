"""
Chạy song song các recorder trong N phút rồi dừng gọn gàng.

    python record.py --minutes 5                 # chạy thử
    python record.py --minutes 60                # thu chính thức
    python record.py --minutes 60 --sources binance

Ctrl+C bất cứ lúc nào cũng dừng sạch, file vẫn đọc được.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
from pathlib import Path

import binance_ws
import coinbase_ws
import vnstock_poll
from common import OpsLog, RawWriter

HEARTBEAT_SECONDS = 15


async def _heartbeat(writers: dict, stop: asyncio.Event) -> None:
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
            rate = delta / HEARTBEAT_SECONDS
            parts.append(f"{name}={writer.count} ({rate:.1f}/s)")
        print("[tick] " + "  ".join(parts), flush=True)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--minutes", type=float, default=60.0)
    parser.add_argument("--out", default="data/raw")
    parser.add_argument("--symbols", default="symbols.json")
    parser.add_argument("--sources", default="binance,coinbase")
    args = parser.parse_args()

    config = json.loads(Path(args.symbols).read_text(encoding="utf-8"))
    sources = [s.strip() for s in args.sources.split(",") if s.strip()]

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass  # Windows: dựa vào KeyboardInterrupt bên dưới

    writers: dict = {}
    tasks = []
    runners = {
        "binance": binance_ws.run,
        "coinbase": coinbase_ws.run,
        "vnstock": vnstock_poll.run,  # ngoài giờ HOSE thì tự kết thúc, nguồn khác vẫn chạy
    }

    for name in sources:
        if name not in runners:
            raise SystemExit(f"Nguồn không hỗ trợ: {name}")
        writer = RawWriter(args.out, name)
        writers[name] = writer
        ops = OpsLog(args.out, name)
        tasks.append(asyncio.create_task(runners[name](config[name], writer, ops, stop)))

    tasks.append(asyncio.create_task(_heartbeat(writers, stop)))

    print(
        f"Thu {args.minutes} phút từ: {', '.join(sources)} -> {args.out}",
        flush=True,
    )

    try:
        await asyncio.wait_for(stop.wait(), timeout=args.minutes * 60)
    except asyncio.TimeoutError:
        stop.set()
    except KeyboardInterrupt:
        stop.set()

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    for name, writer in writers.items():
        writer.close()
        print(f"{name}: {writer.count} sự kiện", flush=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass