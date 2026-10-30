"""
Recorder cổ phiếu VN30 — poll REST, nguồn VCI (Vietcap).

    python vnstock_poll.py --minutes 60
    python vnstock_poll.py --minutes 5 --interval 5 --out data/raw

Đây chính là endpoint mà vnstock gọi bên trong Quote(symbol, source='VCI')
.intraday(). Ta gọi thẳng thay vì qua thư viện vì intraday() biến đổi dữ
liệu (bỏ trường, đổi tên cột, giá -> float rồi chia 1000), trái nguyên tắc
giữ payload nguyên văn. source vẫn ghi là "vnstock" để thống nhất với kế hoạch.

Đặc điểm nguồn này:
  - KHÔNG có WebSocket: phải poll, mỗi lần lấy N lệnh khớp mới nhất của 1 mã
  - Các lần poll chồng lên nhau -> khử trùng theo (symbol, id).
    Không dùng (symbol, truncTime) vì truncTime chỉ tới giây, một giây có
    nhiều lệnh khớp -> sẽ vứt mất trade thật
  - id là bộ đếm chung toàn sàn, không liên tục theo mã -> không đo "đứt seq"
  - Phản hồi xếp mới nhất trước; ta ghi theo id tăng dần (payload không đổi)
  - Lần poll đầu của mỗi mã trả về trade cũ -> transport.is_snapshot = true
  - HOSE: 9:00-11:30 và 13:00-14:45 giờ VN, thứ 2-6. Ngoài giờ thì thoát.
    Không biết lịch nghỉ lễ: ngày lễ script vẫn poll, sẽ thấy 0 trade mới.
  - Giới hạn khoảng 180 request/phút -> giãn đều các request
"""

from __future__ import annotations

import argparse
import asyncio
import json
import urllib.error
import urllib.request
from collections import deque
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

from common import OpsLog, RawWriter, make_event, ms_to_iso, utc_now_iso

SOURCE = "vnstock"
URL = "https://trading.vietcap.com.vn/api/market-watch/LEData/getAll"
HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Origin": "https://trading.vietcap.com.vn",
    "Referer": "https://trading.vietcap.com.vn/",
    "User-Agent": "Mozilla/5.0",
}

VN_TZ = timezone(timedelta(hours=7))  # Việt Nam không có giờ mùa hè
SESSIONS = ((time(9, 0), time(11, 30)), (time(13, 0), time(14, 45)))

MAX_RPM = 180
PAGE_SIZE = 100
SEEN_PER_SYMBOL = 5000  # đủ lớn so với PAGE_SIZE, giữ bộ nhớ có giới hạn
MAX_BACKOFF = 30.0


def in_session(now: datetime | None = None) -> bool:
    now = (now or datetime.now(timezone.utc)).astimezone(VN_TZ)
    if now.weekday() >= 5:
        return False
    return any(start <= now.time() < end for start, end in SESSIONS)


def canonical_vn(symbol: str) -> str:
    return f"{symbol.upper()}-VND"


def _fetch(symbol: str, limit: int) -> bytes:
    body = json.dumps({"symbol": symbol, "limit": limit, "truncTime": None}).encode()
    req = urllib.request.Request(URL, data=body, method="POST", headers=HEADERS)
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.read()


class _Dedup:
    def __init__(self) -> None:
        self._order: dict[str, deque] = {}
        self._seen: dict[str, set] = {}

    def is_first(self, symbol: str) -> bool:
        return symbol not in self._seen

    def add(self, symbol: str, trade_id) -> bool:
        """True nếu id chưa gặp."""
        seen = self._seen.setdefault(symbol, set())
        order = self._order.setdefault(symbol, deque())
        if trade_id in seen:
            return False
        seen.add(trade_id)
        order.append(trade_id)
        if len(order) > SEEN_PER_SYMBOL:
            seen.discard(order.popleft())
        return True


def _handle(raw: bytes, ingest_time: str, symbol: str, dedup: _Dedup, writer: RawWriter,
            ops: OpsLog, *, poll_interval_s: float, limit: int, req_seq: int) -> int:
    rows = json.loads(raw)
    if isinstance(rows, dict):
        rows = rows.get("data") or []

    first = dedup.is_first(symbol)
    fresh = [row for row in sorted(rows, key=lambda r: int(r["id"])) if dedup.add(symbol, row["id"])]

    # Trả về đủ limit mà không trùng lần nào với lần trước -> có thể đã sót trade ở giữa
    if not first and rows and len(fresh) == len(rows) and len(rows) >= limit:
        ops.event("possible_gap", symbol=symbol, rows_returned=len(rows), limit=limit)

    for row in fresh:
        writer.write(
            make_event(
                source=SOURCE,
                symbol_native=row["symbol"],
                symbol_canonical=canonical_vn(row["symbol"]),
                event_type="trade",
                event_time=ms_to_iso(int(row["truncTime"]) * 1000),
                ingest_time=ingest_time,
                sequence=int(row["id"]),
                payload=row,
                transport={
                    "batch_size": len(fresh),
                    "raw_bytes": len(raw),
                    "msg_seq": req_seq,
                    "is_snapshot": first,
                    "poll_interval_s": poll_interval_s,
                    "rows_returned": len(rows),
                },
            )
        )
    return len(fresh)


async def run(symbols, writer: RawWriter, ops: OpsLog, stop: asyncio.Event,
              interval: float = 5.0, max_rpm: int = MAX_RPM, limit: int = PAGE_SIZE) -> None:
    if not in_session():
        ops.event("market_closed", vn_time=datetime.now(VN_TZ).strftime("%a %H:%M"))
        return

    # Mỗi mã được poll một lần sau mỗi `interval` giây, nhưng tổng không vượt max_rpm
    interval = max(interval, len(symbols) * 60.0 / max_rpm)
    gap = interval / len(symbols)
    ops.event("started", symbols=len(symbols), poll_interval_s=interval,
              rpm=round(60 / gap, 1))

    dedup = _Dedup()
    loop = asyncio.get_running_loop()
    req_seq = 0
    backoff = 1.0
    next_at = loop.time()

    while not stop.is_set():
        for symbol in symbols:
            if stop.is_set():
                break
            if not in_session():
                ops.event("market_closed", requests=req_seq,
                          vn_time=datetime.now(VN_TZ).strftime("%a %H:%M"))
                return

            delay = next_at - loop.time()
            if delay > 0:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                    break
                except asyncio.TimeoutError:
                    pass
            next_at = max(next_at + gap, loop.time())

            req_seq += 1
            try:
                raw = await asyncio.to_thread(_fetch, symbol, limit)
                ingest_time = utc_now_iso()  # ngay khi nhận xong, trước khi parse
                _handle(raw, ingest_time, symbol, dedup, writer, ops,
                        poll_interval_s=interval, limit=limit, req_seq=req_seq)
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — lỗi 1 mã không được làm chết cả vòng
                status = exc.code if isinstance(exc, urllib.error.HTTPError) else None
                ops.event("request_error", symbol=symbol, status=status, error=repr(exc))
                next_at = loop.time() + backoff
                backoff = min(backoff * 2, MAX_BACKOFF)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--minutes", type=float, default=60.0)
    parser.add_argument("--out", default="data/raw")
    parser.add_argument("--symbols", default="config/symbols.json")
    parser.add_argument("--interval", type=float, default=5.0, help="giây giữa 2 lần poll cùng 1 mã")
    parser.add_argument("--max-rpm", type=int, default=MAX_RPM)
    args = parser.parse_args()

    symbols = json.loads(Path(args.symbols).read_text(encoding="utf-8"))[SOURCE]
    writer = RawWriter(args.out, SOURCE)
    ops = OpsLog(args.out, SOURCE)
    stop = asyncio.Event()

    task = asyncio.create_task(run(symbols, writer, ops, stop, args.interval, args.max_rpm))
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=args.minutes * 60)
    except asyncio.TimeoutError:
        stop.set()
        await task
    finally:
        writer.close()
        print(f"{SOURCE}: {writer.count} sự kiện", flush=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
