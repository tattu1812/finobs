"""
Lõi dùng chung cho các recorder.

Ba thứ ở đây:
  - make_event(): đóng vỏ (envelope) chuẩn cho mọi nguồn
  - RawWriter:    ghi NDJSON nén gzip, tự xoay file theo giờ
  - OpsLog:       nhật ký vận hành (connect / disconnect / error)

Nguyên tắc: recorder KHÔNG biến đổi dữ liệu. Payload gốc của sàn được
giữ nguyên trong trường "payload" để sau này parse lại được.
"""

from __future__ import annotations

import gzip
import json
import threading
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_VERSION = "1.0.0"

# Dùng để tách BTCUSDT -> BTC-USDT. Xếp mã dài trước để khớp đúng.
_QUOTE_ASSETS = ("FDUSD", "USDT", "USDC", "BUSD", "TUSD", "BNB", "BTC", "ETH")


# --------------------------------------------------------------------------
# Thời gian: luôn UTC, ISO 8601, chính xác tới mili giây
# --------------------------------------------------------------------------

def _fmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def utc_now_iso() -> str:
    """Thời điểm hiện tại — dùng cho ingest_time."""
    return _fmt(datetime.now(timezone.utc))


def ms_to_iso(ms) -> str:
    """Epoch mili giây (Binance) -> chuỗi ISO."""
    return _fmt(datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc))


def iso_to_iso(value: str) -> str:
    """Chuỗi ISO của Coinbase (micro giây) -> chuẩn hoá về mili giây."""
    text = value.replace("Z", "+00:00")
    return _fmt(datetime.fromisoformat(text).astimezone(timezone.utc))


# --------------------------------------------------------------------------
# Chuẩn hoá mã
# --------------------------------------------------------------------------

def canonical_binance(symbol: str) -> str:
    s = symbol.upper()
    for quote in _QUOTE_ASSETS:
        if s.endswith(quote) and len(s) > len(quote):
            return f"{s[:-len(quote)]}-{quote}"
    return s


def canonical_coinbase(product_id: str) -> str:
    return product_id.upper()


# --------------------------------------------------------------------------
# Envelope
# --------------------------------------------------------------------------

def make_event(
    *,
    source: str,
    symbol_native: str,
    symbol_canonical: str,
    event_type: str,
    event_time: str,
    ingest_time: str,
    sequence,
    payload: dict,
    transport: dict,
) -> dict:
    """
    transport chứa thông tin tầng vận chuyển, KHÔNG phải dữ liệu thị trường:
      batch_size — số trade nằm chung một message (Binance = 1, Coinbase >= 1)
      raw_bytes  — kích thước message gốc, để tính băng thông
      msg_seq    — số thứ tự message của sàn, nếu có
      is_snapshot — (Coinbase) trade cũ sàn gửi lại lúc subscribe, không phải realtime
    """
    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": f"{source}:{symbol_native}:{sequence}",
        "source": source,
        "symbol_native": symbol_native,
        "symbol_canonical": symbol_canonical,
        "event_type": event_type,
        "event_time": event_time,
        "ingest_time": ingest_time,
        "sequence": sequence,
        "transport": transport,
        "payload": payload,
    }


# --------------------------------------------------------------------------
# Ghi file
# --------------------------------------------------------------------------

class RawWriter:
    """Ghi NDJSON nén gzip vào <root>/<source>/<YYYY-MM-DD>/<HH>.jsonl.gz"""

    def __init__(self, root, source: str, flush_every: int = 200):
        self.root = Path(root)
        self.source = source
        self.flush_every = flush_every
        self.count = 0
        self._fh = None
        self._hour_key = None
        self._since_flush = 0
        self._lock = threading.Lock()

    def _open_for(self, now: datetime) -> None:
        if self._fh is not None:
            self._fh.close()
        path = (
            self.root
            / self.source
            / now.strftime("%Y-%m-%d")
            / f"{now.strftime('%H')}.jsonl.gz"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        # "at" = append text, không đè file nếu chạy lại trong cùng giờ
        self._fh = gzip.open(path, "at", encoding="utf-8")
        self._since_flush = 0

    def write(self, record: dict) -> None:
        now = datetime.now(timezone.utc)
        hour_key = now.strftime("%Y%m%d%H")
        line = json.dumps(record, separators=(",", ":"), ensure_ascii=False)
        with self._lock:
            if hour_key != self._hour_key:
                self._open_for(now)
                self._hour_key = hour_key
            self._fh.write(line + "\n")
            self.count += 1
            self._since_flush += 1
            if self._since_flush >= self.flush_every:
                self._fh.flush()
                self._since_flush = 0

    def close(self) -> None:
        with self._lock:
            if self._fh is not None:
                self._fh.flush()
                self._fh.close()
                self._fh = None


class OpsLog:
    """Nhật ký vận hành, ghi thẳng không nén cho dễ đọc khi debug."""

    def __init__(self, root, source: str):
        self.path = Path(root) / "_ops" / f"{source}.ops.jsonl"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def event(self, kind: str, **fields) -> None:
        record = {"ts": utc_now_iso(), "kind": kind, **fields}
        line = json.dumps(record, ensure_ascii=False)
        with self._lock:
            with open(self.path, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")
        print(f"[ops] {line}", flush=True)