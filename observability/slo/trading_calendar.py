"""Freshness theo lịch giao dịch (FR-15, Sprint 3 OB-5; đóng góp C2).

Ý tưởng: độ "cũ" của dữ liệu cổ phiếu tính bằng SỐ GIÂY GIAO DỊCH đã trôi qua kể từ trade cuối,
không phải giây đồng hồ. Nghỉ trưa, cuối tuần, ngày lễ không cộng thêm giây nào, nên không báo trễ
sai lúc thị trường đóng cửa, điều freshness thông thường không làm được.

    cal = TradingCalendar.load()                     # config/trading_calendar.yaml
    cal.is_open(ts, "HOSE")
    cal.trading_seconds_between(last_trade, now, "HOSE")

Metric trading_freshness_seconds{source, symbol_canonical} được collector ghi mỗi phút.
"""

from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CALENDAR_FILE = ROOT / "config" / "trading_calendar.yaml"
SYMBOLS_FILE = ROOT / "config" / "symbols.json"
METRIC = "trading_freshness_seconds"


class TradingCalendar:
    def __init__(self, sessions: dict[str, list[tuple[time, time]]], weekdays: dict[str, set[int]],
                 holidays: set[date], offset_hours: int = 7):
        self.sessions = sessions
        self.weekdays = weekdays
        self.holidays = holidays
        self.tz = timezone(timedelta(hours=offset_hours))

    @classmethod
    def load(cls, path: Path = CALENDAR_FILE) -> TradingCalendar:
        import yaml

        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        sessions, weekdays = {}, {}
        for name, spec in doc["exchanges"].items():
            sessions[name] = [(time.fromisoformat(a), time.fromisoformat(b)) for a, b in spec["sessions"]]
            weekdays[name] = set(spec.get("weekdays", [0, 1, 2, 3, 4]))
        holidays = {h["date"] if isinstance(h["date"], date) else date.fromisoformat(h["date"])
                    for h in doc.get("holidays", [])}
        return cls(sessions, weekdays, holidays, doc.get("timezone_offset_hours", 7))

    def is_trading_day(self, day: date, exchange: str = "HOSE") -> bool:
        return day.weekday() in self.weekdays[exchange] and day not in self.holidays

    def _intervals(self, day: date, exchange: str):
        if not self.is_trading_day(day, exchange):
            return []
        return [(datetime.combine(day, a, self.tz), datetime.combine(day, b, self.tz))
                for a, b in self.sessions[exchange]]

    def is_open(self, ts: datetime, exchange: str = "HOSE") -> bool:
        local = ts.astimezone(self.tz)
        return any(a <= local < b for a, b in self._intervals(local.date(), exchange))

    def trading_seconds_between(self, start: datetime, end: datetime, exchange: str = "HOSE") -> float:
        """Số giây nằm trong phiên giao dịch thuộc khoảng [start, end)."""
        if end <= start:
            return 0.0
        total = 0.0
        day = start.astimezone(self.tz).date()
        last = end.astimezone(self.tz).date()
        while day <= last:
            for a, b in self._intervals(day, exchange):
                lo, hi = max(a, start), min(b, end)
                if hi > lo:
                    total += (hi - lo).total_seconds()
            day += timedelta(days=1)
        return total


_CALENDAR: TradingCalendar | None = None


def calendar() -> TradingCalendar:
    global _CALENDAR
    if _CALENDAR is None:
        _CALENDAR = TradingCalendar.load()
    return _CALENDAR


def vn_symbols(path: Path = SYMBOLS_FILE) -> list[str]:
    config = json.loads(path.read_text(encoding="utf-8"))
    return [f"{s.upper()}-VND" for s in config.get("vnstock", [])]


def trading_freshness_rows(last_live: dict, observed_at: datetime, symbols: list[str] | None = None,
                           cal: TradingCalendar | None = None, exchange: str = "HOSE") -> list[dict]:
    """last_live: {(source, symbol_canonical): ingest_time của trade live mới nhất}.

    Mã chưa có trade nào kể từ khi collector chạy: nếu thị trường đang đóng -> no_data;
    nếu đang mở thì tính từ đầu phiên hôm nay (chưa thấy trade nào trong phiên là đang trễ)."""
    cal = cal or calendar()
    symbols = symbols if symbols is not None else vn_symbols()
    rows = []
    for symbol in symbols:
        last = last_live.get(("vnstock", symbol))
        if last is None:
            local = observed_at.astimezone(cal.tz)
            opens = [a for a, _ in cal._intervals(local.date(), exchange) if a <= local]
            last = opens[0] if opens else None
        value = round(cal.trading_seconds_between(last, observed_at, exchange), 3) if last else None
        rows.append({
            "metric_name": METRIC,
            "dimensions": {"source": "vnstock", "symbol_canonical": symbol},
            "observed_at": observed_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "window_start": None, "window_end": None,
            "value": value, "status": "ok" if value is not None else "no_data", "sample_count": None,
        })
    return rows
