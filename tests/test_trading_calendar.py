"""Freshness theo lịch giao dịch (OB-5) và lịch poll vnstock."""

from __future__ import annotations

import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from observability.slo.trading_calendar import TradingCalendar, trading_freshness_rows

ROOT = Path(__file__).resolve().parents[1]
VN = timezone(timedelta(hours=7))


def vn(y, m, d, hh, mm=0) -> datetime:
    return datetime(y, m, d, hh, mm, tzinfo=VN)


class CalendarTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cal = TradingCalendar.load()

    def test_open_hours(self):
        self.assertTrue(self.cal.is_open(vn(2026, 11, 23, 10)))        # thứ Hai 10:00
        self.assertFalse(self.cal.is_open(vn(2026, 11, 23, 12)))       # nghỉ trưa
        self.assertFalse(self.cal.is_open(vn(2026, 11, 22, 10)))       # Chủ nhật
        self.assertFalse(self.cal.is_open(vn(2026, 9, 2, 10)))         # Quốc khánh
        self.assertFalse(self.cal.is_open(vn(2026, 11, 23, 14, 45)))   # đúng 14:45 đã đóng

    def test_lunch_weekend_holiday_add_no_trading_seconds(self):
        self.assertEqual(self.cal.trading_seconds_between(vn(2026, 11, 23, 11, 30), vn(2026, 11, 23, 13)), 0)
        self.assertEqual(self.cal.trading_seconds_between(vn(2026, 11, 20, 14, 45), vn(2026, 11, 23, 9)), 0)
        self.assertEqual(self.cal.trading_seconds_between(vn(2026, 9, 1, 9), vn(2026, 9, 2, 15)), 0)

    def test_across_lunch_counts_only_session_time(self):
        # 11:20 -> 13:10 = 10 phút trước nghỉ trưa + 10 phút sau
        self.assertEqual(self.cal.trading_seconds_between(vn(2026, 11, 23, 11, 20), vn(2026, 11, 23, 13, 10)), 1200)

    def test_freshness_rows_not_late_over_weekend(self):
        last = vn(2026, 11, 20, 14, 44)  # trade cuối chiều thứ Sáu
        rows = trading_freshness_rows({("vnstock", "FPT-VND"): last}, vn(2026, 11, 22, 20), ["FPT-VND"], self.cal)
        self.assertEqual(rows[0]["value"], 60.0)  # chỉ 1 phút giao dịch, không phải 2 ngày

    def test_freshness_without_trade_during_session(self):
        rows = trading_freshness_rows({}, vn(2026, 11, 23, 9, 30), ["HPG-VND"], self.cal)
        self.assertEqual(rows[0]["value"], 1800.0)  # chưa thấy trade nào từ 09:00
        closed = trading_freshness_rows({}, vn(2026, 11, 22, 10), ["HPG-VND"], self.cal)
        self.assertEqual(closed[0]["status"], "no_data")


class VnstockScheduleTest(unittest.TestCase):
    def test_next_session(self):
        sys.path.insert(0, str(ROOT / "platform" / "ingest"))
        import vnstock_poll

        self.assertEqual(vnstock_poll.next_session_start(vn(2026, 11, 23, 12)), vn(2026, 11, 23, 13))
        self.assertEqual(vnstock_poll.next_session_start(vn(2026, 11, 21, 10)), vn(2026, 11, 23, 9))  # thứ 7
        self.assertEqual(vnstock_poll.next_session_start(vn(2026, 11, 23, 15)), vn(2026, 11, 24, 9))


if __name__ == "__main__":
    unittest.main()
