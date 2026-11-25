"""ingest_delay_p95_ms, last_live_trade_age_seconds và vòng collector chạy liên tục."""

from __future__ import annotations

import gzip
import io
import json
import tempfile
import unittest
import zlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

from observability.collectors import freshness
from observability.collectors.run_loop import IncrementalReader, compute, trade_count_rows

ROOT = Path(__file__).resolve().parents[1]
T0 = datetime(2026, 10, 27, 2, 49, tzinfo=timezone.utc)


def trade(source: str, symbol: str, received: datetime, delay_ms: float, snapshot: bool = False):
    event = received - timedelta(milliseconds=delay_ms)
    return received, {
        "source": source, "symbol_canonical": symbol,
        "event_time": event.strftime("%Y-%m-%dT%H:%M:%S.") + f"{event.microsecond // 1000:03d}Z",
        "transport": {"is_snapshot": snapshot},
    }


def world(*trades) -> dict:
    return {f"id{i}": t for i, t in enumerate(trades)}


class NearestRankTest(unittest.TestCase):
    def test_nearest_rank(self):
        values = list(range(1, 101))
        self.assertEqual(freshness.nearest_rank(values, 0.95), 95)
        self.assertEqual(freshness.nearest_rank([7], 0.95), 7)
        self.assertEqual(freshness.nearest_rank([10, 1, 5], 0.95), 10)


class DelayTest(unittest.TestCase):
    def test_p95_per_minute_and_snapshot_excluded(self):
        first = world(*(trade("binance", "BTC-USDT", T0 + timedelta(seconds=i), d)
                        for i, d in enumerate([5, 6, 7, 8, 100])),
                      trade("binance", "BTC-USDT", T0 + timedelta(seconds=30), 99999, snapshot=True))
        rows = freshness.ingest_delay_p95(first)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["value"], 100.0)
        self.assertEqual(rows[0]["sample_count"], 5)
        self.assertEqual(rows[0]["window_end"], "2026-10-27T02:50:00Z")

    def test_negative_delay_is_kept(self):
        rows = freshness.ingest_delay_p95(world(trade("binance", "ETH-USDT", T0, -40)))
        self.assertEqual(rows[0]["value"], -40.0)

    def test_expected_pair_without_trades_is_no_data(self):
        end = T0 + timedelta(minutes=1)
        rows = freshness.ingest_delay_p95({}, end, {("coinbase", "BTC-USD")})
        self.assertEqual((rows[0]["value"], rows[0]["status"]), (None, "no_data"))


class AgeTest(unittest.TestCase):
    def test_age_uses_latest_live_trade_before_observed_at(self):
        observed = T0 + timedelta(minutes=1)
        first = world(trade("coinbase", "BTC-USD", T0 + timedelta(seconds=10), 150),
                      trade("coinbase", "BTC-USD", T0 + timedelta(seconds=45), 150),
                      trade("coinbase", "BTC-USD", T0 + timedelta(seconds=50), 150, snapshot=True),
                      trade("coinbase", "BTC-USD", observed + timedelta(seconds=1), 150))
        rows = freshness.last_live_trade_age(first, observed, {("coinbase", "ETH-USD")})
        by_symbol = {r["dimensions"]["symbol_canonical"]: r for r in rows}
        self.assertEqual(by_symbol["BTC-USD"]["value"], 15.0)
        self.assertIsNone(by_symbol["BTC-USD"]["window_start"])
        self.assertEqual(by_symbol["ETH-USD"]["status"], "no_data")


class RunLoopTest(unittest.TestCase):
    def test_zero_when_source_active_and_unavailable_when_silent(self):
        end = T0 + timedelta(minutes=1)
        first = world(trade("binance", "BTC-USDT", T0 + timedelta(seconds=3), 7))
        expected = {("binance", "BTC-USDT"), ("binance", "ETH-USDT"), ("coinbase", "BTC-USD")}
        rows = {(r["dimensions"]["source"], r["dimensions"]["symbol_canonical"]): r
                for r in trade_count_rows(first, end, expected)}
        self.assertEqual(rows[("binance", "BTC-USDT")]["value"], 1)
        self.assertEqual((rows[("binance", "ETH-USDT")]["value"], rows[("binance", "ETH-USDT")]["status"]), (0, "ok"))
        self.assertEqual(rows[("coinbase", "BTC-USD")]["status"], "source_unavailable")
        self.assertIsNone(rows[("coinbase", "BTC-USD")]["value"])

    def test_compute_emits_three_metrics_per_recomputed_minute(self):
        end = T0 + timedelta(minutes=2)
        first = world(trade("binance", "BTC-USDT", T0 + timedelta(seconds=70), 7))
        rows = compute(first, end, 2, {("binance", "BTC-USDT")})
        names = sorted({r["metric_name"] for r in rows})
        self.assertEqual(names, ["ingest_delay_p95_ms", "last_live_trade_age_seconds", "trade_count_1m"])
        self.assertEqual(len(rows), 6)

    def test_incremental_reader_handles_file_being_written(self):
        fixture = (ROOT / "tests" / "fixtures" / "raw" / "binance" / "sample.jsonl").read_text(encoding="utf-8")
        lines = [line for line in fixture.splitlines() if line.strip()]
        now = datetime.now(timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "binance" / now.strftime("%Y-%m-%d") / f"{now:%H}.jsonl.gz"
            path.parent.mkdir(parents=True)
            # Mô phỏng RawWriter: gzip mở chế độ append, flush giữa chừng, chưa đóng file.
            fh = gzip.open(path, "at", encoding="utf-8")
            fh.write(lines[0] + "\n")
            fh.flush()
            reader = IncrementalReader(Path(tmp), keep_minutes=10**7)
            reader.poll(now)
            self.assertEqual(reader.stats["records_read"], 1)

            for line in lines[1:]:
                fh.write(line + "\n")
            fh.flush()
            reader.poll(now)
            fh.close()
            reader.poll(now)  # đọc lại sau khi đóng: không được đếm trùng

        self.assertEqual(reader.stats["records_read"], len(lines))
        self.assertEqual(len(reader.first), len({json.loads(x)["event_id"] for x in lines}))

    def test_corrupted_file_does_not_crash_reader(self):
        """Tái hiện lỗi gặp khi chạy thật: tiến trình ghi bị giết giữa khối nén,
        tiến trình mới nối member gzip mới vào cùng file -> zlib.error khi đọc."""
        fixture = (ROOT / "tests" / "fixtures" / "raw" / "binance" / "sample.jsonl").read_text(encoding="utf-8")
        body = ("\n".join(line for line in fixture.splitlines() if line.strip()) + "\n").encode() * 50
        now = datetime.now(timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp) / "binance" / now.strftime("%Y-%m-%d")
            folder.mkdir(parents=True)
            # Cắt member đầu ngay sau header (giữa khối deflate) rồi nối member mới:
            # đúng dạng hỏng đã gặp, gzip đọc thẳng sẽ ném zlib.error.
            broken = gzip.compress(body)[:20] + gzip.compress(body)
            with self.assertRaises(zlib.error):
                sum(1 for _ in gzip.open(io.BytesIO(broken), "rt"))
            (folder / f"{now:%H}.jsonl.gz").write_bytes(broken)
            reader = IncrementalReader(Path(tmp), keep_minutes=10**7)
            reader.poll(now)  # phải dừng êm ở đoạn hỏng
        self.assertEqual(reader.stats["records_read"], 0)


class RawWriterPartTest(unittest.TestCase):
    def test_file_per_process_never_appends_to_existing_file(self):
        import sys

        sys.path.insert(0, str(ROOT / "platform" / "ingest"))
        from common import RawWriter

        with tempfile.TemporaryDirectory() as tmp:
            writer = RawWriter(tmp, "binance", file_per_process=True)
            writer.write({"x": 1})
            writer.close()
            names = [p.name for p in Path(tmp).rglob("*.jsonl.gz")]
        self.assertEqual(len(names), 1)
        self.assertRegex(names[0], r"^[0-9]{2}\.[0-9]+\.jsonl\.gz$")


if __name__ == "__main__":
    unittest.main()
