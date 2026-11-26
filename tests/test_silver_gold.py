"""Logic silver/gold (DE-6, DE-7) trên Spark local. Chạy trong image finobs-spark: make test-spark."""

from __future__ import annotations

import json
import sys
import unittest
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "platform" / "streaming"))

try:
    from pyspark.sql import SparkSession

    HAS_SPARK = True
except ImportError:
    HAS_SPARK = False

if HAS_SPARK:
    import silver_gold as sg  # lỗi import code thật phải làm test FAIL, không được bị nuốt thành skip


def fixture(source: str) -> list[dict]:
    path = ROOT / "tests" / "fixtures" / "raw" / source / "sample.jsonl"
    return [json.loads(x) for x in path.read_text(encoding="utf-8").splitlines() if x.strip()]


def ts(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)


@unittest.skipUnless(HAS_SPARK, "cần pyspark (chạy trong image finobs-spark: make test-spark)")
class SilverGoldTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[1]").appName("test-silver-gold")
                     .config("spark.sql.session.timeZone", "UTC")
                     # spark-defaults của image trỏ catalog mặc định vào Iceberg REST; test logic thuần thì không cần
                     .config("spark.sql.defaultCatalog", "spark_catalog")
                     .config("spark.sql.shuffle.partitions", "2").getOrCreate())

    def bronze(self, envelopes: list[dict]):
        rows = [(e["source"], e["symbol_native"], e["symbol_canonical"], e["event_id"], ts(e["event_time"]),
                 ts(e["ingest_time"]), json.dumps(e), f"raw.{e['source']}.trade", 0, i)
                for i, e in enumerate(envelopes)]
        return self.spark.createDataFrame(rows, "source string, symbol string, symbol_canonical string, "
                                                "event_id string, event_ts timestamp, ingest_ts timestamp, "
                                                "payload string, kafka_topic string, kafka_partition int, "
                                                "kafka_offset bigint")

    def test_parse_three_sources(self):
        envs = fixture("binance")[:1] + fixture("coinbase")[:1] + fixture("kraken")[:1]
        rows = {r["source"]: r for r in sg.parse_bronze(self.bronze(envs)).collect()}
        for env in envs:
            row = rows[env["source"]]
            price = env["payload"].get("p") or env["payload"]["price"]
            self.assertEqual(row["price"], Decimal(price))
            self.assertEqual(row["trade_id"], env["sequence"])
            self.assertIn(row["side"], ("buy", "sell"))
        for env in envs:  # USDT, USD đều quy về BASE/USD
            base = env["symbol_canonical"].split("-")[0]
            self.assertEqual(rows[env["source"]]["symbol_std"], f"{base}/USD")
        self.assertEqual(rows["binance"]["quote_asset"], "USDT")
        self.assertEqual(rows["kraken"]["quote_asset"], "USD")

    def test_binance_side_is_taker(self):
        env = fixture("binance")[0]
        env["payload"]["m"] = True  # người mua là maker -> taker bán
        self.assertEqual(sg.parse_bronze(self.bronze([env])).first()["side"], "sell")

    def test_late_rows_dropped_and_duplicates_removed(self):
        base = fixture("kraken")[0]
        dup = json.loads(json.dumps(base))
        dup["ingest_time"] = (ts(base["ingest_time"]) + timedelta(milliseconds=5)).isoformat()[:23] + "Z"
        late = json.loads(json.dumps(fixture("kraken")[1]))
        late["ingest_time"] = (ts(late["event_time"]) + timedelta(minutes=11)).isoformat()[:23] + "Z"
        keep, stats = sg.clean(sg.parse_bronze(self.bronze([base, dup, late])))
        self.assertEqual(keep.count(), 1)
        self.assertEqual({(r["source"], r["_drop"]): r["count"] for r in stats.collect()}, {("kraken", "late"): 1})
        self.assertEqual(keep.first()["ingest_ts"], ts(base["ingest_time"]))  # giữ bản nhận sớm nhất

    def test_ohlcv_and_vwap(self):
        minute = datetime(2026, 11, 25, 10, 0)
        rows = [("kraken", "BTC/USD", "BTC-USD", minute + timedelta(seconds=s), tid, Decimal(p), Decimal(q))
                for s, tid, p, q in [(1, 1, "100", "1"), (20, 2, "110", "3"), (40, 3, "90", "1")]]
        df = self.spark.createDataFrame(rows, f"source string, symbol_std string, symbol_canonical string, "
                                              f"event_ts timestamp, trade_id bigint, price {sg.DEC}, qty {sg.DEC}")
        bar = sg.ohlcv(df).first()
        self.assertEqual((bar["open"], bar["high"], bar["low"], bar["close"]),
                         (Decimal(100), Decimal(110), Decimal(90), Decimal(90)))
        self.assertEqual((bar["trade_count"], bar["volume"]), (3, Decimal(5)))
        self.assertEqual(bar["vwap"], Decimal(520) / Decimal(5))

    def test_spread_three_and_two_sources(self):
        minute = datetime(2026, 11, 25, 10, 0)
        rows = [("binance", "BTC/USD", minute, Decimal("100.0")), ("coinbase", "BTC/USD", minute, Decimal("100.5")),
                ("kraken", "BTC/USD", minute, Decimal("99.5")), ("binance", "ETH/USD", minute, Decimal("10")),
                ("kraken", "ETH/USD", minute, Decimal("10.1")), ("binance", "ONE/USD", minute, Decimal("1"))]
        df = self.spark.createDataFrame(rows, f"source string, symbol_std string, minute timestamp, close {sg.DEC}")
        out = {r["symbol_std"]: r for r in sg.spread(df).collect()}
        self.assertNotIn("ONE/USD", out)  # chỉ 1 sàn: không có spread
        self.assertEqual(out["BTC/USD"]["median_price"], Decimal("100.0"))
        self.assertAlmostEqual(out["BTC/USD"]["spread_bps"], 100.0)
        self.assertEqual(out["ETH/USD"]["n_sources"], 2)
        self.assertAlmostEqual(out["ETH/USD"]["spread_bps"], 0.1 / 10.05 * 10_000)


if __name__ == "__main__":
    unittest.main()
