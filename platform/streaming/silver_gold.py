"""
Silver và gold (FR-03, FR-04, Sprint 3: DE-6, DE-7).

    spark-submit silver_gold.py                                     # chạy liên tục (mặc định)
    spark-submit silver_gold.py --mode rebuild                      # tính lại toàn bộ từ bronze
    spark-submit silver_gold.py --mode rebuild --from 2026-11-27 --to 2026-11-28

Luồng:
  bronze.trades_raw --(silver_trades)--> silver.trades --(gold_ohlcv)--> gold.ohlcv_1m
                                                                      +-> gold.cross_exchange_spread
  Một Spark app, một streaming query đọc bronze; mỗi micro-batch làm cả silver lẫn gold.
  Lineage vẫn ghi hai job logic silver_trades và gold_ohlcv.

Silver: parse payload theo nguồn (binance, coinbase, kraken), giá/khối lượng DECIMAL(38,18),
side = phía taker, symbol_std = BASE/USD (USDT, USDC coi như USD, đặc tả Bảng 11).
  - Bỏ bản ghi trễ: ingest_ts - event_ts > 10 phút (watermark 10 phút của đặc tả). Quy tắc tính
    trên chính bản ghi nên kết quả không phụ thuộc cách chia micro-batch: stream và rebuild ra
    cùng một tập dòng. Số bản ghi trễ được đếm (metric finobs_silver_late_rows_total).
  - Khử trùng theo (source, symbol, trade_id) bằng MERGE ... WHEN NOT MATCHED THEN INSERT:
    idempotent, chạy lại bao nhiêu lần cũng không sinh bản trùng.
Gold: mỗi lần silver có dữ liệu mới, tính lại các phút bị ảnh hưởng từ silver rồi MERGE.
  Gold vì vậy luôn khớp đúng silver (kiểm bằng make gold-check), kể cả khi có dữ liệu trễ.
  spread: giá đóng cửa từng sàn trong phút, spread_bps = (max - min) / trung vị * 10 000.
  Binance báo giá theo USDT, hai sàn kia theo USD: spread gồm cả chênh lệch USDT/USD.

Rebuild (--mode rebuild) dùng đúng các hàm của chế độ stream, theo từng ngày; mặc định xóa
dữ liệu silver/gold của ngày đó trước khi tính lại. Dừng service silver-gold trước khi rebuild
(make reprocess làm việc này).

Phân vùng theo ngày (không theo mã + giờ như đặc tả): trigger 1 phút x 13 mã x 24 giờ sẽ sinh
khoảng 18 000 file/ngày. Iceberg vẫn lọc theo mã và thời gian nhờ thống kê min/max của file.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parent)]
from lineage import LineageEmitter  # noqa: E402
from progress_exporter import ProgressExporter  # noqa: E402

BRONZE = "iceberg.bronze.trades_raw"
SILVER = "iceberg.silver.trades"
OHLCV = "iceberg.gold.ohlcv_1m"
SPREAD = "iceberg.gold.cross_exchange_spread"
SYMBOL_MAP = "iceberg.ref.symbol_map"

CRYPTO_SOURCES = ("binance", "coinbase", "kraken")
USD_QUOTES = ("USD", "USDT", "USDC")
LATE_LIMIT_S = 600
DEC = "decimal(38,18)"

CHECKPOINT_ROOT = os.environ.get("CHECKPOINT_ROOT", "/checkpoints")
TRIGGER = f"{int(os.environ.get('TRIGGER_SECONDS', '60'))} seconds"
METRICS_PORT = int(os.environ.get("METRICS_PORT", "9108"))
MAX_FILES_PER_BATCH = os.environ.get("MAX_FILES_PER_BATCH", "200")
SYMBOLS_FILE = os.environ.get("SYMBOLS_FILE", "/opt/finobs/config/symbols.json")

DDL = {
    SILVER: f"""
        CREATE TABLE IF NOT EXISTS {SILVER} (
            source STRING, symbol STRING, symbol_canonical STRING,
            symbol_std STRING COMMENT 'BASE/USD; USDT, USDC coi như USD',
            quote_asset STRING COMMENT 'đồng định giá gốc (USDT, USD...)',
            trade_id BIGINT, price {DEC}, qty {DEC},
            side STRING COMMENT 'phía taker: buy | sell',
            is_snapshot BOOLEAN, event_id STRING, event_ts TIMESTAMP, ingest_ts TIMESTAMP,
            kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT,
            silver_ts TIMESTAMP)
        USING iceberg PARTITIONED BY (days(event_ts))
        TBLPROPERTIES ('format-version'='2', 'write.metadata.delete-after-commit.enabled'='true',
                       'write.metadata.previous-versions-max'='100')""",
    OHLCV: f"""
        CREATE TABLE IF NOT EXISTS {OHLCV} (
            source STRING, symbol_std STRING, symbol_canonical STRING, minute TIMESTAMP,
            open {DEC}, high {DEC}, low {DEC}, close {DEC},
            volume {DEC}, quote_volume DECIMAL(38,6), vwap {DEC},
            trade_count BIGINT, first_trade_id BIGINT, last_trade_id BIGINT, updated_at TIMESTAMP)
        USING iceberg PARTITIONED BY (days(minute))
        TBLPROPERTIES ('format-version'='2', 'write.metadata.delete-after-commit.enabled'='true',
                       'write.metadata.previous-versions-max'='100')""",
    SPREAD: f"""
        CREATE TABLE IF NOT EXISTS {SPREAD} (
            symbol_std STRING, minute TIMESTAMP,
            price_binance {DEC}, price_coinbase {DEC}, price_kraken {DEC},
            n_sources INT, median_price {DEC}, spread_bps DOUBLE, updated_at TIMESTAMP)
        USING iceberg PARTITIONED BY (days(minute))
        TBLPROPERTIES ('format-version'='2', 'write.metadata.delete-after-commit.enabled'='true',
                       'write.metadata.previous-versions-max'='100')""",
    SYMBOL_MAP: """
        CREATE TABLE IF NOT EXISTS iceberg.ref.symbol_map (
            source STRING, symbol_native STRING, symbol_canonical STRING,
            symbol_std STRING, quote_asset STRING)
        USING iceberg""",
}


# ---------------------------------------------------------------------- silver
def symbol_std_col(canonical):
    base = F.split(canonical, "-").getItem(0)
    quote = F.split(canonical, "-").getItem(1)
    return (F.when(quote.isin(*USD_QUOTES), F.concat(base, F.lit("/USD")))
            .otherwise(F.concat_ws("/", base, quote))), quote


def parse_bronze(bronze: DataFrame) -> DataFrame:
    """bronze.trades_raw -> cột silver (chưa lọc trễ, chưa khử trùng). Chỉ nguồn crypto."""
    env = F.col("payload")

    def j(path):
        return F.get_json_object(env, path)

    src = F.col("source")
    std, quote = symbol_std_col(F.col("symbol_canonical"))
    return (
        bronze.where(src.isin(*CRYPTO_SOURCES))
        .select(
            src.alias("source"), F.col("symbol"), F.col("symbol_canonical"),
            std.alias("symbol_std"), quote.alias("quote_asset"),
            F.when(src == "binance", j("$.payload.t")).otherwise(j("$.payload.trade_id"))
            .cast("bigint").alias("trade_id"),
            F.when(src == "binance", j("$.payload.p")).otherwise(j("$.payload.price")).cast(DEC).alias("price"),
            F.when(src == "binance", j("$.payload.q"))
            .when(src == "coinbase", j("$.payload.size"))
            .otherwise(j("$.payload.qty")).cast(DEC).alias("qty"),
            # Binance: m = true nghĩa là người mua là maker -> taker bán.
            F.when(src == "binance", F.when(j("$.payload.m") == "true", "sell").otherwise("buy"))
            .otherwise(F.lower(j("$.payload.side"))).alias("side"),
            F.coalesce(j("$.transport.is_snapshot") == "true", F.lit(False)).alias("is_snapshot"),
            F.col("event_id"), F.col("event_ts"), F.col("ingest_ts"),
            F.col("kafka_topic"), F.col("kafka_partition"), F.col("kafka_offset"),
        )
    )


def clean(parsed: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Trả (dòng giữ lại đã khử trùng trong lô, thống kê bỏ dòng theo nguồn)."""
    lateness = F.col("ingest_ts").cast("double") - F.col("event_ts").cast("double")
    marked = parsed.withColumn(
        "_drop",
        F.when(F.col("trade_id").isNull() | F.col("price").isNull() | F.col("qty").isNull()
               | F.col("event_ts").isNull(), "unparseable")
        .when(lateness > LATE_LIMIT_S, "late"),
    )
    stats = marked.where(F.col("_drop").isNotNull()).groupBy("source", "_drop").count()
    keep = marked.where(F.col("_drop").isNull()).drop("_drop")
    first = Window.partitionBy("source", "symbol", "trade_id").orderBy("ingest_ts", "kafka_offset")
    keep = (keep.withColumn("_rn", F.row_number().over(first)).where("_rn = 1").drop("_rn")
            .withColumn("silver_ts", F.current_timestamp()))
    return keep, stats


def _ts(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S.%f")


def merge_silver(spark: SparkSession, rows: DataFrame) -> tuple[datetime, datetime] | None:
    """MERGE chỉ chèn dòng chưa có -> idempotent. Trả khoảng event_ts của lô (để tính gold)."""
    bounds = rows.agg(F.min("event_ts"), F.max("event_ts")).first()
    if bounds is None or bounds[0] is None:
        return None
    lo, hi = bounds[0], bounds[1]
    # Trong foreachBatch, DataFrame thuộc một session riêng của micro-batch: temp view phải được
    # tạo và truy vấn trên CHÍNH session đó, không phải session ngoài.
    session = rows.sparkSession
    rows.createOrReplaceTempView("silver_batch")
    session.sql(f"""
        MERGE INTO {SILVER} t USING silver_batch s
        ON t.source = s.source AND t.symbol = s.symbol AND t.trade_id = s.trade_id
           AND t.event_ts = s.event_ts
           AND t.event_ts BETWEEN TIMESTAMP '{_ts(lo)}' AND TIMESTAMP '{_ts(hi)}'
        WHEN NOT MATCHED THEN INSERT *""")
    return lo, hi


# ---------------------------------------------------------------------- gold
def minute_floor(value: datetime) -> datetime:
    return value.replace(second=0, microsecond=0)


def ohlcv(silver: DataFrame) -> DataFrame:
    order = F.struct("event_ts", "trade_id")
    agg = (
        silver.groupBy("source", "symbol_std", "symbol_canonical",
                       F.date_trunc("minute", "event_ts").alias("minute"))
        .agg(
            F.min_by("price", order).alias("open"), F.max("price").alias("high"),
            F.min("price").alias("low"), F.max_by("price", order).alias("close"),
            F.sum("qty").alias("volume"),
            F.sum(F.col("price") * F.col("qty")).cast("decimal(38,6)").alias("quote_volume"),
            F.count(F.lit(1)).alias("trade_count"),
            F.min("trade_id").alias("first_trade_id"), F.max("trade_id").alias("last_trade_id"),
        )
    )
    return agg.select(
        "source", "symbol_std", "symbol_canonical", "minute", "open", "high", "low", "close",
        "volume", "quote_volume",
        (F.col("quote_volume") / F.col("volume")).cast(DEC).alias("vwap"),
        "trade_count", "first_trade_id", "last_trade_id",
        F.current_timestamp().alias("updated_at"),
    )


def spread(ohlcv_rows: DataFrame) -> DataFrame:
    def close_of(name):
        return F.max(F.when(F.col("source") == name, F.col("close"))).alias(f"price_{name}")

    pivot = (ohlcv_rows.groupBy("symbol_std", "minute")
             .agg(close_of("binance"), close_of("coinbase"), close_of("kraken"),
                  F.countDistinct("source").cast("int").alias("n_sources"))
             .where("n_sources >= 2"))
    prices = F.array_sort(F.filter(F.array("price_binance", "price_coinbase", "price_kraken"),
                                   lambda x: x.isNotNull()))
    median = F.when(F.size(prices) == 3, prices.getItem(1)).otherwise(
        (prices.getItem(0) + prices.getItem(1)) / 2)
    return (pivot.withColumn("median_price", median.cast(DEC))
            .withColumn("spread_bps", ((F.array_max(prices) - F.array_min(prices)).cast("double")
                                       / F.col("median_price").cast("double") * 10_000))
            .withColumn("updated_at", F.current_timestamp())
            .select("symbol_std", "minute", "price_binance", "price_coinbase", "price_kraken",
                    "n_sources", "median_price", "spread_bps", "updated_at"))


def recompute_gold(spark: SparkSession, lo: datetime, hi: datetime) -> None:
    """Tính lại gold cho mọi phút trong [floor(lo), floor(hi) + 1 phút) từ silver."""
    start, end = minute_floor(lo), minute_floor(hi) + timedelta(minutes=1)
    silver = spark.table(SILVER).where(f"event_ts >= TIMESTAMP '{_ts(start)}' AND event_ts < TIMESTAMP '{_ts(end)}'")
    ohlcv(silver).createOrReplaceTempView("ohlcv_batch")
    spark.sql(f"""
        MERGE INTO {OHLCV} t USING ohlcv_batch s
        ON t.source = s.source AND t.symbol_std = s.symbol_std AND t.minute = s.minute
           AND t.minute >= TIMESTAMP '{_ts(start)}' AND t.minute < TIMESTAMP '{_ts(end)}'
        WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
    gold = spark.table(OHLCV).where(f"minute >= TIMESTAMP '{_ts(start)}' AND minute < TIMESTAMP '{_ts(end)}'")
    spread(gold).createOrReplaceTempView("spread_batch")
    spark.sql(f"""
        MERGE INTO {SPREAD} t USING spread_batch s
        ON t.symbol_std = s.symbol_std AND t.minute = s.minute
           AND t.minute >= TIMESTAMP '{_ts(start)}' AND t.minute < TIMESTAMP '{_ts(end)}'
        WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")


# ---------------------------------------------------------------------- chung
def session(app: str) -> SparkSession:
    builder = SparkSession.builder.appName(app)
    if os.environ.get("MINIO_ROOT_USER"):
        builder = (builder.config("spark.sql.catalog.iceberg.s3.access-key-id", os.environ["MINIO_ROOT_USER"])
                   .config("spark.sql.catalog.iceberg.s3.secret-access-key", os.environ["MINIO_ROOT_PASSWORD"]))
    spark = builder.getOrCreate()
    for namespace in ("iceberg.silver", "iceberg.gold", "iceberg.ref"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    for ddl in DDL.values():
        spark.sql(ddl)
    return spark


def refresh_symbol_map(spark: SparkSession) -> None:
    """ref.symbol_map (đặc tả Bảng 11) dựng lại từ config/symbols.json mỗi lần khởi động."""
    try:
        config = json.loads(Path(SYMBOLS_FILE).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return
    rows = []
    for source in CRYPTO_SOURCES:
        for native in config.get(source, []):
            canonical = native.upper().replace("/", "-")
            if source == "binance":
                for quote in ("FDUSD", "USDT", "USDC", "BUSD", "TUSD", "BNB", "BTC", "ETH"):
                    if canonical.endswith(quote):
                        canonical = f"{canonical[:-len(quote)]}-{quote}"
                        break
            base, quote = canonical.split("-")
            std = f"{base}/USD" if quote in USD_QUOTES else f"{base}/{quote}"
            rows.append((source, native, canonical, std, quote))
    df = spark.createDataFrame(rows, "source string, symbol_native string, symbol_canonical string, "
                                     "symbol_std string, quote_asset string")
    df.writeTo(SYMBOL_MAP).overwritePartitions()


def iceberg_stream(spark: SparkSession, table: str, checkpoint: str) -> DataFrame:
    reader = (spark.readStream.format("iceberg")
              .option("streaming-skip-overwrite-snapshots", "true")
              .option("streaming-skip-delete-snapshots", "true")
              .option("streaming-max-files-per-micro-batch", MAX_FILES_PER_BATCH))
    marker = Path(CHECKPOINT_ROOT) / "silver_gold.rebuilt_at"
    if not (Path(checkpoint) / "offsets").exists() and marker.exists():
        # Checkpoint mất sau một lần rebuild: đọc lại từ trước mốc rebuild 15 phút (MERGE idempotent
        # nên phần chồng lấn không sinh bản trùng), thay vì quét lại toàn bộ lịch sử.
        start_ms = int(float(marker.read_text().strip()) * 1000) - 15 * 60 * 1000
        reader = reader.option("stream-from-timestamp", str(start_ms))
    return reader.load(table)


# ---------------------------------------------------------------------- chế độ chạy
def run_stream() -> None:
    spark = session("finobs-silver-gold")
    refresh_symbol_map(spark)
    exporter = ProgressExporter(port=METRICS_PORT).start()
    spark.streams.addListener(exporter.listener())
    lineage = LineageEmitter.from_env()
    silver_run = lineage.start("silver_trades", inputs=["bronze.trades_raw"], outputs=["silver.trades"], streaming=True)
    gold_run = lineage.start("gold_ohlcv", inputs=["silver.trades"],
                             outputs=["gold.ohlcv_1m", "gold.cross_exchange_spread"], streaming=True)

    def micro_batch(batch: DataFrame, batch_id: int) -> None:
        """Một micro-batch bronze: MERGE silver, rồi tính lại gold cho đúng các phút vừa đổi.
        Gold tính ngay trong cùng batch (không đọc silver bằng stream thứ hai): MERGE vào silver
        có thể tạo snapshot loại overwrite mà stream Iceberg bỏ qua."""
        rows, stats = clean(parse_bronze(batch))
        for row in stats.collect():
            exporter.inc(f"finobs_silver_{row['_drop']}_rows_total", row["count"], source=row["source"])
        rows = rows.cache()
        for row in rows.groupBy("source").count().collect():
            exporter.inc("finobs_silver_candidate_rows_total", row["count"], source=row["source"])
        bounds = merge_silver(spark, rows)
        rows.unpersist()
        if bounds:
            recompute_gold(spark, *bounds)
            exporter.set("finobs_gold_last_minute_timestamp_seconds",
                         minute_floor(bounds[1]).replace(tzinfo=timezone.utc).timestamp())

    checkpoint = f"{CHECKPOINT_ROOT}/silver_gold"
    (iceberg_stream(spark, BRONZE, checkpoint).writeStream.queryName("silver_gold")
     .foreachBatch(micro_batch).trigger(processingTime=TRIGGER)
     .option("checkpointLocation", checkpoint).start())
    print(f"silver_gold: {BRONZE} -> {SILVER} -> {OHLCV}, {SPREAD}; trigger {TRIGGER}", flush=True)
    try:
        spark.streams.awaitAnyTermination()
        lineage.complete(silver_run)
        lineage.complete(gold_run)
    except Exception as exc:
        lineage.fail(silver_run, repr(exc))
        lineage.fail(gold_run, repr(exc))
        raise


def run_rebuild(first: date | None, last: date | None, replace: bool) -> None:
    spark = session("finobs-silver-gold-rebuild")
    refresh_symbol_map(spark)
    lineage = LineageEmitter.from_env()
    run = lineage.start("silver_gold_rebuild", inputs=["bronze.trades_raw"],
                        outputs=["silver.trades", "gold.ohlcv_1m", "gold.cross_exchange_spread"])
    span = spark.table(BRONZE).agg(F.min("event_ts"), F.max("event_ts")).first()
    if span[0] is None:
        print("bronze trống, không có gì để rebuild", flush=True)
        return
    day, last = first or span[0].date(), last or span[1].date()
    started = time.time()
    while day <= last:
        lo = datetime.combine(day, datetime.min.time())
        hi = lo + timedelta(days=1)
        where = f"event_ts >= TIMESTAMP '{_ts(lo)}' AND event_ts < TIMESTAMP '{_ts(hi)}'"
        if replace:
            spark.sql(f"DELETE FROM {SILVER} WHERE {where}")
            spark.sql(f"DELETE FROM {OHLCV} WHERE minute >= TIMESTAMP '{_ts(lo)}' AND minute < TIMESTAMP '{_ts(hi)}'")
            spark.sql(f"DELETE FROM {SPREAD} WHERE minute >= TIMESTAMP '{_ts(lo)}' AND minute < TIMESTAMP '{_ts(hi)}'")
        rows, stats = clean(parse_bronze(spark.table(BRONZE).where(where)))
        dropped = {f"{r['source']}:{r['_drop']}": r["count"] for r in stats.collect()}
        if merge_silver(spark, rows):
            recompute_gold(spark, lo, hi - timedelta(microseconds=1))
        kept = spark.table(SILVER).where(where).count()
        print(json.dumps({"day": day.isoformat(), "silver_rows": kept, "dropped": dropped}), flush=True)
        day += timedelta(days=1)
    Path(CHECKPOINT_ROOT).mkdir(parents=True, exist_ok=True)
    (Path(CHECKPOINT_ROOT) / "silver_gold.rebuilt_at").write_text(str(started))
    lineage.complete(run)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=["stream", "rebuild"], default="stream")
    parser.add_argument("--from", dest="first", type=date.fromisoformat, help="ngày đầu (UTC) khi rebuild")
    parser.add_argument("--to", dest="last", type=date.fromisoformat, help="ngày cuối (UTC) khi rebuild")
    parser.add_argument("--keep", action="store_true", help="rebuild không xóa dữ liệu cũ (chỉ chèn phần thiếu)")
    args = parser.parse_args()
    if args.mode == "stream":
        run_stream()
    else:
        run_rebuild(args.first, args.last, replace=not args.keep)


if __name__ == "__main__":
    main()
