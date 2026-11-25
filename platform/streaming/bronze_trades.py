"""
Spark Structured Streaming (FR-03, phần bronze): Kafka raw.<source>.trade -> iceberg.bronze.trades_raw.

    spark-submit --master spark://spark-master:7077 /opt/finobs/platform/streaming/bronze_trades.py

Bronze chỉ ghi thêm và giữ nguyên bản ghi gốc (cột payload là envelope JSON đầy đủ
producer gửi), cộng vị trí Kafka để truy ngược. Chuẩn hóa và loại trùng là việc của silver (S3).

Exactly-once: Iceberg sink ghi id của micro-batch vào snapshot, nên khi job khởi động
lại từ checkpoint, batch đã commit không bị ghi lần hai.

Biến môi trường:
  KAFKA_BOOTSTRAP       mặc định kafka:29092
  KAFKA_TOPIC_PATTERN   mặc định raw\\.[a-z]+\\.trade  (không đọc raw.dlq)
  BRONZE_TABLE          mặc định iceberg.bronze.trades_raw
  CHECKPOINT_DIR        mặc định /checkpoints/bronze_trades
  TRIGGER_SECONDS       mặc định 15
  MINIO_ROOT_USER / MINIO_ROOT_PASSWORD  khóa S3 cho catalog iceberg
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC_PATTERN = os.environ.get("KAFKA_TOPIC_PATTERN", r"raw\.[a-z]+\.trade")
TABLE = os.environ.get("BRONZE_TABLE", "iceberg.bronze.trades_raw")
CHECKPOINT = os.environ.get("CHECKPOINT_DIR", "/checkpoints/bronze_trades")
TRIGGER = f"{int(os.environ.get('TRIGGER_SECONDS', '15'))} seconds"

DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    source            STRING    COMMENT 'binance, coinbase...',
    symbol            STRING    COMMENT 'mã gốc của sàn (symbol_native)',
    symbol_canonical  STRING    COMMENT 'BASE-QUOTE',
    event_id          STRING    COMMENT '<source>:<symbol_native>:<sequence>',
    event_ts          TIMESTAMP COMMENT 'thời điểm sàn khớp lệnh (UTC)',
    ingest_ts         TIMESTAMP COMMENT 'thời điểm producer nhận (UTC)',
    payload           STRING    COMMENT 'envelope JSON nguyên bản producer gửi',
    kafka_topic       STRING,
    kafka_partition   INT,
    kafka_offset      BIGINT,
    kafka_ts          TIMESTAMP,
    bronze_ts         TIMESTAMP COMMENT 'thời điểm Spark ghi vào bronze'
)
USING iceberg
PARTITIONED BY (source, days(event_ts))
TBLPROPERTIES (
    'format-version' = '2',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '100'
)
"""


def bronze_columns(kafka_df):
    """Tách các trường envelope cần truy vấn nhanh; payload giữ nguyên chuỗi JSON."""
    value = F.col("value").cast("string")

    def field(path: str):
        return F.get_json_object(value, f"$.{path}")

    return kafka_df.select(
        field("source").alias("source"),
        field("symbol_native").alias("symbol"),
        field("symbol_canonical").alias("symbol_canonical"),
        field("event_id").alias("event_id"),
        field("event_time").cast("timestamp").alias("event_ts"),
        field("ingest_time").cast("timestamp").alias("ingest_ts"),
        value.alias("payload"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.current_timestamp().alias("bronze_ts"),
    )


def main() -> None:
    builder = SparkSession.builder.appName("finobs-bronze-trades")
    if os.environ.get("MINIO_ROOT_USER"):
        builder = (
            builder.config("spark.sql.catalog.iceberg.s3.access-key-id", os.environ["MINIO_ROOT_USER"])
            .config("spark.sql.catalog.iceberg.s3.secret-access-key", os.environ["MINIO_ROOT_PASSWORD"])
        )
    spark = builder.getOrCreate()

    namespace = TABLE.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(DDL)

    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribePattern", TOPIC_PATTERN)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 200_000)
        .load()
    )

    query = (
        bronze_columns(kafka)
        .writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime=TRIGGER)
        .option("checkpointLocation", CHECKPOINT)
        .option("fanout-enabled", "true")
        .toTable(TABLE)
    )
    print(f"bronze_trades: {TOPIC_PATTERN} -> {TABLE}, trigger {TRIGGER}", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
