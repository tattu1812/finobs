# Tóm tắt Sprint 2: dữ liệu chạy end-to-end

**Ngày:** 25/11/2026 · **Mục tiêu sprint (Đề tài, Bảng 8):** *giá live trên bảng bronze* ·
**Dữ liệu kiểm thử:** chạy thật 05:10–05:53 UTC ngày 25/11, file chính thức Binance ngày 24/11

Đây là bản tóm tắt cho cả nhóm. Các quyết định đầu sprint (Q1–Q6) và D01–D05 đã được nhóm trưởng chốt
theo đề xuất trong kế hoạch Sprint 2.

## Đã làm được

| Vai | Task | Kết quả |
|---|---|---|
| PL | PL-1 | Sửa trùng cổng 8080 (Spark UI → 8082), thứ tự khởi động Airflow (PR #22) |
| PL | PL-2 | `minio-init` tự tạo bucket `finobs-warehouse` (image `minio/mc` không còn trên Docker Hub, dùng `mc` có sẵn trong image MinIO) |
| PL | PL-3 | Image `finobs-spark`: Spark 3.5.3 + connector Kafka + Iceberg 1.6.1 (S3FileIO), worker 2 core / 2 GB |
| PL | PL-4 | Service `metrics-postgres`, tự chạy `observability/store/*.sql` lần đầu |
| PL | PL-5 | `kafka-init` tạo `raw.binance.trade`, `raw.coinbase.trade`, `raw.dlq` (3 partition, giữ 7 ngày); tắt auto-create topic |
| DE | DE-1, DE-2 | Producer chạy liên tục trong Compose: WebSocket → kiểm contract → Kafka (khóa = mã sàn, idempotent), tự kết nối lại, đo NTP mỗi 10 phút, metric Prometheus |
| DE | DE-3 | Spark Structured Streaming Kafka → `iceberg.bronze.trades_raw`, chỉ ghi thêm, exactly-once |
| DE | DE-4 | DAG `binance_official_daily`: tải zip + `.CHECKSUM`, kiểm SHA-256, nạp `bronze.binance_official_trades` |
| DE | DE-5 | `make bronze-check`, `make official-check` truy vấn qua Trino |
| OB | OB-1, OB-2 | Collector liên tục ghi `trade_count_1m`, `ingest_delay_p95_ms`, `last_live_trade_age_seconds` vào metric store |
| OB | OB-3 | Vi phạm contract → `raw.dlq` + metric; dashboard Grafana *FinObs - Ingest*; 4 alert mới |
| AI | AI-1 | `query_metric` đọc metric store thật (giữ dữ liệu giả làm dự phòng) |
| AI | AI-2 | [Quy ước đặt tên lineage](../conventions/lineage-naming.md): tên dataset = tên topic/bảng thật |
| EV | EV-1 | Scenario v2 + v1 đóng băng (D01 = C), validator chọn theo phiên bản, [migration guide](../../evaluation/scenarios/MIGRATION_v1_to_v2.md) |
| EV | EV-2 | Asset của S001–S003 đổi sang tên thật, khớp lineage |
| EV | EV-3 | Tài liệu này |

## Đối chiếu tiêu chí nghiệm thu

| # | Tiêu chí | Kết quả | Trạng thái |
|---|---|---|---|
| 1 | `make up` trên máy sạch, không thao tác tay | Bucket, topic, bảng, tài khoản Airflow đều tự tạo | ✅ |
| 2 | Producer chạy ≥ 12 giờ, ghi reconnect, 0 vi phạm contract | Đã chạy 43 phút: 0 vi phạm, tự kết nối lại sau sự cố mạng 5,5 phút. **Chưa đủ 12 giờ** | 🟡 Cần chạy tiếp |
| 3 | Giá live trên `bronze.trades_raw`, trễ < 1 phút | 20 mã, trễ ingest → bronze p50 ≈ 9–10 s | ✅ |
| 4 | `bronze.binance_official_trades` có ≥ 1 ngày đã kiểm SHA-256 | Ngày 24/11: 10 mã, 9.220.592 trade | ✅ |
| 5 | `trade_count_1m` live | Có trong metric store và đọc được qua agent | ✅ |
| 6 | CI xanh, PR có người duyệt | 66 unit test + ruff đạt trên máy; CI chạy khi mở PR | ⏳ Chờ PR |
| 7 | D01–D05 có quyết định | C / V2 / có migration guide / có cross-team review / duyệt qua PR | ✅ |

## Số liệu chính

| | Binance | Coinbase |
|---|---:|---:|
| Bản ghi trên bronze (43 phút) | 181.702 | 35.565 |
| Trễ ingest → bronze p50 | 8,7 s | 9,8 s |
| Vi phạm contract (→ `raw.dlq`) | 0 | 0 |
| Kết nối lại (ngoài lần khởi động đầu) | 2 do restart container, 1 sau sự cố mạng | 2 do restart container, 1 sau sự cố mạng, 2 do sàn tự ngắt |

| File chính thức 24/11 | Trade | Dãy ID |
|---|---:|---|
| BTCUSDT | 4.852.266 | liền mạch |
| ETHUSDT | 2.702.759 | liền mạch |
| 8 mã còn lại | 1.665.567 | liền mạch |
| **Tổng** | **9.220.592** | 0 ID trùng, mỗi partition đúng 1 checksum |

Kiểm chứng khác: khởi động lại job Spark → **0 offset Kafka bị ghi hai lần**; nạp lại cùng ngày → số dòng
không đổi; file bị sửa 1 byte → `ChecksumMismatch`, không ghi gì.

## Năm phát hiện quan trọng

1. **Đồng hồ máy ảo Docker chậm khoảng 1 giây** (đo được +172 ms đến +981 ms so với NTP, dao động). Hệ quả:
   `ingest_delay_p95_ms` của Binance ra số âm (−368 ms). Alert `ClockSkew` đã phát đúng. Đây là phát hiện 1 của
   Sprint 1 lặp lại trên môi trường Docker. **Phải đồng bộ giờ trước khi chạy liên tục 14 ngày.**
2. **Sự cố mạng thật 05:34–05:40 UTC (5,5 phút)**: cả hai sàn lỗi DNS cùng lúc, trùng lúc máy ảo thiếu RAM
   (Airflow đang nạp BTCUSDT). Producer tự kết nối lại. Khoảng trống này là một ca **mất dữ liệu thật (FT01)**,
   dùng được làm đáp án cho đối soát C2 ở Sprint 4.
3. **Coinbase gửi lại snapshot sau mỗi lần kết nối lại**: 2.457 trade xuất hiện 2–6 lần trên bronze, tất cả có
   cờ `is_snapshot`. Bronze giữ nguyên theo thiết kế; silver phải khử trùng theo `event_id`.
4. **Nối file gzip sau khi tiến trình bị giết làm hỏng cả file** (`zlib.error`). Đã sửa: mỗi tiến trình producer
   ghi file riêng, collector bỏ qua đoạn hỏng thay vì crash. Có test tái hiện đúng lỗi.
5. **Commit Iceberg đồng thời bị xung đột** khi hai task Airflow ghi cùng bảng. Đã thêm thử lại khi commit và
   cho DAG chạy tuần tự. Bộ nạp cũng chuyển sang đọc CSV theo khối: RAM đỉnh của scheduler giảm từ 2,57 GB xuống 1,25 GB.

## Các thành viên khác cần biết

- **Cập nhật `.env`** theo `.env.example`: biến mới `AIRFLOW_ADMIN_PASSWORD`, `METRICS_DB_PASSWORD`,
  `METRICS_DB_PORT`, `SPARK_WORKER_*`, `BRONZE_TRIGGER_SECONDS`, `PRODUCER_SOURCES`; `SPARK_UI_PORT=8082`.
- **Tên dataset đã đổi** (`raw.binance_trades` → `raw.binance.trade`, `bronze.trades` → `bronze.trades_raw`...).
  Xem [quy ước](../conventions/lineage-naming.md) trước khi viết job hay scenario mới.
- **Scenario mới dùng `scenario_version: 2`.**
- **Đọc bronze:** cột `payload` là envelope JSON nguyên bản; khử trùng theo `event_id`, bỏ `is_snapshot = true`.
- **RAM:** toàn stack FinObs dùng 4,5–5 GB khi Airflow đang nạp file. Máy 8 GB nên tắt dịch vụ không dùng
  (`docker stop finobs-airflow-webserver ...`).

## Việc tiếp theo (Sprint 3)

1. Đồng bộ giờ máy chạy producer, rồi **chạy liên tục ≥ 12 giờ** để đóng tiêu chí 2; sau đó bắt đầu thu 14 ngày.
2. Silver: chuẩn hóa mã, khử trùng theo `event_id`, watermark riêng từng nguồn; gold: OHLCV 1 phút, VWAP, spread.
3. Spark và Airflow phát OpenLineage về Marquez theo [quy ước tên](../conventions/lineage-naming.md).
4. Compaction định kỳ cho `bronze.trades_raw`: 43 phút đã sinh 285 file dữ liệu (6 MB) và 136 snapshot.
5. Injector v1 (5 loại lỗi), RCA heuristic trên đồ thị Marquez thật.
