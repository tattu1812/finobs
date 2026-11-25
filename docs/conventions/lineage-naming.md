# Quy ước đặt tên dataset và job trong lineage

Áp dụng cho: sự kiện OpenLineage gửi Marquez (DE, từ Sprint 3), đồ thị lineage và RCA của agent (AI),
asset trong kịch bản đánh giá (EV). Căn cứ: SolutionDesign, Bảng 11 – *"Tên dataset trong OpenLineage
trùng tên bảng Iceberg, để đồ thị Marquez nối được với metric"*.

## Quy tắc

1. **Namespace** OpenLineage của mọi dataset và job là `finobs`. Asset trong scenario viết `finobs:<tên>`.
2. **Topic Kafka**: tên dataset = tên topic. Ví dụ `raw.binance.trade`.
3. **Bảng Iceberg**: tên dataset = `<namespace Iceberg>.<bảng>`, không có tiền tố catalog.
   Ví dụ bảng `iceberg.bronze.trades_raw` → dataset `bronze.trades_raw`.
4. **Job**: tên app/DAG/service đang chạy thật, viết thường, gạch dưới.
5. **Thành phần không phải dữ liệu** (máy, tiến trình) dùng tiền tố `host.`, ví dụ `host.producer`.
   Chỉ xuất hiện trong ground truth của scenario (lỗi đồng hồ...), không phải node trong Marquez.
6. Đổi tên dataset là thay đổi hợp đồng: sửa cùng lúc producer/job, `agent/tools/fake_world.py`,
   scenario version 2 và tài liệu này trong **một** PR.

## Bảng tên hiện tại

| Loại | Tên | Thành phần thật | Có từ |
|---|---|---|---|
| dataset | `raw.binance.trade` | Topic Kafka, producer ghi | S2 |
| dataset | `raw.coinbase.trade` | Topic Kafka, producer ghi | S2 |
| dataset | `raw.dlq` | Topic Kafka, bản ghi vi phạm contract | S2 |
| dataset | `bronze.trades_raw` | Bảng Iceberg, job `bronze_trades` ghi | S2 |
| dataset | `bronze.binance_official_trades` | Bảng Iceberg, DAG `binance_official_daily` ghi | S2 |
| dataset | `silver.trades` | Bảng Iceberg (đặc tả Bảng 9) | S3 (dự kiến) |
| dataset | `gold.ohlcv_1m` | Bảng Iceberg | S3 (dự kiến) |
| dataset | `gold.cross_exchange_spread` | Bảng Iceberg | S3 (dự kiến) |
| dataset | `gold.reconciliation_daily` | Bảng Iceberg, kết quả C2 | S4 (dự kiến) |
| job | `producer` | Service `producer` (platform/ingest/produce.py) | S2 |
| job | `bronze_trades` | Spark Structured Streaming (platform/streaming/bronze_trades.py) | S2 |
| job | `binance_official_daily` | DAG Airflow (infra/airflow/dags/) | S2 |
| host | `host.producer` | Máy/container chạy producer | S2 |

Thế giới giả của agent ([`agent/tools/fake_world.py`](../../agent/tools/fake_world.py)) vẫn tách
`ingest_binance` / `ingest_coinbase` thành hai job để kịch bản chẩn đoán rõ ràng hơn; dataset thì dùng đúng
tên trong bảng trên.

## Tên cũ đã bỏ

| Tên tạm (S1, scenario v1) | Tên thật |
|---|---|
| `raw.binance_trades` | `raw.binance.trade` |
| `raw.coinbase_trades` | `raw.coinbase.trade` |
| `bronze.trades` | `bronze.trades_raw` |
| `silver.trades_dedup` | `silver.trades` |
| `gold.cross_source_spread` | `gold.cross_exchange_spread` |
| `host.collector` | `host.producer` |
| job `bronze_load` | job `bronze_trades` |

Scenario version 1 (đóng băng) và các fixture lỗi trong `tests/fixtures/` giữ tên cũ vì là dữ liệu lịch sử.
