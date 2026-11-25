# Collector metric

Ba metric trong `observability/metric_catalog.yaml` đã chạy thật (Sprint 2):

| Metric | Trụ cột | File | Ý nghĩa |
|---|---|---|---|
| `trade_count_1m` | volume | [`trade_count.py`](trade_count.py) | Số trade live khác nhau trong phút |
| `ingest_delay_p95_ms` | freshness | [`freshness.py`](freshness.py) | p95 của `ingest_time - event_time` trong phút |
| `last_live_trade_age_seconds` | freshness | [`freshness.py`](freshness.py) | Tuổi trade live mới nhất tại mốc phút |

Quy tắc chung: chỉ bản ghi đạt data contract (`platform/contracts/validate.py`), khử trùng theo
`event_id` (giữ bản nhận sớm nhất), bỏ trade snapshot của Coinbase, cửa sổ phút tính theo `ingest_time`.
p95 dùng phân vị nearest-rank (không nội suy). Độ trễ âm được giữ nguyên: đó là dấu hiệu lệch đồng hồ.

## Chạy liên tục (service `metrics-collector`)

[`run_loop.py`](run_loop.py) chạy trong Docker Compose: mỗi phút đọc file raw producer ghi ra
(volume `raw_data`), tính 3 metric và upsert vào metric store (`metrics-postgres`).

- Đọc tăng dần: nhớ số dòng đã xử lý của từng file, chỉ parse phần mới.
- Mỗi vòng tính lại 3 phút gần nhất, để phần producer flush muộn vẫn được cập nhật (upsert, không trùng).
- Mọi cặp (nguồn, mã) trong `config/symbols.json` đều có dòng:
  nguồn còn trade khác nhưng mã này không có → `trade_count_1m = 0`;
  cả nguồn im lặng → `status = source_unavailable`, `value = null` (đúng catalog).
- File gzip hỏng (tiến trình ghi bị giết) chỉ được đọc tới chỗ hỏng, không làm collector dừng.

```bash
python observability/collectors/run_loop.py --raw data/raw --once --dry-run   # xem trước, không cần DB
docker exec finobs-metrics-postgres psql -U finobs -d finobs_metrics \
  -c "SELECT metric_name, status, count(*) FROM metric_observations GROUP BY 1, 2"
```

## Chạy một lần trên file (Sprint 1)

```bash
python observability/collectors/trade_count.py --in tests/fixtures/raw
python observability/collectors/trade_count.py --in data/raw --window-end 2026-10-27T02:49:00Z
```

`--write-db` ghi vào PostgreSQL qua `upsert_metric_observation` (cần `FINOBS_METRIC_DSN`).

## Giới hạn đã biết

- Bộ nhớ khử trùng của vòng liên tục giữ `event_id` trong 65 phút gần nhất. Trade Coinbase phát lại
  cũ hơn mốc đó (Sprint 1 thấy tới 19,7 giờ) vẫn bị tính là trade mới.
- Độ trễ phụ thuộc đồng hồ máy chạy producer. Xem metric `finobs_producer_clock_offset_ms` và alert
  `ClockSkew` trước khi tin vào `ingest_delay_p95_ms`.
- Collector đọc file raw thay vì Kafka; khi có tầng silver (S3) nên chuyển sang đọc silver/gold.
