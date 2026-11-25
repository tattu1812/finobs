# Injector v1 (FinObs-Bench)

Phát lại dữ liệu raw của một scenario, tiêm lỗi đúng như YAML, ghi kết quả vào **namespace riêng**
và lưu đáp án vào bảng `bench.scenarios` của metric store.

```bash
make bench                                            # mọi scenario S0*.yaml -> bench/
python evaluation/injector/inject.py evaluation/scenarios/S004_binance_volume_loss.yaml
python evaluation/injector/inject.py S005.yaml --kafka localhost:9092 \
  --dsn postgresql://finobs:<mật khẩu>@localhost:5435/finobs_metrics
```

## 5 loại lỗi

| Injector | Mã | Tham số | Scenario mẫu |
|---|---|---|---|
| `drop_records` | FT02 mất tin | `fraction` | S004: bỏ 30% BTCUSDT trong 5 phút |
| `duplicate_records` | FT03 trùng tin | `fraction`, `keep_event_id`, `max_delay_ms` | S001: nhân bản 5% Coinbase |
| `shift_ingest_time` | FT06 lệch đồng hồ | `offset_ms`, `drift_ms_per_hour` | S003: lùi `ingest_time` 500 ms |
| `mutate_payload` | FT07 đổi schema | `rename_fields`, `cast_fields` | S002: đổi `p` thành `price` |
| `scale_price` | FT09 sai đơn vị | `factor` | S005: giá Coinbase BTC-USD × 1000 |

## Nguyên tắc

- **Không đụng luồng thật** (quyết định Q7): file ra ở `bench/<run_id>/...`; Kafka (tùy chọn) vào topic
  `bench.<scenario_id>.raw.<source>.trade`, không khớp mẫu `raw\.[a-z]+\.trade` của job bronze.
- **Tất định:** seed lấy từ `input.seed` của YAML; cùng YAML và cùng dữ liệu vào cho cùng `output_sha256`
  (đã kiểm: chạy 2 lần, 5/5 scenario trùng hash).
- **Đáp án thật:** `injected_from` / `injected_to` là thời điểm ingest của bản ghi bị tác động đầu tiên và cuối
  cùng, dùng để đo MTTD; khoảng trong YAML chỉ là dự kiến.
- Scenario phải hợp lệ với `evaluation/scenarios/validate.py` (version 1 hoặc 2) thì injector mới chạy.

## Kết quả chạy trên dữ liệu Sprint 1 (2026-10-27)

| Scenario | Bản ghi vào | Bị tác động | Ghi chú |
|---|---:|---:|---|
| S001 trùng tin | 10 893 | 46 | 5% trong 2 phút |
| S002 đổi schema | 55 611 | 15 564 | contract bắt đủ 15 564 |
| S003 lệch đồng hồ | 125 627 | 36 668 | |
| S004 mất tin | 55 611 | 1 894 | khoảng 30% BTCUSDT trong 5 phút |
| S005 sai đơn vị | 34 746 | 868 | contract **không** bắt được (0 vi phạm): cần detector C1 |

## Việc tiếp theo (S4)

Injector v2 (≥ 8 loại: thêm FT04 phát lại lịch sử, FT05 trễ/sai thứ tự, FT11 job lỗi, FT12 null), harness
chạy pipeline trên namespace `bench` và đo MTTD / MTTDiag theo `bench.scenarios`.
