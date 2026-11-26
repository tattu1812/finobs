# Đánh giá và Injector v2 (Sprint 4 — EV)

## 1. Tập lỗi Injector v2 (8 loại)
`evaluation/injector/injector_v2.py` hỗ trợ 8 loại lỗi khớp với các detector của DE và OB:
- `FT01_stale_feed`: Ngừng phát dữ liệu (test `SourceStale`, `ProducerSilentWindow`)
- `FT02_volume_loss`: Mất một phần bản ghi theo tỷ lệ (test DE-A `trade_id_gap_count` & DE-B `MISSING_IN_STREAM`)
- `FT03_duplicate_events`: Nhân bản bản ghi giữ nguyên ID (test DE-A `trade_id_duplicate_count`)
- `FT05_late_out_of_order`: Trì hoãn & đảo thứ tự `ingest_time` (test DE-A `trade_id_out_of_order_count`)
- `FT06_clock_skew`: Dịch đồng hồ `ingest_time` (test `ClockSkew`)
- `FT07_schema_drift`: Đổi tên trường payload (test `ContractViolations`)
- `FT09_value_anomaly`: Nhân giá `x1000` trên một sàn (test DE-B `VALUE_MISMATCH` & OB distribution)
- `FT12_null_spike`: Gán `null` vào trường giá (test schema & completeness)

## 2. Định nghĩa MTTD
- $\text{MTTD} = t_{\text{first\_valid\_alert}} - t_{\text{gt\_start}}$ (giây, chuẩn UTC).
- **Miss**: Không có cảnh báo khớp trong $[t_{\text{start}}, t_{\text{end}} + \text{grace}]$.
- **False Positive**: Cảnh báo không khớp bất kỳ ground truth nào, hoặc đến trước $t_{\text{start}}$ ngoài ngưỡng dung sai sớm.
