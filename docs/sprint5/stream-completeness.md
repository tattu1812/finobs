# Stream Completeness Dashboard & Đối soát C2 — DE (Sprint 5)

## 1. Dashboard Grafana (`dashboards/finobs-stream-completeness.json`)

Dashboard được provision tự động vào thư mục **FinObs** trên Grafana (không cần cấu hình tay), có biến lọc `$source` và `$symbol`:

| Panel | Metric sử dụng | Ý nghĩa |
|---|---|---|
| **Độ đầy đủ stream (%)** | `finobs:reconciliation_match_ratio:avg` | Tỷ lệ trade khớp với file chính thức D+1 |
| **Tổng khoảng hở Trade ID (24h)** | `finobs:trade_id_gap_count:sum` | Số khoảng hở đứt quãng trên stream |
| **Trade thiếu** | `finobs:trade_id_gap_missing_total:sum` | Tổng số trade ID bị hụt |
| **Trạng thái thu liên tục** | Tính từ mốc 2026-11-26 | Số ngày thu liên tục tính từ ngày bắt đầu |
| **Khoảng hở & Trade thiếu theo thời gian** | `trade_id_gap_count`, `trade_id_gap_missing_total` | Theo dõi thời điểm phát sinh mất gói |
| **Độ trễ ingest p95 & Bronze** | `finobs:ingest_delay_p95_ms:avg`, `finobs_stream_batch_duration_ms` | Kiểm tra nghẽn đường ghi |
| **Tỷ lệ khớp đối soát D+1 theo ngày** | `finobs:reconciliation_match_ratio:avg` | Theo dõi xu hướng độ đầy đủ hằng ngày |
| **Trạng thái đối soát từng ngày** | `finobs:reconciliation_missing_trades:sum` | Bảng phân biệt `OK`, `MISSING_IN_STREAM`, `INCOMPLETE_DAY` |

> **Lưu ý hiệu năng**: các truy vấn nặng sử dụng recording rule tiền tính toán (`finobs:*`) thay vì quét raw/Iceberg trực tiếp từ Grafana.

---

## 2. Xử lý các trường hợp biên (Edge Cases)

1. **Ngày thu dở dang** (ví dụ `2026-11-26` bắt đầu thu từ 15:00 giờ VN):
   - Backfill gắn nhãn `is_partial_day=True` và đổi trạng thái thiếu đầu ngày thành `INCOMPLETE_DAY` để không báo nhầm là mất dữ liệu hệ thống.
2. **File chính thức D+1 chưa công bố**:
   - `reconcile.py` / `backfill.py` trả `status="OFFICIAL_NOT_AVAILABLE"`, không ghi sai `match_ratio=0`.
3. **Đồng hồ lệch (Clock Skew)**:
   - So khớp D+1 dùng **`trade_id`** làm khóa chính (không phụ thuộc đồng hồ máy thu); trường `time` chỉ kiểm tra trong dung sai `time_tolerance_ms` (`config/reconcile.yaml`).

---

## 3. Chạy Backfill & Đo M3

```bash
# Backfill nhiều ngày trên fixture mô phỏng (sơ bộ)
python observability/reconcile/backfill.py --from-date 2026-11-26 --to-date 2026-11-28 --fixture-dir tests/fixtures/reconcile

# Đo M3 trên dữ liệu mô phỏng
python observability/reconcile/measure_m3.py
```
