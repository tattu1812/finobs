# Chính sách Sao lưu và Khôi phục FinObs (PL-B)

## 1. Kiến trúc sao lưu (không dừng dịch vụ)
- **MinIO / Iceberg (`finobs-warehouse`)**: Dùng `mc mirror --overwrite --preserve` đồng bộ tăng dần ra `/var/backups/finobs/minio/latest/`. Không khoá ghi, không làm gián đoạn `bronze-stream`.
- **PostgreSQL (`finobs_metrics`, `marquez`, `airflow`)**: Dùng `pg_dump -Fc` qua `docker exec` (snapshot nhất quán MVCC, không khoá ghi).
- **Toàn vẹn**: Sinh `SHA256SUMS` và `manifest.json` cho mỗi lần chạy.

## 2. Lịch chạy & Chính sách giữ bản
- Chạy qua cron trên VM mỗi ngày 1 lần lúc `04:00 UTC` (sau DAG `binance_official_daily`):
  ```cron
  0 4 * * * /opt/finobs/infra/scripts/backup.sh >> /var/log/finobs-backup.log 2>&1
  ```
- **Lý do chọn cron trên VM**: Không tốn thêm RAM cho container mới trên VM 8 GB, chạy trực tiếp được cả `mc` lẫn `docker exec`.
- **Retention**: MinIO giữ 1 bản mirror tăng dần; PostgreSQL dump giữ **7 ngày** (`RETENTION_DAYS=7`), phù hợp với ổ đĩa VM 124 GB (đang dùng ~14 GB).
- **Giám sát**: Script ghi metric `finobs_backup_last_success_timestamp_seconds`; Prometheus bắn cảnh báo `BackupStale` nếu quá 26 giờ không có bản sao lưu mới.

## 3. Quy trình khôi phục kiểm chứng
Chạy `infra/scripts/restore.sh <đường-dẫn-dump-postgres>` để kiểm tra SHA-256 và đọc mục lục `pg_restore --list` trên bản sao tách biệt, không chạm vào volume live.
