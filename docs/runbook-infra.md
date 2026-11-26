# Runbook Vận hành Hạ tầng FinObs (PL-C)

## 1. Nguyên tắc an toàn (BẮT BUỘC)
- **TUYỆT ĐỐI KHÔNG** chạy `docker compose down -v`, `docker volume rm`, `docker system prune`, hoặc `rm -rf` vào thư mục dữ liệu.
- **KHÔNG** chỉnh sửa hay tắt `producer` hoặc `bronze-stream` khi không có kế hoạch bảo trì vì VM đang thu 14 ngày liên tục.

## 2. Kiểm tra sức khoẻ nhanh
```bash
make ps
make bronze-check
make official-check
docker logs finobs-producer --tail 20
```

## 3. Xử lý các sự cố thường gặp

| Tình huống | Dấu hiệu / Cảnh báo | Cách xử lý |
|---|---|---|
| **Ổ đĩa sắp đầy (< 15%)** | `HostDiskAlmostFull` | Kiểm tra `df -h`, dọn log Docker cũ hoặc giảm `RETENTION_DAYS` của `backup.sh`. Không xoá volume MinIO/Kafka. |
| **RAM thấp (< 15%, máy 8 GB)** | `HostLowMemory` | Kiểm tra `docker stats --no-stream`. Xem lại khuyến nghị giới hạn RAM Spark/Trino trong mục 4. |
| **Container chết / restart** | `TargetDown`, `ProducerDown`, `SparkWorkerDown` | Xem `docker logs <container> --tail 100`. Nếu chỉ 1 service chết, chạy `docker compose --env-file .env -f infra/docker-compose.yml restart <service>`. |
| **Mất mạng tới sàn** | `SourceStale`, `ProducerSilentWindow` | Producer tự reconnect với exponential backoff. Kiểm tra `data/raw/_ops/*.ops.jsonl` và kết nối DNS/Internet trên VM. |
| **Sau khi reboot VM** | Các container có `restart: unless-stopped` tự lên | Chờ 1–2 phút rồi chạy `make ps` và `make bronze-check` để xác nhận dòng dữ liệu tiếp tục. |

## 4. Khuyến nghị tối ưu bộ nhớ (rủi ro cao — cần duyệt trước khi áp dụng)
- Hiện tại `spark-worker` đặt `2G` và `trino` chưa giới hạn `mem_limit` trong `docker-compose.yml`. Trên VM 8 GB không swap, khuyến nghị thêm `deploy.resources.limits.memory` cho `trino` (`1536m`) và `marquez-api` (`512m`) trong một đợt bảo trì riêng sau khi kết thúc 14 ngày thu.
