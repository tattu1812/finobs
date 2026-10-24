# FinObs Monitoring Guide

## 1. Overview

FinObs sử dụng Prometheus và Grafana để giám sát hạ tầng.

- Prometheus: Thu thập metrics và đánh giá Alert Rules.
- Grafana: Hiển thị dashboard giám sát.
- Spark Metrics: Cung cấp metrics của Spark Master và Spark Worker.

## 2. Start Monitoring Services

Tại thư mục gốc của dự án:

```bash
make check
make up
make ps
```

Truy cập:

| Service | URL |
|---|---|
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |
| Spark Master UI | http://localhost:8082 |

Các cổng có thể thay đổi tùy theo cấu hình môi trường.

## 3. Prometheus Targets

Mở:

http://localhost:9090/targets

Các targets được cấu hình:

- prometheus
- spark-master
- spark-worker

Trạng thái UP nghĩa là Prometheus thu thập metrics thành công.

## 4. Grafana Dashboard

Mở:

http://localhost:3000/dashboards

Vào thư mục FinObs, chọn dashboard:

**FinObs - Infrastructure Monitoring**

Dashboard gồm 3 panels:

1. Spark Master & Worker - Health Status
2. Spark Worker - CPU Cores Used
3. Spark Worker - Memory Used (MB)

Dashboard được tự động nạp bằng Grafana Provisioning.

Các file liên quan:

- `infra/configs/grafana/dashboards/finobs-infrastructure.json`
- `infra/configs/grafana/provisioning/dashboards/finobs.yml`
- `infra/configs/grafana/provisioning/datasources/prometheus.yml`

## 5. Alert Rules

Prometheus hiện có hai cảnh báo:

| Alert | Condition | Duration | Severity |
|---|---|---|---|
| SparkMasterDown | Spark Master không phản hồi | 1 phút | critical |
| SparkWorkerDown | Spark Worker không phản hồi | 1 phút | critical |

File cấu hình:

`infra/configs/prometheus/rules/spark-alerts.yml`

Kiểm tra cảnh báo tại:

http://localhost:9090/alerts

Các trạng thái:

- inactive: Không có điều kiện cảnh báo.
- pending: Phát hiện sự cố nhưng chưa đủ thời gian.
- firing: Điều kiện cảnh báo đã kéo dài đủ thời gian.

Lưu ý: Alert Rules hiện chỉ được đánh giá trong Prometheus.
Chưa cấu hình gửi thông báo qua email, Telegram hoặc Alertmanager.

## 6. Verification

Kiểm tra Docker Compose:

```bash
make check
make ps
```

Kiểm tra Prometheus:

```bash
docker exec finobs-prometheus promtool check config \
  /etc/prometheus/prometheus.yml
```

Kiểm tra Alert Rules:

```bash
docker exec finobs-prometheus promtool check rules \
  /etc/prometheus/rules/spark-alerts.yml
```

## 7. Troubleshooting

### Prometheus target DOWN

1. Kiểm tra container tương ứng bằng `docker ps`.
2. Kiểm tra log bằng `docker logs <container-name>`.
3. Kiểm tra endpoint metrics và kết nối Docker network.

### Grafana không hiển thị dữ liệu

1. Kiểm tra Prometheus tại http://localhost:9090.
2. Kiểm tra datasource Prometheus trong Grafana.
3. Kiểm tra các targets có trạng thái UP.
4. Kiểm tra biểu thức PromQL trong dashboard.

### Alert FIRING

1. Xác định dịch vụ bị lỗi tại `/alerts`.
2. Kiểm tra trạng thái và log của container.
3. Khôi phục dịch vụ sau khi xác định nguyên nhân.
4. Xác nhận target trở về UP và alert trở về inactive.

## 8. Tested Scenario

Đã kiểm thử cảnh báo SparkWorkerDown:

1. Dừng Spark Worker.
2. Prometheus ghi nhận `up{job="spark-worker"} = 0`.
3. Alert chuyển từ inactive sang pending.
4. Alert chuyển sang firing sau khi đủ thời gian.
5. Khởi động lại Spark Worker.
6. Prometheus ghi nhận target UP.
7. Alert trở về inactive.

Kết quả: PASS.

Không dừng các container đang xử lý công việc thực tế chỉ để thử nghiệm cảnh báo.
