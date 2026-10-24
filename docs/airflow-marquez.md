# Triển khai Airflow và Marquez – FinObs

## 1. Mục tiêu

Triển khai Apache Airflow và Marquez bằng Docker Compose để phục vụ điều phối pipeline và quản lý metadata lineage trong FinObs.

## 2. Thành phần

- Apache Airflow 2.10.5 (Python 3.11): Webserver, Scheduler, LocalExecutor.
- PostgreSQL 16: Lưu metadata Airflow.
- Marquez 0.50.0: Tiếp nhận và quản lý sự kiện OpenLineage.
- PostgreSQL 16: Lưu metadata Marquez.

Hai ứng dụng sử dụng database PostgreSQL riêng biệt.

## 3. Khởi động dịch vụ

Tạo file `.env` từ `.env.example` nếu chưa có và thiết lập các biến môi trường, bao gồm:

- `AIRFLOW_DB_PASSWORD`
- `MARQUEZ_DB_PASSWORD`

Không commit file `.env` chứa mật khẩu thật.

Kiểm tra cấu hình:

```bash
make check
```

Khởi động các dịch vụ:

```bash
docker compose --env-file .env -f infra/docker-compose.yml up -d airflow-postgres airflow-webserver airflow-scheduler marquez-postgres marquez-api
```

Kiểm tra trạng thái:

```bash
docker compose --env-file .env -f infra/docker-compose.yml ps
```

## 4. Khởi tạo Airflow

Ở lần triển khai đầu tiên, chạy migration:

```bash
docker compose --env-file .env -f infra/docker-compose.yml run --rm --entrypoint airflow airflow-init db migrate
```

Tạo tài khoản quản trị:

```bash
docker compose --env-file .env -f infra/docker-compose.yml run --rm --entrypoint airflow airflow-init users create --username admin --firstname FinObs --lastname Admin --role Admin --email admin@finobs.local
```

Nhập mật khẩu khi được yêu cầu.

Truy cập Airflow Web UI:

http://localhost:8080

Kiểm tra Scheduler:

```bash
docker compose --env-file .env -f infra/docker-compose.yml exec airflow-scheduler airflow jobs check --job-type SchedulerJob
```

Kết quả mong đợi:

```text
Found one alive job.
```

## 5. Kiểm tra Marquez API

Marquez sử dụng file cấu hình:

`infra/configs/marquez/marquez.yml`

Kiểm tra API:

```bash
curl -i http://localhost:5000/api/v1/namespaces
```

Kết quả mong đợi: HTTP `200 OK`.

## 6. Kiểm thử OpenLineage

Gửi sự kiện mẫu:

```bash
curl -i -X POST http://localhost:5000/api/v1/lineage \
  -H "Content-Type: application/json" \
  -d '{
    "eventType": "COMPLETE",
    "eventTime": "2026-10-24T09:50:00Z",
    "run": {
      "runId": "b8c27e19-42f5-4a63-9b10-d7e98c651234"
    },
    "job": {
      "namespace": "finobs",
      "name": "test_openlineage"
    },
    "inputs": [],
    "outputs": [],
    "producer": "https://github.com/tattu1812/finobs",
    "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json"
  }'
```

Kết quả kiểm thử: HTTP `201 Created`.

Truy vấn lại job:

```bash
curl -i http://localhost:5000/api/v1/namespaces/finobs/jobs/test_openlineage
```

Kết quả kiểm thử: HTTP `200 OK`, job `test_openlineage` có trạng thái `COMPLETED`.

## 7. Phạm vi hiện tại

- Airflow Webserver và Scheduler hoạt động.
- Marquez API tiếp nhận và lưu được sự kiện OpenLineage.
- Chưa tích hợp Airflow tự động phát sự kiện OpenLineage.
- Chưa triển khai giao diện Marquez Web UI; hiện kiểm thử qua API.

## 8. Lưu ý

Không sử dụng `docker compose down -v` nếu muốn giữ dữ liệu PostgreSQL và các dịch vụ khác.

Không đưa mật khẩu thật vào GitHub.
