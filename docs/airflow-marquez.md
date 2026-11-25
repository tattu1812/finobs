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
- `AIRFLOW_ADMIN_PASSWORD` (mật khẩu tài khoản `admin` của Web UI)
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

Image Airflow là `finobs-airflow:local` (build từ `infra/airflow/Dockerfile`: thêm `pyiceberg` và mã
`platform/batch`). Container `airflow-init` tự chạy migration và **tự tạo tài khoản `admin`** với mật khẩu
`AIRFLOW_ADMIN_PASSWORD` (qua các biến `_AIRFLOW_DB_MIGRATE`, `_AIRFLOW_WWW_USER_*` của image chính thức).
Webserver/scheduler chỉ khởi động sau khi bước này thành công. Không cần thao tác tay.

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

## 7. DAG `binance_official_daily` (Sprint 2)

Mỗi ngày 03:30 UTC nạp file trades chính thức của ngày hôm trước từ Binance Public Data vào
`iceberg.bronze.binance_official_trades` (mã nguồn: `platform/batch/binance_official.py`).

- Một task cho mỗi mã Binance trong `config/symbols.json`, chạy lần lượt (`max_active_tasks=1`).
- Tải zip + `.CHECKSUM`, kiểm SHA-256; **sai checksum thì task FAILED, không ghi gì**.
- Đọc CSV theo khối 16 MB (BTCUSDT một ngày khoảng 4,8 triệu trade) và ghi trong một transaction Iceberg;
  chạy lại cùng ngày thay đúng partition `(file_date, symbol)`, không sinh bản trùng.
- Commit Iceberg bị xung đột (task khác vừa commit) thì tự thử lại tối đa 5 lần.
- Param `simulate_corruption=true` khi trigger thủ công: cố ý làm hỏng file để thấy nhánh lỗi.

```bash
make load-official DATE=2026-11-24     # nạp tay một ngày (chạy trong container scheduler)
make official-check                    # Trino: số trade, dãy ID, số file theo ngày và mã
```

Kết quả chạy thật ngày 2026-11-24: 10/10 task thành công, 9.220.592 trade, dãy trade ID liền mạch ở cả 10 mã.

## 7b. DAG `iceberg_maintenance` (Sprint 3)

Mỗi ngày 04:30 UTC, lần lượt từng bảng bronze/silver/gold: `optimize` (gộp file nhỏ), `expire_snapshots`
(giữ 7 ngày), `remove_orphan_files`, qua Trino. Bảng chưa tồn tại thì task bỏ qua, các bảng sau vẫn chạy.
Chạy thử ngày 2026-11-25: `bronze.trades_raw` từ 909 file còn 26, `bronze.binance_official_trades` từ 49 còn 10.

Các job (producer, Spark, DAG) tự phát OpenLineage về Marquez qua `platform/lineage.py`. Job streaming gửi
facet `jobType.processingType = STREAMING`: thiếu facet này, Marquez chỉ dựng cạnh lineage khi run COMPLETE,
mà job streaming không bao giờ kết thúc.

## 8. Phạm vi hiện tại

- Airflow Webserver và Scheduler hoạt động, DAG `binance_official_daily` chạy thật.
- Marquez API tiếp nhận và lưu được sự kiện OpenLineage.
- Job tự phát OpenLineage (S3); đồ thị Marquez đủ raw → bronze → silver → gold.
- Chưa triển khai giao diện Marquez Web UI; hiện kiểm thử qua API.

## 9. Lưu ý

Không sử dụng `docker compose down -v` nếu muốn giữ dữ liệu PostgreSQL và các dịch vụ khác.

Không đưa mật khẩu thật vào GitHub.
