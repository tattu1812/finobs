Hiện tại t đã cấu hình bảo vệ nhánh `main`, nên các Pull Request cần có ít nhất 1 người review và approve, đồng thời CI phải chạy xanh thì mới được merge vào `main`. PR phần hạ tầng S1 của t đang chờ Trung review, nên trước mắt mọi người checkout nhánh `tu` để chạy thử nhé.
## 1. Yêu cầu cài đặt

Máy phát triển cần có:

- Git
- Docker Desktop hoặc Docker Engine
- Docker Compose
- GNU Make
- Docker daemon đang hoạt động

Với Windows, khuyến nghị sử dụng WSL2 Ubuntu và bật Docker Desktop WSL integration.

Kiểm tra môi trường:

```bash
git --version
docker --version
docker compose version
make --version
docker info
```

## 2. Tải source code

```bash
git clone https://github.com/tattu1812/finobs.git
cd finobs
```

Trong khi Pull Request Sprint 1 chưa được merge vào `main`, chuyển sang nhánh chứa hạ tầng S1:

```bash
git fetch origin
git checkout tu
```


## 3. Cấu hình môi trường

Tạo file `.env` từ mẫu:

```bash
cp .env.example .env
```

Mở `.env` và kiểm tra các cổng, tài khoản, mật khẩu MinIO.

Không commit file `.env` hoặc mật khẩu thật lên GitHub.

## 4. Khởi chạy FinObs

Kiểm tra cấu hình Docker Compose:

```bash
make check
```

Khởi chạy các dịch vụ:

```bash
make up
```

Xem trạng thái container:

```bash
make ps
```

Kỳ vọng các dịch vụ Kafka, MinIO, Spark Master và Spark Worker đều chạy.

## 5. Kiểm tra giao diện

Sau khi khởi chạy:

- MinIO Console: http://localhost:9001
- Spark Master UI: http://localhost:8080

Đăng nhập MinIO bằng tài khoản và mật khẩu đã cấu hình trong `.env`.


## 6. Các lệnh Makefile

| Lệnh | Công dụng |
|---|---|
| `make help` | Xem danh sách lệnh |
| `make check` | Kiểm tra cấu hình Docker Compose |
| `make up` | Khởi chạy dịch vụ |
| `make ps` | Xem trạng thái dịch vụ |
| `make logs` | Theo dõi log |
| `make stop` | Dừng dịch vụ |
| `make restart` | Khởi động lại dịch vụ |
| `make down` | Dừng và xóa container thuộc Compose project |

Lệnh `make down` không mặc định xóa named volumes. Không sử dụng `docker compose down -v` nếu cần giữ dữ liệu.

## 7. Kiểm thử trên máy các thành viên

Mỗi thành viên thực hiện:

1. Clone repository và checkout nhánh có hạ tầng S1.
2. Tạo `.env` từ `.env.example`.
3. Chạy `make check`.
4. Chạy `make up`.
5. Chạy `make ps`.
6. Kiểm tra MinIO Console và Spark Master UI.
7. Gửi ảnh chụp `make ps` cùng thông tin lỗi nếu có.

Nếu cổng đã được ứng dụng khác sử dụng, kiểm tra và điều chỉnh cấu hình theo hướng dẫn của nhóm trước khi chạy lại.


## 8. Iceberg + Trino(S2)
Bổ sung Apache Iceberg REST Catalog và Trino vào hạ tầng FinObs. Cấu hình nằm trên nhánh tattu/3-iceberg-trino

### 8.1. Các thành phần: MinIO, Iceberg REST Catalog, SQLite, Trino, iceberg-init
### 8.2. Chuẩn bị MinIO
1. Truy cập MinIO Console tại http://localhost:9001.

2. Đăng nhập bằng thông tin trong file .env.

3. Mở mục Buckets.

4. Tạo bucket có tên finobs-warehouse.
### 8.3. Khởi động
Chạy lần lượt các lệnh:

make check -> make up -> make ps

Các dịch vụ bổ sung gồm:


Iceberg REST Catalog -> http://localhost:8181

Trino HTTP endpoint -> http://localhost:8081

MinIO Console -> http://localhost:9001

iceberg-init kết thúc với trạng thái Exited (0) là bình thường, vì container này chỉ thực hiện bước khởi tạo quyền cho volume.
### 8.4. Kiểm tra kết nối Iceberg
```bash
docker exec finobs-trino trino --execute "SHOW CATALOGS"
```
Kết quả cần có catalog iceberg.

### 8.5. Tạo schema và bảng mẫu
```bash
docker exec finobs-trino trino --execute \
"CREATE SCHEMA iceberg.finobs_test
WITH (location = 's3://finobs-warehouse/finobs_test/')"

docker exec finobs-trino trino --execute \
"CREATE TABLE iceberg.finobs_test.transactions (
    transaction_id BIGINT,
    amount DOUBLE,
    status VARCHAR
)"
```
Các lệnh tạo schema và bảng chỉ cần chạy một lần. Nếu đối tượng đã tồn tại, bỏ qua bước tạo tương ứng.

### 8.6. Ghi và truy vấn dữ liệu
```bash
docker exec finobs-trino trino --execute \
"INSERT INTO iceberg.finobs_test.transactions
VALUES
    (1, 150.50, 'SUCCESS'),
    (2, 299.99, 'SUCCESS'),
    (3, 75.00, 'FAILED')"

docker exec finobs-trino trino --execute \
"SELECT transaction_id, amount, status
FROM iceberg.finobs_test.transactions
ORDER BY transaction_id"
```
Kết quả mẫu:

"1","150.5","SUCCESS"
"2","299.99","SUCCESS"
"3","75.0","FAILED"

Lưu ý: Chạy lại lệnh INSERT sẽ thêm các bản ghi trùng ID vì bảng mẫu chưa khai báo ràng buộc duy nhất.

### 8.7. Kiểm tra dữ liệu sau khi tạo lại Iceberg REST Catalog
```bash
docker compose --env-file .env -f infra/docker-compose.yml \
  up -d --force-recreate iceberg-rest
```
Sau đó chạy lại câu lệnh SELECT ở mục 8.6. Dữ liệu đã ghi cần tiếp tục truy vấn được.

SQLite Catalog được lưu trong Docker named volume iceberg_catalog_data, còn dữ liệu bảng được lưu trong MinIO. Không chạy docker compose down -v nếu muốn giữ dữ liệu.

### 8.8. Lưu ý triển khai

Đây là cấu hình phục vụ môi trường phát triển và demo, chưa phải cấu hình production.

Bucket finobs-warehouse hiện cần tạo thủ công.

Không commit .env hoặc mật khẩu thật.

Trino sử dụng cổng 8081 trên máy host để tránh xung đột với Spark Master UI ở cổng 8080.

Cảnh báo Unable to create a system terminal, creating a dumb terminal từ Trino CLI không ảnh hưởng đến kết quả truy vấn.
