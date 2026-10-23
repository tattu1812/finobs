# FinObs – Financial Data Observability Platform

FinObs là dự án xây dựng nền tảng quan sát và đánh giá độ tin cậy của hệ thống dữ liệu quy mô lớn.

## 1. Hạ tầng Sprint 1

Sprint 1 cung cấp môi trường phát triển cục bộ bằng Docker Compose, gồm:

| Thành phần | Vai trò | Địa chỉ mặc định |
|---|---|---|
| Apache Kafka | Message broker cho luồng dữ liệu | `localhost:9092` |
| MinIO | Object storage tương thích S3 | `http://localhost:9001` (Console) |
| Apache Spark Master | Điều phối xử lý dữ liệu | `http://localhost:8080` |
| Apache Spark Worker | Thực thi tác vụ Spark | Kết nối tới Spark Master |

Lưu ý: Các thành phần này phục vụ môi trường phát triển S1, chưa phải hệ thống production.

## 2. Yêu cầu cài đặt

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

## 3. Tải source code

```bash
git clone https://github.com/tattu1812/finobs.git
cd finobs
```

Trong khi Pull Request Sprint 1 chưa được merge vào `main`, chuyển sang nhánh chứa hạ tầng S1:

```bash
git fetch origin
git checkout tu
```

Sau khi PR được merge, có thể sử dụng nhánh `main` theo quy trình chung của nhóm.

## 4. Cấu hình môi trường

Tạo file `.env` từ mẫu:

```bash
cp .env.example .env
```

Mở `.env` và kiểm tra các cổng, tài khoản, mật khẩu MinIO.

Không commit file `.env` hoặc mật khẩu thật lên GitHub.

## 5. Khởi chạy FinObs

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

## 6. Kiểm tra giao diện

Sau khi khởi chạy:

- MinIO Console: http://localhost:9001
- Spark Master UI: http://localhost:8080

Đăng nhập MinIO bằng tài khoản và mật khẩu đã cấu hình trong `.env`.

Trên Spark Master UI, kiểm tra có ít nhất một Spark Worker kết nối và trạng thái Master là `ALIVE`.

Kafka không có giao diện web mặc định trong cấu hình S1; có thể kiểm tra bằng lệnh Docker hoặc Kafka CLI.

## 7. Các lệnh Makefile

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

## 8. Kiểm thử trên máy các thành viên

Mỗi thành viên thực hiện:

1. Clone repository và checkout nhánh có hạ tầng S1.
2. Tạo `.env` từ `.env.example`.
3. Chạy `make check`.
4. Chạy `make up`.
5. Chạy `make ps`.
6. Kiểm tra MinIO Console và Spark Master UI.
7. Gửi ảnh chụp `make ps` cùng thông tin lỗi nếu có.

Nếu cổng đã được ứng dụng khác sử dụng, kiểm tra và điều chỉnh cấu hình theo hướng dẫn của nhóm trước khi chạy lại.

## 9. Tài liệu nghiên cứu

Ghi chú bài báo Lakehouse:

`docs/papers/lakehouse-notes.md`

Nguồn: https://www.cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf

## 10. Quy trình đóng góp

- Mỗi thành viên làm việc trên nhánh riêng.
- Tạo Pull Request vào `main`.
- CI phải chạy thành công.
- Pull Request cần ít nhất một approval theo quy tắc bảo vệ nhánh.
- Không push trực tiếp lên `main`.
