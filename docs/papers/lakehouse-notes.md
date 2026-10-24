# Ghi chú bài báo Lakehouse – FinObs

**Người thực hiện:** Tú - Platform Engineer  
**Sprint:** S1 - Nghiên cứu, thiết kế  
**Bài báo:** *Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics*  
**Tác giả:** Michael Armbrust, Ali Ghodsi, Reynold Xin, Matei Zaharia  
**Hội nghị:** CIDR 2021  
**Nguồn:** https://www.cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf

## 1. Vấn đề bài báo đặt ra

Kiến trúc truyền thống thường sử dụng hai tầng riêng biệt: Data Lake để lưu trữ dữ liệu và Data Warehouse để phục vụ truy vấn phân tích.

Việc duy trì hai tầng gây ra một số hạn chế:

- Dữ liệu phải được sao chép và chuyển đổi qua nhiều bước ETL/ELT.
- Dữ liệu trong Warehouse có thể chậm hơn dữ liệu nguồn.
- Nhiều bước xử lý làm tăng nguy cơ lỗi và sai lệch dữ liệu.
- Chi phí lưu trữ, vận hành và chuyển đổi giữa các hệ thống tăng lên.
- Các ứng dụng machine learning khó truy cập trực tiếp dữ liệu trong Warehouse.

## 2. Khái niệm Lakehouse

Lakehouse là kiến trúc quản lý dữ liệu kết hợp:

- Khả năng lưu trữ linh hoạt và định dạng mở của Data Lake.
- Các chức năng quản lý và tối ưu của Data Warehouse.

Lakehouse có thể sử dụng object storage và các định dạng dữ liệu như Apache Parquet hoặc ORC, đồng thời bổ sung lớp metadata để quản lý dữ liệu.

## 3. Ba ý tưởng kỹ thuật quan trọng

### 3.1. Quản lý dữ liệu bằng metadata layer

Lớp metadata xác định những file dữ liệu thuộc một phiên bản của bảng.

Nhờ đó, hệ thống có thể hỗ trợ:

- Giao dịch ACID.
- Quản lý phiên bản dữ liệu.
- Khôi phục hoặc truy vấn phiên bản trước.
- Kiểm soát schema và chất lượng dữ liệu.

Bài báo đề cập Delta Lake và Apache Iceberg như các ví dụ về công nghệ quản lý bảng trên Data Lake.

### 3.2. Hỗ trợ Machine Learning và Data Science

Dữ liệu ở định dạng mở cho phép nhiều công cụ phân tích truy cập trực tiếp.

Các DataFrame API còn tạo điều kiện cho hệ thống tối ưu các phép biến đổi dữ liệu.

### 3.3. Tối ưu hiệu năng SQL

Lakehouse có thể cải thiện hiệu năng truy vấn thông qua:

- Caching.
- Index và thống kê dữ liệu.
- Tối ưu cách bố trí file dữ liệu.
- Query optimization.

Mục tiêu là đạt hiệu năng phân tích cạnh tranh với Data Warehouse mà vẫn giữ khả năng truy cập dữ liệu qua định dạng mở.

## 4. Liên hệ với dự án FinObs

*Phần này là đề xuất áp dụng vào FinObs, không phải kiến trúc được bài báo triển khai trực tiếp.*

| Thành phần FinObs | Vai trò dự kiến |
|---|---|
| MinIO | Object storage tương thích S3, lưu trữ dữ liệu |
| Apache Spark | Xử lý dữ liệu streaming và batch |
| Apache Iceberg | Quản lý bảng Lakehouse, metadata và phiên bản |
| Trino | Truy vấn SQL trên dữ liệu Lakehouse |
| Kafka | Tiếp nhận luồng sự kiện trước khi xử lý |

Luồng dữ liệu dự kiến:

Nguồn dữ liệu → Kafka → Spark → MinIO/Iceberg → Trino

Các tầng Bronze, Silver và Gold là cách tổ chức pipeline dự kiến của FinObs:

- **Bronze:** Lưu dữ liệu đầu vào.
- **Silver:** Dữ liệu đã làm sạch và chuẩn hóa.
- **Gold:** Dữ liệu sẵn sàng cho truy vấn, phân tích.

## 5. Ý nghĩa đối với nhiệm vụ Platform Engineer

Từ bài báo, các nguyên tắc đáng chú ý khi thiết kế hạ tầng FinObs gồm:

1. Tách biệt lưu trữ và xử lý để các công cụ có thể dùng chung dữ liệu.
2. Ưu tiên định dạng mở, tránh phụ thuộc vào một công cụ duy nhất.
3. Sử dụng lớp metadata quản lý bảng thay vì chỉ lưu các file rời rạc.
4. Hỗ trợ kiểm soát schema, phiên bản và tính nhất quán dữ liệu.
5. Theo dõi độ trễ và độ tin cậy của pipeline trong quá trình vận hành.

## 6. Kết luận

Lakehouse là hướng tiếp cận phù hợp để xây dựng nền tảng dữ liệu có khả năng phục vụ nhiều loại tác vụ phân tích trên cùng hệ thống lưu trữ.

Đối với FinObs, bài báo cung cấp cơ sở lý thuyết để lựa chọn MinIO làm object storage, kết hợp Apache Iceberg, Spark và Trino trong các sprint tiếp theo.

Việc tích hợp và kiểm chứng các thành phần này thuộc giai đoạn triển khai sau S1; ở S1, mục tiêu là nghiên cứu và ghi nhận định hướng kiến trúc.

## Tài liệu tham khảo

Armbrust, M., Ghodsi, A., Xin, R., & Zaharia, M. (2021). *Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics*. CIDR 2021.

https://www.cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf
