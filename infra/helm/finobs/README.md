# FinObs Helm Chart

## 1. Mục tiêu

Bản phác thảo Helm chart cho hạ tầng FinObs trong Sprint 2.

Chart được sử dụng để thiết kế cấu hình triển khai
FinObs trên Kubernetes trong các sprint tiếp theo.

## 2. Cấu trúc

- Chart.yaml: Thông tin Helm chart.
- values.yaml: Cấu hình dự kiến cho các thành phần FinObs.
- templates/_helpers.tpl: Hàm đặt tên và gắn nhãn Kubernetes.
- templates/trino-service.yaml: Kubernetes Service cho Trino.
- templates/trino-deployment.yaml: Kubernetes Deployment cho Trino.

## 3. Thành phần trong values.yaml

- Kafka
- MinIO
- Spark
- Iceberg REST Catalog
- Trino
- Airflow
- Marquez

Lưu ý: Hiện tại chỉ Trino có Kubernetes templates.
Các thành phần còn lại mới được khai báo cấu hình dự kiến.

## 4. Kiểm tra Helm chart

Kiểm tra cú pháp:

    helm lint infra/helm/finobs

Render Kubernetes manifests:

    helm template finobs infra/helm/finobs

Kiểm tra tắt Trino:

    helm template finobs infra/helm/finobs --set trino.enabled=false

## 5. Giới hạn hiện tại

- Chưa triển khai thực tế lên Kubernetes.
- Chưa có templates cho Kafka, MinIO, Spark, Iceberg, Airflow và Marquez.
- Chưa cấu hình Iceberg Catalog và MinIO cho Trino trên Kubernetes.
- Chưa cấu hình Secret, PersistentVolumeClaim và các phụ thuộc đầy đủ.
- Chưa sẵn sàng cho môi trường production.

## 6. Hướng phát triển

- Bổ sung templates cho các thành phần còn lại.
- Quản lý credentials bằng Kubernetes Secrets.
- Bổ sung persistent storage và health checks.
- Kiểm thử tích hợp trên Kubernetes.
