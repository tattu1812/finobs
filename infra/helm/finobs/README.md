# FinObs Helm Chart

## 1. Muc tieu

Ban phac thao Helm chart cho ha tang FinObs trong Sprint 2.

Chart duoc su dung de thiet ke cau hinh trien khai
FinObs tren Kubernetes trong cac sprint tiep theo.

## 2. Cau truc

- Chart.yaml: Thong tin Helm chart.
- values.yaml: Cau hinh du kien cho cac thanh phan FinObs.
- templates/_helpers.tpl: Ham dat ten va gan nhan Kubernetes.
- templates/trino-service.yaml: Kubernetes Service cho Trino.
- templates/trino-deployment.yaml: Kubernetes Deployment cho Trino.

## 3. Thanh phan trong values.yaml

- Kafka
- MinIO
- Spark
- Iceberg REST Catalog
- Trino
- Airflow
- Marquez

Luu y: Hien tai chi Trino co Kubernetes templates.
Cac thanh phan con lai moi duoc khai bao cau hinh du kien.

## 4. Kiem tra Helm chart

Kiem tra cu phap:

    helm lint infra/helm/finobs

Render Kubernetes manifests:

    helm template finobs infra/helm/finobs

Kiem tra tat Trino:

    helm template finobs infra/helm/finobs --set trino.enabled=false

## 5. Gioi han hien tai

- Chua trien khai thuc te len Kubernetes.
- Chua co templates cho Kafka, MinIO, Spark, Iceberg, Airflow va Marquez.
- Chua cau hinh Iceberg Catalog va MinIO cho Trino tren Kubernetes.
- Chua cau hinh Secret, PersistentVolumeClaim va cac phu thuoc day du.
- Chua san sang cho moi truong production.

## 6. Huong phat trien

- Bo sung templates cho cac thanh phan con lai.
- Quan ly credentials bang Kubernetes Secrets.
- Bo sung persistent storage va health checks.
- Kiem thu tich hop tren Kubernetes.
