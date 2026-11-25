# Image Airflow của FinObs: thêm pyiceberg để task batch ghi thẳng Iceberg,
# và mã nguồn platform/batch + config cho DAG. Build context: gốc repo.
FROM apache/airflow:2.10.5-python3.11

# Ghim lại phiên bản Airflow khi cài thêm gói, như tài liệu image chính thức khuyến nghị.
RUN pip install --no-cache-dir "apache-airflow==2.10.5" "pyiceberg[pyarrow]==0.8.1"

COPY --chown=airflow:root config/ /opt/finobs/config/
COPY --chown=airflow:root platform/batch/ /opt/finobs/platform/batch/
