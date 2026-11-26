# Image finobs-app: producer (platform/ingest/produce.py) và collector metric
# (observability/collectors/run_loop.py). Build context: gốc repo.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

COPY requirements-runtime.txt .
RUN pip install --no-cache-dir -r requirements-runtime.txt

COPY config/ config/
COPY platform/contracts/ platform/contracts/
COPY platform/ingest/ platform/ingest/
COPY platform/lineage.py platform/lineage.py
COPY observability/collectors/ observability/collectors/
COPY observability/slo/ observability/slo/
COPY observability/store/ observability/store/
COPY observability/metric_catalog.yaml observability/

# Volume raw dùng chung giữa producer (ghi) và collector (đọc).
RUN mkdir -p /data/raw && chown nobody /data/raw
USER nobody
EXPOSE 8000

# Script trong platform/ingest import nhau theo tên file (common, sinks...),
# nên chạy bằng đường dẫn file như README hướng dẫn, không dùng python -m platform...
CMD ["python", "platform/ingest/produce.py", "--help"]
