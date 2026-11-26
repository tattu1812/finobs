#!/usr/bin/env bash
# Sao lưu tăng dần (incremental) dữ liệu FinObs không dừng dịch vụ và không khoá ghi:
#   - MinIO bucket finobs-warehouse (Iceberg bronze/silver/gold) qua mc mirror
#   - PostgreSQL (metrics, marquez, airflow) qua pg_dump trong docker exec
#   - Sinh manifest.json + SHA256SUMS và cập nhật metric textfile cho Prometheus.
#
# Chính sách giữ bản:
#   - MinIO: mirror tăng dần vào thư mục latest/ (không nhân bản dữ liệu tĩnh)
#   - Postgres dumps: giữ 7 ngày gần nhất (phù hợp VM ~124 GB)
set -euo pipefail

BACKUP_ROOT="${BACKUP_ROOT:-/var/backups/finobs}"
RETENTION_DAYS="${RETENTION_DAYS:-7}"
MINIO_ALIAS="${MINIO_ALIAS:-finobs}"
MINIO_BUCKET="${MINIO_BUCKET:-finobs-warehouse}"
TEXTFILE_DIR="${TEXTFILE_DIR:-/var/lib/node_exporter/textfile_collector}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
PG_DIR="${BACKUP_ROOT}/postgres/${STAMP}"

mkdir -p "${BACKUP_ROOT}/minio" "${PG_DIR}"

echo "[backup] Bắt đầu sao lưu lúc ${STAMP}"

# 1. MinIO incremental mirror (không dừng hay khoá MinIO)
if command -v mc >/dev/null 2>&1; then
  mc mirror --overwrite --preserve "${MINIO_ALIAS}/${MINIO_BUCKET}" "${BACKUP_ROOT}/minio/latest/"
else
  echo "[backup] Cảnh báo: không tìm thấy mc trên host; bỏ qua MinIO mirror (dùng docker run minio/mc nếu cần)."
fi

# 2. PostgreSQL pg_dump (online consistent snapshot, không khoá ghi)
dump_pg() {
  local container="$1" user="$2" db="$3" out="$4"
  if docker ps --format '{{.Names}}' | grep -qx "$container"; then
    docker exec "$container" pg_dump -U "$user" -Fc "$db" > "$out"
    sha256sum "$out" >> "${PG_DIR}/SHA256SUMS"
    echo "[backup] Đã dump ${db} -> ${out}"
  else
    echo "[backup] Bỏ qua ${container} (không chạy)"
  fi
}

dump_pg "finobs-metrics-postgres" "finobs" "finobs_metrics" "${PG_DIR}/finobs_metrics.dump"
dump_pg "finobs-marquez-postgres" "marquez" "marquez" "${PG_DIR}/marquez.dump"
dump_pg "finobs-airflow-postgres" "airflow" "airflow" "${PG_DIR}/airflow.dump"

# 3. Manifest
cat > "${PG_DIR}/manifest.json" <<EOF
{
  "timestamp_utc": "${STAMP}",
  "minio_mirror": "${BACKUP_ROOT}/minio/latest",
  "postgres_dumps": "$(ls -1 "${PG_DIR}"/*.dump 2>/dev/null | tr '\n' ',' | sed 's/,$//')"
}
EOF

# 4. Dọn bản dump Postgres cũ hơn RETENTION_DAYS
find "${BACKUP_ROOT}/postgres" -mindepth 1 -maxdepth 1 -type d -mtime "+${RETENTION_DAYS}" -exec rm -rf {} +

# 5. Xuất metric thời điểm sao lưu thành công gần nhất
if [ -d "$TEXTFILE_DIR" ] || mkdir -p "$TEXTFILE_DIR" 2>/dev/null; then
  cat > "${TEXTFILE_DIR}/finobs_backup.prom.$$" <<EOF
# HELP finobs_backup_last_success_timestamp_seconds Epoch seconds of last successful FinObs backup
# TYPE finobs_backup_last_success_timestamp_seconds gauge
finobs_backup_last_success_timestamp_seconds $(date -u +%s)
EOF
  mv "${TEXTFILE_DIR}/finobs_backup.prom.$$" "${TEXTFILE_DIR}/finobs_backup.prom"
fi

echo "[backup] Hoàn tất ${STAMP}"
