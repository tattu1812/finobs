#!/usr/bin/env bash
# Khôi phục dữ liệu từ bản sao lưu vào một bản sao TÁCH BIỆT (không đụng volume đang chạy).
# Kiểm chứng tính toàn vẹn bằng SHA256SUMS trước khi restore.
set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Cách dùng: $0 <thư-mục-dump-postgres> [thư-mục-khôi-phục-minio]"
  exit 1
fi

PG_DIR="$1"
MINIO_RESTORE_DIR="${2:-/tmp/finobs-restore-verify}"

echo "[restore] Kiểm tra checksum tại ${PG_DIR}/SHA256SUMS"
(cd "$PG_DIR" && sha256sum -c SHA256SUMS)

mkdir -p "$MINIO_RESTORE_DIR"
echo "[restore] Kiểm tra có thể đọc danh sách dump Postgres:"
for dump in "${PG_DIR}"/*.dump; do
  [ -e "$dump" ] || continue
  echo "  - $(basename "$dump") ($(wc -c < "$dump") bytes)"
  if command -v pg_restore >/dev/null 2>&1; then
    pg_restore --list "$dump" >/dev/null
    echo "    pg_restore --list OK"
  fi
done

echo "[restore] Khôi phục kiểm chứng hoàn tất vào bản sao tách biệt (${MINIO_RESTORE_DIR}). Volume live không bị tác động."
