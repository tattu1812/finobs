#!/bin/sh
# Tạo /tmp/alertmanager.yml từ biến môi trường; không nhúng token vào file config.
# Nếu thiếu TELEGRAM_BOT_TOKEN hoặc TELEGRAM_CHAT_ID -> dùng receiver "null" để Alertmanager không sập.
set -e

CONF="/tmp/alertmanager.yml"
TOKEN_FILE="/tmp/telegram_bot_token"

if [ -n "${TELEGRAM_BOT_TOKEN:-}" ] && [ -n "${TELEGRAM_CHAT_ID:-}" ]; then
  printf '%s' "$TELEGRAM_BOT_TOKEN" > "$TOKEN_FILE"
  chmod 600 "$TOKEN_FILE"
  cat > "$CONF" <<EOF
global:
  resolve_timeout: 5m

route:
  receiver: telegram-finobs
  group_by: ['alertname', 'source', 'symbol', 'pillar']
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h
  routes:
    - matchers:
        - alertname="Watchdog"
      receiver: "null"
      repeat_interval: 15m

receivers:
  - name: "null"
  - name: telegram-finobs
    telegram_configs:
      - bot_token_file: "$TOKEN_FILE"
        chat_id: ${TELEGRAM_CHAT_ID}
        parse_mode: HTML
        message: |
          <b>[{{ .Status | toUpper }}] {{ .CommonLabels.alertname }}</b>
          Severity: {{ .CommonLabels.severity }} | Source: {{ .CommonLabels.source }} | Symbol: {{ .CommonLabels.symbol }}
          {{ range .Alerts }}{{ .Annotations.summary }}
          {{ end }}
EOF
else
  cat > "$CONF" <<EOF
global:
  resolve_timeout: 5m

route:
  receiver: "null"
  group_by: ['alertname', 'source', 'symbol']

receivers:
  - name: "null"
EOF
fi

exec /bin/alertmanager --config.file="$CONF" --storage.path=/alertmanager
