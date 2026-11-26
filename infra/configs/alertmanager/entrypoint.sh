#!/bin/sh
# Alertmanager không đọc biến môi trường trong file cấu hình: chèn chat_id bằng sed, token qua file.
# Chưa đặt TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID thì chạy cấu hình chỉ ghi nhận (xem alert trên UI :9093).
set -e
if [ -n "$TELEGRAM_BOT_TOKEN" ] && [ -n "$TELEGRAM_CHAT_ID" ]; then
  printf %s "$TELEGRAM_BOT_TOKEN" > /tmp/telegram_token
  sed "s/__TELEGRAM_CHAT_ID__/$TELEGRAM_CHAT_ID/" /etc/finobs-alertmanager/telegram.yml > /tmp/alertmanager.yml
  echo "alertmanager: gửi cảnh báo qua Telegram (chat $TELEGRAM_CHAT_ID)"
else
  cp /etc/finobs-alertmanager/log-only.yml /tmp/alertmanager.yml
  echo "alertmanager: chưa cấu hình Telegram, chỉ ghi nhận alert"
fi
exec /bin/alertmanager --config.file=/tmp/alertmanager.yml --storage.path=/alertmanager
