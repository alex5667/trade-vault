#!/bin/bash
# Скрипт для отправки тестового сигнала в Telegram

set -e

BOT_TOKEN="8210822109:AAGnm0lXNQXtLsFvlutijocZIx4hjPYmmOM"
CHAT_ID="8257274242"

echo "📨 Отправка тестового сигнала..."
echo ""

# Генерируем красивый тестовый сигнал
CURRENT_TIME=$(date '+%H:%M:%S %d.%m.%Y')
TIMESTAMP=$(date +%s)

TEST_MESSAGE="🧪 ТЕСТОВЫЙ СИГНАЛ

💥 🟢 XAUUSD LONG @ 4025.50, Volume 0.10 lot
📝 Тестирование системы уведомлений
🛑 SL 4023.00 | TP1 4028.00 (RR 1.0); TP2 4030.50 (RR 2.0); TP3 4033.00 (RR 3.0)
🕐 ${CURRENT_TIME} UTC
🔧 Source: System Test | ID: TEST_${TIMESTAMP}

✅ Все системы работают корректно!
📊 Notify-worker активен
🌐 HTTP 200 OK от Telegram API"

# Отправляем через Telegram API
RESPONSE=$(curl -s -X POST "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
  -H "Content-Type: application/json" \
  -d "{
    \"chat_id\": \"${CHAT_ID}\",
    \"text\": \"${TEST_MESSAGE}\",
    \"parse_mode\": \"HTML\"
  }")

# Проверяем результат
if echo "$RESPONSE" | jq -e '.ok' > /dev/null 2>&1; then
    MESSAGE_ID=$(echo "$RESPONSE" | jq -r '.result.message_id')
    echo "✅ Тестовый сигнал успешно отправлен!"
    echo "   Message ID: $MESSAGE_ID"
    echo "   Chat ID: $CHAT_ID"
    echo "   Время: $CURRENT_TIME"
    echo ""
    echo "📱 Проверьте бот @my_trd_56_bot в Telegram!"
else
    echo "❌ Ошибка отправки сигнала:"
    echo "$RESPONSE" | jq '.'
    exit 1
fi

