#!/bin/bash
# Скрипт для применения исправления дублирования уведомлений
# 31 октября 2025

set -e

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🔧 ПРИМЕНЕНИЕ ИСПРАВЛЕНИЯ ДУБЛИРОВАНИЯ УВЕДОМЛЕНИЙ"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo "1️⃣  Останавливаем текущий go-gateway..."
docker stop scanner-go-gateway 2>/dev/null || true
docker rm scanner-go-gateway 2>/dev/null || true
echo "✅ Контейнер остановлен"
echo ""

echo "2️⃣  Пересобираем go-gateway без кэша..."
docker-compose build --no-cache go-gateway 2>&1 | grep -E "(Step|DONE|exporting)" | tail -10
echo "✅ Образ пересобран"
echo ""

echo "3️⃣  Запускаем go-gateway через docker..."
# Получаем переменные окружения из docker-compose.yml
REDIS_URL=$(grep -A 50 "go-gateway:" docker-compose.yml | grep "REDIS_URL" | head -1 | cut -d'=' -f2)
TELEGRAM_BOT_TOKEN=$(grep -A 50 "go-gateway:" docker-compose.yml | grep "TELEGRAM_BOT_TOKEN" | head -1 | cut -d'=' -f2)
TELEGRAM_CHAT_ID=$(grep -A 50 "go-gateway:" docker-compose.yml | grep "TELEGRAM_CHAT_ID" | head -1 | cut -d'=' -f2)
SYMBOL=$(grep -A 50 "go-gateway:" docker-compose.yml | grep -E "^\s+-\s+SYMBOL=" | head -1 | sed 's/.*=//; s/^\s*//')

# Запускаем контейнер
docker run -d \
  --name scanner-go-gateway \
  --network scanner_infra_scanner-network \
  -p 8090:8090 \
  -e REDIS_URL=${REDIS_URL:-redis://scanner-redis:6379/0} \
  -e TELEGRAM_BOT_TOKEN="${TELEGRAM_BOT_TOKEN}" \
  -e TELEGRAM_CHAT_ID="${TELEGRAM_CHAT_ID}" \
  -e SYMBOL=${SYMBOL:-XAUUSD} \
  -e PAPER_MODE=true \
  -e OBI_SERVICE_URL=http://py-obi-service:8088 \
  --restart unless-stopped \
  scanner_infra_go-gateway

echo "✅ Контейнер запущен"
echo ""

echo "4️⃣  Ожидание инициализации (5 секунд)..."
sleep 5
echo ""

echo "5️⃣  Проверка логов..."
docker logs --tail 15 scanner-go-gateway 2>&1 | grep -E "(Ready|Telegram|Order|queued)" || docker logs --tail 5 scanner-go-gateway
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ ИСПРАВЛЕНИЕ ПРИМЕНЕНО"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📝 Проверка:"
echo "   - Telegram Bot должен быть активен (true)"
echo "   - При новых сигналах НЕ должно быть строк '📱 Preparing Telegram message'"
echo "   - Вместо этого должно быть: '✅ Order ... queued (notifications handled by notify-worker)'"
echo ""
echo "🔍 Мониторинг:"
echo "   docker logs -f scanner-go-gateway | grep -E 'POST.*orders|queued|Telegram'"
echo ""

