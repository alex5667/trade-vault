#!/bin/bash
# Скрипт для проверки статуса telegram-worker и отправки сигналов

set -e

echo "🔍 TELEGRAM WORKER - СТАТУС ПРОВЕРКА"
echo "======================================"
echo ""

# Цвета для вывода
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 1. Проверка контейнеров
echo "📦 1. Статус контейнеров:"
echo "-------------------------"
if docker ps | grep -q scanner-notify-worker; then
    echo -e "${GREEN}✅ notify-worker: RUNNING${NC}"
else
    echo -e "${RED}❌ notify-worker: STOPPED${NC}"
fi

if docker ps | grep -q scanner-aggregated-hub; then
    echo -e "${GREEN}✅ aggregated-hub: RUNNING${NC}"
else
    echo -e "${RED}❌ aggregated-hub: STOPPED${NC}"
fi
echo ""

# 2. Проверка Redis
echo "💾 2. Redis notify:telegram stream:"
echo "-----------------------------------"
STREAM_LEN=$(docker exec scanner-redis-worker-1 redis-cli XLEN notify:telegram 2>/dev/null || echo "0")
echo -e "Сообщений в очереди: ${GREEN}${STREAM_LEN}${NC}"
echo ""

# 3. Последние логи notify-worker
echo "📝 3. Последние отправки (notify-worker):"
echo "-----------------------------------------"
docker logs scanner-notify-worker --tail 20 2>&1 | grep -E "(✅|❌|Уведомление.*отправлено)" | tail -5
echo ""

# 4. HTTP статус из логов
echo "🌐 4. HTTP статус отправок:"
echo "---------------------------"
docker exec scanner-notify-worker tail -10 improved_notifier.log 2>/dev/null | grep "HTTP" | tail -3
echo ""

# 5. Проверка бота через API
echo "🤖 5. Проверка Telegram бота:"
echo "-----------------------------"
BOT_TOKEN="8210822109:AAGnm0lXNQXtLsFvlutijocZIx4hjPYmmOM"
BOT_INFO=$(curl -s "https://api.telegram.org/bot${BOT_TOKEN}/getMe")
BOT_USERNAME=$(echo $BOT_INFO | jq -r '.result.username' 2>/dev/null || echo "unknown")
BOT_FIRST_NAME=$(echo $BOT_INFO | jq -r '.result.first_name' 2>/dev/null || echo "unknown")

if [ "$BOT_USERNAME" != "null" ] && [ "$BOT_USERNAME" != "unknown" ]; then
    echo -e "${GREEN}✅ Бот: @${BOT_USERNAME} (${BOT_FIRST_NAME})${NC}"
else
    echo -e "${RED}❌ Не удалось получить информацию о боте${NC}"
fi
echo ""

# 6. Статистика ошибок
echo "📊 6. Статистика (за последние 100 строк):"
echo "-------------------------------------------"
TOTAL_LOGS=$(docker logs scanner-notify-worker --tail 100 2>&1 | wc -l)
SUCCESS_COUNT=$(docker logs scanner-notify-worker --tail 100 2>&1 | grep -c "✅.*отправлен" || echo "0")
ERROR_COUNT=$(docker logs scanner-notify-worker --tail 100 2>&1 | grep -c "❌" || echo "0")

echo "Всего записей: $TOTAL_LOGS"
echo -e "Успешных отправок: ${GREEN}${SUCCESS_COUNT}${NC}"
echo -e "Ошибок: ${RED}${ERROR_COUNT}${NC}"
echo ""

# 7. Последние сообщения в Redis
echo "💬 7. Последние 2 сообщения в Redis:"
echo "------------------------------------"
docker exec scanner-redis-worker-1 redis-cli XRANGE notify:telegram - + COUNT 2 2>/dev/null | \
    grep -A 1 "text" | head -8 | sed 's/^/  /'
echo ""

# 8. Итоговый статус
echo "═══════════════════════════════════════"
if [ "$SUCCESS_COUNT" -gt 0 ] && [ "$ERROR_COUNT" -eq 0 ]; then
    echo -e "${GREEN}✅ СИСТЕМА РАБОТАЕТ КОРРЕКТНО${NC}"
elif [ "$SUCCESS_COUNT" -gt 0 ] && [ "$ERROR_COUNT" -gt 0 ]; then
    echo -e "${YELLOW}⚠️  СИСТЕМА РАБОТАЕТ С ОШИБКАМИ${NC}"
else
    echo -e "${RED}❌ СИСТЕМА НЕ РАБОТАЕТ${NC}"
fi
echo "═══════════════════════════════════════"
echo ""

# 9. Опции для действий
echo "🔧 Доступные команды:"
echo "--------------------"
echo "  Отправить тест:        ./scripts/send_test_signal.sh"
echo "  Просмотр логов:        docker logs -f scanner-notify-worker"
echo "  Перезапуск:            docker restart scanner-notify-worker"
echo ""

