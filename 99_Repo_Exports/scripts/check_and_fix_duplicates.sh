#!/bin/bash

# Скрипт для проверки и исправления дубликатов в Redis Streams
# Автор: AI Assistant
# Дата: 25 октября 2025

set -e

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🔍 ПРОВЕРКА СИСТЕМЫ НА ДУБЛИКАТЫ СООБЩЕНИЙ"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Функция для проверки контейнера
check_container() {
    local container_name=$1
    if docker ps --format '{{.Names}}' | grep -q "^${container_name}$"; then
        echo -e "${GREEN}✅ ${container_name} запущен${NC}"
        return 0
    else
        echo -e "${RED}❌ ${container_name} не запущен${NC}"
        return 1
    fi
}

# Функция для подключения к Redis
redis_cli() {
    docker exec scanner-redis-worker-1 redis-cli "$@"
}

echo ""
echo -e "${BLUE}📦 Проверка контейнеров...${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Проверяем основные контейнеры
check_container "scanner-telegram-worker" || exit 1
check_container "scanner-signal-parser-worker" || exit 1
check_container "scanner-notify-worker" || exit 1

# Проверяем, что forward-all-worker НЕ запущен (это хорошо!)
if docker ps --format '{{.Names}}' | grep -q "^scanner-forward-all-worker$"; then
    echo -e "${RED}❌ scanner-forward-all-worker запущен (должен быть отключен!)${NC}"
    echo -e "${YELLOW}⚠️  Этот воркер вызывает дубликаты. Останавливаем...${NC}"
    docker stop scanner-forward-all-worker 2>/dev/null || true
    docker rm scanner-forward-all-worker 2>/dev/null || true
    echo -e "${GREEN}✅ scanner-forward-all-worker остановлен${NC}"
else
    echo -e "${GREEN}✅ scanner-forward-all-worker отключен (правильно!)${NC}"
fi

echo ""
echo -e "${BLUE}📊 Статистика Redis Streams...${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Проверяем количество сообщений в стримах
raw_count=$(redis_cli XLEN signal:telegram:raw 2>/dev/null || echo "0")
parsed_count=$(redis_cli XLEN signal:telegram:parsed 2>/dev/null || echo "0")
notify_count=$(redis_cli XLEN notify:telegram 2>/dev/null || echo "0")

echo -e "📥 signal:telegram:raw:    ${BLUE}${raw_count}${NC} сообщений"
echo -e "📝 signal:telegram:parsed: ${BLUE}${parsed_count}${NC} сообщений"
echo -e "📢 notify:telegram:        ${BLUE}${notify_count}${NC} сообщений"

echo ""
echo -e "${BLUE}👥 Consumer Groups...${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Проверяем consumer groups
echo -e "${YELLOW}signal:telegram:raw (signal-parser-group):${NC}"
redis_cli XINFO GROUPS signal:telegram:raw 2>/dev/null | head -20 || echo "  Нет групп"

echo ""
echo -e "${YELLOW}notify:telegram (notify-group):${NC}"
redis_cli XINFO GROUPS notify:telegram 2>/dev/null | head -20 || echo "  Нет групп"

echo ""
echo -e "${BLUE}🔍 Проверка логов воркеров (последние 10 строк)...${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo ""
echo -e "${YELLOW}📝 signal-parser-worker:${NC}"
docker logs scanner-signal-parser-worker --tail 10 2>&1 | tail -10

echo ""
echo -e "${YELLOW}📢 notify-worker:${NC}"
docker logs scanner-notify-worker --tail 10 2>&1 | tail -10

echo ""
echo -e "${BLUE}💡 Рекомендации:${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo "1. Если видите дубликаты, очистите стримы:"
echo -e "   ${YELLOW}./clear_duplicate_streams.sh${NC}"
echo ""
echo "2. Мониторьте логи в реальном времени:"
echo -e "   ${YELLOW}docker logs scanner-signal-parser-worker -f${NC}"
echo -e "   ${YELLOW}docker logs scanner-notify-worker -f${NC}"
echo ""
echo "3. Проверьте, что в Telegram боте нет дубликатов:"
echo "   - Отправьте тестовое сообщение в канал"
echo "   - Проверьте, что в боте появилось ОДНО сообщение"
echo ""
echo "4. При необходимости перезапустите воркеры:"
echo -e "   ${YELLOW}docker-compose restart signal-parser-worker notify-worker${NC}"

echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}✅ Проверка завершена${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

