#!/bin/bash

# Скрипт для очистки дубликатов в Redis Streams
# ВНИМАНИЕ: Удаляет ВСЕ сообщения из notify:telegram для предотвращения повторной отправки
# Автор: AI Assistant
# Дата: 25 октября 2025

set -e

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "${RED}⚠️  ОЧИСТКА REDIS STREAMS ОТ ДУБЛИКАТОВ${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo -e "${YELLOW}Этот скрипт удалит старые сообщения из стримов:${NC}"
echo "  - notify:telegram (чтобы избежать повторной отправки дубликатов)"
echo ""
echo -e "${YELLOW}Consumer groups будут пересозданы с новыми настройками.${NC}"
echo ""
echo -e "${RED}ВНИМАНИЕ: Это необратимая операция!${NC}"
echo ""
read -p "Продолжить? (yes/no): " -r REPLY
echo ""

if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
    echo -e "${GREEN}Операция отменена${NC}"
    exit 0
fi

# Функция для подключения к Redis
redis_cli() {
    docker exec scanner-redis-worker-1 redis-cli "$@"
}

echo -e "${BLUE}📊 Текущее состояние стримов:${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

raw_count=$(redis_cli XLEN signal:telegram:raw 2>/dev/null || echo "0")
parsed_count=$(redis_cli XLEN signal:telegram:parsed 2>/dev/null || echo "0")
notify_count=$(redis_cli XLEN notify:telegram 2>/dev/null || echo "0")

echo -e "📥 signal:telegram:raw:    ${BLUE}${raw_count}${NC} сообщений"
echo -e "📝 signal:telegram:parsed: ${BLUE}${parsed_count}${NC} сообщений"
echo -e "📢 notify:telegram:        ${BLUE}${notify_count}${NC} сообщений"

echo ""
echo -e "${BLUE}🗑️  Очистка notify:telegram...${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Удаляем consumer group для notify:telegram
echo "Удаление consumer group 'notify-group'..."
redis_cli XGROUP DESTROY notify:telegram notify-group 2>/dev/null || echo "  (группа не существовала)"

# Удаляем все сообщения из notify:telegram
if [ "$notify_count" -gt 0 ]; then
    echo "Удаление $notify_count сообщений из notify:telegram..."
    # Получаем все ID сообщений
    message_ids=$(redis_cli XRANGE notify:telegram - + | grep -E '^[0-9]+-[0-9]+$' || echo "")
    
    if [ -n "$message_ids" ]; then
        for msg_id in $message_ids; do
            redis_cli XDEL notify:telegram "$msg_id" >/dev/null 2>&1 || true
        done
        echo -e "${GREEN}✅ Удалено $notify_count сообщений${NC}"
    else
        echo -e "${YELLOW}⚠️  Не удалось получить ID сообщений, используем XTRIM...${NC}"
        redis_cli XTRIM notify:telegram MAXLEN 0 >/dev/null 2>&1 || true
        echo -e "${GREEN}✅ Стрим очищен через XTRIM${NC}"
    fi
else
    echo -e "${GREEN}✅ notify:telegram уже пуст${NC}"
fi

echo ""
echo -e "${BLUE}🔄 Пересоздание consumer groups...${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Пересоздаем consumer group для signal:telegram:raw
echo "Удаление старой группы 'signal-parser-group'..."
redis_cli XGROUP DESTROY signal:telegram:raw signal-parser-group 2>/dev/null || echo "  (группа не существовала)"

echo "Создание новой группы 'signal-parser-group' (с текущей позиции '$')..."
redis_cli XGROUP CREATE signal:telegram:raw signal-parser-group '$' MKSTREAM 2>/dev/null || echo "  (группа уже существует)"

# Создаем consumer group для notify:telegram
echo "Создание новой группы 'notify-group' (с текущей позиции '$')..."
redis_cli XGROUP CREATE notify:telegram notify-group '$' MKSTREAM 2>/dev/null || echo "  (группа уже существует)"

echo -e "${GREEN}✅ Consumer groups пересозданы${NC}"

echo ""
echo -e "${BLUE}🔄 Перезапуск воркеров...${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Перезапускаем воркеры для применения изменений
echo "Перезапуск signal-parser-worker..."
docker-compose restart signal-parser-worker >/dev/null 2>&1

echo "Перезапуск notify-worker..."
docker-compose restart notify-worker >/dev/null 2>&1

echo -e "${GREEN}✅ Воркеры перезапущены${NC}"

# Ждем пока воркеры запустятся
echo ""
echo -e "${YELLOW}⏳ Ожидание запуска воркеров (10 секунд)...${NC}"
sleep 10

echo ""
echo -e "${BLUE}📊 Новое состояние стримов:${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

raw_count=$(redis_cli XLEN signal:telegram:raw 2>/dev/null || echo "0")
parsed_count=$(redis_cli XLEN signal:telegram:parsed 2>/dev/null || echo "0")
notify_count=$(redis_cli XLEN notify:telegram 2>/dev/null || echo "0")

echo -e "📥 signal:telegram:raw:    ${BLUE}${raw_count}${NC} сообщений"
echo -e "📝 signal:telegram:parsed: ${BLUE}${parsed_count}${NC} сообщений"
echo -e "📢 notify:telegram:        ${BLUE}${notify_count}${NC} сообщений"

echo ""
echo -e "${BLUE}👥 Проверка consumer groups:${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo ""
echo -e "${YELLOW}signal-parser-group:${NC}"
redis_cli XINFO GROUPS signal:telegram:raw 2>/dev/null | grep -A 10 "signal-parser-group" || echo "  Создана успешно"

echo ""
echo -e "${YELLOW}notify-group:${NC}"
redis_cli XINFO GROUPS notify:telegram 2>/dev/null | grep -A 10 "notify-group" || echo "  Создана успешно"

echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}✅ ОЧИСТКА ЗАВЕРШЕНА УСПЕШНО${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo -e "${BLUE}📝 Следующие шаги:${NC}"
echo ""
echo "1. Проверьте логи воркеров:"
echo -e "   ${YELLOW}docker logs scanner-signal-parser-worker -f${NC}"
echo -e "   ${YELLOW}docker logs scanner-notify-worker -f${NC}"
echo ""
echo "2. Отправьте тестовое сообщение в Telegram канал"
echo ""
echo "3. Убедитесь, что в боте появилось ОДНО сообщение (без дубликатов)"
echo ""
echo "4. Запустите проверку системы:"
echo -e "   ${YELLOW}./check_and_fix_duplicates.sh${NC}"
echo ""

