#!/bin/bash
"""
Запуск Unified Signal Reader для объединения XAUUSD и канальных сигналов
"""

set -e

# Цвета для логов
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 ЗАПУСК UNIFIED SIGNAL READER${NC}"
echo -e "${BLUE}=================================${NC}"

# Проверяем переменные окружения
echo -e "${YELLOW}📋 Проверка переменных окружения...${NC}"

if [ -z "$TELEGRAM_BOT_TOKEN" ]; then
    echo -e "${RED}❌ TELEGRAM_BOT_TOKEN не установлен${NC}"
    exit 1
fi

if [ -z "$TELEGRAM_NOTIFY_CHAT_IDS" ]; then
    echo -e "${RED}❌ TELEGRAM_NOTIFY_CHAT_IDS не установлен${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Bot Token: ${TELEGRAM_BOT_TOKEN:0:10}...${NC}"
echo -e "${GREEN}✅ Chat IDs: $TELEGRAM_NOTIFY_CHAT_IDS${NC}"

# Проверяем Redis подключение
echo -e "${YELLOW}🔍 Проверка Redis подключения...${NC}"
REDIS_URL=${REDIS_URL:-"redis://scanner-redis-worker-1:6379/0"}

if ! python3 -c "import redis; r=redis.from_url('$REDIS_URL'); r.ping(); print('Redis OK')" > /dev/null 2>&1; then
    echo -e "${RED}❌ Redis недоступен: $REDIS_URL${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Redis подключение успешно${NC}"

# Устанавливаем зависимости если нужно
echo -e "${YELLOW}📦 Проверка зависимостей...${NC}"
python3 -c "import redis, requests, asyncio" 2>/dev/null || {
    echo -e "${YELLOW}⚠️ Устанавливаем зависимости...${NC}"
    pip3 install redis requests asyncio
}

echo -e "${GREEN}✅ Зависимости готовы${NC}"

# Запускаем unified signal reader
echo -e "${BLUE}🔄 Запуск Unified Signal Reader...${NC}"
echo -e "${BLUE}=================================${NC}"

# Экспортируем переменные для Python скрипта
export REDIS_URL=${REDIS_URL}
export NOTIFY_STREAM=${NOTIFY_STREAM:-"notify:telegram"}
export PARSED_STREAM=${PARSED_STREAM:-"signal:telegram:parsed"}

cd "$(dirname "$0")"

# Запускаем с restart при падении
while true; do
    echo -e "${GREEN}🚀 Запуск unified_signal_reader.py...${NC}"
    
    python3 unified_signal_reader.py
    exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}✅ Unified Signal Reader завершился успешно${NC}"
        break
    else
        echo -e "${RED}❌ Unified Signal Reader упал с кодом $exit_code${NC}"
        echo -e "${YELLOW}🔄 Перезапуск через 5 секунд...${NC}"
        sleep 5
    fi
done
