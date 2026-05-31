#!/bin/bash

# Быстрая проверка настроек Redis для scanner-infra
# Проверяет основные параметры производительности и памяти

set -e

# Цвета
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Конфигурация
REDIS_HOST=${REDIS_HOST:-localhost}
REDIS_PORT=${REDIS_PORT:-6379}
REDIS_CLI="redis-cli -h $REDIS_HOST -p $REDIS_PORT"

echo -e "${GREEN}🔍 Быстрая проверка Redis для scanner-infra${NC}"
echo -e "${GREEN}========================================${NC}"

# Проверка подключения
echo -e "${YELLOW}📡 Проверка подключения...${NC}"
if $REDIS_CLI ping > /dev/null 2>&1; then
    echo -e "${GREEN}✅ Redis доступен${NC}"
else
    echo -e "${RED}❌ Redis недоступен${NC}"
    exit 1
fi

echo

# Проверка основных настроек
echo -e "${YELLOW}⚙️ Основные настройки:${NC}"

# Память
maxmemory=$($REDIS_CLI config get maxmemory | tail -1)
maxmemory_policy=$($REDIS_CLI config get maxmemory-policy | tail -1)
used_memory=$($REDIS_CLI info memory | grep "used_memory_human" | cut -d: -f2)

echo -e "  💾 Максимальная память: ${GREEN}$maxmemory${NC}"
echo -e "  🎯 Политика памяти: ${GREEN}$maxmemory_policy${NC}"
echo -e "  📊 Используется памяти: ${GREEN}$used_memory${NC}"

# Клиенты
maxclients=$($REDIS_CLI config get maxclients | tail -1)
connected_clients=$($REDIS_CLI info clients | grep "connected_clients" | cut -d: -f2)

echo -e "  🔌 Максимум клиентов: ${GREEN}$maxclients${NC}"
echo -e "  📱 Подключено клиентов: ${GREEN}$connected_clients${NC}"

# Производительность
io_threads=$($REDIS_CLI config get io-threads | tail -1)
ops_per_sec=$($REDIS_CLI info stats | grep "instantaneous_ops_per_sec" | cut -d: -f2)

echo -e "  ⚡ I/O потоков: ${GREEN}$io_threads${NC}"
echo -e "  🚀 Операций/сек: ${GREEN}$ops_per_sec${NC}"

echo

# Проверка Streams
echo -e "${YELLOW}📡 Проверка Redis Streams:${NC}"

streams=("stream:kline_1m" "stream:kline_5m" "stream:symbol-to-redis" "signal:telegram:raw" "signal:telegram:parsed")

for stream in "${streams[@]}"; do
    length=$($REDIS_CLI xlen "$stream" 2>/dev/null || echo "0")
    if [ "$length" != "0" ]; then
        echo -e "  ${GREEN}✅ $stream: $length сообщений${NC}"
    else
        echo -e "  ${YELLOW}⏸️ $stream: пустой${NC}"
    fi
done

echo

# Проверка здоровья
echo -e "${YELLOW}🏥 Проверка здоровья:${NC}"

# Проверка памяти
if [ "$maxmemory" != "0" ]; then
    used_memory_bytes=$($REDIS_CLI info memory | grep "used_memory:" | cut -d: -f2)
    memory_usage=$((used_memory_bytes * 100 / maxmemory))
    
    if [ $memory_usage -gt 90 ]; then
        echo -e "  ${RED}⚠️ Память: ${memory_usage}% (критично)${NC}"
    elif [ $memory_usage -gt 80 ]; then
        echo -e "  ${YELLOW}⚠️ Память: ${memory_usage}% (внимание)${NC}"
    else
        echo -e "  ${GREEN}✅ Память: ${memory_usage}%${NC}"
    fi
fi

# Проверка клиентов
if [ "$maxclients" != "0" ]; then
    client_usage=$((connected_clients * 100 / maxclients))
    
    if [ $client_usage -gt 80 ]; then
        echo -e "  ${RED}⚠️ Клиенты: ${client_usage}% (критично)${NC}"
    elif [ $client_usage -gt 60 ]; then
        echo -e "  ${YELLOW}⚠️ Клиенты: ${client_usage}% (внимание)${NC}"
    else
        echo -e "  ${GREEN}✅ Клиенты: ${client_usage}%${NC}"
    fi
fi

# Проверка uptime
uptime=$($REDIS_CLI info server | grep "uptime_in_seconds" | cut -d: -f2)
uptime_hours=$((uptime / 3600))
echo -e "  ${GREEN}⏰ Uptime: ${uptime_hours} часов${NC}"

echo

# Рекомендации
echo -e "${YELLOW}💡 Рекомендации:${NC}"

if [ "$maxmemory" = "0" ]; then
    echo -e "  ${YELLOW}⚠️ Установите лимит памяти: redis-cli config set maxmemory 512mb${NC}"
fi

if [ "$maxmemory_policy" != "volatile-lru" ] && [ "$maxmemory_policy" != "allkeys-lru" ]; then
    echo -e "  ${YELLOW}⚠️ Рекомендуется LRU политика: redis-cli config set maxmemory-policy volatile-lru${NC}"
fi

if [ "$io_threads" = "1" ]; then
    echo -e "  ${YELLOW}⚠️ Увеличьте I/O потоки: redis-cli config set io-threads 4${NC}"
fi

echo -e "${GREEN}✅ Проверка завершена${NC}"
echo -e "${GREEN}Для детального мониторинга используйте: ./redis-monitor.sh all${NC}" 