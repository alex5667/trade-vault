#!/bin/bash

# Быстрая проверка количества записей в Redis каналах stream:kline
# Простая версия без сложной обработки

set -e

# Цвета
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

# Конфигурация Redis
REDIS_HOST=${REDIS_HOST:-localhost}
REDIS_PORT=${REDIS_PORT:-6379}
REDIS_CLI="redis-cli -h $REDIS_HOST -p $REDIS_PORT"

echo -e "${BLUE}🔍 Быстрая проверка kline streams в Redis${NC}"
echo -e "${BLUE}==========================================${NC}"

# Проверка подключения
if ! $REDIS_CLI ping > /dev/null 2>&1; then
    echo -e "${RED}❌ Redis недоступен на $REDIS_HOST:$REDIS_PORT${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Redis доступен${NC}\n"

# Поиск kline streams
echo -e "${YELLOW}🔍 Поиск kline streams...${NC}"

# Используем SCAN для поиска
cursor=0
kline_streams=""

while true; do
    result=$($REDIS_CLI scan $cursor match "*" count 1000 2>/dev/null)
    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ Ошибка при сканировании${NC}"
        exit 1
    fi
    
    cursor=$(echo "$result" | head -1)
    keys=$(echo "$result" | tail -n +2 | grep "stream:kline")
    
    if [ -n "$keys" ]; then
        kline_streams="$kline_streams $keys"
    fi
    
    if [ "$cursor" = "0" ]; then
        break
    fi
done

# Обработка результатов
if [ -z "$kline_streams" ]; then
    echo -e "${YELLOW}⏸️ Kline streams не найдены${NC}"
    exit 0
fi

# Сортируем и подсчитываем
streams_array=($(echo "$kline_streams" | tr ' ' '\n' | grep -v '^$' | sort | uniq))
total_streams=${#streams_array[@]}

echo -e "${GREEN}✅ Найдено kline streams: $total_streams${NC}\n"

# Проверяем каждый stream
echo -e "${YELLOW}📊 Статистика по kline streams:${NC}"
echo -e "${BLUE}Название                    | Записей | Размер${NC}"
echo -e "${BLUE}----------------------------|---------|--------${NC}"

total_records=0
total_memory=0

for stream in "${streams_array[@]}"; do
    if [ -z "$stream" ]; then
        continue
    fi
    
    # Количество записей
    length=$($REDIS_CLI xlen "$stream" 2>/dev/null || echo "0")
    if ! [[ "$length" =~ ^[0-9]+$ ]]; then
        length="0"
    fi
    
    # Размер в памяти
    memory=$($REDIS_CLI memory usage "$stream" 2>/dev/null || echo "0")
    if [ "$memory" != "0" ] && [ "$memory" != "nil" ] && [[ "$memory" =~ ^[0-9]+$ ]]; then
        if [ "$memory" -gt 1048576 ]; then
            memory_human=$(echo "scale=1; $memory/1048576" | bc -l 2>/dev/null || echo "N/A")
            memory_human="${memory_human}MB"
        elif [ "$memory" -gt 1024 ]; then
            memory_human=$(echo "scale=1; $memory/1024" | bc -l 2>/dev/null || echo "N/A")
            memory_human="${memory_human}KB"
        else
            memory_human="${memory}B"
        fi
    else
        memory_human="N/A"
    fi
    
    # Выводим информацию
    printf "%-28s | %7s | %s\n" "$stream" "$length" "$memory_human"
    
    # Суммируем
    total_records=$((total_records + length))
    if [ "$memory" != "0" ] && [ "$memory" != "nil" ] && [[ "$memory" =~ ^[0-9]+$ ]]; then
        total_memory=$((total_memory + memory))
    fi
done

echo -e "${BLUE}----------------------------|---------|--------${NC}"

# Общая статистика
echo -e "\n${GREEN}📈 Общая статистика:${NC}"
echo -e "  📊 Всего записей: ${GREEN}$total_records${NC}"

if [ "$total_memory" -gt 0 ]; then
    if [ "$total_memory" -gt 1048576 ]; then
        total_memory_mb=$(echo "scale=1; $total_memory/1048576" | bc -l 2>/dev/null || echo "N/A")
        echo -e "  💾 Общий размер: ${GREEN}${total_memory_mb}MB${NC}"
    elif [ "$total_memory" -gt 1024 ]; then
        total_memory_kb=$(echo "scale=1; $total_memory/1024" | bc -l 2>/dev/null || echo "N/A")
        echo -e "  💾 Общий размер: ${GREEN}${total_memory_kb}KB${NC}"
    else
        echo -e "  💾 Общий размер: ${GREEN}${total_memory}B${NC}"
    fi
fi

echo -e "\n${GREEN}✅ Проверка завершена${NC}" 