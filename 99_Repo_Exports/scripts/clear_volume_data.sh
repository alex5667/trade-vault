#!/bin/bash

# Скрипт для очистки всех данных volume
# Удаляет volume сигналы и связанные данные

set -e

# Цвета
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

# Конфигурация Redis контейнеров
REDIS_CONTAINERS=("redis" "redis-ticks")
AVAILABLE_CONTAINERS=()

# Паттерны ключей для очистки volume
KEY_PATTERNS=("volume:*" "*volume*")
STREAM_PATTERNS=("stream:volume-signals" "volume:*")

# Проверка флага --yes
AUTO_CONFIRM=false
if [ "$1" = "--yes" ] || [ "$1" = "-y" ]; then
    AUTO_CONFIRM=true
fi

echo -e "${RED}🧹 ОЧИСТКА ДАННЫХ VOLUME${NC}"
echo -e "${RED}========================================${NC}"
echo -e "${YELLOW}⚠️  ВНИМАНИЕ: Это удалит все данные volume!${NC}"
echo ""
echo -e "${YELLOW}Будут удалены:${NC}"
echo -e "  • Все volume сигналы (volume:*, *volume*)"
echo -e "  • Все volume streams (stream:volume-signals)"
echo ""

# Подтверждение
if [ "$AUTO_CONFIRM" = false ]; then
    read -p "Вы уверены, что хотите продолжить? (yes/no): " confirm
    if [ "$confirm" != "yes" ]; then
        echo -e "${YELLOW}❌ Очистка отменена${NC}"
        exit 0
    fi
else
    echo -e "${BLUE}Автоматическое подтверждение (--yes)${NC}"
fi

# Функция для выполнения команд Redis
redis_cmd() {
    local container=$1
    shift
    docker exec "$container" redis-cli -p 6379 "$@" 2>/dev/null || echo "0"
}

# Функция для подсчета ключей по паттерну
count_keys() {
    local container=$1
    local pattern=$2
    redis_cmd "$container" --scan --pattern "$pattern" 2>/dev/null | wc -l
}

# Функция для удаления ключей по паттерну
delete_keys() {
    local container=$1
    local pattern=$2
    local count=$(count_keys "$container" "$pattern")
    if [ "$count" -gt 0 ]; then
        redis_cmd "$container" --scan --pattern "$pattern" 2>/dev/null | xargs -I {} redis_cmd "$container" DEL {} > /dev/null 2>&1 || true
        echo "$count"
    else
        echo "0"
    fi
}

# Функция для очистки stream
clear_stream() {
    local container=$1
    local stream_name=$2
    local length=$(redis_cmd "$container" XLEN "$stream_name" 2>/dev/null || echo "0")
    if [ "$length" -gt 0 ]; then
        redis_cmd "$container" DEL "$stream_name" > /dev/null 2>&1 || true
        echo "$length"
    else
        echo "0"
    fi
}

# Проверка доступности контейнеров
echo -e "${BLUE}🔍 Проверка доступности Redis контейнеров...${NC}"

check_container() {
    local container=$1
    if docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
        if redis_cmd "$container" ping > /dev/null 2>&1; then
            echo -e "  ${GREEN}✅ $container доступен${NC}"
            AVAILABLE_CONTAINERS+=("$container")
            return 0
        else
            echo -e "  ${YELLOW}⚠️  $container существует, но не отвечает${NC}"
            return 1
        fi
    else
        echo -e "  ${YELLOW}⚠️  $container не запущен (пропуск)${NC}"
        return 1
    fi
}

for container in "${REDIS_CONTAINERS[@]}"; do
    check_container "$container"
done

if [ ${#AVAILABLE_CONTAINERS[@]} -eq 0 ]; then
    echo -e "${RED}❌ Ни один Redis контейнер не доступен${NC}"
    exit 1
fi

echo ""
echo -e "${BLUE}📊 Подсчет данных для очистки...${NC}"

for container in "${AVAILABLE_CONTAINERS[@]}"; do
    echo -e "${YELLOW}$container:${NC}"
    for pattern in "${KEY_PATTERNS[@]}"; do
        count=$(count_keys "$container" "$pattern")
        echo -e "  $pattern: $count"
    done
    for stream in "${STREAM_PATTERNS[@]}"; do
        length=$(redis_cmd "$container" XLEN "$stream" 2>/dev/null || echo "0")
        echo -e "  $stream: $length"
    done
done

echo ""
echo -e "${BLUE}🧹 Начало очистки...${NC}"

TOTAL_DELETED=0

for container in "${AVAILABLE_CONTAINERS[@]}"; do
    echo -e "${YELLOW}Очистка $container...${NC}"
    
    # Очистка ключей по паттернам
    for pattern in "${KEY_PATTERNS[@]}"; do
        deleted=$(delete_keys "$container" "$pattern")
        TOTAL_DELETED=$((TOTAL_DELETED + deleted))
        [ "$deleted" -gt 0 ] && echo -e "  ${GREEN}✅ Удалено $pattern: $deleted${NC}"
    done
    
    # Очистка stream'ов для всех контейнеров
    for stream in "${STREAM_PATTERNS[@]}"; do
        deleted=$(clear_stream "$container" "$stream")
        TOTAL_DELETED=$((TOTAL_DELETED + deleted))
        [ "$deleted" -gt 0 ] && echo -e "  ${GREEN}✅ Удалено из $stream: $deleted${NC}"
    done
done

# Очистка памяти
echo ""
echo -e "${BLUE}🧹 Очистка памяти...${NC}"
for container in "${AVAILABLE_CONTAINERS[@]}"; do
    redis_cmd "$container" memory purge > /dev/null 2>&1 || true
    echo -e "  ${GREEN}✅ Память $container очищена${NC}"
done

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}✅ ОЧИСТКА VOLUME ЗАВЕРШЕНА${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "${BLUE}Всего удалено ключей/записей: $TOTAL_DELETED${NC}"
echo -e "${GREEN}========================================${NC}"

