#!/bin/bash

# Скрипт полной очистки всех данных Redis и Volume
# Удаляет ВСЕ ключи, стримы, сигналы, сделки, volume и другие данные

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

# Все паттерны ключей для полной очистки
KEY_PATTERNS=(
    "*"  # Все ключи - полная очистка
)

# Паттерны стримов для очистки
STREAM_PATTERNS=(
    "*"  # Все стримы
)

# Проверка флага --yes
AUTO_CONFIRM=false
if [ "$1" = "--yes" ] || [ "$1" = "-y" ]; then
    AUTO_CONFIRM=true
fi

echo -e "${RED}🧹 ПОЛНАЯ ОЧИСТКА ВСЕХ ДАННЫХ REDIS${NC}"
echo -e "${RED}=====================================${NC}"
echo -e "${YELLOW}⚠️  ВНИМАНИЕ: Это удалит ВСЕ данные из Redis!${NC}"
echo -e "${YELLOW}   Включая сигналы, сделки, volume, стримы, свечи и т.д.${NC}"
echo ""

# Подтверждение
if [ "$AUTO_CONFIRM" = false ]; then
    read -p "Вы уверены, что хотите удалить ВСЕ данные? (yes/no): " confirm
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

# Функция для полной очистки всех ключей в контейнере
clear_all_keys() {
    local container=$1
    local total_keys=$(redis_cmd "$container" DBSIZE)
    if [ "$total_keys" -gt 0 ]; then
        # Используем FLUSHDB для полной очистки
        redis_cmd "$container" FLUSHDB > /dev/null 2>&1 || true
        echo "$total_keys"
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
echo -e "${BLUE}📊 Подсчет общего количества данных...${NC}"

for container in "${AVAILABLE_CONTAINERS[@]}"; do
    echo -e "${YELLOW}$container:${NC}"
    total_keys=$(redis_cmd "$container" DBSIZE)
    echo -e "  Всего ключей: $total_keys"

    # Подсчет по типам данных
    signal_keys=$(count_keys "$container" "*signal*")
    trade_keys=$(count_keys "$container" "*trade*")
    volume_keys=$(count_keys "$container" "*volume*")
    kline_keys=$(count_keys "$container" "*kline*")
    stream_count=$(redis_cmd "$container" --scan --pattern "*" | xargs -I {} sh -c 'docker exec '"$container"' redis-cli -p 6379 TYPE {} 2>/dev/null | grep -c stream' 2>/dev/null || echo "0")

    echo -e "  Сигналы: $signal_keys"
    echo -e "  Сделки: $trade_keys"
    echo -e "  Volume: $volume_keys"
    echo -e "  Свечи: $kline_keys"
    echo -e "  Стримов: $stream_count"
done

echo ""
echo -e "${BLUE}🧹 Начало полной очистки...${NC}"

TOTAL_DELETED=0

for container in "${AVAILABLE_CONTAINERS[@]}"; do
    echo -e "${YELLOW}Очистка $container...${NC}"

    # Полная очистка всех ключей с помощью FLUSHDB
    deleted=$(clear_all_keys "$container")
    TOTAL_DELETED=$((TOTAL_DELETED + deleted))
    echo -e "  ${GREEN}✅ Удалено всех ключей: $deleted${NC}"
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
echo -e "${GREEN}✅ ПОЛНАЯ ОЧИСТКА ЗАВЕРШЕНА${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "${BLUE}Всего удалено ключей: $TOTAL_DELETED${NC}"
echo -e "${GREEN}========================================${NC}"

# Финальная проверка
echo ""
echo -e "${BLUE}📊 Финальная проверка...${NC}"
for container in "${AVAILABLE_CONTAINERS[@]}"; do
    final_keys=$(redis_cmd "$container" DBSIZE)
    echo -e "${YELLOW}$container:${NC} $final_keys ключей осталось"
done
