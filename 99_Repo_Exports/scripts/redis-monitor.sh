#!/bin/bash

# Redis Monitor Script для scanner-infra
# Мониторинг производительности и диагностика проблем

REDIS_HOST=${REDIS_HOST:-scanner-redis-worker-1}
REDIS_PORT=${REDIS_PORT:-6379}
REDIS_CLI="redis-cli -h $REDIS_HOST -p $REDIS_PORT"

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Redis Monitor для scanner-infra ===${NC}"
echo "Host: $REDIS_HOST:$REDIS_PORT"
echo "Time: $(date)"
echo

# Функция для проверки соединения
check_connection() {
    echo -e "${BLUE}1. Проверка соединения...${NC}"
    if $REDIS_CLI ping > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Redis доступен${NC}"
        return 0
    else
        echo -e "${RED}❌ Redis недоступен${NC}"
        return 1
    fi
}

# Функция для проверки памяти
check_memory() {
    echo -e "${BLUE}2. Проверка памяти...${NC}"
    MEMORY_INFO=$($REDIS_CLI info memory 2>/dev/null)
    if [ $? -eq 0 ]; then
        USED_MEMORY=$(echo "$MEMORY_INFO" | grep "used_memory_human:" | cut -d: -f2 | tr -d '\r')
        MAX_MEMORY=$(echo "$MEMORY_INFO" | grep "maxmemory_human:" | cut -d: -f2 | tr -d '\r')
        MEMORY_FRAGMENTATION=$(echo "$MEMORY_INFO" | grep "mem_fragmentation_ratio:" | cut -d: -f2 | tr -d '\r')
        
        echo "  Используется памяти: $USED_MEMORY"
        echo "  Максимум памяти: $MAX_MEMORY"
        echo "  Фрагментация: $MEMORY_FRAGMENTATION"
        
        # Проверка на высокую фрагментацию
        FRAG_RATIO=$(echo "$MEMORY_FRAGMENTATION" | cut -d. -f1)
        if [ "$FRAG_RATIO" -gt 2 ]; then
            echo -e "${YELLOW}⚠️ Высокая фрагментация памяти${NC}"
        else
            echo -e "${GREEN}✅ Фрагментация в норме${NC}"
        fi
    else
        echo -e "${RED}❌ Не удалось получить информацию о памяти${NC}"
    fi
}

# Функция для проверки клиентов
check_clients() {
    echo -e "${BLUE}3. Проверка клиентов...${NC}"
    CLIENTS_INFO=$($REDIS_CLI info clients 2>/dev/null)
    if [ $? -eq 0 ]; then
        CONNECTED_CLIENTS=$(echo "$CLIENTS_INFO" | grep "connected_clients:" | cut -d: -f2 | tr -d '\r')
        BLOCKED_CLIENTS=$(echo "$CLIENTS_INFO" | grep "blocked_clients:" | cut -d: -f2 | tr -d '\r')
        
        echo "  Подключенных клиентов: $CONNECTED_CLIENTS"
        echo "  Заблокированных клиентов: $BLOCKED_CLIENTS"
        
        # Проверка на большое количество клиентов
        if [ "$CONNECTED_CLIENTS" -gt 1000 ]; then
            echo -e "${YELLOW}⚠️ Много подключенных клиентов${NC}"
        else
            echo -e "${GREEN}✅ Количество клиентов в норме${NC}"
        fi
    else
        echo -e "${RED}❌ Не удалось получить информацию о клиентах${NC}"
    fi
}

# Функция для проверки стримов
check_streams() {
    echo -e "${BLUE}4. Проверка стримов...${NC}"
    STREAMS=$($REDIS_CLI keys "stream:*" 2>/dev/null)
    if [ $? -eq 0 ] && [ -n "$STREAMS" ]; then
        echo "  Найдено стримов: $(echo "$STREAMS" | wc -l)"
        for stream in $STREAMS; do
            LENGTH=$($REDIS_CLI xlen "$stream" 2>/dev/null)
            echo "    $stream: $LENGTH сообщений"
        done
    else
        echo -e "${YELLOW}⚠️ Стримы не найдены или недоступны${NC}"
    fi
}

# Функция для проверки медленных команд
check_slowlog() {
    echo -e "${BLUE}5. Проверка медленных команд...${NC}"
    SLOWLOG=$($REDIS_CLI slowlog get 10 2>/dev/null)
    if [ $? -eq 0 ]; then
        SLOW_COUNT=$(echo "$SLOWLOG" | grep -c "slowlog")
        if [ "$SLOW_COUNT" -gt 0 ]; then
            echo -e "${YELLOW}⚠️ Найдено $SLOW_COUNT медленных команд${NC}"
            echo "$SLOWLOG" | head -20
        else
            echo -e "${GREEN}✅ Медленных команд не найдено${NC}"
        fi
    else
        echo -e "${RED}❌ Не удалось получить slowlog${NC}"
    fi
}

# Функция для проверки репликации
check_replication() {
    echo -e "${BLUE}6. Проверка репликации...${NC}"
    REPLICATION_INFO=$($REDIS_CLI info replication 2>/dev/null)
    if [ $? -eq 0 ]; then
        ROLE=$(echo "$REPLICATION_INFO" | grep "role:" | cut -d: -f2 | tr -d '\r')
        echo "  Роль: $ROLE"
        
        if [ "$ROLE" = "master" ]; then
            CONNECTED_SLAVES=$(echo "$REPLICATION_INFO" | grep "connected_slaves:" | cut -d: -f2 | tr -d '\r')
            echo "  Подключенных реплик: $CONNECTED_SLAVES"
        fi
    else
        echo -e "${RED}❌ Не удалось получить информацию о репликации${NC}"
    fi
}

# Функция для проверки производительности
check_performance() {
    echo -e "${BLUE}7. Проверка производительности...${NC}"
    STATS_INFO=$($REDIS_CLI info stats 2>/dev/null)
    if [ $? -eq 0 ]; then
        OPS_PER_SEC=$(echo "$STATS_INFO" | grep "instantaneous_ops_per_sec:" | cut -d: -f2 | tr -d '\r')
        KEYS_PER_SEC=$(echo "$STATS_INFO" | grep "instantaneous_input_kbps:" | cut -d: -f2 | tr -d '\r')
        OUTPUT_KBPS=$(echo "$STATS_INFO" | grep "instantaneous_output_kbps:" | cut -d: -f2 | tr -d '\r')
        
        echo "  Операций в секунду: $OPS_PER_SEC"
        echo "  Входящий трафик: $KEYS_PER_SEC KB/s"
        echo "  Исходящий трафик: $OUTPUT_KBPS KB/s"
        
        # Проверка на высокую нагрузку
        if [ "$OPS_PER_SEC" -gt 10000 ]; then
            echo -e "${YELLOW}⚠️ Высокая нагрузка на Redis${NC}"
        else
            echo -e "${GREEN}✅ Нагрузка в норме${NC}"
        fi
    else
        echo -e "${RED}❌ Не удалось получить статистику${NC}"
    fi
}

# Функция для проверки конфигурации
check_config() {
    echo -e "${BLUE}8. Проверка конфигурации...${NC}"
    CONFIG_INFO=$($REDIS_CLI config get "*" 2>/dev/null | grep -E "(timeout|tcp-keepalive|maxclients|maxmemory)" | head -10)
    if [ $? -eq 0 ]; then
        echo "  Ключевые настройки:"
        echo "$CONFIG_INFO" | while read line; do
            echo "    $line"
        done
    else
        echo -e "${RED}❌ Не удалось получить конфигурацию${NC}"
    fi
}

# Основная функция
main() {
    if ! check_connection; then
        echo -e "${RED}Redis недоступен, завершение мониторинга${NC}"
        exit 1
    fi
    
    check_memory
    echo
    check_clients
    echo
    check_streams
    echo
    check_slowlog
    echo
    check_replication
    echo
    check_performance
    echo
    check_config
    echo
    
    echo -e "${GREEN}=== Мониторинг завершен ===${NC}"
}

# Запуск мониторинга
main "$@"
