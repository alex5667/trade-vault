#!/bin/bash

# Redis Stress Test Monitor - мониторинг в реальном времени
# Отображает статистику Redis во время stress test

REDIS_HOST=${REDIS_HOST:-scanner-redis-worker-1}
REDIS_PORT=${REDIS_PORT:-6379}
REDIS_CLI="redis-cli -h $REDIS_HOST -p $REDIS_PORT"

# Цвета
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m'

# Функция очистки экрана
clear_screen() {
    clear
    echo -e "${PURPLE}🔥 Redis Stress Test Monitor${NC}"
    echo -e "${PURPLE}============================${NC}"
    echo
}

# Функция получения статистики
get_stats() {
    local stats=$($REDIS_CLI info stats 2>/dev/null)
    local memory=$($REDIS_CLI info memory 2>/dev/null)
    local clients=$($REDIS_CLI info clients 2>/dev/null)
    local replication=$($REDIS_CLI info replication 2>/dev/null)
    
    # Извлекаем ключевые метрики
    local ops_per_sec=$(echo "$stats" | grep "instantaneous_ops_per_sec:" | cut -d: -f2 | tr -d '\r')
    local total_commands=$(echo "$stats" | grep "total_commands_processed:" | cut -d: -f2 | tr -d '\r')
    local keyspace_hits=$(echo "$stats" | grep "keyspace_hits:" | cut -d: -f2 | tr -d '\r')
    local keyspace_misses=$(echo "$stats" | grep "keyspace_misses:" | cut -d: -f2 | tr -d '\r')
    local expired_keys=$(echo "$stats" | grep "expired_keys:" | cut -d: -f2 | tr -d '\r')
    local evicted_keys=$(echo "$stats" | grep "evicted_keys:" | cut -d: -f2 | tr -d '\r')
    
    local used_memory=$(echo "$memory" | grep "used_memory:" | cut -d: -f2 | tr -d '\r')
    local used_memory_human=$(echo "$memory" | grep "used_memory_human:" | cut -d: -f2 | tr -d '\r')
    local max_memory=$(echo "$memory" | grep "maxmemory:" | cut -d: -f2 | tr -d '\r')
    local mem_fragmentation=$(echo "$memory" | grep "mem_fragmentation_ratio:" | cut -d: -f2 | tr -d '\r')
    
    local connected_clients=$(echo "$clients" | grep "connected_clients:" | cut -d: -f2 | tr -d '\r')
    local blocked_clients=$(echo "$clients" | grep "blocked_clients:" | cut -d: -f2 | tr -d '\r')
    
    local role=$(echo "$replication" | grep "role:" | cut -d: -f2 | tr -d '\r')
    
    # Вычисляем процент использования памяти
    local memory_percent=0
    if [ "$max_memory" -gt 0 ]; then
        memory_percent=$((used_memory * 100 / max_memory))
    fi
    
    # Вычисляем hit rate
    local hit_rate=0
    local total_requests=$((keyspace_hits + keyspace_misses))
    if [ "$total_requests" -gt 0 ]; then
        hit_rate=$((keyspace_hits * 100 / total_requests))
    fi
    
    # Отображаем статистику
    clear_screen
    
    echo -e "${CYAN}📊 ПРОИЗВОДИТЕЛЬНОСТЬ:${NC}"
    echo -e "  Операций/сек: ${GREEN}$ops_per_sec${NC}"
    echo -e "  Всего команд: ${GREEN}$total_commands${NC}"
    echo -e "  Hit Rate: ${GREEN}$hit_rate%${NC}"
    echo -e "  Истекших ключей: ${YELLOW}$expired_keys${NC}"
    echo -e "  Вытесненных ключей: ${YELLOW}$evicted_keys${NC}"
    echo
    
    echo -e "${CYAN}💾 ПАМЯТЬ:${NC}"
    echo -e "  Использовано: ${GREEN}$used_memory_human${NC}"
    echo -e "  Процент: ${GREEN}$memory_percent%${NC}"
    echo -e "  Фрагментация: ${GREEN}$mem_fragmentation${NC}"
    echo
    
    echo -e "${CYAN}🔗 КЛИЕНТЫ:${NC}"
    echo -e "  Подключено: ${GREEN}$connected_clients${NC}"
    echo -e "  Заблокировано: ${YELLOW}$blocked_clients${NC}"
    echo
    
    echo -e "${CYAN}🔄 РЕПЛИКАЦИЯ:${NC}"
    echo -e "  Роль: ${GREEN}$role${NC}"
    echo
    
    # Проверяем на проблемы
    echo -e "${CYAN}⚠️ ПРОВЕРКИ:${NC}"
    
    if [ "$ops_per_sec" -gt 10000 ]; then
        echo -e "  ${YELLOW}⚠️ Высокая нагрузка (>10k ops/sec)${NC}"
    else
        echo -e "  ${GREEN}✅ Нагрузка в норме${NC}"
    fi
    
    if [ "$memory_percent" -gt 90 ]; then
        echo -e "  ${RED}❌ Критическое использование памяти (>90%)${NC}"
    elif [ "$memory_percent" -gt 80 ]; then
        echo -e "  ${YELLOW}⚠️ Высокое использование памяти (>80%)${NC}"
    else
        echo -e "  ${GREEN}✅ Использование памяти в норме${NC}"
    fi
    
    if [ "$blocked_clients" -gt 100 ]; then
        echo -e "  ${RED}❌ Много заблокированных клиентов (>100)${NC}"
    else
        echo -e "  ${GREEN}✅ Клиенты в норме${NC}"
    fi
    
    if [ "$mem_fragmentation" != "1.00" ] && [ "$mem_fragmentation" != "1" ]; then
        local frag_ratio=$(echo "$mem_fragmentation" | cut -d. -f1)
        if [ "$frag_ratio" -gt 2 ]; then
            echo -e "  ${YELLOW}⚠️ Высокая фрагментация памяти${NC}"
        else
            echo -e "  ${GREEN}✅ Фрагментация в норме${NC}"
        fi
    else
        echo -e "  ${GREEN}✅ Фрагментация в норме${NC}"
    fi
    
    echo
    echo -e "${PURPLE}Обновление каждые 5 секунд... (Ctrl+C для выхода)${NC}"
}

# Основной цикл
main() {
    echo -e "${GREEN}🚀 Запуск мониторинга Redis...${NC}"
    echo
    
    while true; do
        get_stats
        sleep 5
    done
}

# Обработка сигналов
trap 'echo -e "\n${YELLOW}👋 Мониторинг остановлен${NC}"; exit 0' INT TERM

# Запуск
main "$@"
