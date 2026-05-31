#!/bin/bash

# Скрипт установки TTL для ключей Redis
# Автоматически устанавливает время жизни для различных типов данных

set -e

# Цвета
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

# Конфигурация
REDIS_HOST=${REDIS_HOST:-localhost}
REDIS_PORT=${REDIS_PORT:-6379}
REDIS_CLI="redis-cli -h $REDIS_HOST -p $REDIS_PORT"

echo -e "${BLUE}⏰ Настройка TTL для ключей Redis${NC}"
echo -e "${BLUE}========================================${NC}"

# Проверка подключения к Redis
check_redis_connection() {
    echo -e "${YELLOW}🔍 Проверка подключения к Redis...${NC}"
    
    if $REDIS_CLI ping > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Redis доступен на $REDIS_HOST:$REDIS_PORT${NC}"
        return 0
    else
        echo -e "${RED}❌ Redis недоступен на $REDIS_HOST:$REDIS_PORT${NC}"
        return 1
    fi
}

# Установка TTL для ключей по паттерну
set_ttl_by_pattern() {
    local pattern="$1"
    local ttl_seconds="$2"
    local description="$3"
    
    echo -e "\n${YELLOW}🔧 Установка TTL для: ${description}${NC}"
    echo -e "  📍 Паттерн: ${GREEN}$pattern${NC}"
    echo -e "  ⏰ TTL: ${GREEN}${ttl_seconds} секунд${NC}"
    
    # Получаем список ключей по паттерну
    local keys=$($REDIS_CLI keys "$pattern" 2>/dev/null || echo "")
    
    if [ -z "$keys" ]; then
        echo -e "  ${YELLOW}⏸️ Ключи не найдены${NC}"
        return
    fi
    
    local count=0
    for key in $keys; do
        # Устанавливаем TTL
        $REDIS_CLI expire "$key" "$ttl_seconds" > /dev/null 2>&1
        if [ $? -eq 1 ]; then
            echo -e "  ${GREEN}✅ TTL установлен для: $key${NC}"
            count=$((count + 1))
        else
            echo -e "  ${RED}❌ Ошибка установки TTL для: $key${NC}"
        fi
    done
    
    echo -e "  ${GREEN}✅ Всего обновлено ключей: $count${NC}"
}

# Установка TTL для всех типов данных
setup_all_ttl() {
    echo -e "\n${BLUE}⏰ Установка TTL для всех типов данных${NC}"
    
    # TTL для временных данных (1 час)
    set_ttl_by_pattern "temp:*" "3600" "Временные данные (1 час)"
    set_ttl_by_pattern "cache:*" "3600" "Кэш данные (1 час)"
    set_ttl_by_pattern "session:*" "3600" "Сессии (1 час)"
    
    # TTL для данных WebSocket (24 часа)
    set_ttl_by_pattern "ws:*" "86400" "WebSocket данные (24 часа)"
    set_ttl_by_pattern "connection:*" "86400" "Данные подключений (24 часа)"
    
    # TTL для аналитических данных (7 дней)
    set_ttl_by_pattern "analytics:*" "604800" "Аналитические данные (7 дней)"
    set_ttl_by_pattern "stats:*" "604800" "Статистика (7 дней)"
    
    # TTL для логов (30 дней)
    set_ttl_by_pattern "log:*" "2592000" "Логи (30 дней)"
    set_ttl_by_pattern "debug:*" "2592000" "Отладочные данные (30 дней)"
    
    # TTL для уведомлений (7 дней)
    set_ttl_by_pattern "notify:*" "604800" "Уведомления (7 дней)"
    set_ttl_by_pattern "alert:*" "604800" "Алерты (7 дней)"
    
    # TTL для метрик (1 день)
    set_ttl_by_pattern "metric:*" "86400" "Метрики (1 день)"
    set_ttl_by_pattern "prometheus:*" "86400" "Prometheus данные (1 день)"
}

# Установка TTL для конкретных ключей
setup_specific_ttl() {
    echo -e "\n${BLUE}🎯 Установка TTL для конкретных ключей${NC}"
    
    # TTL для ключей статуса каналов (30 дней)
    echo -e "${YELLOW}Установка TTL для статусов каналов...${NC}"
    local channel_keys=$($REDIS_CLI keys "telegram:channel:*:status" 2>/dev/null || echo "")
    
    if [ -n "$channel_keys" ]; then
        for key in $channel_keys; do
            $REDIS_CLI expire "$key" "2592000" > /dev/null 2>&1
            echo -e "  ${GREEN}✅ TTL 30 дней для: $key${NC}"
        done
    fi
    
    # TTL для ключей конфигурации (7 дней)
    echo -e "${YELLOW}Установка TTL для конфигурации...${NC}"
    local config_keys=$($REDIS_CLI keys "config:*" 2>/dev/null || echo "")
    
    if [ -n "$config_keys" ]; then
        for key in $config_keys; do
            $REDIS_CLI expire "$key" "604800" > /dev/null 2>&1
            echo -e "  ${GREEN}✅ TTL 7 дней для: $key${NC}"
        done
    fi
}

# Мониторинг TTL ключей
monitor_ttl_keys() {
    echo -e "\n${BLUE}📊 Мониторинг TTL ключей${NC}"
    
    # Получаем все ключи с TTL
    local ttl_keys=$($REDIS_CLI keys "*" 2>/dev/null | head -20)
    
    if [ -z "$ttl_keys" ]; then
        echo -e "${YELLOW}⏸️ Ключи не найдены${NC}"
        return
    fi
    
    echo -e "${YELLOW}Ключ                    | TTL (сек) | TTL (человекочитаемо)${NC}"
    echo -e "${YELLOW}------------------------|-----------|------------------------${NC}"
    
    for key in $ttl_keys; do
        local ttl=$($REDIS_CLI ttl "$key" 2>/dev/null || echo "-1")
        
        if [ "$ttl" != "-1" ] && [ "$ttl" != "-2" ]; then
            local ttl_human=""
            if [ $ttl -lt 60 ]; then
                ttl_human="${ttl} сек"
            elif [ $ttl -lt 3600 ]; then
                ttl_human="$((ttl / 60)) мин"
            elif [ $ttl -lt 86400 ]; then
                ttl_human="$((ttl / 3600)) час"
            else
                ttl_human="$((ttl / 86400)) дней"
            fi
            
            printf "%-25s | %9s | %s\n" "$key" "$ttl" "$ttl_human"
        fi
    done
}

# Автоматическая очистка ключей с истекшим TTL
cleanup_expired_keys() {
    echo -e "\n${BLUE}🧹 Автоматическая очистка истекших ключей${NC}"
    
    # Получаем количество ключей с истекшим TTL
    local expired_count=$($REDIS_CLI info stats | grep "expired_keys" | cut -d: -f2)
    
    echo -e "${YELLOW}Количество истекших ключей: ${GREEN}$expired_count${NC}"
    
    # Принудительная очистка (Redis делает это автоматически, но можно ускорить)
    echo -e "${YELLOW}Запуск принудительной очистки...${NC}"
    
    # Используем SCAN для поиска и удаления истекших ключей
    local cursor=0
    local cleaned=0
    
    while true; do
        local result=$($REDIS_CLI scan "$cursor" count 100 2>/dev/null)
        cursor=$(echo "$result" | head -1)
        local keys=$(echo "$result" | tail -n +2)
        
        for key in $keys; do
            local ttl=$($REDIS_CLI ttl "$key" 2>/dev/null || echo "-1")
            if [ "$ttl" = "-2" ]; then
                $REDIS_CLI del "$key" > /dev/null 2>&1
                cleaned=$((cleaned + 1))
            fi
        done
        
        if [ "$cursor" = "0" ]; then
            break
        fi
    done
    
    echo -e "${GREEN}✅ Очищено ключей: $cleaned${NC}"
}

# Настройка автоматической очистки
setup_auto_cleanup() {
    echo -e "\n${BLUE}⚙️ Настройка автоматической очистки${NC}"
    
    # Устанавливаем политику памяти для автоматического удаления
    echo -e "${YELLOW}Установка политики памяти...${NC}"
    $REDIS_CLI config set maxmemory-policy "volatile-lru" > /dev/null 2>&1
    echo -e "${GREEN}✅ Политика памяти: volatile-lru${NC}"
    
    # Устанавливаем частоту проверки TTL
    echo -e "${YELLOW}Установка частоты проверки TTL...${NC}"
    $REDIS_CLI config set hz "10" > /dev/null 2>&1
    echo -e "${GREEN}✅ Частота проверки TTL: 10 раз в секунду${NC}"
    
    # Устанавливаем активную очистку памяти
    echo -e "${YELLOW}Включение активной очистки памяти...${NC}"
    $REDIS_CLI config set activedefrag "yes" > /dev/null 2>&1
    echo -e "${GREEN}✅ Активная очистка памяти включена${NC}"
}

# Основная функция
main() {
    case "${1:-all}" in
        "ttl")
            check_redis_connection && setup_all_ttl
            ;;
        "specific")
            check_redis_connection && setup_specific_ttl
            ;;
        "monitor")
            check_redis_connection && monitor_ttl_keys
            ;;
        "cleanup")
            check_redis_connection && cleanup_expired_keys
            ;;
        "auto")
            check_redis_connection && setup_auto_cleanup
            ;;
        "all")
            check_redis_connection && {
                setup_all_ttl
                setup_specific_ttl
                echo
                monitor_ttl_keys
                echo
                cleanup_expired_keys
                echo
                setup_auto_cleanup
            }
            ;;
        "help"|*)
            echo -e "${BLUE}Использование: $0 [команда]${NC}"
            echo
            echo "Команды:"
            echo "  ttl      - Установить TTL для всех типов данных"
            echo "  specific - Установить TTL для конкретных ключей"
            echo "  monitor  - Показать TTL для всех ключей"
            echo "  cleanup  - Очистить истекшие ключи"
            echo "  auto     - Настроить автоматическую очистку"
            echo "  all      - Выполнить все настройки (по умолчанию)"
            echo "  help     - Показать эту справку"
            ;;
    esac
}

# Запуск скрипта
main "$@" 