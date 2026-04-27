#!/bin/bash

# Скрипт для проверки количества записей в Redis во всех каналах, содержащих "stream:kline"
# Проверяет все kline streams и показывает детальную статистику

set -e

# Цвета для вывода
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Конфигурация Redis
REDIS_HOST=${REDIS_HOST:-localhost}
REDIS_PORT=${REDIS_PORT:-6379}
REDIS_CLI="redis-cli -h $REDIS_HOST -p $REDIS_PORT"

echo -e "${BLUE}🔍 Проверка количества записей в Redis каналах stream:kline${NC}"
echo -e "${BLUE}=======================================================${NC}"

# Проверка подключения к Redis
check_redis_connection() {
    echo -e "${YELLOW}📡 Проверка подключения к Redis...${NC}"
    
    if $REDIS_CLI ping > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Redis доступен на $REDIS_HOST:$REDIS_PORT${NC}"
        return 0
    else
        echo -e "${RED}❌ Redis недоступен на $REDIS_HOST:$REDIS_PORT${NC}"
        echo -e "${YELLOW}💡 Убедитесь, что Redis запущен и доступен${NC}"
        echo -e "${YELLOW}💡 Проверьте переменные окружения REDIS_HOST и REDIS_PORT${NC}"
        exit 1
    fi
}

# Поиск всех каналов, содержащих "stream:kline"
find_kline_streams() {
    echo -e "\n${YELLOW}🔍 Поиск всех каналов, содержащих 'stream:kline'...${NC}"
    
    # Используем SCAN вместо keys для совместимости с Redis 7+
    local all_keys=""
    local cursor=0
    local temp_keys=""
    
    # Сканируем все ключи и собираем те, что содержат "stream:kline"
    while true; do
        temp_keys=$($REDIS_CLI scan $cursor match "*" count 1000 2>/dev/null)
        if [ $? -ne 0 ]; then
            echo -e "${RED}❌ Ошибка при сканировании Redis${NC}"
            return 1
        fi
        
        # Парсим результат SCAN (формат: cursor key1 key2 ...)
        cursor=$(echo "$temp_keys" | head -1)
        local keys=$(echo "$temp_keys" | tail -n +2 | grep "stream:kline")
        
        if [ -n "$keys" ]; then
            all_keys="$all_keys $keys"
        fi
        
        # Если cursor = 0, значит сканирование завершено
        if [ "$cursor" = "0" ]; then
            break
        fi
    done
    
    # Очищаем и сортируем ключи
    all_keys=$(echo "$all_keys" | tr ' ' '\n' | grep -v '^$' | sort | uniq)
    
    if [ -z "$all_keys" ]; then
        echo -e "${YELLOW}⏸️ Каналы stream:kline не найдены${NC}"
        return
    fi
    
    local count=$(echo "$all_keys" | wc -l)
    echo -e "${GREEN}✅ Найдено каналов: $count${NC}"
    echo
}

# Проверка детальной статистики для каждого kline stream
check_kline_streams_detailed() {
    echo -e "${YELLOW}📊 Детальная статистика по каждому kline stream:${NC}"
    echo -e "${CYAN}Название канала                    | Записей | Размер | Последнее обновление${NC}"
    echo -e "${CYAN}-----------------------------------|---------|--------|---------------------${NC}"
    
    local total_records=0
    local total_memory=0
    
    # Получаем все kline streams используя SCAN
    local all_keys=""
    local cursor=0
    local temp_keys=""
    
    while true; do
        temp_keys=$($REDIS_CLI scan $cursor match "*" count 1000 2>/dev/null)
        if [ $? -ne 0 ]; then
            echo -e "${RED}❌ Ошибка при сканировании Redis${NC}"
            return 1
        fi
        
        cursor=$(echo "$temp_keys" | head -1)
        local keys=$(echo "$temp_keys" | tail -n +2 | grep "stream:kline")
        
        if [ -n "$keys" ]; then
            all_keys="$all_keys $keys"
        fi
        
        if [ "$cursor" = "0" ]; then
            break
        fi
    done
    
    # Очищаем и сортируем ключи
    local kline_streams=$(echo "$all_keys" | tr ' ' '\n' | grep -v '^$' | sort | uniq)
    
    if [ -z "$kline_streams" ]; then
        echo -e "${YELLOW}⏸️ Kline streams не найдены${NC}"
        return
    fi
    
    # Разбиваем на отдельные streams
    local streams_array=($(echo "$kline_streams"))
    
    for stream in "${streams_array[@]}"; do
        # Пропускаем пустые строки
        if [ -z "$stream" ]; then
            continue
        fi
        
        # Получаем количество записей
        local length=$($REDIS_CLI xlen "$stream" 2>/dev/null || echo "0")
        
        # Проверяем, что length это число
        if ! [[ "$length" =~ ^[0-9]+$ ]]; then
            length="0"
        fi
        
        # Получаем размер в памяти (если доступно)
        local memory=$($REDIS_CLI memory usage "$stream" 2>/dev/null || echo "0")
        local memory_human=""
        
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
        
        # Получаем время последнего обновления
        local last_update="N/A"
        if [ "$length" -gt 0 ]; then
            local last_id=$($REDIS_CLI xrevrange "$stream" + - count 1 2>/dev/null | head -1 | cut -d' ' -f1)
            if [ -n "$last_id" ] && [ "$last_id" != "nil" ]; then
                # Извлекаем timestamp из ID (формат: timestamp-sequence)
                local timestamp=$(echo "$last_id" | cut -d'-' -f1)
                if [ -n "$timestamp" ] && [ "$timestamp" != "nil" ] && [[ "$timestamp" =~ ^[0-9]+$ ]]; then
                    # Конвертируем Unix timestamp в читаемый формат
                    last_update=$(date -d "@$timestamp" '+%H:%M:%S' 2>/dev/null || echo "N/A")
                fi
            fi
        fi
        
        # Выводим информацию о stream
        printf "%-35s | %7s | %6s | %s\n" "$stream" "$length" "$memory_human" "$last_update"
        
        # Суммируем общую статистику
        total_records=$((total_records + length))
        if [ "$memory" != "0" ] && [ "$memory" != "nil" ] && [[ "$memory" =~ ^[0-9]+$ ]]; then
            total_memory=$((total_memory + memory))
        fi
    done
    
    echo -e "${CYAN}-----------------------------------|---------|--------|---------------------${NC}"
    
    # Выводим общую статистику
    echo -e "${GREEN}📈 Общая статистика:${NC}"
    echo -e "  📊 Всего записей: ${GREEN}$total_records${NC}"
    
    if [ "$total_memory" -gt 0 ]; then
        if [ "$total_memory" -gt 1048576 ]; then
            local total_memory_mb=$(echo "scale=1; $total_memory/1048576" | bc -l 2>/dev/null || echo "N/A")
            echo -e "  💾 Общий размер в памяти: ${GREEN}${total_memory_mb}MB${NC}"
        elif [ "$total_memory" -gt 1024 ]; then
            local total_memory_kb=$(echo "scale=1; $total_memory/1024" | bc -l 2>/dev/null || echo "N/A")
            echo -e "  💾 Общий размер в памяти: ${GREEN}${total_memory_kb}KB${NC}"
        else
            echo -e "  💾 Общий размер в памяти: ${GREEN}${total_memory}B${NC}"
        fi
    fi
}

# Проверка производительности kline streams
check_kline_performance() {
    echo -e "\n${YELLOW}⚡ Проверка производительности kline streams:${NC}"
    
    # Получаем общую статистику Redis
    local total_ops=$($REDIS_CLI info stats | grep "total_commands_processed" | cut -d: -f2)
    local ops_per_sec=$($REDIS_CLI info stats | grep "instantaneous_ops_per_sec" | cut -d: -f2)
    local keyspace_hits=$($REDIS_CLI info stats | grep "keyspace_hits" | cut -d: -f2)
    local keyspace_misses=$($REDIS_CLI info stats | grep "keyspace_misses" | cut -d: -f2)
    
    # Проверяем, что значения являются числами
    if [[ "$total_ops" =~ ^[0-9]+$ ]]; then
        echo -e "  🚀 Всего команд: ${GREEN}$total_ops${NC}"
    else
        echo -e "  🚀 Всего команд: ${YELLOW}N/A${NC}"
    fi
    
    if [[ "$ops_per_sec" =~ ^[0-9]+$ ]]; then
        echo -e "  ⚡ Операций/сек: ${GREEN}$ops_per_sec${NC}"
    else
        echo -e "  ⚡ Операций/сек: ${YELLOW}N/A${NC}"
    fi
    
    if [[ "$keyspace_hits" =~ ^[0-9]+$ ]] && [[ "$keyspace_misses" =~ ^[0-9]+$ ]]; then
        local total_requests=$((keyspace_hits + keyspace_misses))
        if [ "$total_requests" -gt 0 ]; then
            local hit_rate=$(echo "scale=2; $keyspace_hits * 100 / $total_requests" | bc -l 2>/dev/null || echo "N/A")
            echo -e "  🎯 Hit rate: ${GREEN}${hit_rate}%${NC}"
        else
            echo -e "  🎯 Hit rate: ${YELLOW}N/A${NC}"
        fi
    else
        echo -e "  🎯 Hit rate: ${YELLOW}N/A${NC}"
    fi
}

# Проверка здоровья kline streams
check_kline_health() {
    echo -e "\n${YELLOW}🏥 Проверка здоровья kline streams:${NC}"
    
    # Получаем все kline streams используя SCAN
    local all_keys=""
    local cursor=0
    local temp_keys=""
    
    while true; do
        temp_keys=$($REDIS_CLI scan $cursor match "*" count 1000 2>/dev/null)
        if [ $? -ne 0 ]; then
            echo -e "${RED}❌ Ошибка при сканировании Redis${NC}"
            return 1
        fi
        
        cursor=$(echo "$temp_keys" | head -1)
        local keys=$(echo "$temp_keys" | tail -n +2 | grep "stream:kline")
        
        if [ -n "$keys" ]; then
            all_keys="$all_keys $keys"
        fi
        
        if [ "$cursor" = "0" ]; then
            break
        fi
    done
    
    # Очищаем и сортируем ключи
    local kline_streams=$(echo "$all_keys" | tr ' ' '\n' | grep -v '^$' | sort | uniq)
    local healthy_streams=0
    local total_streams=0
    
    if [ -z "$kline_streams" ]; then
        echo -e "${YELLOW}⏸️ Kline streams не найдены${NC}"
        return
    fi
    
    # Разбиваем на отдельные streams
    local streams_array=($(echo "$kline_streams"))
    
    for stream in "${streams_array[@]}"; do
        # Пропускаем пустые строки
        if [ -z "$stream" ]; then
            continue
        fi
        
        total_streams=$((total_streams + 1))
        local length=$($REDIS_CLI xlen "$stream" 2>/dev/null || echo "0")
        
        # Проверяем, что length это число
        if ! [[ "$length" =~ ^[0-9]+$ ]]; then
            length="0"
        fi
        
        # Проверяем, есть ли активность в stream
        if [ "$length" -gt 0 ]; then
            local last_id=$($REDIS_CLI xrevrange "$stream" + - count 1 2>/dev/null | head -1 | cut -d' ' -f1)
            if [ -n "$last_id" ] && [ "$last_id" != "nil" ]; then
                local timestamp=$(echo "$last_id" | cut -d'-' -f1)
                if [ -n "$timestamp" ] && [ "$timestamp" != "nil" ] && [[ "$timestamp" =~ ^[0-9]+$ ]]; then
                    local current_time=$(date +%s)
                    local time_diff=$((current_time - timestamp))
                    
                    # Если последнее обновление было менее часа назад, считаем stream здоровым
                    if [ "$time_diff" -lt 3600 ]; then
                        healthy_streams=$((healthy_streams + 1))
                        echo -e "  ${GREEN}✅ $stream: активен (${length} записей)${NC}"
                    else
                        echo -e "  ${YELLOW}⚠️ $stream: неактивен ${time_diff} сек (${length} записей)${NC}"
                    fi
                fi
            fi
        else
            echo -e "  ${YELLOW}⏸️ $stream: пустой${NC}"
        fi
    done
    
    if [ "$total_streams" -gt 0 ]; then
        local health_percentage=$(echo "scale=1; $healthy_streams * 100 / $total_streams" | bc -l 2>/dev/null || echo "N/A")
        echo -e "\n  📊 Здоровье kline streams: ${GREEN}${health_percentage}%${NC} (${healthy_streams}/${total_streams})"
    fi
}

# Основная функция
main() {
    case "${1:-all}" in
        "connection")
            check_redis_connection
            ;;
        "streams")
            check_redis_connection && find_kline_streams
            ;;
        "detailed")
            check_redis_connection && check_kline_streams_detailed
            ;;
        "performance")
            check_redis_connection && check_kline_performance
            ;;
        "health")
            check_redis_connection && check_kline_health
            ;;
        "all")
            check_redis_connection && {
                find_kline_streams
                check_kline_streams_detailed
                check_kline_performance
                check_kline_health
            }
            ;;
        "help"|*)
            echo -e "${BLUE}Использование: $0 [команда]${NC}"
            echo
            echo "Команды:"
            echo "  connection - Проверить подключение к Redis"
            echo "  streams    - Найти все kline streams"
            echo "  detailed   - Детальная статистика по streams"
            echo "  performance- Проверка производительности"
            echo "  health     - Проверка здоровья streams"
            echo "  all        - Выполнить все проверки (по умолчанию)"
            echo "  help       - Показать эту справку"
            echo
            echo "Переменные окружения:"
            echo "  REDIS_HOST - Хост Redis (по умолчанию: localhost)"
            echo "  REDIS_PORT - Порт Redis (по умолчанию: 6379)"
            ;;
    esac
}

# Запуск скрипта
main "$@" 