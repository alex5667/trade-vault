#!/bin/bash

# Redis Memory Monitor v2 для scanner-infra
# Улучшенный мониторинг с использованием wrapper скрипта

set -e

# Цвета
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

# Конфигурация
REDIS_WRAPPER="./redis-connect.sh"
WARNING_THRESHOLD=80  # Процент использования памяти для предупреждения
CRITICAL_THRESHOLD=90 # Процент использования памяти для критического алерта
LOG_FILE="/tmp/redis-memory-monitor-v2.log"

# Функция логирования
log_message() {
    local level=$1
    local message=$2
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [$level] $message" | tee -a "$LOG_FILE"
}

# Функция получения информации о памяти
get_memory_info() {
    $REDIS_WRAPPER info memory
}

# Функция расчета процента использования памяти
calculate_memory_usage() {
    local memory_info=$1
    local used_memory=$(echo "$memory_info" | grep "used_memory:" | cut -d: -f2 | tr -d '\r')
    local max_memory=$(echo "$memory_info" | grep "maxmemory:" | cut -d: -f2 | tr -d '\r')
    
    if [ "$max_memory" = "0" ]; then
        echo "0"
        return
    fi
    
    local usage_percent=$((used_memory * 100 / max_memory))
    echo "$usage_percent"
}

# Функция проверки фрагментации памяти
check_memory_fragmentation() {
    local memory_info=$1
    local frag_ratio=$(echo "$memory_info" | grep "mem_fragmentation_ratio:" | cut -d: -f2 | tr -d '\r')
    
    # Округляем до 2 знаков после запятой
    local frag_ratio_int=$(echo "$frag_ratio" | cut -d. -f1)
    
    if [ "$frag_ratio_int" -gt 150 ]; then
        echo "HIGH"
    elif [ "$frag_ratio_int" -gt 120 ]; then
        echo "MEDIUM"
    else
        echo "LOW"
    fi
}

# Функция отправки алерта
send_alert() {
    local level=$1
    local message=$2
    
    case $level in
        "WARNING")
            echo -e "${YELLOW}⚠️  WARNING: $message${NC}"
            log_message "WARNING" "$message"
            ;;
        "CRITICAL")
            echo -e "${RED}🚨 CRITICAL: $message${NC}"
            log_message "CRITICAL" "$message"
            ;;
        "INFO")
            echo -e "${GREEN}ℹ️  INFO: $message${NC}"
            log_message "INFO" "$message"
            ;;
    esac
}

# Функция очистки памяти
cleanup_memory() {
    echo -e "${BLUE}🧹 Выполняю очистку памяти...${NC}"
    
    # Очистка устаревших ключей
    $REDIS_WRAPPER --latency-history -i 1 > /dev/null 2>&1 &
    local latency_pid=$!
    sleep 5
    kill $latency_pid 2>/dev/null || true
    
    # Принудительная очистка памяти
    $REDIS_WRAPPER memory purge > /dev/null 2>&1 || true
    
    echo -e "${GREEN}✅ Очистка памяти завершена${NC}"
}

# Функция получения статистики ключей
get_keys_stats() {
    echo -e "${BLUE}📊 Статистика ключей:${NC}"
    
    # Общее количество ключей
    local total_keys=$($REDIS_WRAPPER dbsize)
    echo -e "  📦 Всего ключей: ${GREEN}$total_keys${NC}"
    
    # Ключи с TTL
    local keys_with_ttl=$($REDIS_WRAPPER eval "return #redis.call('keys', '*')" 0 2>/dev/null | wc -l || echo "N/A")
    echo -e "  ⏰ Ключи с TTL: ${GREEN}$keys_with_ttl${NC}"
    
    # Статистика по типам ключей
    echo -e "  🔍 Анализ типов ключей:"
    local key_types=$($REDIS_WRAPPER eval "
        local keys = redis.call('scan', 0, 'match', '*', 'count', 100)
        local types = {}
        for i = 1, #keys[2] do
            local key = keys[2][i]
            local type = redis.call('type', key).ok
            types[type] = (types[type] or 0) + 1
        end
        return types
    " 0 2>/dev/null || echo "{}")
    
    if [ "$key_types" != "{}" ]; then
        echo "$key_types" | jq -r 'to_entries[] | "    \(.key): \(.value)"' 2>/dev/null || echo "    Не удалось получить типы ключей"
    fi
}

# Функция мониторинга производительности
get_performance_stats() {
    echo -e "${BLUE}⚡ Статистика производительности:${NC}"
    
    local stats=$($REDIS_WRAPPER info stats)
    
    # Операции в секунду
    local ops_per_sec=$(echo "$stats" | grep "instantaneous_ops_per_sec:" | cut -d: -f2 | tr -d '\r')
    echo -e "  🚀 Операций в секунду: ${GREEN}$ops_per_sec${NC}"
    
    # Всего команд
    local total_commands=$(echo "$stats" | grep "total_commands_processed:" | cut -d: -f2 | tr -d '\r')
    echo -e "  📈 Всего команд: ${GREEN}$total_commands${NC}"
    
    # Подключения
    local total_connections=$(echo "$stats" | grep "total_connections_received:" | cut -d: -f2 | tr -d '\r')
    echo -e "  🔌 Всего подключений: ${GREEN}$total_connections${NC}"
    
    # Заблокированные клиенты
    local blocked_clients=$(echo "$stats" | grep "blocked_clients:" | cut -d: -f2 | tr -d '\r')
    echo -e "  ⏳ Заблокированные клиенты: ${GREEN}$blocked_clients${NC}"
}

# Основная функция мониторинга
monitor_memory() {
    echo -e "${BLUE}🔍 Redis Memory Monitor v2 для scanner-infra${NC}"
    echo -e "${BLUE}============================================${NC}"
    
    # Проверяем доступность Redis
    if ! $REDIS_WRAPPER ping > /dev/null 2>&1; then
        echo -e "${RED}❌ Redis недоступен${NC}"
        exit 1
    fi
    
    # Получаем информацию о памяти
    local memory_info=$(get_memory_info)
    
    # Извлекаем ключевые метрики
    local used_memory_human=$(echo "$memory_info" | grep "used_memory_human:" | cut -d: -f2 | tr -d '\r')
    local max_memory_human=$(echo "$memory_info" | grep "maxmemory_human:" | cut -d: -f2 | tr -d '\r')
    local mem_fragmentation_ratio=$(echo "$memory_info" | grep "mem_fragmentation_ratio:" | cut -d: -f2 | tr -d '\r')
    local mem_fragmentation_bytes=$(echo "$memory_info" | grep "mem_fragmentation_bytes:" | cut -d: -f2 | tr -d '\r')
    
    # Рассчитываем процент использования
    local usage_percent=$(calculate_memory_usage "$memory_info")
    
    # Проверяем фрагментацию
    local frag_level=$(check_memory_fragmentation "$memory_info")
    
    # Выводим текущее состояние
    echo -e "${GREEN}📊 Текущее состояние памяти:${NC}"
    echo -e "  💾 Используется: ${GREEN}$used_memory_human${NC} / ${GREEN}$max_memory_human${NC}"
    echo -e "  📈 Процент использования: ${GREEN}$usage_percent%${NC}"
    echo -e "  🔧 Фрагментация: ${GREEN}$mem_fragmentation_ratio${NC} (${GREEN}$frag_level${NC})"
    echo -e "  📊 Фрагментация в байтах: ${GREEN}$mem_fragmentation_bytes${NC}"
    
    # Проверяем пороги и отправляем алерты
    if [ "$usage_percent" -ge "$CRITICAL_THRESHOLD" ]; then
        send_alert "CRITICAL" "Использование памяти Redis критическое: $usage_percent% (>= $CRITICAL_THRESHOLD%)"
        
        # Предлагаем очистку
        echo -e "${YELLOW}🔄 Рекомендуется выполнить очистку памяти${NC}"
        read -p "Выполнить очистку памяти? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            cleanup_memory
        fi
        
    elif [ "$usage_percent" -ge "$WARNING_THRESHOLD" ]; then
        send_alert "WARNING" "Использование памяти Redis высокое: $usage_percent% (>= $WARNING_THRESHOLD%)"
        
    else
        send_alert "INFO" "Использование памяти Redis нормальное: $usage_percent%"
    fi
    
    # Проверяем фрагментацию
    if [ "$frag_level" = "HIGH" ]; then
        send_alert "WARNING" "Высокая фрагментация памяти: $mem_fragmentation_ratio"
    elif [ "$frag_level" = "MEDIUM" ]; then
        send_alert "INFO" "Средняя фрагментация памяти: $mem_fragmentation_ratio"
    fi
    
    echo
    get_keys_stats
    echo
    get_performance_stats
    
    echo
    echo -e "${BLUE}📋 Рекомендации:${NC}"
    
    if [ "$usage_percent" -ge 80 ]; then
        echo -e "  • Рассмотрите увеличение maxmemory в конфигурации"
        echo -e "  • Проверьте TTL ключей и удалите неиспользуемые"
        echo -e "  • Выполните очистку памяти: ./redis-memory-monitor-v2.sh cleanup"
    fi
    
    if [ "$frag_level" = "HIGH" ]; then
        echo -e "  • Выполните дефрагментацию: $REDIS_WRAPPER memory purge"
        echo -e "  • Перезапустите Redis для полной очистки памяти"
    fi
    
    echo -e "  • Настройте автоматический мониторинг: ./redis-memory-monitor-v2.sh daemon"
}

# Функция демона мониторинга
start_daemon() {
    echo -e "${BLUE}🚀 Запуск демона мониторинга памяти Redis v2${NC}"
    echo -e "${BLUE}Нажмите Ctrl+C для остановки${NC}"
    echo
    
    while true; do
        monitor_memory
        echo -e "${BLUE}⏰ Следующая проверка через 60 секунд...${NC}"
        sleep 60
    done
}

# Функция очистки
cleanup() {
    echo -e "${BLUE}🧹 Очистка памяти Redis${NC}"
    cleanup_memory
    monitor_memory
}

# Функция показа справки
show_help() {
    echo -e "${BLUE}Redis Memory Monitor v2 для scanner-infra${NC}"
    echo
    echo "Использование: $0 [команда]"
    echo
    echo "Команды:"
    echo "  monitor    - Однократная проверка памяти (по умолчанию)"
    echo "  daemon     - Запуск демона мониторинга"
    echo "  cleanup    - Очистка памяти"
    echo "  help       - Показать эту справку"
    echo
    echo "Переменные окружения:"
    echo "  WARNING_THRESHOLD  - Порог предупреждения (по умолчанию: 80%)"
    echo "  CRITICAL_THRESHOLD - Порог критического алерта (по умолчанию: 90%)"
    echo
    echo "Примеры:"
    echo "  $0 monitor"
    echo "  $0 daemon"
    echo "  WARNING_THRESHOLD=70 $0 monitor"
}

# Основная логика
case "${1:-monitor}" in
    "monitor")
        monitor_memory
        ;;
    "daemon")
        start_daemon
        ;;
    "cleanup")
        cleanup
        ;;
    "help"|"-h"|"--help")
        show_help
        ;;
    *)
        echo -e "${RED}❌ Неизвестная команда: $1${NC}"
        show_help
        exit 1
        ;;
esac
