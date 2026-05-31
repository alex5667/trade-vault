#!/bin/bash

# Мониторинг стабильности Redis подключений

set -e

# Цвета
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

REDIS_HOST="localhost"
REDIS_PORT="6379"
LOG_FILE="/tmp/redis-stability-monitor.log"

# Функция логирования
log_message() {
    local level=$1
    local message=$2
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [$level] $message" | tee -a "$LOG_FILE"
}

# Функция тестирования подключения
test_connection() {
    if timeout 5 redis-cli -h $REDIS_HOST -p $REDIS_PORT ping 2>/dev/null | grep -q "PONG"; then
        return 0
    else
        return 1
    fi
}

# Функция мониторинга
monitor_stability() {
    echo -e "${BLUE}🔍 Мониторинг стабильности Redis подключений${NC}"
    echo -e "${BLUE}============================================${NC}"
    echo -e "${BLUE}Нажмите Ctrl+C для остановки${NC}"
    echo
    
    local success_count=0
    local total_count=0
    local consecutive_failures=0
    local max_consecutive_failures=5
    
    while true; do
        total_count=$((total_count + 1))
        
        if test_connection; then
            success_count=$((success_count + 1))
            consecutive_failures=0
            echo -e "${GREEN}✅ Подключение успешно ($success_count/$total_count)${NC}"
            log_message "SUCCESS" "Connection successful"
        else
            consecutive_failures=$((consecutive_failures + 1))
            echo -e "${RED}❌ Подключение неудачно ($consecutive_failures подряд)${NC}"
            log_message "ERROR" "Connection failed (consecutive: $consecutive_failures)"
            
            if [ $consecutive_failures -ge $max_consecutive_failures ]; then
                echo -e "${RED}🚨 Критическое количество неудачных подключений подряд!${NC}"
                log_message "CRITICAL" "Too many consecutive failures: $consecutive_failures"
            fi
        fi
        
        sleep 5
    done
}

# Запуск мониторинга
monitor_stability
