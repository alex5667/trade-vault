#!/bin/bash

# Redis Health Check и Auto-Recovery Script
# Автоматически перезапускает Redis при обнаружении проблем

REDIS_HOST=${REDIS_HOST:-scanner-redis-worker-1}
REDIS_PORT=${REDIS_PORT:-6379}
REDIS_CLI="redis-cli -h $REDIS_HOST -p $REDIS_PORT"
CONTAINER_NAME="scanner-redis"
MAX_FAILURES=3
FAILURE_COUNT_FILE="/tmp/redis_failure_count"
LOG_FILE="/var/log/redis-health-check.log"

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Функция логирования
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Функция проверки соединения
check_redis_connection() {
    if $REDIS_CLI ping > /dev/null 2>&1; then
        return 0
    else
        return 1
    fi
}

# Функция проверки производительности
check_redis_performance() {
    # Проверяем время отклика
    START_TIME=$(date +%s%3N)
    $REDIS_CLI ping > /dev/null 2>&1
    END_TIME=$(date +%s%3N)
    RESPONSE_TIME=$((END_TIME - START_TIME))
    
    # Если время отклика больше 5 секунд, считаем это проблемой
    if [ "$RESPONSE_TIME" -gt 5000 ]; then
        log "⚠️ Медленный отклик Redis: ${RESPONSE_TIME}ms"
        return 1
    fi
    
    return 0
}

# Функция проверки памяти
check_redis_memory() {
    MEMORY_INFO=$($REDIS_CLI info memory 2>/dev/null)
    if [ $? -eq 0 ]; then
        USED_MEMORY=$(echo "$MEMORY_INFO" | grep "used_memory:" | cut -d: -f2 | tr -d '\r')
        MAX_MEMORY=$(echo "$MEMORY_INFO" | grep "maxmemory:" | cut -d: -f2 | tr -d '\r')
        
        if [ "$MAX_MEMORY" -gt 0 ] && [ "$USED_MEMORY" -gt 0 ]; then
            USAGE_PERCENT=$((USED_MEMORY * 100 / MAX_MEMORY))
            if [ "$USAGE_PERCENT" -gt 90 ]; then
                log "⚠️ Высокое использование памяти: ${USAGE_PERCENT}%"
                return 1
            fi
        fi
    fi
    
    return 0
}

# Функция проверки клиентов
check_redis_clients() {
    CLIENTS_INFO=$($REDIS_CLI info clients 2>/dev/null)
    if [ $? -eq 0 ]; then
        CONNECTED_CLIENTS=$(echo "$CLIENTS_INFO" | grep "connected_clients:" | cut -d: -f2 | tr -d '\r')
        BLOCKED_CLIENTS=$(echo "$CLIENTS_INFO" | grep "blocked_clients:" | cut -d: -f2 | tr -d '\r')
        
        # Если много заблокированных клиентов, это проблема
        if [ "$BLOCKED_CLIENTS" -gt 100 ]; then
            log "⚠️ Много заблокированных клиентов: $BLOCKED_CLIENTS"
            return 1
        fi
        
        # Если слишком много подключенных клиентов
        if [ "$CONNECTED_CLIENTS" -gt 5000 ]; then
            log "⚠️ Слишком много подключенных клиентов: $CONNECTED_CLIENTS"
            return 1
        fi
    fi
    
    return 0
}

# Функция сброса счетчика ошибок
reset_failure_count() {
    echo "0" > "$FAILURE_COUNT_FILE"
}

# Функция увеличения счетчика ошибок
increment_failure_count() {
    if [ -f "$FAILURE_COUNT_FILE" ]; then
        CURRENT_COUNT=$(cat "$FAILURE_COUNT_FILE")
    else
        CURRENT_COUNT=0
    fi
    
    NEW_COUNT=$((CURRENT_COUNT + 1))
    echo "$NEW_COUNT" > "$FAILURE_COUNT_FILE"
    echo "$NEW_COUNT"
}

# Функция перезапуска Redis
restart_redis() {
    log "🔄 Перезапуск Redis контейнера..."
    
    # Останавливаем контейнер
    if docker stop "$CONTAINER_NAME" > /dev/null 2>&1; then
        log "✅ Контейнер $CONTAINER_NAME остановлен"
    else
        log "⚠️ Не удалось остановить контейнер $CONTAINER_NAME"
    fi
    
    # Ждем немного
    sleep 5
    
    # Запускаем контейнер
    if docker start "$CONTAINER_NAME" > /dev/null 2>&1; then
        log "✅ Контейнер $CONTAINER_NAME запущен"
    else
        log "❌ Не удалось запустить контейнер $CONTAINER_NAME"
        return 1
    fi
    
    # Ждем, пока Redis станет доступен
    log "⏳ Ожидание доступности Redis..."
    for i in {1..30}; do
        if check_redis_connection; then
            log "✅ Redis доступен после перезапуска"
            reset_failure_count
            return 0
        fi
        sleep 2
    done
    
    log "❌ Redis не стал доступен после перезапуска"
    return 1
}

# Функция проверки здоровья
health_check() {
    log "🔍 Проверка здоровья Redis..."
    
    # Проверяем соединение
    if ! check_redis_connection; then
        log "❌ Redis недоступен"
        return 1
    fi
    
    # Проверяем производительность
    if ! check_redis_performance; then
        log "❌ Проблемы с производительностью Redis"
        return 1
    fi
    
    # Проверяем память
    if ! check_redis_memory; then
        log "❌ Проблемы с памятью Redis"
        return 1
    fi
    
    # Проверяем клиентов
    if ! check_redis_clients; then
        log "❌ Проблемы с клиентами Redis"
        return 1
    fi
    
    log "✅ Redis работает нормально"
    return 0
}

# Основная функция
main() {
    log "🚀 Запуск проверки здоровья Redis"
    
    if health_check; then
        # Если все хорошо, сбрасываем счетчик ошибок
        reset_failure_count
        log "✅ Redis здоров, счетчик ошибок сброшен"
    else
        # Если есть проблемы, увеличиваем счетчик
        FAILURE_COUNT=$(increment_failure_count)
        log "⚠️ Обнаружены проблемы с Redis (попытка $FAILURE_COUNT/$MAX_FAILURES)"
        
        # Если достигли максимального количества ошибок, перезапускаем
        if [ "$FAILURE_COUNT" -ge "$MAX_FAILURES" ]; then
            log "🔄 Достигнуто максимальное количество ошибок, перезапуск Redis"
            if restart_redis; then
                log "✅ Redis успешно перезапущен"
            else
                log "❌ Не удалось перезапустить Redis"
                exit 1
            fi
        fi
    fi
    
    log "🏁 Проверка завершена"
}

# Запуск скрипта
main "$@"
