#!/bin/bash

# Мониторинг с автоматическими уведомлениями
# Можно добавить в cron для периодической проверки

LOG_FILE="/tmp/scanner_monitor.log"
ALERT_FILE="/tmp/scanner_alerts.log"

log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

send_alert() {
    local message="$1"
    echo "[ALERT] $(date '+%Y-%m-%d %H:%M:%S') - $message" >> "$ALERT_FILE"
    # Здесь можно добавить отправку в Telegram, Slack и т.д.
    echo "🚨 ALERT: $message"
}

log_message "========== Запуск мониторинга =========="

# 1. Проверка критичных сервисов
log_message "Проверка сервисов..."

CRITICAL_SERVICES=(
    "scanner-redis-worker-1"
    "scanner-go-worker"
    "scanner-python-worker"
)

failed_services=()
for service in "${CRITICAL_SERVICES[@]}"; do
    status=$(docker inspect -f '{{.State.Status}}' "$service" 2>/dev/null)
    
    if [ "$status" != "running" ]; then
        failed_services+=("$service")
        send_alert "Сервис $service не работает! Статус: $status"
    fi
done

if [ ${#failed_services[@]} -eq 0 ]; then
    log_message "✅ Все критичные сервисы работают"
else
    log_message "❌ Не работает сервисов: ${#failed_services[@]}"
fi

# 2. Проверка Redis
log_message "Проверка Redis..."

if ! docker exec scanner-redis-worker-1 redis-cli PING 2>/dev/null | grep -q PONG; then
    send_alert "Redis scanner-redis-worker-1 не отвечает!"
else
    log_message "✅ Redis работает"
    
    # Проверка количества клиентов
    clients=$(docker exec scanner-redis-worker-1 redis-cli INFO clients 2>/dev/null | grep connected_clients | cut -d: -f2 | tr -d '\r')
    log_message "Подключено клиентов: $clients"
    
    if [ "$clients" -lt 10 ]; then
        send_alert "Мало клиентов Redis: $clients (ожидается >10)"
    fi
    
    # Проверка памяти
    memory_bytes=$(docker exec scanner-redis-worker-1 redis-cli INFO memory 2>/dev/null | grep used_memory: | cut -d: -f2 | tr -d '\r')
    maxmemory_bytes=$(docker exec scanner-redis-worker-1 redis-cli INFO memory 2>/dev/null | grep maxmemory: | cut -d: -f2 | tr -d '\r')
    
    if [ "$maxmemory_bytes" -gt 0 ]; then
        usage_percent=$((memory_bytes * 100 / maxmemory_bytes))
        log_message "Использование памяти Redis: ${usage_percent}%"
        
        if [ "$usage_percent" -gt 80 ]; then
            send_alert "Высокое использование памяти Redis: ${usage_percent}%"
        fi
    fi
fi

# 3. Проверка ошибок в логах
log_message "Проверка логов на ошибки..."

redis_errors=$(docker-compose logs --tail=100 --since=5m 2>&1 | grep -i "error.*redis\|connection.*refused" | wc -l)

if [ "$redis_errors" -gt 0 ]; then
    send_alert "Обнаружено $redis_errors ошибок Redis в логах за последние 5 минут"
    log_message "❌ Ошибок Redis: $redis_errors"
else
    log_message "✅ Ошибок Redis не найдено"
fi

# 4. Проверка disk space
log_message "Проверка дискового пространства..."

disk_usage=$(df -h / | awk 'NR==2 {print $5}' | sed 's/%//')

if [ "$disk_usage" -gt 90 ]; then
    send_alert "Критически мало места на диске: ${disk_usage}%"
elif [ "$disk_usage" -gt 80 ]; then
    log_message "⚠️  Мало места на диске: ${disk_usage}%"
else
    log_message "✅ Диск: ${disk_usage}% использовано"
fi

# 5. Финальная сводка
log_message "========== Мониторинг завершен =========="
log_message ""

# Показать алерты, если есть
if [ -f "$ALERT_FILE" ]; then
    alert_count=$(wc -l < "$ALERT_FILE")
    if [ "$alert_count" -gt 0 ]; then
        echo ""
        echo "🚨 ВНИМАНИЕ: Обнаружено алертов: $alert_count"
        echo "Подробности в: $ALERT_FILE"
        echo ""
        echo "Последние алерты:"
        tail -5 "$ALERT_FILE"
    fi
fi

# Возвращаем код ошибки если были проблемы
if [ ${#failed_services[@]} -gt 0 ] || [ "$redis_errors" -gt 10 ]; then
    exit 1
fi

exit 0

