#!/bin/bash

# Скрипт автоматического восстановления Redis
# Мониторит состояние и автоматически исправляет проблемы

echo "🔄 Автоматическое восстановление Redis"
echo "====================================="

# Функция для проверки состояния Redis
check_redis_health() {
    local container_name=$1
    local port=$2
    
    # Проверяем, запущен ли контейнер
    if ! docker ps | grep -q "$container_name"; then
        echo "❌ Контейнер $container_name не запущен"
        return 1
    fi
    
    # Проверяем health check
    local health=$(docker inspect --format='{{.State.Health.Status}}' $container_name 2>/dev/null)
    if [ "$health" != "healthy" ]; then
        echo "⚠️  $container_name нездоров: $health"
        return 1
    fi
    
    # Проверяем подключение
    if ! docker exec $container_name redis-cli ping >/dev/null 2>&1; then
        echo "❌ Не удается подключиться к $container_name"
        return 1
    fi
    
    echo "✅ $container_name здоров"
    return 0
}

# Функция для восстановления Redis
recover_redis() {
    local container_name=$1
    local service_name=$2
    
    echo "🔧 Восстанавливаем $service_name ($container_name)..."
    
    # Останавливаем контейнер
    echo "   Останавливаем контейнер..."
    docker stop $container_name >/dev/null 2>&1
    
    # Удаляем контейнер
    echo "   Удаляем контейнер..."
    docker rm $container_name >/dev/null 2>&1
    
    # Очищаем память
    echo "   Очищаем память..."
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null 2>&1
    
    # Запускаем заново
    echo "   Запускаем контейнер заново..."
    docker-compose up -d $service_name
    
    # Ждем запуска
    echo "   Ждем запуска..."
    sleep 15
    
    # Проверяем восстановление
    if check_redis_health $container_name; then
        echo "   ✅ $service_name успешно восстановлен"
        return 0
    else
        echo "   ❌ Не удалось восстановить $service_name"
        return 1
    fi
}

# Функция для очистки памяти Redis
cleanup_redis_memory() {
    local container_name=$1
    
    echo "🧹 Очищаем память $container_name..."
    
    # Очищаем память
    docker exec $container_name redis-cli memory purge >/dev/null 2>&1
    
    # Сбрасываем статистику
    docker exec $container_name redis-cli config resetstat >/dev/null 2>&1
    
    # Очищаем устаревшие ключи
    docker exec $container_name redis-cli eval "redis.call('del', unpack(redis.call('keys', '*')))" 0 >/dev/null 2>&1
    
    echo "   ✅ Память очищена"
}

# Функция для проверки и исправления конфигурации
fix_redis_config() {
    local container_name=$1
    
    echo "⚙️  Проверяем конфигурацию $container_name..."
    
    # Проверяем основные настройки
    local maxmemory=$(docker exec $container_name redis-cli config get maxmemory 2>/dev/null | tail -1)
    local maxclients=$(docker exec $container_name redis-cli config get maxclients 2>/dev/null | tail -1)
    local tcp_backlog=$(docker exec $container_name redis-cli config get tcp-backlog 2>/dev/null | tail -1)
    
    echo "   📊 Максимальная память: $maxmemory"
    echo "   📊 Максимальные подключения: $maxclients"
    echo "   📊 TCP backlog: $tcp_backlog"
    
    # Применяем оптимальные настройки
    docker exec $container_name redis-cli config set maxmemory 8gb >/dev/null 2>&1
    docker exec $container_name redis-cli config set maxclients 10000 >/dev/null 2>&1
    docker exec $container_name redis-cli config set tcp-backlog 65535 >/dev/null 2>&1
    
    echo "   ✅ Конфигурация обновлена"
}

# Функция для мониторинга и автоматического восстановления
monitor_and_recover() {
    local check_interval=30
    local max_failures=3
    local failure_count=0
    
    echo "👁️  Начинаем мониторинг Redis (проверка каждые ${check_interval}с)..."
    echo "Нажмите Ctrl+C для остановки"
    echo ""
    
    while true; do
        local all_healthy=true
        
        # Проверяем все Redis контейнеры
        if ! check_redis_health "scanner-redis" "6379"; then
            all_healthy=false
            failure_count=$((failure_count + 1))
        fi
        
        if ! check_redis_health "scanner-redis-worker-1" "6380"; then
            all_healthy=false
            failure_count=$((failure_count + 1))
        fi
        
        if ! check_redis_health "scanner-redis-worker-2" "6381"; then
            all_healthy=false
            failure_count=$((failure_count + 1))
        fi
        
        if [ "$all_healthy" = true ]; then
            failure_count=0
            echo "✅ Все Redis контейнеры здоровы ($(date '+%H:%M:%S'))"
        else
            echo "⚠️  Обнаружены проблемы с Redis ($(date '+%H:%M:%S'))"
            
            if [ $failure_count -ge $max_failures ]; then
                echo "🚨 Критическое количество сбоев ($failure_count), запускаем восстановление..."
                
                # Восстанавливаем проблемные контейнеры
                if ! check_redis_health "scanner-redis" "6379"; then
                    recover_redis "scanner-redis" "redis"
                fi
                
                if ! check_redis_health "scanner-redis-worker-1" "6380"; then
                    recover_redis "scanner-redis-worker-1" "redis-worker-1"
                fi
                
                if ! check_redis_health "scanner-redis-worker-2" "6381"; then
                    recover_redis "scanner-redis-worker-2" "redis-worker-2"
                fi
                
                failure_count=0
            fi
        fi
        
        # Периодическая очистка памяти
        if [ $((failure_count % 10)) -eq 0 ] && [ $failure_count -gt 0 ]; then
            cleanup_redis_memory "scanner-redis"
            cleanup_redis_memory "scanner-redis-worker-1"
            cleanup_redis_memory "scanner-redis-worker-2"
        fi
        
        sleep $check_interval
    done
}

# Обработка сигнала прерывания
trap 'echo -e "\n\n👋 Мониторинг остановлен"; exit 0' INT

# Запуск мониторинга
monitor_and_recover
