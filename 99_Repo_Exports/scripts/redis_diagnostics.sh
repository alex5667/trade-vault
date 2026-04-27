#!/bin/bash

# Скрипт диагностики и исправления проблем с Redis
# Автор: AI Assistant

echo "🔍 Диагностика Redis - scanner-infra"
echo "====================================="

# Функция для проверки статуса Redis
check_redis_status() {
    local container_name=$1
    local port=$2
    
    echo "📊 Проверяем $container_name (порт $port)..."
    
    # Проверяем, запущен ли контейнер
    if ! docker ps | grep -q "$container_name"; then
        echo "❌ Контейнер $container_name не запущен"
        return 1
    fi
    
    # Проверяем health check
    local health=$(docker inspect --format='{{.State.Health.Status}}' $container_name 2>/dev/null)
    if [ "$health" = "healthy" ]; then
        echo "✅ $container_name здоров"
    else
        echo "⚠️  $container_name нездоров: $health"
    fi
    
    # Проверяем подключение
    if docker exec $container_name redis-cli ping >/dev/null 2>&1; then
        echo "✅ Подключение к $container_name работает"
    else
        echo "❌ Не удается подключиться к $container_name"
        return 1
    fi
    
    # Проверяем использование памяти
    local memory=$(docker exec $container_name redis-cli info memory | grep used_memory_human | cut -d: -f2 | tr -d '\r')
    echo "💾 Использование памяти: $memory"
    
    # Проверяем количество подключений
    local connections=$(docker exec $container_name redis-cli info clients | grep connected_clients | cut -d: -f2 | tr -d '\r')
    echo "🔗 Активных подключений: $connections"
    
    # Проверяем количество ключей
    local keys=$(docker exec $container_name redis-cli dbsize | tr -d '\r')
    echo "🗝️  Количество ключей: $keys"
    
    # Проверяем ошибки
    local errors=$(docker exec $container_name redis-cli info stats | grep total_errors_received | cut -d: -f2 | tr -d '\r')
    echo "❌ Ошибок получено: $errors"
    
    echo ""
}

# Функция для проверки логов на ошибки
check_redis_logs() {
    local container_name=$1
    echo "📋 Проверяем логи $container_name на ошибки..."
    
    local error_count=$(docker logs $container_name --tail 100 2>&1 | grep -i error | wc -l)
    local warning_count=$(docker logs $container_name --tail 100 2>&1 | grep -i warning | wc -l)
    
    echo "   Ошибок в последних 100 строках: $error_count"
    echo "   Предупреждений в последних 100 строках: $warning_count"
    
    if [ $error_count -gt 0 ]; then
        echo "⚠️  Найдены ошибки в логах:"
        docker logs $container_name --tail 100 2>&1 | grep -i error | tail -5
    fi
    
    if [ $warning_count -gt 0 ]; then
        echo "⚠️  Найдены предупреждения в логах:"
        docker logs $container_name --tail 100 2>&1 | grep -i warning | tail -5
    fi
    
    echo ""
}

# Функция для проверки производительности
check_redis_performance() {
    local container_name=$1
    echo "⚡ Проверяем производительность $container_name..."
    
    # Тест ping
    local ping_time=$(docker exec $container_name redis-cli --latency-history -i 1 -c 1 ping 2>/dev/null | tail -1 | awk '{print $1}')
    if [ -n "$ping_time" ]; then
        echo "   Ping время: ${ping_time}ms"
    fi
    
    # Тест производительности
    echo "   Запускаем тест производительности..."
    docker exec $container_name redis-cli --latency-history -i 1 -c 1 --latency 2>/dev/null | head -10
    
    echo ""
}

# Функция для исправления проблем
fix_redis_issues() {
    local container_name=$1
    echo "🔧 Исправляем проблемы $container_name..."
    
    # Очищаем память
    echo "   Очищаем память..."
    docker exec $container_name redis-cli memory purge >/dev/null 2>&1
    
    # Сбрасываем статистику
    echo "   Сбрасываем статистику..."
    docker exec $container_name redis-cli config resetstat >/dev/null 2>&1
    
    # Проверяем конфигурацию
    echo "   Проверяем конфигурацию..."
    docker exec $container_name redis-cli config get maxmemory
    docker exec $container_name redis-cli config get maxclients
    docker exec $container_name redis-cli config get tcp-backlog
    
    echo ""
}

# Основная диагностика
echo "🔍 Начинаем диагностику Redis контейнеров..."
echo ""

# Проверяем все Redis контейнеры
check_redis_status "scanner-redis" "6379"
check_redis_status "scanner-redis-worker-1" "6380"
check_redis_status "scanner-redis-worker-2" "6381"

# Проверяем логи
echo "📋 Проверяем логи на ошибки..."
check_redis_logs "scanner-redis"
check_redis_logs "scanner-redis-worker-1"
check_redis_logs "scanner-redis-worker-2"

# Проверяем производительность
echo "⚡ Проверяем производительность..."
check_redis_performance "scanner-redis"

# Исправляем проблемы
echo "🔧 Исправляем проблемы..."
fix_redis_issues "scanner-redis"

echo "====================================="
echo "🏁 Диагностика завершена!"
echo ""
echo "💡 Рекомендации:"
echo "   1. Если есть ошибки - перезапустите контейнеры"
echo "   2. Если высокая нагрузка - увеличьте ресурсы"
echo "   3. Если проблемы с памятью - очистите кэш"
echo "   4. Если проблемы с сетью - проверьте настройки"
