#!/bin/bash

# Скрипт мониторинга Redis в реальном времени
# Отслеживает производительность, память, соединения и ошибки

echo "📊 Мониторинг Redis - scanner-infra"
echo "=================================="
echo "Нажмите Ctrl+C для выхода"
echo ""

# Функция для получения метрик Redis
get_redis_metrics() {
    local container_name=$1
    local port=$2
    
    # Основные метрики
    local memory=$(docker exec $container_name redis-cli info memory 2>/dev/null | grep used_memory_human | cut -d: -f2 | tr -d '\r')
    local connections=$(docker exec $container_name redis-cli info clients 2>/dev/null | grep connected_clients | cut -d: -f2 | tr -d '\r')
    local keys=$(docker exec $container_name redis-cli dbsize 2>/dev/null | tr -d '\r')
    local ops_per_sec=$(docker exec $container_name redis-cli info stats 2>/dev/null | grep instantaneous_ops_per_sec | cut -d: -f2 | tr -d '\r')
    local total_commands=$(docker exec $container_name redis-cli info stats 2>/dev/null | grep total_commands_processed | cut -d: -f2 | tr -d '\r')
    local keyspace_hits=$(docker exec $container_name redis-cli info stats 2>/dev/null | grep keyspace_hits | cut -d: -f2 | tr -d '\r')
    local keyspace_misses=$(docker exec $container_name redis-cli info stats 2>/dev/null | grep keyspace_misses | cut -d: -f2 | tr -d '\r')
    local expired_keys=$(docker exec $container_name redis-cli info stats 2>/dev/null | grep expired_keys | cut -d: -f2 | tr -d '\r')
    local evicted_keys=$(docker exec $container_name redis-cli info stats 2>/dev/null | grep evicted_keys | cut -d: -f2 | tr -d '\r')
    local rejected_connections=$(docker exec $container_name redis-cli info stats 2>/dev/null | grep rejected_connections | cut -d: -f2 | tr -d '\r')
    local total_errors=$(docker exec $container_name redis-cli info stats 2>/dev/null | grep total_errors_received | cut -d: -f2 | tr -d '\r')
    
    # Вычисляем hit rate
    local hit_rate="0.00"
    if [ "$keyspace_hits" != "0" ] && [ "$keyspace_misses" != "0" ]; then
        local total_requests=$((keyspace_hits + keyspace_misses))
        if [ $total_requests -gt 0 ]; then
            hit_rate=$(echo "scale=2; $keyspace_hits * 100 / $total_requests" | bc -l 2>/dev/null || echo "0.00")
        fi
    fi
    
    echo "$memory|$connections|$keys|$ops_per_sec|$total_commands|$hit_rate|$expired_keys|$evicted_keys|$rejected_connections|$total_errors"
}

# Функция для отображения метрик
display_metrics() {
    local container_name=$1
    local port=$2
    local metrics=$3
    
    IFS='|' read -r memory connections keys ops_per_sec total_commands hit_rate expired_keys evicted_keys rejected_connections total_errors <<< "$metrics"
    
    echo "🔍 $container_name (порт $port):"
    echo "   💾 Память: $memory"
    echo "   🔗 Подключения: $connections"
    echo "   🗝️  Ключи: $keys"
    echo "   ⚡ Ops/sec: $ops_per_sec"
    echo "   📊 Всего команд: $total_commands"
    echo "   🎯 Hit rate: ${hit_rate}%"
    echo "   ⏰ Истекших ключей: $expired_keys"
    echo "   🗑️  Вытесненных ключей: $evicted_keys"
    echo "   ❌ Отклоненных подключений: $rejected_connections"
    echo "   🚨 Ошибок: $total_errors"
    echo ""
}

# Функция для проверки здоровья
check_health() {
    local container_name=$1
    local health=$(docker inspect --format='{{.State.Health.Status}}' $container_name 2>/dev/null)
    
    if [ "$health" = "healthy" ]; then
        echo "✅ $container_name здоров"
    else
        echo "❌ $container_name нездоров: $health"
    fi
}

# Основной цикл мониторинга
monitor_loop() {
    local refresh_rate=5
    local iteration=0
    
    while true; do
        clear
        echo "📊 Мониторинг Redis - scanner-infra (обновление каждые ${refresh_rate}с)"
        echo "================================================================"
        echo "Итерация: $((iteration + 1)) | Время: $(date '+%H:%M:%S')"
        echo ""
        
        # Проверяем здоровье всех контейнеров
        echo "🏥 Статус здоровья:"
        check_health "scanner-redis"
        check_health "scanner-redis-worker-1"
        check_health "scanner-redis-worker-2"
        echo ""
        
        # Получаем и отображаем метрики
        echo "📈 Метрики производительности:"
        echo "----------------------------------------"
        
        # Основной Redis
        local main_metrics=$(get_redis_metrics "scanner-redis" "6379")
        display_metrics "scanner-redis" "6379" "$main_metrics"
        
        # Worker 1
        local worker1_metrics=$(get_redis_metrics "scanner-redis-worker-1" "6380")
        display_metrics "scanner-redis-worker-1" "6380" "$worker1_metrics"
        
        # Worker 2
        local worker2_metrics=$(get_redis_metrics "scanner-redis-worker-2" "6381")
        display_metrics "scanner-redis-worker-2" "6381" "$worker2_metrics"
        
        # Общие рекомендации
        echo "💡 Рекомендации:"
        echo "   - Hit rate должен быть > 90%"
        echo "   - Ops/sec показывает нагрузку"
        echo "   - Отклоненные подключения указывают на проблемы"
        echo "   - Вытесненные ключи означают нехватку памяти"
        echo ""
        
        echo "Нажмите Ctrl+C для выхода..."
        
        sleep $refresh_rate
        iteration=$((iteration + 1))
    done
}

# Обработка сигнала прерывания
trap 'echo -e "\n\n👋 Мониторинг остановлен"; exit 0' INT

# Запуск мониторинга
monitor_loop
