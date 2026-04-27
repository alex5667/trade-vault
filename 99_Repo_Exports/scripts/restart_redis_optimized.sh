#!/bin/bash

# Скрипт для перезапуска Redis с оптимизированной конфигурацией
# Решает проблемы с нестабильностью и высокой нагрузкой

echo "🔄 Перезапуск Redis с оптимизированной конфигурацией"
echo "=================================================="

# Функция для безопасного перезапуска контейнера
restart_container() {
    local container_name=$1
    local service_name=$2
    
    echo "🔄 Перезапускаем $service_name ($container_name)..."
    
    # Останавливаем контейнер
    echo "   Останавливаем контейнер..."
    docker stop $container_name >/dev/null 2>&1
    
    # Удаляем контейнер
    echo "   Удаляем контейнер..."
    docker rm $container_name >/dev/null 2>&1
    
    # Ждем немного
    sleep 2
    
    # Запускаем заново
    echo "   Запускаем контейнер заново..."
    docker-compose up -d $service_name
    
    # Ждем запуска
    echo "   Ждем запуска..."
    sleep 10
    
    # Проверяем статус
    if docker ps | grep -q "$container_name"; then
        echo "   ✅ $service_name успешно перезапущен"
    else
        echo "   ❌ Ошибка при перезапуске $service_name"
        return 1
    fi
    
    echo ""
}

# Функция для проверки готовности Redis
wait_for_redis() {
    local container_name=$1
    local max_attempts=30
    local attempt=1
    
    echo "⏳ Ждем готовности $container_name..."
    
    while [ $attempt -le $max_attempts ]; do
        if docker exec $container_name redis-cli ping >/dev/null 2>&1; then
            echo "   ✅ $container_name готов (попытка $attempt/$max_attempts)"
            return 0
        fi
        
        echo "   ⏳ Попытка $attempt/$max_attempts..."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    echo "   ❌ $container_name не готов после $max_attempts попыток"
    return 1
}

# Основной процесс
echo "🔍 Проверяем текущее состояние..."

# Останавливаем все сервисы, зависящие от Redis
echo "🛑 Останавливаем зависимые сервисы..."
docker-compose stop go-worker python-worker telegram-worker signal-parser-worker notify-worker

# Перезапускаем Redis контейнеры
echo "🔄 Перезапускаем Redis контейнеры..."

restart_container "scanner-redis" "redis"
restart_container "scanner-redis-worker-1" "redis-worker-1"
restart_container "scanner-redis-worker-2" "redis-worker-2"

# Ждем готовности Redis
echo "⏳ Ждем готовности Redis..."
wait_for_redis "scanner-redis"
wait_for_redis "scanner-redis-worker-1"
wait_for_redis "scanner-redis-worker-2"

# Проверяем конфигурацию
echo "🔍 Проверяем новую конфигурацию..."
echo "   Максимальная память:"
docker exec scanner-redis redis-cli config get maxmemory
echo "   Максимальные подключения:"
docker exec scanner-redis redis-cli config get maxclients
echo "   TCP backlog:"
docker exec scanner-redis redis-cli config get tcp-backlog

# Запускаем зависимые сервисы
echo "🚀 Запускаем зависимые сервисы..."
docker-compose up -d go-worker python-worker telegram-worker signal-parser-worker notify-worker

# Ждем запуска всех сервисов
echo "⏳ Ждем запуска всех сервисов..."
sleep 15

# Проверяем финальное состояние
echo "📊 Проверяем финальное состояние..."
echo ""

# Запускаем диагностику
./redis_diagnostics.sh

echo "=================================================="
echo "🏁 Перезапуск Redis завершен!"
echo ""
echo "�� Рекомендации:"
echo "   1. Мониторьте логи: docker logs scanner-redis -f"
echo "   2. Проверяйте метрики в Grafana"
echo "   3. При проблемах запустите: ./redis_diagnostics.sh"
