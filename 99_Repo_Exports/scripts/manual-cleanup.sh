#!/bin/bash

# Скрипт для ручного запуска очистки Redis Streams

echo "🧹 Ручная очистка Redis Streams"
echo "================================"

# Проверяем, что Redis контейнер запущен
if ! docker ps | grep -q scanner-redis; then
    echo "❌ Redis контейнер не запущен"
    exit 1
fi

# Проверяем, что Lua скрипт существует
if ! docker exec scanner-redis test -f /tmp/cleanup.lua; then
    echo "❌ Lua скрипт cleanup.lua не найден в Redis контейнере"
    echo "Скопируйте скрипт: docker cp cleanup.lua scanner-redis:/tmp/cleanup.lua"
    exit 1
fi

echo "✅ Redis контейнер запущен"
echo "✅ Lua скрипт найден"

# Показываем текущее состояние streams
echo ""
echo "📊 Текущее состояние streams:"
docker exec scanner-redis redis-cli xlen "stream:kline_1m" | xargs -I {} echo "  stream:kline_1m: {} записей"

# Запускаем очистку
echo ""
echo "🔧 Запуск очистки..."
cleaned_count=$(docker exec scanner-redis redis-cli --eval /tmp/cleanup.lua)

echo ""
echo "✅ Очистка завершена"
echo "📊 Удалено записей: $cleaned_count"

# Показываем состояние после очистки
echo ""
echo "📊 Состояние после очистки:"
docker exec scanner-redis redis-cli xlen "stream:kline_1m" | xargs -I {} echo "  stream:kline_1m: {} записей"

echo ""
echo "🎯 Для автоматической очистки настроен cron:"
echo "   0 2 * * * - каждый день в 2:00 утра"
echo "   Логи: /home/alex/front/trade/scanner_infra/redis_cleanup.log" 