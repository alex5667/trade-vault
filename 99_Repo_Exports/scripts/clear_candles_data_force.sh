#!/bin/bash
# Быстрая очистка candles:data БЕЗ подтверждения

echo "🧹 БЫСТРАЯ ОЧИСТКА CANDLES:DATA"
echo "=========================================================================="

# Определяем контейнер Redis
REDIS_CONTAINER="scanner-redis-worker-1"
REDIS_PORT="6379"

# Проверяем, существует ли контейнер
if ! docker ps --format '{{.Names}}' | grep -q "$REDIS_CONTAINER"; then
    REDIS_CONTAINER="scanner-redis"
fi

# Функция для выполнения команд Redis
redis_cmd() {
    docker exec "$REDIS_CONTAINER" redis-cli -p "$REDIS_PORT" "$@" 2>/dev/null
}

# Показываем текущий размер
CURRENT_SIZE=$(redis_cmd XLEN candles:data || echo "0")
echo "📊 Текущее количество записей: $CURRENT_SIZE"

if [ "$CURRENT_SIZE" = "0" ]; then
    echo "✅ candles:data уже пуст"
    exit 0
fi

# Очистка данных
echo "🧹 Удаление данных..."
redis_cmd DEL candles:data > /dev/null

# Проверяем результат
NEW_SIZE=$(redis_cmd XLEN candles:data || echo "0")

echo ""
echo "=========================================================================="
echo "✅ ОЧИСТКА ЗАВЕРШЕНА"
echo "=========================================================================="
echo "   Удалено записей: $CURRENT_SIZE"
echo "   Осталось записей: $NEW_SIZE"
echo "=========================================================================="

