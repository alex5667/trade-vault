#!/bin/bash
# Скрипт для очистки candles:data в Redis

echo "🧹 ОЧИСТКА CANDLES:DATA"
echo "=========================================================================="

# Определяем контейнер Redis
REDIS_CONTAINER="scanner-redis-worker-1"
REDIS_PORT="6379"

# Проверяем, существует ли контейнер
if ! docker ps --format '{{.Names}}' | grep -q "$REDIS_CONTAINER"; then
    echo "⚠️ Контейнер $REDIS_CONTAINER не найден, пробуем scanner-redis..."
    REDIS_CONTAINER="scanner-redis"
fi

# Функция для выполнения команд Redis
redis_cmd() {
    docker exec "$REDIS_CONTAINER" redis-cli -p "$REDIS_PORT" "$@"
}

# Показываем текущий размер
echo "📊 Проверка текущего размера..."
CURRENT_SIZE=$(redis_cmd XLEN candles:data 2>/dev/null || echo "0")
echo "   Текущее количество записей: $CURRENT_SIZE"

if [ "$CURRENT_SIZE" = "0" ]; then
    echo "✅ candles:data уже пуст"
    exit 0
fi

# Спрашиваем подтверждение
echo ""
echo "⚠️  ВНИМАНИЕ: Будет удалено $CURRENT_SIZE записей!"
read -p "Продолжить? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "❌ Отменено пользователем"
    exit 1
fi

# Очистка данных
echo ""
echo "🧹 Очистка candles:data..."

# Вариант 1: Полное удаление ключа (быстрее)
redis_cmd DEL candles:data

# Проверяем результат
NEW_SIZE=$(redis_cmd XLEN candles:data 2>/dev/null || echo "0")

echo ""
echo "=========================================================================="
echo "✅ ОЧИСТКА ЗАВЕРШЕНА"
echo "=========================================================================="
echo "   Было записей: $CURRENT_SIZE"
echo "   Осталось записей: $NEW_SIZE"
echo "   Удалено записей: $((CURRENT_SIZE - NEW_SIZE))"
echo "=========================================================================="

# Дополнительная статистика
echo ""
echo "📊 Дополнительная информация:"
MEMORY=$(redis_cmd INFO memory | grep used_memory_human | cut -d: -f2 | tr -d '\r')
echo "   Использование памяти Redis: $MEMORY"
echo ""
echo "💡 Совет: Данные будут автоматически наполняться заново от go-worker"
echo "=========================================================================="

