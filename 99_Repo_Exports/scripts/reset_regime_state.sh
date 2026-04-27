#!/bin/bash
# Скрипт для сброса состояния regime-worker и начала с чистого листа

echo "🔄 СБРОС СОСТОЯНИЯ REGIME WORKER"
echo "================================"

# 1. Остановить regime-worker
echo "1️⃣ Остановка regime-worker..."
docker-compose stop regime-worker

# 2. Очистить consumer groups из старых kline стримов
echo "2️⃣ Очистка consumer groups..."
for tf in 1m 5m 15m 30m 1h 4h 1d 1w 1M 3M 1y; do
    echo "   Удаление group для stream:kline_$tf"
    docker exec scanner-redis redis-cli -p 6379 XGROUP DESTROY "stream:kline_$tf" "regime-worker-group" 2>/dev/null || true
done

# 3. Очистить stream:regime на redis-worker-1 (если есть)
echo "3️⃣ Очистка stream:regime..."
docker exec scanner-redis-worker-1 redis-cli -p 6379 DEL stream:regime

# 4. Запустить regime-worker
echo "4️⃣ Запуск regime-worker..."
docker-compose up -d regime-worker

echo ""
echo "✅ Сброс завершен!"
echo "📊 Теперь worker будет обрабатывать только новые данные"
echo ""
echo "Проверка через 30 секунд:"
echo "  docker logs -f scanner-regime-worker"

