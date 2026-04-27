#!/bin/bash

# Скрипт для инициализации Redis Streams и Consumer Groups
# Решает проблему NOGROUP ошибок в go-worker

echo "🔧 Инициализация Redis Streams и Consumer Groups..."

# Проверяем, что Redis доступен
if ! docker-compose exec -T redis redis-cli ping >/dev/null 2>&1; then
    echo "❌ Redis недоступен. Запустите Redis сначала: docker-compose up -d redis"
    exit 1
fi

echo "✅ Redis доступен"

# Список стримов, которые нужны go-worker
STREAMS=(
    "stream:volatility"
    "stream:volatilityRange"
    "stream:top-gainers"
    "stream:top-losers"
    "stream:ws-new-pairs"
    "stream:binance:klines"
    "stream:binance:tickers"
    "stream:binance:funding"
    "stream:signal:telegram:raw"
    "stream:signal:telegram:parsed"
    "stream:notify:telegram"
    "candles:data"
)

# Consumer groups
CONSUMER_GROUPS=(
    "scanner-consumer-group"
    "candles-consumer-group"
    "telegram-consumer-group"
    "signal-consumer-group"
)

echo "📊 Инициализация стримов..."

# Создаем стримы и consumer groups
for stream in "${STREAMS[@]}"; do
    echo "🔄 Обработка стрима: $stream"
    
    # Создаем стрим с начальным сообщением
    docker-compose exec -T redis redis-cli XADD "$stream" "*" "type" "init" "timestamp" "$(date +%s)000" "message" "Stream initialized" >/dev/null 2>&1
    
    # Создаем consumer groups для каждого стрима
    for group in "${CONSUMER_GROUPS[@]}"; do
        echo "  👥 Создание consumer group: $group"
        docker-compose exec -T redis redis-cli XGROUP CREATE "$stream" "$group" "0" MKSTREAM >/dev/null 2>&1
    done
    
    echo "  ✅ Стрим $stream инициализирован"
done

echo "📊 Проверка созданных стримов..."

# Показываем информацию о созданных стримах
for stream in "${STREAMS[@]}"; do
    echo "🔍 Стрим: $stream"
    docker-compose exec -T redis redis-cli XLEN "$stream" 2>/dev/null || echo "  ❌ Стрим не найден"
    docker-compose exec -T redis redis-cli XINFO GROUPS "$stream" 2>/dev/null | head -5 || echo "  ❌ Consumer groups не найдены"
    echo ""
done

echo "🎉 Инициализация завершена!"
echo "💡 Теперь go-worker должен работать без NOGROUP ошибок"


echo "📊 Инициализация стримов на redis-worker-1..."
WORKER_1_STREAMS=(
    "stream:signals:outbox"
    "stream:trade:entry_candidate"
    "events:decision_snapshot"
    "stream:signals_news"
)
for stream in "${WORKER_1_STREAMS[@]}"; do
    echo "🔄 Обработка стрима: $stream"
    docker-compose exec -T redis-worker-1 redis-cli XADD "$stream" "*" "type" "init" "timestamp" "$(date +%s)000" "message" "Stream initialized" >/dev/null 2>&1
done

echo "📊 Инициализация стримов на redis-worker-2..."
WORKER_2_STREAMS=(
    "orders:exec"
    "orders:queue:binance"
)
for stream in "${WORKER_2_STREAMS[@]}"; do
    echo "🔄 Обработка стрима: $stream"
    docker-compose exec -T redis-worker-2 redis-cli XADD "$stream" "*" "type" "init" "timestamp" "$(date +%s)000" "message" "Stream initialized" >/dev/null 2>&1
done

