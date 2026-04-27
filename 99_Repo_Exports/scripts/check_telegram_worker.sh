#!/bin/bash
# Проверка работы telegram-worker

echo "════════════════════════════════════════════════════════════════"
echo "ПРОВЕРКА TELEGRAM WORKER"
echo "════════════════════════════════════════════════════════════════"
echo ""

# Проверка статуса контейнера
echo "📦 Статус контейнера:"
docker ps --filter name=scanner-telegram-worker --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo ""

# Проверка количества подписанных каналов
echo "📡 Подписанные каналы:"
CHANNELS=$(docker exec scanner-redis redis-cli SCARD telegram:channels:usernames 2>/dev/null)
echo "   Всего каналов: $CHANNELS"
echo ""

# Проверка Rocket каналов
echo "🚀 Rocket каналы:"
docker exec scanner-redis redis-cli SMEMBERS telegram:channels:usernames 2>/dev/null | grep -i rocket | while read channel; do
    STATUS=$(docker exec scanner-redis redis-cli GET "telegram:channel:$channel:status" 2>/dev/null)
    echo "   - $channel (статус: ${STATUS:-не установлен})"
done
echo ""

# Проверка последних логов
echo "📋 Последние логи (20 строк):"
docker logs scanner-telegram-worker 2>&1 | tail -20 | grep -v "deploy sub-keys"
echo ""

# Проверка наличия сообщений в Redis stream
echo "📊 Redis Streams:"
RAW_COUNT=$(docker exec scanner-redis redis-cli XLEN signal:telegram:raw 2>/dev/null || echo "0")
PARSED_COUNT=$(docker exec scanner-redis redis-cli XLEN signal:telegram:parsed 2>/dev/null || echo "0")
echo "   signal:telegram:raw: $RAW_COUNT сообщений"
echo "   signal:telegram:parsed: $PARSED_COUNT сообщений"
echo ""

# Проверка обработчика событий
echo "🎯 Статус обработчика событий:"
if docker logs scanner-telegram-worker 2>&1 | grep -q "Обработчик зарегистрирован"; then
    echo "   ✅ Обработчик зарегистрирован"
else
    echo "   ❌ Обработчик НЕ зарегистрирован"
fi

if docker logs scanner-telegram-worker 2>&1 | grep -q "event loop активен"; then
    echo "   ✅ Event loop активен"
else
    echo "   ❌ Event loop НЕ активен"
fi
echo ""

echo "════════════════════════════════════════════════════════════════"
echo "МОНИТОРИНГ В РЕАЛЬНОМ ВРЕМЕНИ (Ctrl+C для выхода):"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "Отправьте сообщение в любой подписанный канал..."
echo ""

# Мониторинг в реальном времени
docker logs scanner-telegram-worker --follow 2>&1 | grep --line-buffered -E "(🔔|📨|СОБЫТИЕ|СООБЩЕНИЕ|ERROR|❌)"

