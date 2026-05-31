#!/bin/bash

# 🎯 БЫСТРАЯ ПРОВЕРКА РАБОТЫ XAUUSD СИГНАЛОВ
# =========================================

echo "🔍 ПРОВЕРКА СИСТЕМЫ XAUUSD СИГНАЛОВ"
echo "===================================="
echo ""

# 1. Проверка статуса контейнеров
echo "📦 1. Статус контейнеров:"
echo "------------------------"
docker ps | grep -E "(py-obi|python-worker|notify-worker)" | awk '{print $NF, "-", $(NF-1)}'
echo ""

# 2. Проверка тиков в Redis
echo "📊 2. Тики в Redis:"
echo "------------------"
TICKS=$(docker exec scanner-redis-worker-1 redis-cli XLEN stream:tick_XAUUSD 2>/dev/null || echo "0")
echo "Тиков в stream:tick_XAUUSD: $TICKS"
echo ""

# 3. Проверка сигналов в очереди
echo "🔔 3. Сигналы в очереди:"
echo "----------------------"
SIGNALS=$(docker exec scanner-redis-worker-1 redis-cli XLEN notify:telegram 2>/dev/null || echo "0")
echo "Сигналов в notify:telegram: $SIGNALS"
echo ""

# 4. Последний сгенерированный сигнал
echo "📤 4. Последний сгенерированный сигнал:"
echo "-------------------------------------"
docker logs --tail=100 scanner-python-worker 2>&1 | grep "📤 Сигнал опубликован" | tail -1
echo ""

# 5. Последний отправленный сигнал в бот
echo "✅ 5. Последний отправленный в бот:"
echo "---------------------------------"
docker logs --tail=50 scanner-notify-worker 2>&1 | grep "✅ notifier: сигнал XAUUSD" | tail -1
echo ""

# 6. Последнее сообщение в Redis
echo "💾 6. Последнее сообщение в Redis:"
echo "--------------------------------"
docker exec scanner-redis-worker-1 redis-cli XREVRANGE notify:telegram + - COUNT 1 2>/dev/null | grep -A 1 "text" | tail -1
echo ""

# 7. Статистика notify-worker
echo "📈 7. Статистика notify-worker:"
echo "-----------------------------"
docker logs --tail=20 scanner-notify-worker 2>&1 | grep "total:" | tail -1
echo ""

# 8. Pivot уровни
echo "📐 8. Актуальные Pivot уровни:"
echo "----------------------------"
docker exec scanner-redis-worker-1 redis-cli GET pivots:latest 2>/dev/null || echo "Нет данных"
echo ""

# 9. Проверка ошибок
echo "❌ 9. Последние ошибки (если есть):"
echo "---------------------------------"
ERRORS=$(docker logs --tail=50 scanner-notify-worker 2>&1 | grep "❌" | wc -l)
if [ "$ERRORS" -gt 0 ]; then
    docker logs --tail=50 scanner-notify-worker 2>&1 | grep "❌" | tail -3
else
    echo "✅ Ошибок не обнаружено"
fi
echo ""

# Итог
echo "=================================="
echo "🎯 ИТОГ:"
echo "=================================="

if [ "$TICKS" -gt 0 ] && [ "$SIGNALS" -gt 0 ]; then
    echo "✅ Система работает нормально!"
    echo "   - Тики приходят от MT5"
    echo "   - Сигналы генерируются"
    echo "   - Очередь обрабатывается"
elif [ "$TICKS" -gt 0 ] && [ "$SIGNALS" -eq 0 ]; then
    echo "⚠️ Тики приходят, но нет сигналов в очереди"
    echo "   Возможно, условия для сигналов не выполнены"
elif [ "$TICKS" -eq 0 ]; then
    echo "❌ Нет тиков от MT5!"
    echo "   Проверьте MT5 EA и py-obi-service"
else
    echo "⚠️ Статус неопределен, проверьте логи вручную"
fi

echo ""
echo "📝 Для детальной диагностики:"
echo "   docker logs --tail=100 scanner-notify-worker"
echo "   docker logs --tail=100 scanner-python-worker"
echo "   docker logs --tail=100 scanner-py-obi"

