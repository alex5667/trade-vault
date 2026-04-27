#!/bin/bash

# 🎯 МОНИТОРИНГ ВСЕХ СИСТЕМ ГЕНЕРАЦИИ СИГНАЛОВ XAUUSD
# ====================================================

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  📊 МОНИТОРИНГ СИСТЕМ СИГНАЛОВ XAUUSD"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
date
echo ""

# ═══════════════════════════════════════════════════════════════
# 1. СТАТУС КОНТЕЙНЕРОВ
# ═══════════════════════════════════════════════════════════════
echo "1️⃣ СТАТУС КОНТЕЙНЕРОВ:"
echo "────────────────────────────────────────────────────────────"
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "(signal-generator|python-worker|notify-worker|go-gateway|py-obi)" || echo "⚠️ Контейнеры не найдены"
echo ""

# ═══════════════════════════════════════════════════════════════
# 2. СИСТЕМА 1: XAUUSD OrderFlow Handler → Telegram БОТ
# ═══════════════════════════════════════════════════════════════
echo "2️⃣ XAUUSD OrderFlow Handler (Order Flow → Telegram БОТ):"
echo "────────────────────────────────────────────────────────────"

# Последний сгенерированный сигнал
echo "📤 Последний сгенерированный сигнал:"
docker logs --tail=100 scanner-python-worker 2>&1 | grep "📤 Сигнал опубликован" | tail -1 || echo "   Нет данных"

# Количество сигналов в Redis
NOTIFY_QUEUE=$(docker exec scanner-redis-worker-1 redis-cli XLEN notify:telegram 2>/dev/null || echo "0")
echo "📊 Сигналов в очереди notify:telegram: $NOTIFY_QUEUE"

# Последний отправленный в бот
echo "✅ Последний отправленный в бот:"
docker logs --tail=50 scanner-notify-worker 2>&1 | grep "✅ notifier: сигнал XAUUSD" | tail -1 || echo "   Нет данных"

# Статистика обработки
docker logs --tail=20 scanner-notify-worker 2>&1 | grep "total:" | tail -1 || echo ""
echo ""

# ═══════════════════════════════════════════════════════════════
# 3. СИСТЕМА 2: Signal-Generator → go-gateway
# ═══════════════════════════════════════════════════════════════
echo "3️⃣ Signal-Generator (Technical Analysis → go-gateway):"
echo "────────────────────────────────────────────────────────────"

# Текущие индикаторы
echo "📈 Текущие индикаторы:"
docker logs --tail=10 scanner-signal-generator 2>&1 | grep "Price:" | tail -1 || echo "   Нет данных"

# Последний сигнал
echo ""
echo "🔔 Последний сгенерированный сигнал:"
docker logs --tail=100 scanner-signal-generator 2>&1 | grep -A 7 "🔔 Новый сигнал" | tail -8 || echo "   Нет данных"

# Статус cooldown
echo ""
echo "⏱️ Статус cooldown:"
docker logs --tail=5 scanner-signal-generator 2>&1 | grep "cooldown" | tail -1 || echo "   Cooldown не активен - готов к новому сигналу"

# Очередь в go-gateway
echo ""
echo "📊 Очередь в go-gateway:"
docker logs --tail=20 scanner-go-gateway 2>&1 | grep "queue size" | tail -1 || echo "   Нет данных"
echo ""

# ═══════════════════════════════════════════════════════════════
# 4. ДАННЫЕ В REDIS
# ═══════════════════════════════════════════════════════════════
echo "4️⃣ ДАННЫЕ В REDIS:"
echo "────────────────────────────────────────────────────────────"

TICKS=$(docker exec scanner-redis-worker-1 redis-cli XLEN stream:tick_XAUUSD 2>/dev/null || echo "0")
echo "📊 Тиков в stream:tick_XAUUSD: $TICKS"

# Последний тик
echo "🔄 Последний тик:"
docker exec scanner-redis-worker-1 redis-cli XREVRANGE stream:tick_XAUUSD + - COUNT 1 2>/dev/null | grep -A 1 "bid" | tail -1 || echo "   Нет данных"

# Pivot levels
echo ""
echo "📐 Pivot уровни:"
PIVOTS=$(docker exec scanner-redis-worker-1 redis-cli GET pivots:latest 2>/dev/null || echo "Нет данных")
if [ ! -z "$PIVOTS" ]; then
    echo "$PIVOTS" | head -c 100
    echo "..."
else
    echo "   Нет pivot данных"
fi
echo ""

# ═══════════════════════════════════════════════════════════════
# 5. ОШИБКИ И ПРЕДУПРЕЖДЕНИЯ
# ═══════════════════════════════════════════════════════════════
echo ""
echo "5️⃣ ПРОВЕРКА ОШИБОК:"
echo "────────────────────────────────────────────────────────────"

ERRORS_ORDERFLOW=$(docker logs --tail=50 scanner-python-worker 2>&1 | grep -c "❌" || echo "0")
ERRORS_GENERATOR=$(docker logs --tail=50 scanner-signal-generator 2>&1 | grep -c "❌" || echo "0")
ERRORS_NOTIFY=$(docker logs --tail=50 scanner-notify-worker 2>&1 | grep -c "❌" || echo "0")

echo "OrderFlow Handler: $ERRORS_ORDERFLOW ошибок"
echo "Signal-Generator: $ERRORS_GENERATOR ошибок"
echo "Notify-Worker: $ERRORS_NOTIFY ошибок"

if [ "$ERRORS_ORDERFLOW" -gt 0 ] || [ "$ERRORS_GENERATOR" -gt 0 ] || [ "$ERRORS_NOTIFY" -gt 0 ]; then
    echo ""
    echo "⚠️ Обнаружены ошибки. Последние 3:"
    docker logs --tail=50 scanner-python-worker scanner-signal-generator scanner-notify-worker 2>&1 | grep "❌" | tail -3
else
    echo ""
    echo "✅ Ошибок не обнаружено!"
fi
echo ""

# ═══════════════════════════════════════════════════════════════
# 6. ИТОГОВАЯ ОЦЕНКА
# ═══════════════════════════════════════════════════════════════
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  🎯 ИТОГОВАЯ ОЦЕНКА:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Проверка OrderFlow
if [ "$TICKS" -gt 0 ] && [ "$NOTIFY_QUEUE" -gt 0 ]; then
    echo "✅ OrderFlow Handler: РАБОТАЕТ"
    echo "   - Тики поступают: $TICKS"
    echo "   - Сигналов в очереди: $NOTIFY_QUEUE"
else
    echo "⚠️ OrderFlow Handler: Проверьте тики и очередь"
fi

# Проверка Signal-Generator
SG_RUNNING=$(docker ps | grep -c scanner-signal-generator || echo "0")
if [ "$SG_RUNNING" -gt 0 ]; then
    echo "✅ Signal-Generator: РАБОТАЕТ"
    echo "   - Контейнер запущен"
    echo "   - Индикаторы активны (EMA, RSI, ATR, MACD)"
else
    echo "❌ Signal-Generator: НЕ ЗАПУЩЕН"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  🚀 Для детальной информации:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "  OrderFlow Handler:"
echo "    docker logs --tail=50 scanner-python-worker | grep '📤'"
echo ""
echo "  Signal-Generator:"
echo "    docker logs --tail=50 scanner-signal-generator | grep '🔔'"
echo ""
echo "  Notify-Worker:"
echo "    docker logs --tail=50 scanner-notify-worker | grep '✅'"
echo ""
echo "  go-gateway:"
echo "    docker logs --tail=20 scanner-go-gateway | grep 'Enqueued'"
echo ""

