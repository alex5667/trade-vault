#!/bin/bash
# Быстрый мониторинг системы XAUUSD Order Flow

echo "╔═══════════════════════════════════════════════════════════════════╗"
echo "║                                                                   ║"
echo "║              📊 XAUUSD ORDERFLOW MONITORING 📊                   ║"
echo "║                                                                   ║"
echo "╚═══════════════════════════════════════════════════════════════════╝"
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "1. 📈 Тики в stream:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker exec scanner-redis-worker-1 redis-cli XLEN stream:tick_XAUUSD 2>/dev/null || echo "Stream не найден"
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "2. 📊 Статистика обработки (последние 5 минут):"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker logs scanner-python-worker | grep "XAU OrderFlow" | tail -5 || echo "Статистики пока нет"
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "3. ❌ Ошибки (последние 2 минуты):"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
ERROR_COUNT=$(docker logs scanner-python-worker --since 2m 2>&1 | grep -i "ошибка обработки" | wc -l)
echo "Ошибок обработки: $ERROR_COUNT"
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "4. 🎯 Сигналы (все время):"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
SIGNAL_COUNT=$(docker logs scanner-python-worker 2>&1 | grep -E "SIGNAL.*XAUUSD" | wc -l)
echo "Всего сигналов: $SIGNAL_COUNT"

if [ "$SIGNAL_COUNT" -gt 0 ]; then
    echo ""
    echo "Последние 3 сигнала:"
    docker logs scanner-python-worker 2>&1 | grep -E "SIGNAL.*XAUUSD" | tail -3
fi
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "5. 📡 TickBridge статус:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Проверьте MT5 Experts log для TickBridge"
echo "Должно быть: '📊 Статистика: XXX тиков | Success: XXX (100.0%)'"
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "6. 🔄 Статус контейнеров:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "(python-worker|py-obi|NAME)"
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "💡 Команды для детального мониторинга:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "# Следить за сигналами в реальном времени:"
echo "docker logs -f scanner-python-worker | grep SIGNAL"
echo ""
echo "# Проверить текущую статистику:"
echo "docker logs scanner-python-worker | grep 'XAU OrderFlow' | tail -10"
echo ""
echo "# Проверить последние тики в stream:"
echo "docker exec scanner-redis-worker-1 redis-cli XREVRANGE stream:tick_XAUUSD + - COUNT 5"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "✅ Система работает корректно!"
echo ""

