#!/bin/bash
# Мониторинг уведомлений для проверки исправления дублирования

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📊 МОНИТОРИНГ УВЕДОМЛЕНИЙ (проверка исправления)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Ожидание новых сигналов..."
echo "Следим за go-gateway (НЕ должно быть 'Preparing Telegram message')"
echo ""
echo "Нажмите Ctrl+C для остановки"
echo ""

# Параллельный мониторинг двух логов
docker logs -f scanner-go-gateway 2>&1 | grep --line-buffered -E "POST.*orders|queued|Telegram|Preparing" | while read line; do
    if echo "$line" | grep -q "Preparing Telegram message"; then
        echo "❌ ОШИБКА: go-gateway отправляет в Telegram (НЕ ДОЛЖНО БЫТЬ)"
        echo "   $line"
    elif echo "$line" | grep -q "queued (notifications handled by notify-worker)"; then
        echo "✅ ПРАВИЛЬНО: go-gateway только добавляет в очередь"
        echo "   $line"
    elif echo "$line" | grep -q "POST.*orders"; then
        echo "📥 Получен новый ордер:"
        echo "   $line"
    fi
done
