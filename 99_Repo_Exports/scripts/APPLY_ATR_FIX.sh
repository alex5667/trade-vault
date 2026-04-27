#!/bin/bash

echo "======================================"
echo "Применение исправления ATR fallback"
echo "======================================"
echo ""

echo "🔍 Проблема:"
echo "  'skip emit: bad atr/entry' - сигналы не отправлялись из-за ATR=0"
echo ""

echo "✅ Решение:"
echo "  Добавлен 5-уровневый fallback механизм для ATR:"
echo "  1. ATR из snapshot"
echo "  2. Кэшированный ATR (60 сек)"
echo "  3. ATR из Redis (3 формата ключей)"
echo "  4. ATR из go-gateway API"
echo "  5. Fallback значения (1m: 1.2, 5m: 3.5, 15m: 6.5)"
echo ""

echo "🔄 Перезапуск aggregated-hub..."
docker compose restart aggregated-hub

echo ""
echo "⏳ Ждем 5 секунд..."
sleep 5

echo ""
echo "📋 Проверяем логи (последние 50 строк)..."
docker compose logs --tail=50 aggregated-hub

echo ""
echo "✅ Готово!"
echo ""
echo "🔍 Проверка:"
echo "  1. Смотрите логи - НЕ должно быть 'skip emit: bad atr/entry'"
echo "  2. Должны быть логи: 'Calling write_and_push: ... atr=X.XXXX'"
echo "  3. Если есть '⚠️  Using fallback ATR' - это нормально (fallback работает)"
echo ""
echo "📊 Дополнительные команды:"
echo "  ./view_logs.sh aggregated-hub          # Логи в real-time"
echo "  docker compose exec redis redis-cli    # Проверить ATR в Redis"
echo "  GET atr:val:XAUUSD:1m                  # (в Redis CLI)"
echo ""
echo "📖 Подробности: BUGFIX_ATR_AGGREGATED_HUB_V2.md"
echo ""

