#!/bin/bash
# Быстрый перезапуск БЕЗ пересборки (только для изменений в Python коде)

echo "======================================"
echo "Быстрый перезапуск (без пересборки)"
echo "======================================"
echo ""

if [ -z "$1" ]; then
    echo "🔄 Перезапускаем все сервисы..."
    docker compose restart
else
    echo "🔄 Перезапускаем сервис: $1"
    docker compose restart "$1"
fi

echo ""
echo "⏳ Ждем 3 секунды..."
sleep 3

echo ""
echo "📊 Статус контейнеров:"
docker compose ps

echo ""
echo "✅ Готово!"
echo ""
echo "Использование:"
echo "  ./quick_restart.sh                  # Перезапустить все"
echo "  ./quick_restart.sh aggregated-hub   # Перезапустить только aggregated-hub"
echo "  ./quick_restart.sh telegram-worker  # Перезапустить только telegram-worker"
echo ""

