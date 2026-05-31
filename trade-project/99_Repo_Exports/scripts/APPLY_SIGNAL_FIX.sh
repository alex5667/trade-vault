#!/bin/bash

echo "======================================"
echo "Применение исправления формата сигналов"
echo "======================================"
echo ""

echo "📝 Изменения:"
echo "- Обновлен aggregated_signal_hub_v2.py"
echo "- Теперь используется XAUUSDSignalFormatter"
echo "- Сигналы будут содержать: время, entry, source"
echo ""

echo "🔄 Перезапуск контейнера aggregated-hub..."
docker-compose restart aggregated-hub

echo ""
echo "⏳ Ждем 5 секунд для старта контейнера..."
sleep 5

echo ""
echo "📋 Показываем последние 50 строк логов..."
docker-compose logs --tail=50 aggregated-hub

echo ""
echo "✅ Готово!"
echo ""
echo "📊 Проверьте сигналы в Telegram - они должны содержать:"
echo "  ✓ Время сигнала (🕐)"
echo "  ✓ Точку входа (@ price)"
echo "  ✓ Источник (🔧 Source: AggregatedHub-V2)"
echo "  ✓ Risk/Reward ratio для TP"
echo ""
echo "📖 Подробности: см. SIGNAL_FORMAT_FIX_SUMMARY.md"
echo ""
