#!/bin/bash
# Скрипт для мониторинга сигналов в реальном времени

echo "🔴 LIVE МОНИТОРИНГ СИГНАЛОВ"
echo "==========================="
echo "Нажмите Ctrl+C для выхода"
echo ""

# Счетчики
SIGNAL_COUNT=0
ERROR_COUNT=0

# Мониторим логи notify-worker
docker logs -f scanner-notify-worker 2>&1 | while read -r line; do
    # Детектируем новые сигналы
    if echo "$line" | grep -q "📬 ПРОЧИТАНО ИЗ REDIS"; then
        SIGNAL_COUNT=$((SIGNAL_COUNT + 1))
        echo ""
        echo "═══════════════════════════════════════"
        echo "📥 НОВЫЙ СИГНАЛ #$SIGNAL_COUNT"
        echo "═══════════════════════════════════════"
    fi
    
    # Показываем детали сигнала
    if echo "$line" | grep -qE "(Symbol:|Entry:|Direction:)"; then
        echo "$line"
    fi
    
    # Показываем успешные отправки
    if echo "$line" | grep -q "✅.*отправлен"; then
        echo "$(date '+%H:%M:%S') - ✅ $line"
    fi
    
    # Показываем ошибки
    if echo "$line" | grep -q "❌"; then
        ERROR_COUNT=$((ERROR_COUNT + 1))
        echo "$(date '+%H:%M:%S') - ❌ ОШИБКА: $line"
    fi
    
    # Показываем статистику каждые 10 сигналов
    if echo "$line" | grep -q "notify_worker stats:"; then
        echo ""
        echo "📊 СТАТИСТИКА: $line"
        echo ""
    fi
done

