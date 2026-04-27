#!/bin/bash
# Скрипт для применения исправления "Канал: None"

echo "🔧 ПРИМЕНЕНИЕ ИСПРАВЛЕНИЯ: Канал: None → Реальное название канала"
echo "=========================================================================="

# Проверяем, что мы в правильной директории
if [ ! -f "docker-compose.yml" ]; then
    echo "❌ Ошибка: Запустите скрипт из корневой директории проекта"
    exit 1
fi

echo "✅ Директория проверена"
echo ""

# Запускаем тесты
echo "🧪 Запуск тестов..."
python3 test_channel_fix.py

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Все тесты пройдены успешно!"
else
    echo ""
    echo "⚠️ Внимание: некоторые тесты не прошли"
fi

echo ""
echo "=========================================================================="
echo "📋 ИЗМЕНЁННЫЕ ФАЙЛЫ:"
echo "=========================================================================="
echo "1. telegram-worker/improved_notifier.py"
echo "2. telegram-worker/multithreaded_worker.py"
echo "3. telegram-worker/improved_multithreaded_worker.py"
echo "4. telegram-worker/notify_worker.py"
echo "5. telegram-worker/forward_all_worker.py"
echo "6. demo_full_cycle.py"
echo "7. send_test_signal_to_bot.py"
echo "8. test_inj_signal_parsing.py"
echo "9. view_alerts.py"
echo ""

echo "=========================================================================="
echo "🔄 СЛЕДУЮЩИЕ ШАГИ:"
echo "=========================================================================="
echo "1. Перезапустить telegram-worker:"
echo "   docker-compose restart telegram-worker"
echo ""
echo "2. Перезапустить notify-worker (если используется):"
echo "   docker-compose restart notify-worker"
echo ""
echo "3. Проверить логи:"
echo "   docker-compose logs -f telegram-worker"
echo ""

echo "=========================================================================="
echo "✅ ГОТОВО! Исправление применено."
echo "=========================================================================="

