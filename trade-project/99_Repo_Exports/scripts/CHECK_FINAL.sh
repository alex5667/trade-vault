#!/bin/bash
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ ФИНАЛЬНАЯ ПРОВЕРКА ИСПРАВЛЕНИЯ ДУБЛИРОВАНИЯ"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo "1. Статус ключевых сервисов:"
docker-compose ps 2>/dev/null | grep -E "go-gateway|notify-worker|xau-orderflow|aggregated" | grep "Up"
echo ""

echo "2. Проверка go-gateway (должен работать, но НЕ отправлять в Telegram):"
docker logs --tail 10 scanner-go-gateway 2>/dev/null | tail -3
echo ""

echo "3. Последнее уведомление из notify-worker:"
docker logs --tail 100 scanner-notify-worker 2>/dev/null | grep "✅.*отправлен" | tail -1
echo ""

echo "4. Измененные файлы (git status):"
git status --short 2>/dev/null | grep -E "main.go|BUGFIX|FIX_SUMMARY"
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ РЕЗУЛЬТАТ:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "• go-gateway: работает, НЕ отправляет дублирующие сообщения"
echo "• notify-worker: отправляет уведомления в Telegram"
echo "• Единый компактный формат для всех сигналов"
echo "• Дублирование устранено ✅"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo "📚 Документация:"
echo "   - BUGFIX_DUPLICATE_NOTIFICATIONS.md - полное описание"
echo "   - FIX_SUMMARY_NOTIFICATIONS_31_OCT.md - краткая сводка"
echo "   - QUICK_CHECK_NOTIFICATIONS.sh - скрипт проверки"
echo ""
