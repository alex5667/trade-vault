#!/bin/bash

echo "======================================"
echo "Полный перезапуск с пересборкой"
echo "======================================"
echo ""

echo "🛑 Останавливаем все запущенные контейнеры..."
docker compose down

echo ""
echo "🧹 Очистка старых контейнеров (если есть конфликты)..."
# Удаляем возможные "зависшие" контейнеры
docker ps -a --filter "name=scanner-" --format "{{.ID}}" | xargs -r docker rm -f 2>/dev/null || true
echo "✅ Очистка завершена"

echo ""
echo "🧹 Очистка (опционально - закомментировано)..."
# docker compose down -v  # Добавьте -v если нужно удалить volumes
# docker system prune -f  # Очистить неиспользуемые образы

echo ""
echo "🔨 Пересборка образов и запуск контейнеров..."
docker compose up --build -d

echo ""
echo "⏳ Ждем 5 секунд для запуска контейнеров..."
sleep 5

echo ""
echo "📊 Статус контейнеров:"
docker compose ps

echo ""
echo "✅ Готово!"
echo ""
echo "📋 Полезные команды:"
echo "  docker compose logs -f aggregated-hub    # Логи aggregated-hub"
echo "  docker compose logs -f telegram-worker   # Логи telegram-worker"
echo "  docker compose logs -f --tail=100        # Логи всех сервисов (последние 100 строк)"
echo "  docker compose down                      # Остановить все"
echo ""

