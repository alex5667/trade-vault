#!/bin/bash

echo "======================================"
echo "ПРИНУДИТЕЛЬНЫЙ перезапуск"
echo "======================================"
echo ""
echo "⚠️  Этот скрипт принудительно удалит ВСЕ контейнеры и перезапустит систему"
echo ""

# Подтверждение (можно закомментировать для автоматического режима)
read -p "Продолжить? (y/n) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Отменено"
    exit 1
fi

echo ""
echo "🛑 Останавливаем Docker Compose..."
docker compose down 2>/dev/null || true

echo ""
echo "🧹 Принудительно удаляем все контейнеры проекта..."
docker ps -a --filter "name=scanner-" --format "{{.ID}} {{.Names}}" | while read id name; do
    echo "  Удаляем: $name ($id)"
    docker rm -f "$id" 2>/dev/null || true
done

echo ""
echo "🗑️  Удаляем неиспользуемые сети..."
docker network prune -f 2>/dev/null || true

echo ""
echo "✅ Очистка завершена"

echo ""
echo "🔨 Пересборка и запуск..."
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
echo "📋 Проверка логов:"
echo "  docker compose logs -f aggregated-hub"
echo ""

