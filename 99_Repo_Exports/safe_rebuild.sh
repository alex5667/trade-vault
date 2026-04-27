#!/bin/bash
set -e

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║                                                                  ║"
echo "║         🔨 БЕЗОПАСНАЯ ПЕРЕСБОРКА (Legacy Builder)                ║"
echo "║                                                                  ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

# Отключаем buildkit для обхода паники
export DOCKER_BUILDKIT=0
export COMPOSE_DOCKER_CLI_BUILD=0

echo "✅ Legacy builder активирован (buildkit отключен)"
echo ""

# Останавливаем контейнеры
echo "🛑 Остановка контейнеров..."
docker-compose down
echo ""

# Опционально: очистка cache (раскомментируйте если нужно)
# echo "🧹 Очистка Docker cache..."
# docker builder prune -af
# echo ""

# Пересборка
echo "🔨 Пересборка образов..."
docker-compose build --progress=plain

echo ""
echo "✅ Сборка завершена!"
echo ""

# Запуск
echo "🚀 Запуск сервисов..."
docker-compose up -d

echo ""
echo "✅ Сервисы запущены!"
echo ""

# Статус
echo "📊 Статус контейнеров:"
docker-compose ps

echo ""
echo "═══════════════════════════════════════════════════════════════════"
echo "✅ Готово! Используйте 'make logs' для просмотра логов"
echo "═══════════════════════════════════════════════════════════════════"
