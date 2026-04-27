#!/bin/bash
# Статус всех сервисов

echo "======================================"
echo "Статус сервисов"
echo "======================================"
echo ""

echo "📊 Статус контейнеров:"
docker compose ps

echo ""
echo "💾 Использование ресурсов:"
docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}" \
    $(docker compose ps -q 2>/dev/null) 2>/dev/null || echo "Нет запущенных контейнеров"

echo ""
echo "📋 Полезные команды:"
echo "  ./restart_with_build.sh    # Полная пересборка и запуск"
echo "  ./quick_restart.sh         # Быстрый перезапуск"
echo "  ./view_logs.sh <service>   # Просмотр логов"
echo "  docker compose down        # Остановить все"
echo ""

