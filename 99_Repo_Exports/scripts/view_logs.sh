#!/bin/bash
# Просмотр логов сервисов

echo "======================================"
echo "Просмотр логов"
echo "======================================"
echo ""

if [ -z "$1" ]; then
    echo "📋 Доступные сервисы:"
    docker compose ps --format "table {{.Service}}\t{{.Status}}"
    echo ""
    echo "Использование:"
    echo "  ./view_logs.sh aggregated-hub       # Логи aggregated-hub (live)"
    echo "  ./view_logs.sh telegram-worker      # Логи telegram-worker (live)"
    echo "  ./view_logs.sh aggregated-hub 100   # Последние 100 строк"
    echo "  ./view_logs.sh all                  # Все логи (live)"
    echo ""
    exit 0
fi

SERVICE="$1"
LINES="${2:-}"

if [ "$SERVICE" = "all" ]; then
    if [ -n "$LINES" ]; then
        echo "📋 Показываем последние $LINES строк всех логов..."
        docker compose logs --tail="$LINES"
    else
        echo "📋 Показываем логи всех сервисов (Ctrl+C для выхода)..."
        docker compose logs -f
    fi
else
    if [ -n "$LINES" ]; then
        echo "📋 Показываем последние $LINES строк логов $SERVICE..."
        docker compose logs --tail="$LINES" "$SERVICE"
    else
        echo "📋 Показываем логи $SERVICE (Ctrl+C для выхода)..."
        docker compose logs -f "$SERVICE"
    fi
fi

