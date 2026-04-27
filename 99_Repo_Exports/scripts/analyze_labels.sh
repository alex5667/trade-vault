#!/bin/bash
# -*- coding: utf-8 -*-
# Скрипт для быстрого анализа меток

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== Анализ меток сигналов ==="
echo ""

# Проверка наличия меток
LABELS_DIR="${LABEL_PARQUET_DIR:-/data/labels}"

if [ ! -d "$LABELS_DIR" ]; then
    echo "❌ Директория с метками не найдена: $LABELS_DIR"
    echo "   Установите переменную LABEL_PARQUET_DIR или создайте /data/labels"
    exit 1
fi

echo "✓ Директория меток: $LABELS_DIR"
echo ""

# Меню
echo "Выберите действие:"
echo "  1) Статистика за последние 24 часа"
echo "  2) Статистика за последнюю неделю"
echo "  3) Полная статистика по XAUUSD"
echo "  4) Примеры сигналов"
echo "  5) Экспорт отчёта"
echo ""

read -p "Выбор (1-5): " choice

case $choice in
    1)
        echo "📊 Статистика за последние 24 часа..."
        python3 "$PROJECT_DIR/analysis/label_analyzer.py" \
            --labels-dir "$LABELS_DIR" \
            --recent-hours 24
        ;;
    2)
        echo "📊 Статистика за последнюю неделю..."
        START_DATE=$(date -d "7 days ago" +%Y-%m-%d)
        python3 "$PROJECT_DIR/analysis/label_analyzer.py" \
            --labels-dir "$LABELS_DIR" \
            --start-date "$START_DATE"
        ;;
    3)
        echo "📊 Полная статистика по XAUUSD..."
        python3 "$PROJECT_DIR/analysis/label_analyzer.py" \
            --labels-dir "$LABELS_DIR" \
            --symbol XAUUSD
        ;;
    4)
        echo "📋 Примеры сигналов (последние 5)..."
        python3 "$PROJECT_DIR/analysis/label_analyzer.py" \
            --labels-dir "$LABELS_DIR" \
            --recent-hours 24 \
            --show-sample
        ;;
    5)
        REPORT_FILE="/tmp/signal_report_$(date +%Y%m%d_%H%M%S).json"
        echo "📄 Экспорт отчёта в $REPORT_FILE..."
        python3 "$PROJECT_DIR/analysis/label_analyzer.py" \
            --labels-dir "$LABELS_DIR" \
            --symbol XAUUSD \
            --export "$REPORT_FILE"
        echo ""
        echo "✅ Отчёт сохранён: $REPORT_FILE"
        ;;
    *)
        echo "❌ Неверный выбор"
        exit 1
        ;;
esac

echo ""
echo "=== Анализ завершён ==="

