#!/bin/bash
# -*- coding: utf-8 -*-
# Скрипт для инициализации symbol specs в Redis

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== Инициализация Symbol Specs в Redis ==="
echo ""

# Проверка Redis
if ! redis-cli ping > /dev/null 2>&1; then
    echo "❌ Redis не доступен. Запустите Redis и попробуйте снова."
    exit 1
fi

echo "✓ Redis доступен"
echo ""

# XAUUSD specs
echo "Установка specs для XAUUSD..."
python3 "$PROJECT_DIR/utils/push_specs.py" XAUUSD 0.1 1.0 0.01 10.0 0.01

# Проверка
XAUUSD_SPECS=$(redis-cli GET "symbol_specs:XAUUSD" 2>/dev/null || echo "")
if [ -n "$XAUUSD_SPECS" ]; then
    echo "✓ XAUUSD specs установлены"
    echo "  $XAUUSD_SPECS"
else
    echo "❌ Ошибка установки XAUUSD specs"
    exit 1
fi

echo ""
echo "=== Specs успешно инициализированы ==="
echo ""
echo "Проверьте командой:"
echo "  redis-cli GET \"symbol_specs:XAUUSD\""
echo ""

