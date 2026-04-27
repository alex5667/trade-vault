#!/bin/bash
# -*- coding: utf-8 -*-
# Быстрый запуск Hub Pro с market data

set -e

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║        Запуск Hub Pro - Полная система                         ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Проверка Redis
if ! redis-cli ping > /dev/null 2>&1; then
    echo "❌ Redis не запущен!"
    echo "   Запустите: redis-server"
    exit 1
fi
echo "✅ Redis работает"
echo ""

# Создание директории для меток
LABELS_DIR="${LABEL_PARQUET_DIR:-/data/labels}"
if [ ! -d "$LABELS_DIR" ]; then
    echo "📁 Создаю директорию: $LABELS_DIR"
    sudo mkdir -p "$LABELS_DIR" 2>/dev/null || mkdir -p "$LABELS_DIR"
    sudo chmod 777 "$LABELS_DIR" 2>/dev/null || chmod 777 "$LABELS_DIR"
fi
echo "✅ Директория меток готова: $LABELS_DIR"
echo ""

# Запуск Market Data Generator в фоне
echo "📊 Запуск Market Data Generator..."
nohup python3 services/simple_market_data_generator.py > /tmp/market_data_gen.log 2>&1 &
MD_PID=$!
echo "  ✅ Generator запущен (PID: $MD_PID)"
echo "     Логи: /tmp/market_data_gen.log"
echo ""

# Ждём появления данных
echo "⏱️  Ждём появления market data (3 сек)..."
sleep 3

# Проверка тиков
TICK=$(redis-cli HGET tick:XAUUSD bid 2>/dev/null || echo "")
ATR=$(redis-cli GET atr:XAUUSD 2>/dev/null || echo "")

if [ -n "$TICK" ]; then
    echo "  ✅ Тики: bid=$TICK"
fi

if [ -n "$ATR" ]; then
    echo "  ✅ ATR: $ATR"
fi

echo ""

# Опционально: запуск симуляции принтов
echo "💡 Запустить симуляцию принтов? (y/n)"
read -t 5 -n 1 ANSWER || ANSWER="n"
echo ""

if [ "$ANSWER" = "y" ] || [ "$ANSWER" = "Y" ]; then
    echo "📈 Запуск симуляции принтов..."
    nohup ./scripts/simulate_trades.py --duration 3600 --trades-per-sec 5 > /tmp/trades_sim.log 2>&1 &
    TRADES_PID=$!
    echo "  ✅ Симуляция запущена (PID: $TRADES_PID)"
    echo "     Логи: /tmp/trades_sim.log"
    echo ""
    sleep 2
fi

# Запуск Hub Pro
echo "🚀 Запуск Hub Pro..."
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

python3 -m hub.aggregated_signal_hub_pro

# Cleanup при выходе
trap "kill $MD_PID ${TRADES_PID:-} 2>/dev/null; echo ''; echo '⚠️  Остановлено'; echo ''" EXIT


