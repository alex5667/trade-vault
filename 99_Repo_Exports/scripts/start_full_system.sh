#!/bin/bash
# -*- coding: utf-8 -*-
# Запуск полной системы для генерации сигналов

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║          Запуск полной системы Pro Hub                         ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Проверка Redis
echo "1️⃣ Проверка Redis..."
if ! redis-cli ping > /dev/null 2>&1; then
    echo "❌ Redis не запущен!"
    echo "   Запустите: redis-server"
    exit 1
fi
echo "  ✅ Redis работает"
echo ""

# Создание директории для меток
LABELS_DIR="${LABEL_PARQUET_DIR:-/data/labels}"
echo "2️⃣ Создание директории для меток..."
if [ ! -d "$LABELS_DIR" ]; then
    echo "  📁 Создаю: $LABELS_DIR"
    sudo mkdir -p "$LABELS_DIR" 2>/dev/null || mkdir -p "$LABELS_DIR"
    sudo chmod 777 "$LABELS_DIR" 2>/dev/null || chmod 777 "$LABELS_DIR"
fi
echo "  ✅ Директория готова: $LABELS_DIR"
echo ""

# Меню выбора режима
echo "Выберите режим запуска:"
echo ""
echo "  1) Полная система (signal-generator + hub_pro)"
echo "  2) Только Hub Pro (требуется внешний market data)"
echo "  3) Hub Pro + симуляция принтов"
echo "  4) Диагностика системы"
echo ""

read -p "Выбор (1-4): " choice

case $choice in
    1)
        echo ""
        echo "🚀 Запуск полной системы..."
        echo ""
        echo "Терминал 1: Signal Generator (market data)"
        echo "Терминал 2: Hub Pro (генератор сигналов)"
        echo "Терминал 3: Симуляция принтов (опционально)"
        echo ""
        
        # Запуск signal generator в фоне
        echo "📊 Запуск Signal Generator..."
        cd "$PROJECT_DIR/signal-generator"
        nohup python3 signal_generator.py > /tmp/signal_generator.log 2>&1 &
        SG_PID=$!
        echo "  ✅ Signal Generator запущен (PID: $SG_PID)"
        echo "     Логи: /tmp/signal_generator.log"
        sleep 3
        
        # Проверка тиков
        echo ""
        echo "⏱️  Ждём появления market data (5 сек)..."
        sleep 5
        
        TICKS=$(redis-cli EXISTS tick:XAUUSD)
        if [ "$TICKS" = "1" ]; then
            echo "  ✅ Market data появились!"
        else
            echo "  ⚠️  Market data ещё не появились, но продолжаем..."
        fi
        
        # Запуск Hub Pro
        echo ""
        echo "🎯 Запуск Hub Pro..."
        cd "$PROJECT_DIR"
        python3 -m hub.aggregated_signal_hub_pro
        ;;
        
    2)
        echo ""
        echo "🎯 Запуск Hub Pro..."
        echo ""
        echo "⚠️  ВНИМАНИЕ: Требуется внешний источник market data!"
        echo "   Убедитесь что запущен signal-generator или другой фид."
        echo ""
        sleep 2
        
        cd "$PROJECT_DIR"
        python3 -m hub.aggregated_signal_hub_pro
        ;;
        
    3)
        echo ""
        echo "🎯 Запуск Hub Pro + симуляция..."
        echo ""
        
        # Запуск симуляции в фоне
        echo "📊 Запуск симуляции market data..."
        cd "$PROJECT_DIR"
        
        # Симулируем тики в Redis
        python3 << 'PYTHON_EOF' &
import redis, time, random, json
r = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
base = 2650.0
print("Симуляция market data запущена...")
while True:
    price = base + random.uniform(-2, 2)
    bid = round(price - 0.1, 2)
    ask = round(price + 0.1, 2)
    
    # Тики
    r.hset("tick:XAUUSD", mapping={
        "bid": str(bid),
        "ask": str(ask),
        "last": str(price),
        "ts": str(int(time.time() * 1000))
    })
    
    # ATR
    r.set("atr:XAUUSD", str(round(random.uniform(4.0, 6.0), 2)))
    
    # DOM (простой)
    dom = []
    for i in range(5):
        dom.append({"side": "bid", "price": bid - i*0.1, "volume": random.randint(10, 100)})
        dom.append({"side": "ask", "price": ask + i*0.1, "volume": random.randint(10, 100)})
    r.set("dom:XAUUSD", json.dumps(dom))
    
    time.sleep(0.5)
PYTHON_EOF
        
        SIM_PID=$!
        echo "  ✅ Симуляция запущена (PID: $SIM_PID)"
        
        sleep 2
        
        # Запуск симуляции принтов
        echo ""
        echo "📈 Запуск симуляции принтов..."
        "$SCRIPT_DIR/simulate_trades.py" --duration 3600 --trades-per-sec 5 &
        TRADES_PID=$!
        echo "  ✅ Принты запущены (PID: $TRADES_PID)"
        
        sleep 2
        
        # Запуск Hub Pro
        echo ""
        echo "🎯 Запуск Hub Pro..."
        cd "$PROJECT_DIR"
        python3 -m hub.aggregated_signal_hub_pro
        
        # Cleanup при выходе
        trap "kill $SIM_PID $TRADES_PID 2>/dev/null" EXIT
        ;;
        
    4)
        echo ""
        echo "🔍 Диагностика системы..."
        echo ""
        cd "$PROJECT_DIR"
        python3 "$SCRIPT_DIR/diagnose_hub.py"
        ;;
        
    *)
        echo "❌ Неверный выбор"
        exit 1
        ;;
esac

echo ""
echo "═════════════════════════════════════════════════════════════════"


