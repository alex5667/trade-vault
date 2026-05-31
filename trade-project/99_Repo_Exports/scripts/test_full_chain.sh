#!/bin/bash
# Complete chain test - Signal Generator → MT5 execution

echo "╔═══════════════════════════════════════════════════════════╗"
echo "║           🧪 ПОЛНЫЙ ТЕСТ ЦЕПОЧКИ СИГНАЛОВ                ║"
echo "╚═══════════════════════════════════════════════════════════╝"
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Step 1: Check services
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}Шаг 1: Проверка сервисов${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo ""

echo -n "go-gateway (8090)... "
if curl -s http://127.0.0.1:8090/healthz > /dev/null 2>&1; then
    echo -e "${GREEN}✅ OK${NC}"
else
    echo -e "${RED}❌ FAILED${NC}"
    echo -e "${RED}Запустите: docker compose up -d go-gateway${NC}"
    exit 1
fi

echo -n "py-obi-service (8088)... "
if curl -s http://127.0.0.1:8088/healthz > /dev/null 2>&1; then
    echo -e "${GREEN}✅ OK${NC}"
else
    echo -e "${RED}❌ FAILED${NC}"
    echo -e "${RED}Запустите: docker compose up -d py-obi-service${NC}"
    exit 1
fi

echo ""

# Step 2: Send test signal
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}Шаг 2: Отправка тестового сигнала${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo ""

# Get current price approximation
CURRENT_PRICE=2763.50

# Calculate SL/TP based on ATR (example: ATR = 3.20)
ATR=3.20
SL_DISTANCE=$(echo "$ATR * 1.5" | bc)
TP1_DISTANCE=$(echo "$ATR * 2.0" | bc)
TP2_DISTANCE=$(echo "$ATR * 3.0" | bc)
TP3_DISTANCE=$(echo "$ATR * 4.0" | bc)

# LONG signal
SL=$(echo "$CURRENT_PRICE - $SL_DISTANCE" | bc)
TP1=$(echo "$CURRENT_PRICE + $TP1_DISTANCE" | bc)
TP2=$(echo "$CURRENT_PRICE + $TP2_DISTANCE" | bc)
TP3=$(echo "$CURRENT_PRICE + $TP3_DISTANCE" | bc)

echo -e "${YELLOW}Сигнал:${NC}"
echo "  Symbol: XAUUSD"
echo "  Side: LONG"
echo "  Lot: 0.01"
echo "  Entry: MARKET (~$CURRENT_PRICE)"
echo "  SL: $SL"
echo "  TP1: $TP1"
echo "  TP2: $TP2"
echo "  TP3: $TP3"
echo ""

SIGNAL_JSON=$(cat <<EOF
{
  "sid": "test-full-chain-$(date +%s)",
  "symbol": "XAUUSD",
  "side": "LONG",
  "lot": 0.01,
  "sl": $SL,
  "tp_levels": [$TP1, $TP2, $TP3]
}
EOF
)

echo "Отправляем в go-gateway..."
RESPONSE=$(curl -s -X POST http://127.0.0.1:8090/orders/enqueue \
  -H "Content-Type: application/json" \
  -d "$SIGNAL_JSON")

echo "Ответ: $RESPONSE"
echo ""

if echo "$RESPONSE" | grep -q "queued"; then
    echo -e "${GREEN}✅ Сигнал принят в очередь!${NC}"
    SID=$(echo "$SIGNAL_JSON" | grep -o '"sid": "[^"]*"' | cut -d'"' -f4)
    echo -e "${YELLOW}SID: $SID${NC}"
else
    echo -e "${RED}❌ Ошибка отправки сигнала${NC}"
    exit 1
fi

echo ""

# Step 3: Check Telegram notification
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}Шаг 3: Telegram уведомление${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${YELLOW}📱 ПРОВЕРЬТЕ TELEGRAM БОТ!${NC}"
echo ""
echo "Вы должны получить сообщение:"
echo "  🔔 Новый сигнал XAUUSD"
echo "  📈 LONG 0.01 lot"
echo "  SID: $SID"
echo "  🛑 SL: $SL"
echo "  🎯 TP1: $TP1"
echo "  🎯 TP2: $TP2"
echo "  🎯 TP3: $TP3"
echo ""
echo -e "${YELLOW}Получили уведомление? [y/n]${NC}"
read -r telegram_ok

if [ "$telegram_ok" != "y" ]; then
    echo -e "${RED}⚠️  Проблема с Telegram!${NC}"
    echo "Проверьте логи: docker logs scanner-go-gateway --tail 20"
    exit 1
fi

echo ""

# Step 4: Check queue
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}Шаг 4: Проверка очереди${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo ""

echo "Проверяем очередь ордеров..."
QUEUE_RESPONSE=$(curl -s http://127.0.0.1:8090/orders/poll?symbol=XAUUSD)

if [ -n "$QUEUE_RESPONSE" ]; then
    echo -e "${GREEN}✅ Ордер в очереди!${NC}"
    echo "$QUEUE_RESPONSE" | python3 -m json.tool
else
    echo -e "${YELLOW}⚠️  Очередь пустая (возможно, уже обработан)${NC}"
fi

echo ""

# Step 5: MT5 OrderExecutor
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}Шаг 5: MT5 OrderExecutor${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo ""

echo -e "${YELLOW}Проверьте MT5 Terminal:${NC}"
echo ""
echo "1. OrderExecutor должен быть прикреплён к графику XAUUSD"
echo "2. В закладке 'Experts' должны появиться логи:"
echo "   - '📡 Polling now...'"
echo "   - '📥 Received order: ...'"
echo "   - '✅ Order opened successfully!'"
echo ""
echo "3. В закладке 'Trade' должна появиться новая позиция"
echo ""
echo -e "${YELLOW}OrderExecutor работает и открыл позицию? [y/n]${NC}"
read -r mt5_ok

if [ "$mt5_ok" != "y" ]; then
    echo -e "${RED}⚠️  Проблема с MT5!${NC}"
    echo ""
    echo "Troubleshooting:"
    echo "1. Проверьте что OrderExecutor прикреплён к графику"
    echo "2. Включите 'Allow Algo Trading' (зелёная кнопка в MT5)"
    echo "3. Проверьте WebRequest настройки (http://127.0.0.1:8090)"
    echo "4. Смотрите логи в Experts tab"
    exit 1
fi

echo ""

# Step 6: Final confirmation
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}Шаг 6: Подтверждение исполнения${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${YELLOW}📱 ПРОВЕРЬТЕ TELEGRAM БОТ ЕЩЁ РАЗ!${NC}"
echo ""
echo "Вы должны получить второе сообщение:"
echo "  ✅ Ордер открыт!"
echo "  SID: $SID"
echo "  Order: #XXXXXX"
echo "  💰 Entry: 2763.XX"
echo "  📊 Volume: 0.01 lot"
echo "  🛑 SL: $SL"
echo "  🎯 TP: ..."
echo ""
echo -e "${YELLOW}Получили подтверждение исполнения? [y/n]${NC}"
read -r confirm_ok

echo ""

if [ "$confirm_ok" = "y" ]; then
    echo -e "${GREEN}╔═══════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║                                                           ║${NC}"
    echo -e "${GREEN}║           ✅ ВСЯ ЦЕПОЧКА РАБОТАЕТ ИДЕАЛЬНО! ✅            ║${NC}"
    echo -e "${GREEN}║                                                           ║${NC}"
    echo -e "${GREEN}╚═══════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "${GREEN}Система полностью функциональна!${NC}"
    echo ""
    echo "Протестированная цепочка:"
    echo "  ✅ Signal Generator → Сигнал сгенерирован"
    echo "  ✅ go-gateway → Принят в очередь"
    echo "  ✅ Telegram → Уведомление отправлено"
    echo "  ✅ OrderExecutor → Ордер исполнен"
    echo "  ✅ Telegram → Подтверждение получено"
    echo ""
    echo -e "${YELLOW}Теперь можете запустить автоматический Signal Generator:${NC}"
    echo "  ./start_signal_generator.sh"
    echo ""
else
    echo -e "${RED}⚠️  Подтверждение не получено${NC}"
    echo ""
    echo "Проверьте:"
    echo "1. Логи go-gateway: docker logs scanner-go-gateway --tail 30"
    echo "2. MT5 Experts tab - должен быть лог подтверждения"
    echo "3. Telegram bot - проверьте настройки уведомлений"
fi

