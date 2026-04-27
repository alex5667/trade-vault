#!/bin/bash
# Start Go+Python Stack (scanner-gw + obi-service)

cd "$(dirname "$0")"

echo "🚀 Starting Go+Python Stack..."
echo ""

# Check credentials
if [ -z "$TELEGRAM_BOT_TOKEN" ]; then
    echo "⚠️  TELEGRAM_BOT_TOKEN not set!"
    echo "Set it with: export TELEGRAM_BOT_TOKEN=\"your_token\""
    echo "Get token from @BotFather in Telegram"
    echo ""
    echo "Continuing without Telegram notifications..."
    echo ""
fi

if [ -z "$TELEGRAM_CHAT_ID" ]; then
    echo "⚠️  TELEGRAM_CHAT_ID not set!"
    echo "Set it with: export TELEGRAM_CHAT_ID=\"your_chat_id\""
    echo "Get ID from @userinfobot in Telegram"
    echo ""
    echo "Continuing without Telegram notifications..."
    echo ""
fi

# Export environment
export PORT=8088
export OBI_HOST=http://127.0.0.1:8090
export OBI_WINDOW_LEVELS=5
export OBI_THRESHOLD=0.25
export OBI_SUSTAIN_MS=1200
export RING_SECONDS=600
export NOTIFY_URL=http://127.0.0.1:8088/notify

echo "📊 Starting OBI Service (Python, port 8090)..."
cd py-obi
if [ ! -d ".venv" ]; then
    echo "❌ Virtual environment not found in py-obi/"
    echo "Run: cd py-obi && python3 -m venv .venv && source .venv/bin/activate && pip install fastapi uvicorn pydantic numpy matplotlib requests"
    exit 1
fi

source .venv/bin/activate
python book_obi_service.py > /tmp/obi_service.log 2>&1 &
OBI_PID=$!
echo "   PID: $OBI_PID"
sleep 2
cd ..

echo "🚪 Starting Go Gateway (port 8088)..."
cd go-gateway
if [ ! -f "scanner-gw" ]; then
    echo "❌ scanner-gw binary not found"
    echo "Run: cd go-gateway && go build -o scanner-gw main.go"
    exit 1
fi

./scanner-gw > /tmp/scanner_gw.log 2>&1 &
GW_PID=$!
echo "   PID: $GW_PID"
sleep 2
cd ..

echo ""
echo "✅ All services started!"
echo ""
echo "Services:"
echo "  OBI Service:     http://127.0.0.1:8090 (PID: $OBI_PID)"
echo "  Go Gateway:      http://127.0.0.1:8088 (PID: $GW_PID)"
echo ""
echo "Check status:"
echo "  curl http://127.0.0.1:8090/healthz"
echo "  curl http://127.0.0.1:8088/healthz"
echo ""
echo "Test OBI PNG:"
echo "  curl 'http://127.0.0.1:8090/render/obi.png?symbol=XAUUSD' -o /tmp/obi_test.png"
echo ""
echo "Test order enqueue:"
echo "  curl -X POST http://127.0.0.1:8088/orders/enqueue -H 'Content-Type: application/json' -d '{\"sid\":\"test-001\",\"symbol\":\"XAUUSD\",\"side\":\"LONG\",\"lot\":0.05}'"
echo ""
echo "View logs:"
echo "  tail -f /tmp/obi_service.log"
echo "  tail -f /tmp/scanner_gw.log"
echo ""
echo "Stop services:"
echo "  kill $OBI_PID $GW_PID"
echo ""

# Save PIDs
echo "$OBI_PID" > /tmp/obi_service.pid
echo "$GW_PID" > /tmp/scanner_gw.pid
EOF

chmod +x start_go_stack.sh
echo "✅ Created start_go_stack.sh"

