#!/bin/bash
# Start Complete Telegram Stack (OBI + Notify + Bot)

cd "$(dirname "$0")/python-worker"

# Check venv
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found!"
    echo "Run: python3 -m venv venv"
    exit 1
fi

# Activate venv
source venv/bin/activate

echo "🚀 Starting Complete Telegram Stack..."
echo ""

# Check credentials
if [ -z "$BOT_TOKEN" ]; then
    echo "⚠️  BOT_TOKEN not set!"
    echo "Set it with: export BOT_TOKEN=\"your_token\""
    echo ""
    echo "Get token from @BotFather in Telegram"
    exit 1
fi

if [ -z "$CHAT_ID" ]; then
    echo "⚠️  CHAT_ID not set!"
    echo "Set it with: export CHAT_ID=\"your_chat_id\""
    echo ""
    echo "Get ID from @userinfobot in Telegram"
    exit 1
fi

# Export environment
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
export OBI_SUSTAIN_MS=${OBI_SUSTAIN_MS:-1200}
export OBI_WINDOW_LEVELS=${OBI_WINDOW_LEVELS:-5}
export OBI_THRESHOLD=${OBI_THRESHOLD:-0.25}
export RING_SECONDS=${RING_SECONDS:-600}
export BOOK_ANALYTICS_PORT=8090
export NOTIFY_BRIDGE_PORT=8089
export NOTIFY_URL=http://127.0.0.1:8089/notify
export OBI_HOST=http://127.0.0.1:8090
export TITLE_PREFIX="${TITLE_PREFIX:-[XAUUSD]}"
export DEFAULT_SYMBOL="${DEFAULT_SYMBOL:-XAUUSD}"
export REDIS_URL=${REDIS_URL:-redis://127.0.0.1:6379/0}

echo "📊 Starting Book Analytics Service (port 8090)..."
python -m services.book_analytics_service > /tmp/book_analytics.log 2>&1 &
BOOK_PID=$!
echo "   PID: $BOOK_PID"
sleep 2

echo "📨 Starting Notify Bridge (port 8089)..."
python -m services.notify_bridge > /tmp/notify_bridge.log 2>&1 &
NOTIFY_PID=$!
echo "   PID: $NOTIFY_PID"
sleep 2

echo "🤖 Starting Telegram Labeler Bot..."
python -m tools.telegram_labeler > /tmp/telegram_labeler.log 2>&1 &
BOT_PID=$!
echo "   PID: $BOT_PID"
sleep 2

echo ""
echo "✅ All services started!"
echo ""
echo "Services:"
echo "  Book Analytics:   http://127.0.0.1:8090 (PID: $BOOK_PID)"
echo "  Notify Bridge:    http://127.0.0.1:8089 (PID: $NOTIFY_PID)"
echo "  Telegram Bot:     Polling (PID: $BOT_PID)"
echo ""
echo "Check status:"
echo "  curl http://127.0.0.1:8090/healthz"
echo "  curl http://127.0.0.1:8089/healthz"
echo ""
echo "Test in Telegram:"
echo "  /start"
echo "  /obi XAUUSD"
echo "  /depth XAUUSD"
echo "  /events XAUUSD"
echo "  /status XAUUSD"
echo ""
echo "View logs:"
echo "  tail -f /tmp/book_analytics.log"
echo "  tail -f /tmp/notify_bridge.log"
echo "  tail -f /tmp/telegram_labeler.log"
echo ""
echo "Stop services:"
echo "  kill $BOOK_PID $NOTIFY_PID $BOT_PID"
echo ""

# Save PIDs
echo "$BOOK_PID" > /tmp/book_analytics.pid
echo "$NOTIFY_PID" > /tmp/notify_bridge.pid
echo "$BOT_PID" > /tmp/telegram_labeler.pid

echo "PIDs saved to /tmp/*.pid"
echo ""
