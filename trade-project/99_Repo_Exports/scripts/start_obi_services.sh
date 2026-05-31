#!/bin/bash
# Start OBI Event & PNG Services

cd "$(dirname "$0")/python-worker"

echo "🚀 Starting OBI Event & PNG Services..."
echo ""

# Export environment
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
export OBI_SUSTAIN_MS=1200
export NOTIFY_URL=http://127.0.0.1:8089/notify
export OBI_WINDOW_LEVELS=5
export OBI_THRESHOLD=0.25
export RING_SECONDS=600
export BOOK_ANALYTICS_PORT=8090
export NOTIFY_RECEIVER_PORT=8089
export NOTIFY_STREAM=notify:telegram
export REDIS_URL=redis://127.0.0.1:6379/0

echo "📊 Starting Book Analytics Service (port 8090)..."
python3 -m services.book_analytics_service > /tmp/book_analytics.log 2>&1 &
BOOK_PID=$!
echo "   PID: $BOOK_PID"
sleep 2

echo "📨 Starting Notification Receiver (port 8089)..."
python3 -m services.notify_receiver > /tmp/notify_receiver.log 2>&1 &
NOTIFY_PID=$!
echo "   PID: $NOTIFY_PID"
sleep 2

echo ""
echo "✅ Services started!"
echo ""
echo "Check status:"
echo "  curl http://127.0.0.1:8090/healthz"
echo "  curl http://127.0.0.1:8089/healthz"
echo ""
echo "Test PNG rendering:"
echo "  curl 'http://127.0.0.1:8090/render/obi.png?symbol=XAUUSD' -o /tmp/obi_test.png"
echo "  curl 'http://127.0.0.1:8090/render/depth.png?symbol=XAUUSD' -o /tmp/depth_test.png"
echo ""
echo "View logs:"
echo "  tail -f /tmp/book_analytics.log"
echo "  tail -f /tmp/notify_receiver.log"
echo ""
echo "Stop services:"
echo "  kill $BOOK_PID $NOTIFY_PID"
echo ""
