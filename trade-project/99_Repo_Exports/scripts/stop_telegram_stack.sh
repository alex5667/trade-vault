#!/bin/bash
# Stop Telegram Stack Services

echo "🛑 Stopping Telegram Stack services..."
echo ""

# Kill by PID files
if [ -f /tmp/book_analytics.pid ]; then
    PID=$(cat /tmp/book_analytics.pid)
    if kill -0 $PID 2>/dev/null; then
        kill $PID && echo "✅ Stopped book_analytics (PID: $PID)"
    fi
    rm -f /tmp/book_analytics.pid
fi

if [ -f /tmp/notify_bridge.pid ]; then
    PID=$(cat /tmp/notify_bridge.pid)
    if kill -0 $PID 2>/dev/null; then
        kill $PID && echo "✅ Stopped notify_bridge (PID: $PID)"
    fi
    rm -f /tmp/notify_bridge.pid
fi

if [ -f /tmp/telegram_labeler.pid ]; then
    PID=$(cat /tmp/telegram_labeler.pid)
    if kill -0 $PID 2>/dev/null; then
        kill $PID && echo "✅ Stopped telegram_labeler (PID: $PID)"
    fi
    rm -f /tmp/telegram_labeler.pid
fi

# Fallback: killall by name
pkill -f "book_analytics_service" 2>/dev/null && echo "✅ Killed remaining book_analytics processes"
pkill -f "notify_bridge" 2>/dev/null && echo "✅ Killed remaining notify_bridge processes"
pkill -f "telegram_labeler" 2>/dev/null && echo "✅ Killed remaining telegram_labeler processes"

echo ""
echo "🛑 All services stopped"
echo ""
