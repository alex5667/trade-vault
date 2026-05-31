#!/bin/bash
# Stop Go+Python Stack

echo "🛑 Stopping Go+Python Stack..."
echo ""

# Kill by PID
if [ -f /tmp/obi_service.pid ]; then
    PID=$(cat /tmp/obi_service.pid)
    if kill -0 $PID 2>/dev/null; then
        kill $PID && echo "✅ Stopped OBI Service (PID: $PID)"
    fi
    rm -f /tmp/obi_service.pid
fi

if [ -f /tmp/scanner_gw.pid ]; then
    PID=$(cat /tmp/scanner_gw.pid)
    if kill -0 $PID 2>/dev/null; then
        kill $PID && echo "✅ Stopped Go Gateway (PID: $PID)"
    fi
    rm -f /tmp/scanner_gw.pid
fi

# Fallback
pkill -f "book_obi_service" 2>/dev/null && echo "✅ Killed remaining OBI processes"
pkill -f "scanner-gw" 2>/dev/null && echo "✅ Killed remaining Go processes"

echo ""
echo "🛑 All services stopped"
echo ""
