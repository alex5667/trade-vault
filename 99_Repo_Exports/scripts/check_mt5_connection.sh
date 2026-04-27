#!/bin/bash
# Check MT5 ↔ Services connection

echo "╔════════════════════════════════════════════════════════════════════════════╗"
echo "║                                                                            ║"
echo "║              🔍 MT5 CONNECTION CHECK 🔍                                   ║"
echo "║                                                                            ║"
echo "╚════════════════════════════════════════════════════════════════════════════╝"
echo ""

# Test services
echo "1️⃣ Services Health Check"
echo "────────────────────────────────────────────────────────────────────────────"

if curl -sf http://localhost:8088/healthz > /dev/null 2>&1; then
    echo "   ✅ Go Gateway (8088): OK"
else
    echo "   ❌ Go Gateway (8088): FAILED"
    exit 1
fi

if curl -sf http://localhost:8090/healthz > /dev/null 2>&1; then
    echo "   ✅ Python OBI (8090): OK"
else
    echo "   ❌ Python OBI (8090): FAILED"
    exit 1
fi

echo ""
echo "2️⃣ Test POST /book"
echo "────────────────────────────────────────────────────────────────────────────"

RESP=$(curl -sf -X POST http://127.0.0.1:8090/book \
  -H 'Content-Type: application/json' \
  -d '{"ts":1698253123456,"symbol":"XAUUSD","bids":[[1875.5,10.0]],"asks":[[1875.6,8.0]]}' 2>&1)

if echo "$RESP" | grep -q "ok"; then
    echo "   ✅ POST /book: OK"
    echo "   Response: $RESP"
else
    echo "   ❌ POST /book: FAILED"
    echo "   Response: $RESP"
fi

echo ""
echo "3️⃣ Test GET /orders/poll"
echo "────────────────────────────────────────────────────────────────────────────"

HTTP_CODE=$(curl -sf -w "%{http_code}" -o /dev/null http://127.0.0.1:8088/orders/poll?symbol=XAUUSD 2>&1)

if [ "$HTTP_CODE" = "204" ]; then
    echo "   ✅ GET /orders/poll: OK (204 - empty queue)"
elif [ "$HTTP_CODE" = "200" ]; then
    echo "   ✅ GET /orders/poll: OK (200 - has orders)"
else
    echo "   ⚠️  GET /orders/poll: HTTP $HTTP_CODE"
fi

echo ""
echo "4️⃣ Check Recent Logs (last 5 lines)"
echo "────────────────────────────────────────────────────────────────────────────"

echo ""
echo "Python OBI:"
docker-compose logs --tail=5 py-obi-service 2>/dev/null | grep -E "POST /book|healthz" | tail -5 | sed 's/^/   /'

echo ""
echo "Go Gateway:"
docker-compose logs --tail=5 go-gateway 2>/dev/null | grep -E "orders/poll|healthz" | tail -5 | sed 's/^/   /'

echo ""
echo "5️⃣ MT5 Process Check"
echo "────────────────────────────────────────────────────────────────────────────"

if ps aux | grep -v grep | grep -q "terminal64.exe"; then
    echo "   ✅ MT5 is running"
    ps aux | grep -v grep | grep "terminal64.exe" | awk '{print "   PID: "$2}'
else
    echo "   ⚠️  MT5 not detected"
    echo "   Start with: wine ~/.wine/drive_c/Program\ Files/MetaTrader\ 5/terminal64.exe"
fi

echo ""
echo "════════════════════════════════════════════════════════════════════════════"
echo ""
echo "📊 SUMMARY"
echo ""

# Count POST /book in last 100 lines
BOOK_COUNT=$(docker-compose logs --tail=100 py-obi-service 2>/dev/null | grep -c "POST /book" || echo "0")
POLL_COUNT=$(docker-compose logs --tail=100 go-gateway 2>/dev/null | grep -c "orders/poll" || echo "0")

echo "Recent activity (last 100 log lines):"
echo "   POST /book requests:     $BOOK_COUNT"
echo "   GET /orders/poll requests: $POLL_COUNT"
echo ""

if [ "$BOOK_COUNT" -gt 0 ] || [ "$POLL_COUNT" -gt 0 ]; then
    echo "   ✅ MT5 ↔ Services: CONNECTED!"
    echo ""
    echo "   🎉 System is working! EAs are sending/receiving data."
else
    echo "   ⚠️  MT5 ↔ Services: NO ACTIVITY"
    echo ""
    echo "   Possible reasons:"
    echo "   1. EAs not attached to chart"
    echo "   2. WebRequest not allowed in MT5"
    echo "   3. Wrong EA inputs (check ports)"
    echo ""
    echo "   Next steps:"
    echo "   1. Open MT5"
    echo "   2. Attach BookBridge.mq5 to XAUUSD chart"
    echo "   3. Attach OrderExecutor.mq5 to XAUUSD chart"
    echo "   4. Check Tools → Options → Expert Advisors → Allow WebRequest"
    echo "   5. Run this script again"
fi

echo ""
echo "════════════════════════════════════════════════════════════════════════════"
echo ""
echo "Monitor live logs with:"
echo "   docker-compose logs -f go-gateway py-obi-service"
echo ""

