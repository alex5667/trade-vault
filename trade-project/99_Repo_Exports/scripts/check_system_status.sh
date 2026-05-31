#!/bin/bash
# Quick system health check for XAUUSD Trading System
# Usage: ./check_system_status.sh

echo ""
echo "╔════════════════════════════════════════════════════════════════════════════╗"
echo "║                                                                            ║"
echo "║           🔍 XAUUSD TRADING SYSTEM STATUS CHECK 🔍                        ║"
echo "║                                                                            ║"
echo "╚════════════════════════════════════════════════════════════════════════════╝"
echo ""

echo "════════════════════════════════════════════════════════════════════════════"
echo "1️⃣  DOCKER SERVICES"
echo "════════════════════════════════════════════════════════════════════════════"
echo ""

if command -v docker &> /dev/null; then
    docker compose ps py-obi-service go-gateway --format "table {{.Name}}\t{{.Status}}" 2>/dev/null || {
        echo "⚠️  Services not found. Run: docker compose up -d py-obi-service go-gateway"
    }
else
    echo "❌ Docker not installed"
fi

echo ""
echo "════════════════════════════════════════════════════════════════════════════"
echo "2️⃣  HEALTH ENDPOINTS"
echo "════════════════════════════════════════════════════════════════════════════"
echo ""

echo -n "Python OBI (8088): "
if curl -s -f http://localhost:8088/healthz > /dev/null 2>&1; then
    SERVICE=$(curl -s http://localhost:8088/healthz | jq -r '.service' 2>/dev/null)
    echo "✅ $SERVICE"
else
    echo "❌ NOT RESPONDING"
fi

echo -n "Go Gateway (8090): "
if curl -s -f http://localhost:8090/healthz > /dev/null 2>&1; then
    SERVICE=$(curl -s http://localhost:8090/healthz | jq -r '.service' 2>/dev/null)
    echo "✅ $SERVICE"
else
    echo "❌ NOT RESPONDING"
fi

echo ""
echo "════════════════════════════════════════════════════════════════════════════"
echo "3️⃣  MT5 CONNECTIVITY (last 1 minute)"
echo "════════════════════════════════════════════════════════════════════════════"
echo ""

echo "BookBridge → Python OBI (POST /book):"
BOOK_COUNT=$(docker compose logs --since 1m py-obi-service 2>/dev/null | grep -c "POST /book")
if [ "$BOOK_COUNT" -gt 0 ]; then
    echo "  ✅ Received $BOOK_COUNT DOM snapshots"
    docker compose logs --since 1m py-obi-service 2>/dev/null | grep "POST /book" | tail -2 | sed 's/^/    /'
else
    echo "  ⚠️  No DOM data received"
    echo "     → Check that BookBridge.mq5 is attached to XAUUSD chart"
    echo "     → Verify EndpointBook = \"http://127.0.0.1:8088/book\""
fi

echo ""
echo "OrderExecutor → Go Gateway (GET /orders/poll):"
POLL_COUNT=$(docker compose logs --since 1m go-gateway 2>/dev/null | grep -c "orders/poll")
if [ "$POLL_COUNT" -gt 0 ]; then
    echo "  ✅ Received $POLL_COUNT poll requests"
    docker compose logs --since 1m go-gateway 2>/dev/null | grep "orders/poll" | tail -2 | sed 's/^/    /'
else
    echo "  ⚠️  No polling detected"
    echo "     → Check that OrderExecutor.mq5 is attached to XAUUSD chart"
    echo "     → Verify EndpointPoll = \"http://127.0.0.1:8090/orders/poll\""
fi

echo ""
echo "════════════════════════════════════════════════════════════════════════════"
echo "4️⃣  ENDPOINT CONFIGURATION"
echo "════════════════════════════════════════════════════════════════════════════"
echo ""

cat << 'CONFIG'
Current Port Allocation:

  8088 → Python OBI Service
         ├── POST /book (BookBridge.mq5)
         ├── POST /tick (TickBridge.mq5, optional)
         ├── GET /render/*.png
         └── GET /healthz

  8090 → Go Gateway
         ├── GET /orders/poll (OrderExecutor.mq5)
         ├── POST /orders/confirm (OrderExecutor.mq5)
         ├── POST /orders/push
         └── GET /healthz

MT5 EA Configuration:
  BookBridge:   EndpointBook = "http://127.0.0.1:8088/book" ✅
  OrderExecutor: EndpointPoll = "http://127.0.0.1:8090/orders/poll" ✅
                 EndpointConfirm = "http://127.0.0.1:8090/orders/confirm" ✅
CONFIG

echo ""
echo "════════════════════════════════════════════════════════════════════════════"
echo "5️⃣  OVERALL STATUS"
echo "════════════════════════════════════════════════════════════════════════════"
echo ""

# Count successes
SUCCESS=0
TOTAL=4

# Check Docker services
if docker compose ps py-obi-service go-gateway 2>/dev/null | grep -q "healthy"; then
    SUCCESS=$((SUCCESS + 1))
fi

# Check health endpoints
if curl -s -f http://localhost:8088/healthz > /dev/null 2>&1; then
    SUCCESS=$((SUCCESS + 1))
fi

# Check MT5 connectivity
BOOK_COUNT=$(docker compose logs --since 1m py-obi-service 2>/dev/null | grep -c "POST /book")
if [ "$BOOK_COUNT" -gt 0 ]; then
    SUCCESS=$((SUCCESS + 1))
fi

POLL_COUNT=$(docker compose logs --since 1m go-gateway 2>/dev/null | grep -c "orders/poll")
if [ "$POLL_COUNT" -gt 0 ]; then
    SUCCESS=$((SUCCESS + 1))
fi

if [ "$SUCCESS" -eq "$TOTAL" ]; then
    echo "🎉 ✅ ALL SYSTEMS OPERATIONAL ($SUCCESS/$TOTAL)"
    echo ""
    echo "✅ Docker services running"
    echo "✅ Health endpoints responding"
    echo "✅ BookBridge sending DOM data"
    echo "✅ OrderExecutor polling for orders"
    echo ""
    echo "🚀 System is ready for automated trading!"
elif [ "$SUCCESS" -ge 2 ]; then
    echo "⚠️  PARTIALLY OPERATIONAL ($SUCCESS/$TOTAL)"
    echo ""
    echo "Infrastructure is running, but MT5 EAs may need attention."
    echo "Review the checks above for details."
else
    echo "❌ SYSTEM NOT OPERATIONAL ($SUCCESS/$TOTAL)"
    echo ""
    echo "Critical issues detected. Please fix the errors above."
    echo ""
    echo "Quick fix:"
    echo "  docker compose up -d py-obi-service go-gateway"
    echo "  # Then attach EAs in MT5"
fi

echo ""
echo "════════════════════════════════════════════════════════════════════════════"
echo ""
echo "📚 Documentation:"
echo "   • Full Guide: docs/XAUUSD/COMPLETE_GUIDE.md"
echo "   • Launch Checklist: FINAL_LAUNCH_CHECKLIST.md"
echo "   • MT5 Config: MT5_EA_CONFIGURATION.md"
echo ""
echo "🔧 Useful Commands:"
echo "   • Restart services: docker compose restart py-obi-service go-gateway"
echo "   • View logs: docker compose logs -f py-obi-service go-gateway"
echo "   • Stop services: docker compose stop py-obi-service go-gateway"
echo ""
echo "════════════════════════════════════════════════════════════════════════════"
echo ""

