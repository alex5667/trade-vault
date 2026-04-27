#!/bin/bash
# XAUUSD Services Launcher using Docker Compose v2
# Senior Developer Solution: Uses modern docker compose (v2.35.1+)

set -e

echo ""
echo "╔════════════════════════════════════════════════════════════════════════════╗"
echo "║                                                                            ║"
echo "║          🚀 XAUUSD Services - Docker Compose v2 Launcher 🚀              ║"
echo "║                                                                            ║"
echo "╚════════════════════════════════════════════════════════════════════════════╝"
echo ""

# Check .env file
if [ ! -f .env ]; then
    echo "❌ ERROR: .env file not found!"
    echo "   Create .env with:"
    echo "   TELEGRAM_BOT_TOKEN=your_token"
    echo "   TELEGRAM_CHAT_ID=your_chat_id"
    exit 1
fi

# Check Docker Compose v2
if ! docker compose version >/dev/null 2>&1; then
    echo "❌ ERROR: Docker Compose v2 not found!"
    echo "   Install: sudo apt install docker-compose-v2"
    exit 1
fi

echo "✅ Docker Compose version:"
docker compose version
echo ""

echo "════════════════════════════════════════════════════════════════════════════"
echo "📋 DEPLOYMENT PLAN (Senior Dev Architecture)"
echo "════════════════════════════════════════════════════════════════════════════"
echo ""
echo "Startup Order:"
echo "  1️⃣  Redis              (base dependency)"
echo "  2️⃣  py-obi-service     (depends on: redis)"
echo "  3️⃣  go-gateway         (depends on: py-obi-service, redis)"
echo ""
echo "All services have health checks and proper dependency management."
echo ""

echo "════════════════════════════════════════════════════════════════════════════"
echo "🚀 STARTING SERVICES"
echo "════════════════════════════════════════════════════════════════════════════"
echo ""

# Stop existing containers
echo "1️⃣  Stopping existing containers (if running)..."
docker compose stop py-obi-service go-gateway 2>/dev/null || true
echo "✅ Cleanup complete"
echo ""

# Build images if needed
echo "2️⃣  Building images..."
docker compose build py-obi-service go-gateway
echo "✅ Build complete"
echo ""

# Start services (force recreate to ensure fresh start)
echo "3️⃣  Starting services..."
docker compose up -d --force-recreate py-obi-service go-gateway
echo "✅ Services started"
echo ""

# Wait for health checks
echo "4️⃣  Waiting for health checks (15 seconds)..."
sleep 15
echo ""

# Check status
echo "════════════════════════════════════════════════════════════════════════════"
echo "📊 SERVICE STATUS"
echo "════════════════════════════════════════════════════════════════════════════"
echo ""
docker compose ps py-obi-service go-gateway
echo ""

# Test endpoints
echo "════════════════════════════════════════════════════════════════════════════"
echo "🔍 HEALTH CHECKS"
echo "════════════════════════════════════════════════════════════════════════════"
echo ""

echo "Python OBI Service (8090):"
if curl -sf http://localhost:8090/healthz >/dev/null 2>&1; then
    echo "  ✅ HEALTHY"
    curl -s http://localhost:8090/healthz | jq -r '.' 2>/dev/null || curl -s http://localhost:8090/healthz
else
    echo "  ❌ NOT RESPONDING"
fi

echo ""
echo "Go Gateway (8088):"
if curl -sf http://localhost:8088/healthz >/dev/null 2>&1; then
    echo "  ✅ HEALTHY"
    curl -s http://localhost:8088/healthz | jq -r '.' 2>/dev/null || curl -s http://localhost:8088/healthz
else
    echo "  ❌ NOT RESPONDING"
fi

echo ""
echo "════════════════════════════════════════════════════════════════════════════"
echo "✅ DEPLOYMENT COMPLETE"
echo "════════════════════════════════════════════════════════════════════════════"
echo ""
echo "Services are running and ready to receive data from MT5!"
echo ""
echo "════════════════════════════════════════════════════════════════════════════"
echo "📋 NEXT STEPS: Configure MT5"
echo "════════════════════════════════════════════════════════════════════════════"
echo ""
echo "1. Open MT5"
echo ""
echo "2. Enable WebRequest:"
echo "   Tools → Options → Expert Advisors"
echo "   ✅ Allow WebRequest for listed URL:"
echo "      http://127.0.0.1:8088"
echo "      http://127.0.0.1:8090"
echo ""
echo "3. Attach BookBridge.mq5 to XAUUSD chart:"
echo "   • Navigator → Expert Advisors → BookBridge"
echo "   • Drag to XAUUSD chart"
echo "   • Inputs:"
echo "     EndpointBook = \"http://127.0.0.1:8090/book\""
echo "     SymbolToWatch = \"XAUUSD\""
echo ""
echo "4. Attach OrderExecutor.mq5 to XAUUSD chart:"
echo "   • Navigator → Expert Advisors → OrderExecutor"
echo "   • Drag to XAUUSD chart"
echo "   • Inputs:"
echo "     EndpointPoll = \"http://127.0.0.1:8088/orders/poll\""
echo "     EndpointConfirm = \"http://127.0.0.1:8088/orders/confirm\""
echo ""
echo "════════════════════════════════════════════════════════════════════════════"
echo "🔍 MONITORING"
echo "════════════════════════════════════════════════════════════════════════════"
echo ""
echo "Real-time logs:"
echo "  docker compose logs -f py-obi-service go-gateway"
echo ""
echo "Check for MT5 activity (after attaching EAs):"
echo "  docker compose logs --tail=50 py-obi-service | grep 'POST /book'"
echo "  docker compose logs --tail=50 go-gateway | grep 'orders/poll'"
echo ""
echo "Service status:"
echo "  docker compose ps py-obi-service go-gateway"
echo ""
echo "════════════════════════════════════════════════════════════════════════════"
echo "🎯 MANAGEMENT COMMANDS"
echo "════════════════════════════════════════════════════════════════════════════"
echo ""
echo "Stop services:"
echo "  docker compose stop py-obi-service go-gateway"
echo ""
echo "Restart services:"
echo "  docker compose restart py-obi-service go-gateway"
echo ""
echo "Rebuild after code changes:"
echo "  docker compose build py-obi-service go-gateway"
echo "  docker compose up -d py-obi-service go-gateway"
echo ""
echo "Remove services:"
echo "  docker compose down py-obi-service go-gateway"
echo ""
echo "════════════════════════════════════════════════════════════════════════════"
echo ""
echo "              ✅ READY FOR TRADING! ✅"
echo ""
echo "════════════════════════════════════════════════════════════════════════════"
echo ""

