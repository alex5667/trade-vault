#!/bin/bash
# Stop all XAUUSD services (manual and Docker)
# Use this before switching between manual and Docker deployments

echo "╔════════════════════════════════════════════════════════════════════════════╗"
echo "║                                                                            ║"
echo "║              🛑 STOPPING ALL XAUUSD SERVICES 🛑                           ║"
echo "║                                                                            ║"
echo "╚════════════════════════════════════════════════════════════════════════════╝"
echo ""

# Stop Docker services
echo "📦 Stopping Docker Compose services..."
docker-compose stop go-gateway py-obi-service 2>/dev/null || true
echo ""

# Stop manual services (from start_go_stack.sh)
echo "🔧 Stopping manual services..."
if [ -f "/tmp/obi_service.pid" ]; then
    echo "   Stopping OBI service (PID: $(cat /tmp/obi_service.pid))..."
    kill $(cat /tmp/obi_service.pid) 2>/dev/null || true
    rm /tmp/obi_service.pid
fi

if [ -f "/tmp/scanner_gw.pid" ]; then
    echo "   Stopping Go Gateway (PID: $(cat /tmp/scanner_gw.pid))..."
    kill $(cat /tmp/scanner_gw.pid) 2>/dev/null || true
    rm /tmp/scanner_gw.pid
fi
echo ""

# Kill any processes on ports 8088 and 8090
echo "🔌 Freeing ports 8088 and 8090..."
if lsof -ti:8088 > /dev/null 2>&1; then
    echo "   Killing process on port 8088..."
    lsof -ti:8088 | xargs kill -9 2>/dev/null || true
fi

if lsof -ti:8090 > /dev/null 2>&1; then
    echo "   Killing process on port 8090..."
    lsof -ti:8090 | xargs kill -9 2>/dev/null || true
fi
echo ""

# Stop any book_analytics_service or obi service
echo "🧹 Cleaning up old Python services..."
pkill -f "book_analytics_service" 2>/dev/null || true
pkill -f "book_obi_service" 2>/dev/null || true
pkill -f "scanner-gw" 2>/dev/null || true
echo ""

# Wait a bit
sleep 2

# Check ports
echo "════════════════════════════════════════════════════════════════════════════"
echo ""
echo "✅ PORT STATUS"
echo ""

if lsof -ti:8088 > /dev/null 2>&1; then
    echo "   ⚠️  Port 8088: STILL IN USE"
    lsof -ti:8088 | xargs ps -p 2>/dev/null | tail -n +2
else
    echo "   ✅ Port 8088: FREE"
fi

if lsof -ti:8090 > /dev/null 2>&1; then
    echo "   ⚠️  Port 8090: STILL IN USE"
    lsof -ti:8090 | xargs ps -p 2>/dev/null | tail -n +2
else
    echo "   ✅ Port 8090: FREE"
fi

echo ""
echo "════════════════════════════════════════════════════════════════════════════"
echo ""
echo "✅ All services stopped!"
echo ""
echo "Now you can start services via:"
echo "  • Docker:  ./quick_start_docker.sh"
echo "  • Manual:  ./start_go_stack.sh"
echo ""

