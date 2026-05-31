#!/bin/bash
# Quick Start Script for XAUUSD Docker Compose Integration
# Run this to start the XAUUSD trading system via Docker

set -e

cd "$(dirname "$0")"

echo "╔════════════════════════════════════════════════════════════════════════════╗"
echo "║                                                                            ║"
echo "║          🚀 XAUUSD Docker Compose Quick Start 🚀                          ║"
echo "║                                                                            ║"
echo "╚════════════════════════════════════════════════════════════════════════════╝"
echo ""

# Check .env file
if [ ! -f ".env" ]; then
    echo "❌ Error: .env file not found!"
    echo "Create it with:"
    echo ""
    echo "cat << 'EOF' > .env"
    echo "TELEGRAM_BOT_TOKEN=your_bot_token_here"
    echo "TELEGRAM_CHAT_ID=your_chat_id_here"
    echo "EOF"
    echo ""
    exit 1
fi

echo "✅ Step 1: Checking .env configuration"
echo "────────────────────────────────────────────────────────────────────────────"
if grep -q "TELEGRAM_BOT_TOKEN=.*" .env && grep -q "TELEGRAM_CHAT_ID=.*" .env; then
    echo "   TELEGRAM_BOT_TOKEN: $(grep TELEGRAM_BOT_TOKEN .env | cut -d'=' -f2 | cut -c1-20)..."
    echo "   TELEGRAM_CHAT_ID: $(grep TELEGRAM_CHAT_ID .env | cut -d'=' -f2)"
    echo "   ✅ Configuration OK"
else
    echo "   ⚠️  Warning: Telegram credentials may be missing"
fi
echo ""

echo "✅ Step 2: Building Docker images"
echo "────────────────────────────────────────────────────────────────────────────"
echo "   This may take 2-3 minutes on first run..."
docker-compose build py-obi-service go-gateway
echo "   ✅ Images built successfully"
echo ""

echo "✅ Step 3: Starting services"
echo "────────────────────────────────────────────────────────────────────────────"
docker-compose up -d py-obi-service go-gateway
echo "   ✅ Services started"
echo ""

echo "✅ Step 4: Waiting for services to be healthy (30s)"
echo "────────────────────────────────────────────────────────────────────────────"
sleep 10
echo "   Waiting... 10s"
sleep 10
echo "   Waiting... 20s"
sleep 10
echo "   Waiting... 30s"
echo ""

echo "✅ Step 5: Checking service health"
echo "────────────────────────────────────────────────────────────────────────────"

# Check Go Gateway
if curl -sf http://localhost:8088/healthz > /dev/null 2>&1; then
    echo "   ✅ Go Gateway (port 8088): HEALTHY"
else
    echo "   ⚠️  Go Gateway (port 8088): NOT RESPONDING"
fi

# Check Python OBI
if curl -sf http://localhost:8090/healthz > /dev/null 2>&1; then
    echo "   ✅ Python OBI Service (port 8090): HEALTHY"
else
    echo "   ⚠️  Python OBI Service (port 8090): NOT RESPONDING"
fi
echo ""

echo "✅ Step 6: Service status"
echo "────────────────────────────────────────────────────────────────────────────"
docker-compose ps go-gateway py-obi-service
echo ""

echo "╔════════════════════════════════════════════════════════════════════════════╗"
echo "║                                                                            ║"
echo "║                    ✅ STARTUP COMPLETE! ✅                                ║"
echo "║                                                                            ║"
echo "╚════════════════════════════════════════════════════════════════════════════╝"
echo ""

echo "📋 Next Steps:"
echo "────────────────────────────────────────────────────────────────────────────"
echo ""
echo "1. View logs:"
echo "   docker-compose logs -f go-gateway py-obi-service"
echo ""
echo "2. Test health:"
echo "   curl http://localhost:8088/healthz"
echo "   curl http://localhost:8090/healthz"
echo ""
echo "3. Configure MT5 EAs:"
echo "   • BookBridge.mq5 → http://127.0.0.1:8090/book"
echo "   • OrderExecutor.mq5 → http://127.0.0.1:8088/orders/poll"
echo "   • ⚠️  TickBridge.mq5 → http://127.0.0.1:8087/tick (port changed!)"
echo ""
echo "4. Stop services:"
echo "   docker-compose stop go-gateway py-obi-service"
echo ""
echo "📚 Documentation:"
echo "   docs/XAUUSD/DOCKER_COMPOSE_README.md"
echo ""
echo "🎯 Ready for trading! 🚀"
echo ""

