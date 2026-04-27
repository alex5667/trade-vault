#!/bin/bash
# Simple launcher for XAUUSD Go+Python services
# Uses docker run instead of docker-compose to avoid ContainerConfig bugs

set -e

echo ""
echo "╔════════════════════════════════════════════════════════════════════════════╗"
echo "║                                                                            ║"
echo "║              🚀 XAUUSD Services Launcher 🚀                               ║"
echo "║                                                                            ║"
echo "╚════════════════════════════════════════════════════════════════════════════╝"
echo ""

# Load environment variables
if [ ! -f .env ]; then
    echo "❌ ERROR: .env file not found!"
    echo "   Create .env with TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID"
    exit 1
fi

source .env

# Check network exists
if ! docker network inspect scanner_infra_scanner-network >/dev/null 2>&1; then
    echo "❌ ERROR: Docker network scanner_infra_scanner-network not found!"
    echo "   Run: docker-compose up -d redis"
    exit 1
fi

# Build images if needed
echo "1️⃣  Building Docker images..."
echo ""

if ! docker images | grep -q scanner_infra_py-obi-service; then
    echo "Building Python OBI Service..."
    docker build -t scanner_infra_py-obi-service ./py-obi/
fi

if ! docker images | grep -q scanner_infra_go-gateway; then
    echo "Building Go Gateway..."
    docker build -t scanner_infra_go-gateway ./go-gateway/
fi

echo "✅ Images ready"
echo ""

# Stop existing containers
echo "2️⃣  Stopping existing containers..."
docker rm -f scanner-py-obi scanner-go-gateway 2>/dev/null || true
echo "✅ Cleanup complete"
echo ""

# Start Python OBI Service
echo "3️⃣  Starting Python OBI Service (port 8090)..."
docker run -d \
  --name scanner-py-obi \
  --network scanner_infra_scanner-network \
  -p 8090:8090 \
  --restart unless-stopped \
  -e PYTHONUNBUFFERED=1 \
  -e OBI_WINDOW_LEVELS=5 \
  -e OBI_THRESHOLD=0.25 \
  -e OBI_SUSTAIN_MS=1200 \
  -e RING_SECONDS=600 \
  -e NOTIFY_URL=http://scanner-go-gateway:8088/notify \
  -e REDIS_URL=redis://scanner-redis:6379/0 \
  scanner_infra_py-obi-service

echo "✅ Python OBI Service started"
echo ""

# Wait for Python OBI to be ready
echo "4️⃣  Waiting for Python OBI Service..."
for i in {1..10}; do
    if curl -s http://localhost:8090/healthz >/dev/null 2>&1; then
        echo "✅ Python OBI Service ready"
        break
    fi
    sleep 1
done
echo ""

# Start Go Gateway
echo "5️⃣  Starting Go Gateway (port 8088)..."
docker run -d \
  --name scanner-go-gateway \
  --network scanner_infra_scanner-network \
  -p 8088:8088 \
  --restart unless-stopped \
  -e PORT=8088 \
  -e TELEGRAM_BOT_TOKEN="${TELEGRAM_BOT_TOKEN}" \
  -e TELEGRAM_CHAT_ID="${TELEGRAM_CHAT_ID}" \
  -e OBI_HOST=http://scanner-py-obi:8090 \
  -e REDIS_URL=redis://scanner-redis:6379/0 \
  scanner_infra_go-gateway

echo "✅ Go Gateway started"
echo ""

# Wait for Go Gateway to be ready
echo "6️⃣  Waiting for Go Gateway..."
for i in {1..10}; do
    if curl -s http://localhost:8088/healthz >/dev/null 2>&1; then
        echo "✅ Go Gateway ready"
        break
    fi
    sleep 1
done
echo ""

echo "════════════════════════════════════════════════════════════════════════════"
echo ""
echo "✅ ALL SERVICES STARTED!"
echo ""
echo "Services:"
echo "  • Python OBI Service: http://localhost:8090"
echo "  • Go Gateway:         http://localhost:8088"
echo ""
echo "Containers:"
docker ps --filter name=scanner-py-obi --filter name=scanner-go-gateway --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo ""
echo "════════════════════════════════════════════════════════════════════════════"
echo ""
echo "📋 NEXT STEPS - Configure MT5:"
echo ""
echo "1. Open MT5"
echo "2. Tools → Options → Expert Advisors"
echo "   ✅ Allow WebRequest for:"
echo "      http://127.0.0.1:8088"
echo "      http://127.0.0.1:8090"
echo ""
echo "3. Attach BookBridge.mq5 to XAUUSD chart"
echo "   • EndpointBook = \"http://127.0.0.1:8090/book\""
echo ""
echo "4. Attach OrderExecutor.mq5 to XAUUSD chart"
echo "   • EndpointPoll = \"http://127.0.0.1:8088/orders/poll\""
echo "   • EndpointConfirm = \"http://127.0.0.1:8088/orders/confirm\""
echo ""
echo "════════════════════════════════════════════════════════════════════════════"
echo ""
echo "🔍 MONITOR:"
echo ""
echo "  docker logs -f scanner-py-obi scanner-go-gateway"
echo ""
echo "════════════════════════════════════════════════════════════════════════════"
echo ""

