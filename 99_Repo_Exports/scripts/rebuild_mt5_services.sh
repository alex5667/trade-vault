#!/bin/bash
# Rebuild and restart MT5-facing services (py-obi + go-gateway)

set -e

echo "═══════════════════════════════════════════════════════════"
echo "  MT5 Services Rebuild Script"
echo "═══════════════════════════════════════════════════════════"
echo ""

cd /home/alex/front/trade/scanner_infra

echo "📋 Step 1: Stopping services..."
docker compose stop py-obi-service go-gateway
echo "✅ Services stopped"
echo ""

echo "🔨 Step 2: Building py-obi-service..."
docker compose build py-obi-service
echo "✅ py-obi-service built"
echo ""

echo "🔨 Step 3: Building go-gateway..."
docker compose build go-gateway
echo "✅ go-gateway built"
echo ""

echo "🚀 Step 4: Starting services..."
docker compose up -d py-obi-service go-gateway
echo "✅ Services started"
echo ""

echo "⏳ Step 5: Waiting for services to be healthy (30s)..."
sleep 30
echo ""

echo "🔍 Step 6: Checking service status..."
docker ps --filter "name=scanner-py-obi" --filter "name=scanner-go-gateway" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo ""

echo "🧪 Step 7: Testing endpoints..."
echo ""

echo -n "  py-obi-service (8088): "
if curl -s -f http://127.0.0.1:8088/healthz > /dev/null; then
    echo "✅ OK"
    curl -s http://127.0.0.1:8088/healthz | jq .
else
    echo "❌ FAILED"
fi
echo ""

echo -n "  go-gateway (8090): "
if curl -s -f http://127.0.0.1:8090/healthz > /dev/null; then
    echo "✅ OK"
    curl -s http://127.0.0.1:8090/healthz | jq .
else
    echo "❌ FAILED"
fi
echo ""

echo "📊 Step 8: Recent logs..."
echo ""
echo "--- py-obi-service ---"
docker logs scanner-py-obi --tail 10
echo ""
echo "--- go-gateway ---"
docker logs scanner-go-gateway --tail 10
echo ""

echo "═══════════════════════════════════════════════════════════"
echo "  ✅ Rebuild Complete!"
echo "═══════════════════════════════════════════════════════════"
echo ""
echo "📝 Next Steps:"
echo ""
echo "1. In MT5, check EA are attached and running:"
echo "   - BookBridge → XAUUSD chart"
echo "   - TickBridge → XAUUSD chart (optional)"
echo "   - OrderExecutor → XAUUSD chart"
echo ""
echo "2. Monitor logs:"
echo "   docker logs -f scanner-py-obi"
echo "   docker logs -f scanner-go-gateway"
echo ""
echo "3. Test order execution:"
echo "   curl -X POST http://127.0.0.1:8090/orders/enqueue \\"
echo "     -H 'Content-Type: application/json' \\"
echo "     -d '{\"sid\":\"test-001\",\"symbol\":\"XAUUSD\",\"side\":\"LONG\",\"lot\":0.01}'"
echo ""

