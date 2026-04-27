#!/bin/bash
# Check all MT5-facing endpoints are working

echo "═══════════════════════════════════════════════════════════"
echo "  MT5 Endpoints Health Check"
echo "═══════════════════════════════════════════════════════════"
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

check_endpoint() {
    local name=$1
    local url=$2
    local expected_code=$3
    
    echo -n "  $name ... "
    
    response=$(curl -s -o /dev/null -w "%{http_code}" "$url" 2>/dev/null)
    
    if [ "$response" = "$expected_code" ]; then
        echo -e "${GREEN}✅ OK${NC} (HTTP $response)"
        return 0
    else
        echo -e "${RED}❌ FAILED${NC} (HTTP $response, expected $expected_code)"
        return 1
    fi
}

check_post_endpoint() {
    local name=$1
    local url=$2
    local data=$3
    local expected_code=$4
    
    echo -n "  $name ... "
    
    response=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$url" \
        -H "Content-Type: application/json" \
        -d "$data" 2>/dev/null)
    
    if [ "$response" = "$expected_code" ]; then
        echo -e "${GREEN}✅ OK${NC} (HTTP $response)"
        return 0
    else
        echo -e "${RED}❌ FAILED${NC} (HTTP $response, expected $expected_code)"
        return 1
    fi
}

failed=0

echo "🔍 Health Endpoints:"
check_endpoint "py-obi-service" "http://127.0.0.1:8088/healthz" "200" || ((failed++))
check_endpoint "go-gateway" "http://127.0.0.1:8090/healthz" "200" || ((failed++))
echo ""

echo "📖 Book Data Endpoints:"
check_post_endpoint "POST /book" "http://127.0.0.1:8088/book" \
    '{"ts":1234567890000,"symbol":"XAUUSD","bids":[[2760.50,10.5]],"asks":[[2760.75,8.3]]}' \
    "200" || ((failed++))
echo ""

echo "📊 Tick Data Endpoints:"
check_post_endpoint "POST /tick" "http://127.0.0.1:8088/tick" \
    '{"ts":1234567890000,"bid":2760.50,"ask":2760.75,"last":2760.60,"volume":1.5,"flags":6,"symbol":"XAUUSD"}' \
    "200" || ((failed++))
echo ""

echo "🎯 Order Endpoints:"
check_endpoint "GET /orders/poll (empty)" "http://127.0.0.1:8090/orders/poll?symbol=XAUUSD" "204" || ((failed++))
check_post_endpoint "POST /orders/enqueue" "http://127.0.0.1:8090/orders/enqueue" \
    '{"sid":"test-check","symbol":"XAUUSD","side":"LONG","lot":0.01}' \
    "200" || ((failed++))
check_endpoint "GET /orders/poll (with order)" "http://127.0.0.1:8090/orders/poll?symbol=XAUUSD" "200" || ((failed++))
check_post_endpoint "POST /orders/confirm" "http://127.0.0.1:8090/orders/confirm" \
    '{"sid":"test-check","status":"opened","order":123456}' \
    "200" || ((failed++))
echo ""

echo "📈 OBI Data Endpoints:"
# First, post a book snapshot
curl -s -X POST http://127.0.0.1:8088/book \
    -H "Content-Type: application/json" \
    -d '{"ts":1234567890000,"symbol":"XAUUSD","bids":[[2760.50,15.0]],"asks":[[2760.75,10.0]]}' > /dev/null

sleep 1

check_endpoint "GET /features/obi" "http://127.0.0.1:8088/features/obi?symbol=XAUUSD&last=10" "200" || ((failed++))
check_endpoint "GET /events/pull" "http://127.0.0.1:8088/events/pull?symbol=XAUUSD&last=10" "200" || ((failed++))
echo ""

echo "🖼️  Rendering Endpoints:"
check_endpoint "GET /render/obi.png" "http://127.0.0.1:8088/render/obi.png?symbol=XAUUSD&last=100" "200" || ((failed++))
check_endpoint "GET /render/depth.png" "http://127.0.0.1:8088/render/depth.png?symbol=XAUUSD" "200" || ((failed++))
echo ""

echo "═══════════════════════════════════════════════════════════"
if [ $failed -eq 0 ]; then
    echo -e "  ${GREEN}✅ All endpoints working! ($((13-failed))/13)${NC}"
    echo "═══════════════════════════════════════════════════════════"
    echo ""
    echo "📝 MT5 EA Configuration:"
    echo ""
    echo "BookBridge.mq5:"
    echo "  EndpointBook = \"http://127.0.0.1:8088/book\""
    echo ""
    echo "TickBridge.mq5:"
    echo "  Endpoint = \"http://127.0.0.1:8088/tick\""
    echo ""
    echo "OrderExecutor.mq5:"
    echo "  EndpointPoll = \"http://127.0.0.1:8090/orders/poll\""
    echo "  EndpointConfirm = \"http://127.0.0.1:8090/orders/confirm\""
    echo ""
    exit 0
else
    echo -e "  ${RED}❌ $failed endpoint(s) failed!${NC}"
    echo "═══════════════════════════════════════════════════════════"
    echo ""
    echo "🔍 Troubleshooting:"
    echo ""
    echo "1. Check containers are running:"
    echo "   docker ps | grep -E '(scanner-py-obi|scanner-go-gateway)'"
    echo ""
    echo "2. Check logs:"
    echo "   docker logs scanner-py-obi --tail 50"
    echo "   docker logs scanner-go-gateway --tail 50"
    echo ""
    echo "3. Restart services:"
    echo "   ./rebuild_mt5_services.sh"
    echo ""
    exit 1
fi

