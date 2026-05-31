#!/bin/bash
# Check XAUUSD Data Flow - Complete diagnostic script
# Senior Developer + Trading Analyst

# Don't exit on errors, we want to show all results
set +e

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║     🔍 XAUUSD DATA FLOW DIAGNOSTIC                            ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check function
check_service() {
    local name=$1
    local container=$2
    
    echo -n "Checking $name..."
    if docker ps --filter "name=$container" --format "{{.Names}}" | grep -q "$container"; then
        status=$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "no healthcheck")
        if [ "$status" = "healthy" ]; then
            echo -e " ${GREEN}✓ HEALTHY${NC}"
            return 0
        elif [ "$status" = "no healthcheck" ]; then
            echo -e " ${YELLOW}⚠ RUNNING (no healthcheck)${NC}"
            return 0
        else
            echo -e " ${RED}✗ UNHEALTHY${NC}"
            return 1
        fi
    else
        echo -e " ${RED}✗ NOT RUNNING${NC}"
        return 1
    fi
}

check_stream() {
    local stream=$1
    local name=$2
    
    echo -n "Checking $name..."
    length=$(docker exec scanner-redis redis-cli XLEN "$stream" 2>/dev/null || echo "0")
    
    if [ "$length" -gt 0 ]; then
        echo -e " ${GREEN}✓ $length messages${NC}"
        return 0
    else
        echo -e " ${YELLOW}⚠ EMPTY${NC}"
        return 1
    fi
}

# 1. Services Check
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "1️⃣  SERVICES STATUS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

check_service "Tick Ingest Server" "scanner-tick-ingest"
check_service "Multi-Symbol OrderFlow" "scanner_infra_multi-symbol-orderflow_1"
check_service "Aggregated Hub V2" "scanner-aggregated-hub"
check_service "Notify Worker" "scanner-notify-worker"
check_service "Go Gateway" "scanner-go-gateway"
check_service "Signal Generator" "scanner-signal-generator"
check_service "ATR Worker" "scanner-atr-worker"

echo ""

# 2. Redis Streams Check
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "2️⃣  REDIS STREAMS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

check_stream "stream:tick_XAUUSD" "Tick Stream"
check_stream "signals:orderflow:XAUUSD" "OrderFlow Signals"
check_stream "signals:ta:XAUUSD" "TA Signals"
check_stream "notify:telegram" "Telegram Notifications"
check_stream "candles:data" "Candles Data"

echo ""

# 3. Consumer Groups Check
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "3️⃣  CONSUMER GROUPS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo -n "Checking tick stream consumer groups..."
groups=$(docker exec scanner-redis redis-cli XINFO GROUPS stream:tick_XAUUSD 2>/dev/null | grep -c "name" || echo "0")
if [ "$groups" -gt 0 ]; then
    echo -e " ${GREEN}✓ $groups groups${NC}"
    docker exec scanner-redis redis-cli XINFO GROUPS stream:tick_XAUUSD 2>/dev/null | grep "name" | awk '{print "  - " $2}'
else
    echo -e " ${YELLOW}⚠ NO GROUPS${NC}"
fi

echo -n "Checking notify stream consumer groups..."
groups=$(docker exec scanner-redis redis-cli XINFO GROUPS notify:telegram 2>/dev/null | grep -c "name" || echo "0")
if [ "$groups" -gt 0 ]; then
    echo -e " ${GREEN}✓ $groups groups${NC}"
    docker exec scanner-redis redis-cli XINFO GROUPS notify:telegram 2>/dev/null | grep "name" | awk '{print "  - " $2}'
else
    echo -e " ${YELLOW}⚠ NO GROUPS${NC}"
fi

echo ""

# 4. Endpoints Check
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "4️⃣  HTTP ENDPOINTS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo -n "Checking Tick Ingest (8087)..."
if curl -s -o /dev/null -w "%{http_code}" http://localhost:8087/health | grep -q "200"; then
    echo -e " ${GREEN}✓ OK${NC}"
else
    echo -e " ${RED}✗ FAILED${NC}"
fi

echo -n "Checking Go Gateway (8090)..."
if curl -s -o /dev/null -w "%{http_code}" http://localhost:8090/healthz | grep -q "200"; then
    echo -e " ${GREEN}✓ OK${NC}"
else
    echo -e " ${RED}✗ FAILED${NC}"
fi

echo ""

# 5. Recent Activity
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "5️⃣  RECENT ACTIVITY"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo "Last tick in stream:tick_XAUUSD:"
docker exec scanner-redis redis-cli XREVRANGE stream:tick_XAUUSD + - COUNT 1 2>/dev/null | head -5 || echo "  No data"

echo ""
echo "Last notification in notify:telegram:"
docker exec scanner-redis redis-cli XREVRANGE notify:telegram + - COUNT 1 2>/dev/null | head -5 || echo "  No data"

echo ""

# 6. Log Samples
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "6️⃣  RECENT LOG SAMPLES (last 5 lines)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo -e "\n${BLUE}Tick Ingest:${NC}"
docker logs scanner-tick-ingest --tail 3 2>&1 | grep -v "Waiting for application" || echo "  No logs"

echo -e "\n${BLUE}OrderFlow Handler:${NC}"
docker logs scanner_infra_multi-symbol-orderflow_1 --tail 3 2>&1 || echo "  Container not running"

echo -e "\n${BLUE}Aggregated Hub:${NC}"
docker logs scanner-aggregated-hub --tail 3 2>&1 | grep -v "Waiting" || echo "  No logs"

echo -e "\n${BLUE}Notify Worker:${NC}"
docker logs scanner-notify-worker --tail 3 2>&1 || echo "  No logs"

echo ""

# 7. Summary
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "7️⃣  SUMMARY & RECOMMENDATIONS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Check if tick stream has data
tick_count=$(docker exec scanner-redis redis-cli XLEN stream:tick_XAUUSD 2>/dev/null || echo "0")

if [ "$tick_count" -gt 0 ]; then
    echo -e "${GREEN}✓ System is receiving ticks${NC}"
    echo "  Data flow is active"
    
    # Check if signals are being generated
    signal_count=$(docker exec scanner-redis redis-cli XLEN signals:orderflow:XAUUSD 2>/dev/null || echo "0")
    if [ "$signal_count" -gt 0 ]; then
        echo -e "${GREEN}✓ Signals are being generated${NC}"
    else
        echo -e "${YELLOW}⚠ No signals generated yet (waiting for conditions)${NC}"
    fi
    
    # Check if notifications are being sent
    notify_count=$(docker exec scanner-redis redis-cli XLEN notify:telegram 2>/dev/null || echo "0")
    if [ "$notify_count" -gt 0 ]; then
        echo -e "${GREEN}✓ Notifications are being queued${NC}"
    else
        echo -e "${YELLOW}⚠ No notifications in queue${NC}"
    fi
else
    echo -e "${RED}✗ No ticks in stream${NC}"
    echo ""
    echo "POSSIBLE CAUSES:"
    echo "1. MT5 is not running or not connected"
    echo "2. TickBridge EA is not installed or not active"
    echo "3. Network connectivity issues"
    echo ""
    echo "SOLUTIONS:"
    echo "1. Check MT5 terminal under Wine:"
    echo "   wine mt5terminal.exe"
    echo ""
    echo "2. Test tick endpoint manually:"
    echo "   curl -X POST http://localhost:8087/tick \\"
    echo "     -H 'Content-Type: application/json' \\"
    echo "     -d '{\"symbol\":\"XAUUSD\",\"ts\":$(date +%s)000,\"bid\":2055.25,\"ask\":2055.35,\"last\":2055.30,\"volume\":1.5,\"flags\":6}'"
    echo ""
    echo "3. Check logs:"
    echo "   docker logs scanner-tick-ingest -f"
fi

echo ""
echo "For detailed analysis, see: XAUUSD_DATA_FLOW_ANALYSIS.md"
echo ""

