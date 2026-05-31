#!/bin/bash
# Comprehensive System Health Check - Oct 31, 2025
# Senior Trading Systems Analyst Tool

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Change to project directory
cd "$(dirname "$0")/.."

echo -e "${CYAN}╔════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║    Trading System Health Check - Oct 31       ║${NC}"
echo -e "${CYAN}╚════════════════════════════════════════════════╝${NC}"
echo ""

# Function to check service status
check_service() {
    local service_name=$1
    local container_name=$2
    
    if docker-compose ps "$service_name" 2>/dev/null | grep -q "Up"; then
        echo -e "${GREEN}✓${NC} $container_name"
        return 0
    else
        echo -e "${RED}✗${NC} $container_name ${RED}(NOT RUNNING)${NC}"
        return 1
    fi
}

# Function to check Redis connection in logs
check_redis_connection() {
    local service_name=$1
    local container_name=$2
    
    if docker-compose logs --tail=100 "$service_name" 2>/dev/null | grep -q "Successfully connected to Redis\|Подключение.*успешно\|Connected to Redis"; then
        echo -e "   ${GREEN}└─ Redis: Connected${NC}"
    elif docker-compose logs --tail=100 "$service_name" 2>/dev/null | grep -qi "redis.*error\|connection.*failed"; then
        echo -e "   ${RED}└─ Redis: Connection Error${NC}"
    else
        echo -e "   ${YELLOW}└─ Redis: Status Unknown${NC}"
    fi
}

# Function to count recent errors
count_errors() {
    local service_name=$1
    local errors=$(docker-compose logs --tail=200 --since=10m "$service_name" 2>/dev/null | grep -ic "error\|exception\|failed" || echo "0")
    # Remove newlines and whitespace
    errors=$(echo "$errors" | tr -d '\n\r' | xargs)
    
    if [ "$errors" -eq 0 ] 2>/dev/null; then
        echo -e "   ${GREEN}└─ Errors (10m): 0${NC}"
    elif [ "$errors" -lt 5 ] 2>/dev/null; then
        echo -e "   ${YELLOW}└─ Errors (10m): $errors${NC}"
    else
        echo -e "   ${RED}└─ Errors (10m): $errors${NC}"
    fi
}

echo -e "${BLUE}═══ Core Infrastructure ═══${NC}"
check_service "redis" "scanner-redis"
check_service "redis-worker-1" "scanner-redis-worker-1"
check_service "redis-worker-2" "scanner-redis-worker-2"
echo ""

echo -e "${BLUE}═══ Gateway Services ═══${NC}"
check_service "go-gateway" "scanner-go-gateway"
check_redis_connection "go-gateway" "Go Gateway"
count_errors "go-gateway"
echo ""

echo -e "${BLUE}═══ Python Workers ═══${NC}"
check_service "python-worker" "scanner-python-worker"
check_redis_connection "python-worker" "Python Worker"
count_errors "python-worker"

check_service "aggregated-hub-v2" "scanner-aggregated-hub-v2"
check_redis_connection "aggregated-hub-v2" "Aggregated Hub V2"
count_errors "aggregated-hub-v2"
echo ""

echo -e "${BLUE}═══ Signal & Notification Services ═══${NC}"
check_service "telegram-worker" "scanner-telegram-worker"
check_redis_connection "telegram-worker" "Telegram Worker"
count_errors "telegram-worker"

check_service "signal-generator" "scanner-signal-generator"
check_redis_connection "signal-generator" "Signal Generator"
count_errors "signal-generator"
echo ""

echo -e "${BLUE}═══ Trading Execution ═══${NC}"
check_service "paper-executor" "scanner-paper-executor"
check_redis_connection "paper-executor" "Paper Executor"
count_errors "paper-executor"
echo ""

# Check Redis memory usage
echo -e "${BLUE}═══ Redis Memory Usage ═══${NC}"
for redis_service in "redis" "redis-worker-1" "redis-worker-2"; do
    if docker-compose ps "$redis_service" 2>/dev/null | grep -q "Up"; then
        memory=$(docker-compose exec -T "$redis_service" redis-cli INFO memory 2>/dev/null | grep "used_memory_human:" | cut -d: -f2 | tr -d '\r' || echo "N/A")
        echo -e "${GREEN}✓${NC} $redis_service: ${CYAN}$memory${NC}"
    fi
done
echo ""

# Check for critical errors in last 5 minutes
echo -e "${BLUE}═══ Critical Errors (Last 5 min) ═══${NC}"
critical_errors=$(docker-compose logs --since=5m 2>&1 | grep -i "critical\|fatal\|panic" | head -5)
if [ -z "$critical_errors" ]; then
    echo -e "${GREEN}✓ No critical errors found${NC}"
else
    echo -e "${RED}⚠ Critical errors detected:${NC}"
    echo "$critical_errors"
fi
echo ""

# Check signal flow
echo -e "${BLUE}═══ Signal Flow Check ═══${NC}"
if docker-compose exec -T redis redis-cli EXISTS "stream:signals:XAUUSD" 2>/dev/null | grep -q "1"; then
    signal_count=$(docker-compose exec -T redis redis-cli XLEN "stream:signals:XAUUSD" 2>/dev/null | tr -d '\r' || echo "0")
    echo -e "${GREEN}✓${NC} XAUUSD signals stream exists: ${CYAN}$signal_count${NC} messages"
else
    echo -e "${YELLOW}⚠${NC} XAUUSD signals stream not found"
fi

if docker-compose exec -T redis redis-cli EXISTS "stream:tick_XAUUSD" 2>/dev/null | grep -q "1"; then
    tick_count=$(docker-compose exec -T redis redis-cli XLEN "stream:tick_XAUUSD" 2>/dev/null | tr -d '\r' || echo "0")
    echo -e "${GREEN}✓${NC} XAUUSD tick stream exists: ${CYAN}$tick_count${NC} messages"
else
    echo -e "${YELLOW}⚠${NC} XAUUSD tick stream not found"
fi
echo ""

# System resources
echo -e "${BLUE}═══ System Resources ═══${NC}"
containers=$(docker-compose ps -q | wc -l)
running=$(docker-compose ps | grep "Up" | wc -l)
echo -e "Containers: ${CYAN}$running${NC}/${CYAN}$containers${NC} running"

# Docker stats
echo -e "\n${BLUE}Top 5 CPU consumers:${NC}"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}" $(docker-compose ps -q) 2>/dev/null | grep scanner | sort -k2 -hr | head -6

echo ""
echo -e "${CYAN}╔════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║            Health Check Complete               ║${NC}"
echo -e "${CYAN}╚════════════════════════════════════════════════╝${NC}"
echo ""
echo "For detailed logs of a specific service, use:"
echo "  docker-compose logs -f <service-name>"
echo ""
echo "To restart a service:"
echo "  docker-compose restart <service-name>"
echo ""

