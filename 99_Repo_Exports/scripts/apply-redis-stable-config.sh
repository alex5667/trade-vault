#!/bin/bash
# ============================================================================
# Apply Stable Redis Configuration
# Safely restart Redis with new production-grade settings
# Author: Senior Infrastructure Specialist
# ============================================================================

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Redis Stable Configuration Deployment                   ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Check if we're in the right directory
if [ ! -f "docker-compose.yml" ]; then
    echo -e "${RED}Error: docker-compose.yml not found!${NC}"
    echo "Please run this script from the scanner_infra directory."
    exit 1
fi

# Backup current configuration
echo -e "${YELLOW}[1/7] Creating backup of current configuration...${NC}"
backup_dir="./redis-backup-$(date +%Y%m%d_%H%M%S)"
mkdir -p "$backup_dir"
cp docker-compose.yml "$backup_dir/"
cp redis-external-access.conf "$backup_dir/" 2>/dev/null || true
echo -e "${GREEN}✓ Backup created: $backup_dir${NC}"
echo ""

# Verify new configuration files exist
echo -e "${YELLOW}[2/7] Verifying new configuration files...${NC}"
if [ ! -f "redis-external-access.conf" ]; then
    echo -e "${RED}Error: redis-external-access.conf not found!${NC}"
    exit 1
fi
if [ ! -f "redis-worker-stable.conf" ]; then
    echo -e "${RED}Error: redis-worker-stable.conf not found!${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Configuration files verified${NC}"
echo ""

# Save current Redis data (optional)
echo -e "${YELLOW}[3/7] Saving Redis data...${NC}"
docker exec scanner-redis redis-cli BGSAVE 2>/dev/null || echo "  (BGSAVE failed or disabled)"
echo -e "${GREEN}✓ Data save initiated${NC}"
echo ""

# Show current stats before restart
echo -e "${YELLOW}[4/7] Current Redis statistics:${NC}"
echo "  Main Redis:"
docker exec scanner-redis redis-cli INFO stats | grep -E "total_commands_processed|connected_clients|instantaneous_ops_per_sec" | sed 's/^/    /'
echo "  Worker 1:"
docker exec scanner-redis-worker-1 redis-cli INFO stats | grep -E "total_commands_processed|connected_clients" | sed 's/^/    /' || echo "    (not running)"
echo "  Worker 2:"
docker exec scanner-redis-worker-2 redis-cli INFO stats | grep -E "total_commands_processed|connected_clients" | sed 's/^/    /' || echo "    (not running)"
echo ""

# Ask for confirmation
echo -e "${YELLOW}[5/7] Ready to restart Redis with new configuration${NC}"
echo -e "${RED}This will cause a brief service interruption!${NC}"
read -p "Continue? (yes/no): " -r
echo
if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
    echo -e "${YELLOW}Deployment cancelled by user${NC}"
    exit 0
fi

# Restart Redis services
echo -e "${YELLOW}[6/7] Restarting Redis services...${NC}"
echo "  Stopping services..."
docker-compose stop redis redis-worker-1 redis-worker-2

echo "  Starting with new configuration..."
docker-compose up -d redis redis-worker-1 redis-worker-2

# Wait for services to be healthy
echo "  Waiting for services to be healthy..."
max_wait=60
elapsed=0
while [ $elapsed -lt $max_wait ]; do
    if docker exec scanner-redis redis-cli ping > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Main Redis is healthy${NC}"
        break
    fi
    sleep 2
    elapsed=$((elapsed + 2))
    echo -n "."
done
echo ""

if [ $elapsed -ge $max_wait ]; then
    echo -e "${RED}Error: Redis did not become healthy within ${max_wait}s${NC}"
    echo "Check logs with: docker logs scanner-redis"
    exit 1
fi

# Wait for worker instances
sleep 5
for worker in scanner-redis-worker-1 scanner-redis-worker-2; do
    if docker exec "$worker" redis-cli ping > /dev/null 2>&1; then
        echo -e "${GREEN}✓ $worker is healthy${NC}"
    else
        echo -e "${YELLOW}⚠ $worker is not responding (may take longer to start)${NC}"
    fi
done
echo ""

# Restart dependent services
echo -e "${YELLOW}Restarting dependent services...${NC}"
docker-compose restart go-worker python-worker telegram-worker signal-parser-worker notify-worker
echo ""

# Verify new configuration
echo -e "${YELLOW}[7/7] Verifying new configuration...${NC}"
echo "  Main Redis:"
docker exec scanner-redis redis-cli CONFIG GET hz | xargs printf "    hz: %s %s\n"
docker exec scanner-redis redis-cli CONFIG GET maxmemory | xargs printf "    maxmemory: %s %s\n"
docker exec scanner-redis redis-cli CONFIG GET appendonly | xargs printf "    appendonly: %s %s\n"
docker exec scanner-redis redis-cli CONFIG GET timeout | xargs printf "    timeout: %s %s\n"

echo "  Worker 1:"
docker exec scanner-redis-worker-1 redis-cli CONFIG GET maxmemory | xargs printf "    maxmemory: %s %s\n"

echo "  Worker 2:"
docker exec scanner-redis-worker-2 redis-cli CONFIG GET maxmemory | xargs printf "    maxmemory: %s %s\n"
echo ""

# Success message
echo -e "${GREEN}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║   ✓ Deployment Complete - Redis Stable Configuration      ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo "Next steps:"
echo "  1. Monitor Redis health: ./redis-health-comprehensive.sh"
echo "  2. Check logs: docker logs -f scanner-redis"
echo "  3. Watch metrics: docker stats scanner-redis scanner-redis-worker-1 scanner-redis-worker-2"
echo ""
echo "If you encounter issues:"
echo "  1. Check logs: docker logs scanner-redis"
echo "  2. Rollback: docker-compose down && cp $backup_dir/* ./ && docker-compose up -d"
echo ""

