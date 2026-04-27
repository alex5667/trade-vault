#!/bin/bash
# Paper Executor Fix Script - Oct 31, 2025
# Fixes Redis connection issues in paper executor service

set -e

echo "============================================"
echo "Paper Executor Redis Connection Fix"
echo "============================================"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Change to script directory
cd "$(dirname "$0")/.."

echo -e "${YELLOW}Step 1: Stopping paper-executor service...${NC}"
docker-compose stop paper-executor 2>/dev/null || echo "Service not running"
echo ""

echo -e "${YELLOW}Step 2: Removing old container...${NC}"
docker-compose rm -f paper-executor 2>/dev/null || echo "No container to remove"
echo ""

echo -e "${YELLOW}Step 3: Rebuilding paper-executor service...${NC}"
docker-compose build paper-executor
echo ""

echo -e "${YELLOW}Step 4: Starting paper-executor service...${NC}"
docker-compose up -d paper-executor
echo ""

echo -e "${YELLOW}Step 5: Waiting for service to initialize (10 seconds)...${NC}"
sleep 10
echo ""

echo -e "${YELLOW}Step 6: Checking service status...${NC}"
if docker-compose ps paper-executor | grep -q "Up"; then
    echo -e "${GREEN}✓ Service is running${NC}"
else
    echo -e "${RED}✗ Service failed to start${NC}"
    echo ""
    echo "Last 50 lines of logs:"
    docker-compose logs --tail=50 paper-executor
    exit 1
fi
echo ""

echo -e "${YELLOW}Step 7: Checking Redis connection...${NC}"
if docker-compose logs paper-executor | grep -q "Successfully connected to Redis"; then
    echo -e "${GREEN}✓ Successfully connected to Redis${NC}"
else
    echo -e "${RED}✗ Redis connection not found in logs${NC}"
    echo ""
    echo "Recent logs:"
    docker-compose logs --tail=30 paper-executor
    exit 1
fi
echo ""

echo -e "${YELLOW}Step 8: Verifying initialization...${NC}"
if docker-compose logs paper-executor | grep -q "Initialized - monitoring"; then
    echo -e "${GREEN}✓ Service initialized successfully${NC}"
else
    echo -e "${YELLOW}⚠ Initialization message not found (might be normal)${NC}"
fi
echo ""

echo "============================================"
echo -e "${GREEN}✓ Paper Executor Fix Applied Successfully!${NC}"
echo "============================================"
echo ""
echo "Service is running and connected to Redis."
echo ""
echo "To monitor logs in real-time:"
echo "  docker-compose logs -f paper-executor"
echo ""
echo "To check service status:"
echo "  docker-compose ps paper-executor"
echo ""
echo "To view recent logs:"
echo "  docker-compose logs --tail=100 paper-executor"
echo ""

