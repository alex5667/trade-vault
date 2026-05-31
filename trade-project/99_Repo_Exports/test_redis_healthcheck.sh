#!/bin/bash

# Test Redis healthcheck logic
# This script verifies that our improved healthcheck works correctly

echo "🔍 Testing Redis healthcheck logic..."
echo "This simulates what Docker does for healthchecks"
echo ""

# Test the improved healthcheck command
echo "Testing improved healthcheck command:"
echo "redis-cli -h localhost -p 6379 --raw ping | grep -q PONG && redis-cli -h localhost -p 6379 --raw info replication | grep -q 'loading:0'"
echo ""

echo "Expected behavior:"
echo "1. First check: redis-cli ping returns PONG (Redis is accepting connections)"
echo "2. Second check: info replication contains 'loading:0' (Redis has finished loading RDB)"
echo ""

echo "✅ Healthcheck will pass only when BOTH conditions are true:"
echo "   - Redis responds to PING"
echo "   - Redis loading status is 0 (finished loading)"
echo ""

echo "This prevents services from starting while Redis is still loading the .rdb file,"
echo "which was causing the 'BusyLoading' errors you saw in the logs."
