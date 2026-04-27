#!/bin/bash
# Fix script for Paper Executor WRONGTYPE Redis error
# This script deletes the corrupted orders:queue key and restarts the paper-executor

set -e

echo "🔍 Checking if redis-worker-1 container is running..."
if ! docker ps --format "{{.Names}}" | grep -q "redis-worker-1"; then
    echo "❌ redis-worker-1 container is not running"
    echo "   Please start your Docker containers first with: make up"
    exit 1
fi

echo "✅ redis-worker-1 is running"
echo ""

echo "🔍 Checking current type of orders:queue key..."
KEY_TYPE=$(docker exec redis-worker-1 redis-cli TYPE "orders:queue" 2>/dev/null || echo "none")
echo "   Current type: $KEY_TYPE"
echo ""

if [ "$KEY_TYPE" = "list" ]; then
    echo "⚠️  orders:queue is a LIST (should be STREAM)"
    echo "🔧 Deleting corrupted key..."
    docker exec redis-worker-1 redis-cli DEL "orders:queue"
    echo "✅ Key deleted"
elif [ "$KEY_TYPE" = "stream" ]; then
    echo "✅ orders:queue is already a STREAM (correct type)"
    echo "   No fix needed"
elif [ "$KEY_TYPE" = "none" ]; then
    echo "ℹ️  orders:queue does not exist yet (will be created as stream)"
else
    echo "⚠️  orders:queue has unexpected type: $KEY_TYPE"
    echo "🔧 Deleting key..."
    docker exec redis-worker-1 redis-cli DEL "orders:queue"
    echo "✅ Key deleted"
fi

echo ""
echo "🔄 Restarting paper-executor..."
if docker ps --format "{{.Names}}" | grep -q "scanner-paper-executor"; then
    docker restart scanner-paper-executor
    echo "✅ Paper executor restarted"
    echo ""
    echo "⏳ Waiting 5 seconds for service to start..."
    sleep 5
    echo ""
    echo "📋 Recent logs:"
    docker logs scanner-paper-executor --tail 20
else
    echo "⚠️  scanner-paper-executor container not found"
    echo "   It will be created when you start the services"
fi

echo ""
echo "🔍 Verifying fix..."
NEW_TYPE=$(docker exec redis-worker-1 redis-cli TYPE "orders:queue" 2>/dev/null || echo "none")
echo "   New type: $NEW_TYPE"

if [ "$NEW_TYPE" = "stream" ]; then
    echo "✅ Fix successful! orders:queue is now a STREAM"
elif [ "$NEW_TYPE" = "none" ]; then
    echo "ℹ️  Key not created yet (will be created on first use)"
else
    echo "⚠️  Unexpected type: $NEW_TYPE"
fi

echo ""
echo "✅ Fix complete!"
echo ""
echo "Next steps:"
echo "  1. Monitor paper-executor logs: docker logs -f scanner-paper-executor"
echo "  2. Check for WRONGTYPE errors (should be gone)"
echo "  3. Verify consumer group creation is successful"
