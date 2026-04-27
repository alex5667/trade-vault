#!/bin/bash

echo "⏳ Ожидание полной готовности Redis..."
# Wait for Redis to be fully loaded and ready for write operations
timeout=120
elapsed=0
while [ $elapsed -lt $timeout ]; do
  # Check if Redis is loading
  loading_status=$(redis-cli -h redis-worker-1 -p 6379 INFO 2>/dev/null | grep -E "^loading:" | cut -d: -f2 | tr -d '\r\n' || echo "1")
  if [ "$loading_status" = "0" ]; then
    # Redis is not loading, try a write operation to confirm it's ready
    if redis-cli -h redis-worker-1 -p 6379 XADD test:ready "*" ready true >/dev/null 2>&1; then
      # Clean up the test stream entry
      redis-cli -h redis-worker-1 -p 6379 DEL test:ready >/dev/null 2>&1 || true
      echo "✅ Redis готов к работе"
      break
    fi
  fi
  echo "Redis еще загружается... (прошло $elapsed с из $timeout с)"
  sleep 5
  elapsed=$((elapsed + 5))
done

if [ $elapsed -ge $timeout ]; then
  echo "❌ Redis не готов через $timeout секунд, но продолжаем..."
fi

echo "🚀 Запуск go-worker-1m..."
./main
