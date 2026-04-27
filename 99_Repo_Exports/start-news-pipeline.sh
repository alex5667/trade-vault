#!/bin/bash

# News Pipeline Startup Script
set -e

echo "🚀 Starting News Pipeline..."

# Check if .env file exists
if [ ! -f "news-pipeline.env" ]; then
    echo "❌ news-pipeline.env not found!"
    echo "📋 Copy news-pipeline.env.example to news-pipeline.env and configure your settings"
    exit 1
fi

# Load environment variables
set -a
source news-pipeline.env
set +a

# Check required environment variables
if [ -z "$GEMINI_API_KEY" ] || [ "$GEMINI_API_KEY" = "your_gemini_api_key_here" ]; then
    echo "❌ GEMINI_API_KEY not set! Please configure it in news-pipeline.env"
    exit 1
fi

if [ -z "$NEWS_RSS_URLS" ]; then
    echo "⚠️  NEWS_RSS_URLS not set, using default feeds"
    export NEWS_RSS_URLS="https://feeds.reuters.com/reuters/topNews"
fi

echo "📡 Redis URL: $REDIS_URL"
echo "📰 RSS Sources: $NEWS_RSS_URLS"
echo "🤖 LLM: $GEMINI_MODEL"

# Function to wait for a container to be healthy
wait_for_health() {
    local container=$1
    local max_retries=60
    local count=0
    echo "⏳ Waiting for $container to be healthy..."
    while [ $count -lt $max_retries ]; do
        status=$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container" 2>/dev/null || echo "not-found")
        if [ "$status" == "healthy" ] || [ "$status" == "running" ]; then
            echo "✅ $container is up and ready"
            return 0
        fi
        echo "   $container status: $status (attempt $((count+1))/$max_retries)"
        sleep 5
        count=$((count+1))
    done
    echo "❌ $container failed to become healthy"
    return 1
}

# Check Redis connectivity
echo "🔍 Checking Redis connectivity..."
# Firstly check if container exists
if ! docker ps -a --format '{{.Names}}' | grep -q "^redis-worker-1$"; then
    echo "❌ redis-worker-1 container not found"
    exit 1
fi

# Wait for health status
if ! wait_for_health "redis-worker-1"; then
    exit 1
fi

# Start services in order
echo "🔄 Starting news pipeline services..."

# Determine docker compose command
# Use the main docker-compose.yml since it includes all dependencies (like redis)
COMPOSE_BASE="docker-compose.yml"
if [ ! -f "$COMPOSE_BASE" ]; then
    COMPOSE_BASE="docker-compose-news-pipeline.yml"
fi

if docker compose version >/dev/null 2>&1; then
    DC="docker compose -f $COMPOSE_BASE"
else
    DC="docker-compose -f $COMPOSE_BASE"
fi

# Suppress warnings about unrelated containers
export COMPOSE_IGNORE_ORPHANS=True
export DATABASE_URL="${DATABASE_URL:-postgresql://trading:${TRADING_PASSWORD}@postgres:5432/scanner_analytics}"

# Start Go ingestor (primary)
echo "🐹 Starting Go news ingestor..."
$DC up -d news-ingestor-go

# Wait a moment
sleep 2

# Start watchdog
echo "👁️  Starting news watchdog..."
$DC up -d news-watchdog

# Start Python services
echo "🐍 Starting Python services..."
$DC up -d news-ingestor-py
$DC up -d news-analyzer
$DC up -d news-feature-store
$DC up -d calendar-feature-store

echo "⏳ Waiting for services to initialize..."
sleep 5

# Check service health
echo "🏥 Checking service health..."

# Check Go ingestor health
if curl -s http://localhost:8097/health | grep -q "ok"; then
    echo "✅ Go news ingestor is healthy"
else
    echo "⚠️  Go news ingestor health check failed"
fi

# Check Redis streams
echo "📊 Checking Redis streams..."
NEWS_COUNT=$(docker exec redis-worker-1 redis-cli XLEN news:raw 2>/dev/null || echo "0")
ANALYSIS_COUNT=$(docker exec redis-worker-1 redis-cli XLEN news:analysis 2>/dev/null || echo "0")

echo "📈 Stream lengths:"
echo "  news:raw: $NEWS_COUNT"
echo "  news:analysis: $ANALYSIS_COUNT"

echo ""
echo "🎉 News Pipeline started successfully!"
echo ""
echo "📋 Monitoring commands:"
echo "  $DC logs -f news-analyzer"
echo "  $DC logs -f news-feature-store"
echo "  docker exec redis-worker-1 redis-cli XLEN news:raw"
echo ""
echo "🛑 To stop: $DC down"

