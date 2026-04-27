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

# Check Redis connectivity
echo "🔍 Checking Redis connectivity..."
if ! docker exec redis-worker-1 redis-cli ping | grep -q PONG; then
    echo "❌ Redis is not accessible"
    exit 1
fi
echo "✅ Redis is ready"

# Start services in order
echo "🔄 Starting news pipeline services..."

# Suppress warnings about unrelated containers
export COMPOSE_IGNORE_ORPHANS=True
export DATABASE_URL="${DATABASE_URL:-postgresql://trading:${TRADING_PASSWORD}@postgres:5432/scanner_analytics}"


# Start Go ingestor (primary)
echo "🐹 Starting Go news ingestor..."
docker-compose up -d news-ingestor-go

# Wait a moment
sleep 2

# Start watchdog
echo "👁️  Starting news watchdog..."
docker-compose up -d news-watchdog

# Start Python services
echo "🐍 Starting Python services..."
docker-compose up -d news-ingestor-py
docker-compose up -d news-analyzer
docker-compose up -d news-feature-store
docker-compose up -d calendar-feature-store

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
echo "  docker-compose logs -f news-analyzer"
echo "  docker-compose logs -f news-feature-store"
echo "  docker exec redis-worker-1 redis-cli XLEN news:raw"
echo ""
echo "🛑 To stop: docker-compose down"
