#!/bin/bash
# scripts/test_tick.sh

echo "🚀 Sending test tick for XAUUSD to scanner-tick-ingest:8087"

curl -X POST http://localhost:8087/tick \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "XAUUSD",
    "ts": '$(date +%s%3N)',
    "bid": 2055.25,
    "ask": 2055.35,
    "last": 2055.30,
    "volume": 1.5,
    "flags": 6
  }'

echo -e "\n\n📊 Checking Redis stream stream:tick_XAUUSD length:"
docker exec redis-ticks redis-cli xlen stream:tick_XAUUSD

echo -e "\n\n🔎 Last entry in stream:tick_XAUUSD:"
docker exec redis-ticks redis-cli xrevrange stream:tick_XAUUSD + - count 1
