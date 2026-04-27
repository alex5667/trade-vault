#!/bin/bash

# Allowed symbols (uppercase)
ALLOWED_SYMBOLS="BTCUSDT ETHUSDT SOLUSDT BNBUSDT XRPUSDT PEPEUSDT DOGEUSDT SHIBUSDT FLOKIUSDT BONKUSDT WIFUSDT SUIUSDT APTUSDT ARBUSDT XAUUSDT"

# Function to clean a Redis instance
clean_redis() {
    local host=$1
    local port=$2
    local name=$3
    
    echo "--- Cleaning $name ($host:$port) ---"
    
    # Get all symbol:details:* keys
    keys=$(redis-cli -h "$host" -p "$port" --scan --pattern "symbol:details:*")
    
    deleted=0
    kept=0
    
    for key in $keys; do
        # Extract symbol from key (format: symbol:details:btcusdt)
        symbol=$(echo "$key" | cut -d':' -f3 | tr '[:lower:]' '[:upper:]')
        
        # Check if symbol is in allowed list
        if echo "$ALLOWED_SYMBOLS" | grep -qw "$symbol"; then
            ((kept++))
        else
            redis-cli -h "$host" -p "$port" DEL "$key" > /dev/null
            ((deleted++))
            if [ $((deleted % 100)) -eq 0 ]; then
                echo "Deleted $deleted symbols (last: $symbol)..."
            fi
        fi
    done
    
    echo "Done for $name. Kept: $kept. DELETED: $deleted"
    echo ""
}

# Clean main Redis (accessible from host)
clean_redis "localhost" "6379" "redis (main)"

# Clean redis-worker-1 (need to exec into container)
echo "--- Cleaning redis-worker-1 (via docker exec) ---"
docker exec redis-worker-1 sh -c '
ALLOWED="BTCUSDT ETHUSDT SOLUSDT BNBUSDT XRPUSDT PEPEUSDT DOGEUSDT SHIBUSDT FLOKIUSDT BONKUSDT WIFUSDT SUIUSDT APTUSDT ARBUSDT XAUUSDT"
deleted=0
kept=0
for key in $(redis-cli --scan --pattern "symbol:details:*"); do
    symbol=$(echo "$key" | cut -d: -f3 | tr "[:lower:]" "[:upper:]")
    if echo "$ALLOWED" | grep -qw "$symbol"; then
        kept=$((kept + 1))
    else
        redis-cli DEL "$key" > /dev/null
        deleted=$((deleted + 1))
        if [ $((deleted % 100)) -eq 0 ]; then
            echo "Deleted $deleted symbols (last: $symbol)..."
        fi
    fi
done
echo "Done for redis-worker-1. Kept: $kept. DELETED: $deleted"
'

echo "✅ Cleanup complete!"
