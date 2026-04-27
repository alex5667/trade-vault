#!/bin/bash

# Monitor Goroutines - Check for IO wait goroutine leaks
# Usage: ./monitor-goroutines.sh [container-name]

CONTAINER="${1:-scanner-go-worker-5m}"
INTERVAL=5
COLOR_RED='\033[0;31m'
COLOR_GREEN='\033[0;32m'
COLOR_YELLOW='\033[1;33m'
COLOR_BLUE='\033[0;34m'
COLOR_NC='\033[0m' # No Color

echo "🔍 Monitoring goroutines for container: $CONTAINER"
echo "Press Ctrl+C to stop"
echo ""

# Function to get goroutine stats
get_goroutine_stats() {
    local total_goroutines=$(docker exec $CONTAINER sh -c 'wget -qO- http://localhost:6060/debug/pprof/goroutine?debug=1 2>/dev/null' | grep -c "^goroutine" || echo "0")
    local io_wait=$(docker exec $CONTAINER sh -c 'wget -qO- http://localhost:6060/debug/pprof/goroutine?debug=1 2>/dev/null' | grep -c "IO wait" || echo "0")
    local tls_handshake=$(docker exec $CONTAINER sh -c 'wget -qO- http://localhost:6060/debug/pprof/goroutine?debug=1 2>/dev/null' | grep -c "crypto/tls.*Handshake" || echo "0")
    local websocket_dial=$(docker exec $CONTAINER sh -c 'wget -qO- http://localhost:6060/debug/pprof/goroutine?debug=1 2>/dev/null' | grep -c "websocket.*Dial" || echo "0")
    
    echo "$total_goroutines|$io_wait|$tls_handshake|$websocket_dial"
}

# Function to colorize output based on thresholds
colorize() {
    local value=$1
    local warning=$2
    local critical=$3
    
    if [ "$value" -ge "$critical" ]; then
        echo -e "${COLOR_RED}$value${COLOR_NC}"
    elif [ "$value" -ge "$warning" ]; then
        echo -e "${COLOR_YELLOW}$value${COLOR_NC}"
    else
        echo -e "${COLOR_GREEN}$value${COLOR_NC}"
    fi
}

# Header
printf "${COLOR_BLUE}%-20s %15s %15s %15s %15s${COLOR_NC}\n" \
    "Timestamp" "Total" "IO Wait" "TLS Handshake" "WS Dial"
echo "--------------------------------------------------------------------------------"

# Continuous monitoring
while true; do
    timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    stats=$(get_goroutine_stats)
    
    IFS='|' read -r total io_wait tls_handshake ws_dial <<< "$stats"
    
    # Colorize based on thresholds
    total_colored=$(colorize $total 500 1000)
    io_wait_colored=$(colorize $io_wait 20 50)
    tls_colored=$(colorize $tls_handshake 10 30)
    ws_colored=$(colorize $ws_dial 10 30)
    
    printf "%-20s %15s %15s %15s %15s\n" \
        "$timestamp" "$total_colored" "$io_wait_colored" "$tls_colored" "$ws_colored"
    
    # Alert if thresholds exceeded
    if [ "$io_wait" -ge 50 ]; then
        echo -e "${COLOR_RED}⚠️  CRITICAL: IO wait goroutines exceeded 50! Possible goroutine leak!${COLOR_NC}"
    fi
    
    if [ "$tls_handshake" -ge 30 ]; then
        echo -e "${COLOR_RED}⚠️  CRITICAL: TLS handshake goroutines exceeded 30! Connection issues detected!${COLOR_NC}"
    fi
    
    sleep $INTERVAL
done

