#!/bin/bash
# ============================================================================
# Redis Health Check - Comprehensive Monitoring Script
# Author: Senior Infrastructure Specialist
# ============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Redis instances
declare -A REDIS_INSTANCES=(
    ["main"]="scanner-redis:6379"
    ["worker-1"]="scanner-redis-worker-1:6380"
    ["worker-2"]="scanner-redis-worker-2:6381"
)

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Redis Health Check - Production Monitoring              ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Function to check Redis instance health
check_redis_health() {
    local name=$1
    local container=$2
    
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}[${name}]${NC} Checking Redis instance..."
    echo ""
    
    # Check if container is running
    if ! docker ps --format '{{.Names}}' | grep -q "^${container%:*}$"; then
        echo -e "${RED}✗ Container ${container%:*} is not running!${NC}"
        return 1
    fi
    echo -e "${GREEN}✓ Container is running${NC}"
    
    # Check connectivity
    if ! docker exec "${container%:*}" redis-cli ping > /dev/null 2>&1; then
        echo -e "${RED}✗ Redis is not responding to PING${NC}"
        return 1
    fi
    echo -e "${GREEN}✓ Redis responds to PING${NC}"
    
    # Memory usage
    echo -e "\n${YELLOW}Memory Statistics:${NC}"
    docker exec "${container%:*}" redis-cli INFO memory | grep -E "used_memory_human|used_memory_peak_human|maxmemory_human|mem_fragmentation_ratio" | sed 's/^/  /'
    
    # Connection statistics
    echo -e "\n${YELLOW}Connection Statistics:${NC}"
    docker exec "${container%:*}" redis-cli INFO clients | grep -E "connected_clients|blocked_clients|client_recent_max_input_buffer" | sed 's/^/  /'
    
    # Performance statistics
    echo -e "\n${YELLOW}Performance Statistics:${NC}"
    docker exec "${container%:*}" redis-cli INFO stats | grep -E "instantaneous_ops_per_sec|total_commands_processed|rejected_connections|keyspace_hits|keyspace_misses" | sed 's/^/  /'
    
    # Persistence status
    echo -e "\n${YELLOW}Persistence Status:${NC}"
    docker exec "${container%:*}" redis-cli INFO persistence | grep -E "aof_enabled|aof_last_write_status|aof_current_size|aof_base_size" | sed 's/^/  /'
    
    # Check slow log
    echo -e "\n${YELLOW}Slow Log (last 5 entries):${NC}"
    local slow_count=$(docker exec "${container%:*}" redis-cli SLOWLOG LEN)
    if [ "$slow_count" -gt 0 ]; then
        echo -e "${RED}  Warning: $slow_count slow queries detected${NC}"
        docker exec "${container%:*}" redis-cli SLOWLOG GET 5 | head -20 | sed 's/^/  /'
    else
        echo -e "${GREEN}  ✓ No slow queries${NC}"
    fi
    
    # Check latency
    echo -e "\n${YELLOW}Latency Check:${NC}"
    local latency=$(docker exec "${container%:*}" redis-cli --latency-history -i 1 -c 3 2>/dev/null | tail -1)
    echo "  $latency"
    
    # Check keyspace
    echo -e "\n${YELLOW}Keyspace Info:${NC}"
    docker exec "${container%:*}" redis-cli INFO keyspace | grep -E "^db[0-9]" | sed 's/^/  /' || echo "  No keys found"
    
    # Configuration check
    echo -e "\n${YELLOW}Critical Config Values:${NC}"
    docker exec "${container%:*}" redis-cli CONFIG GET maxmemory | xargs printf "  maxmemory: %s %s\n"
    docker exec "${container%:*}" redis-cli CONFIG GET maxmemory-policy | xargs printf "  maxmemory-policy: %s %s\n"
    docker exec "${container%:*}" redis-cli CONFIG GET hz | xargs printf "  hz: %s %s\n"
    docker exec "${container%:*}" redis-cli CONFIG GET timeout | xargs printf "  timeout: %s %s\n"
    docker exec "${container%:*}" redis-cli CONFIG GET tcp-keepalive | xargs printf "  tcp-keepalive: %s %s\n"
    
    echo ""
}

# Function to check overall system health
check_system_health() {
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}[SYSTEM]${NC} Checking system resources..."
    echo ""
    
    # Docker stats
    echo -e "${YELLOW}Docker Resource Usage:${NC}"
    docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}" \
        scanner-redis scanner-redis-worker-1 scanner-redis-worker-2 | sed 's/^/  /'
    
    echo ""
    
    # System limits
    echo -e "${YELLOW}System Limits:${NC}"
    echo "  somaxconn: $(cat /proc/sys/net/core/somaxconn)"
    echo "  File descriptors: $(ulimit -n)"
    
    # Check for OOM kills
    echo -e "\n${YELLOW}Recent OOM Events:${NC}"
    local oom_count=$(dmesg | grep -i "oom" | grep -i "redis" | tail -5 | wc -l)
    if [ "$oom_count" -gt 0 ]; then
        echo -e "${RED}  Warning: $oom_count OOM events detected${NC}"
        dmesg | grep -i "oom" | grep -i "redis" | tail -5 | sed 's/^/  /'
    else
        echo -e "${GREEN}  ✓ No OOM events${NC}"
    fi
    
    echo ""
}

# Function to generate recommendations
generate_recommendations() {
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}[RECOMMENDATIONS]${NC}"
    echo ""
    
    for name in "${!REDIS_INSTANCES[@]}"; do
        local container="${REDIS_INSTANCES[$name]%:*}"
        
        # Check memory usage
        local mem_used=$(docker exec "$container" redis-cli INFO memory | grep "used_memory:" | cut -d: -f2 | tr -d '\r')
        local mem_max=$(docker exec "$container" redis-cli CONFIG GET maxmemory | tail -1)
        
        if [ "$mem_max" != "0" ]; then
            local mem_percent=$((mem_used * 100 / mem_max))
            if [ $mem_percent -gt 80 ]; then
                echo -e "${RED}⚠ [$name] Memory usage at ${mem_percent}% - Consider increasing maxmemory${NC}"
            elif [ $mem_percent -gt 60 ]; then
                echo -e "${YELLOW}⚠ [$name] Memory usage at ${mem_percent}% - Monitor closely${NC}"
            else
                echo -e "${GREEN}✓ [$name] Memory usage healthy (${mem_percent}%)${NC}"
            fi
        fi
        
        # Check connection count
        local conn_count=$(docker exec "$container" redis-cli INFO clients | grep "connected_clients:" | cut -d: -f2 | tr -d '\r')
        if [ "$conn_count" -gt 100 ]; then
            echo -e "${YELLOW}⚠ [$name] High connection count ($conn_count) - Review client pooling${NC}"
        fi
        
        # Check hit rate
        local hits=$(docker exec "$container" redis-cli INFO stats | grep "keyspace_hits:" | cut -d: -f2 | tr -d '\r')
        local misses=$(docker exec "$container" redis-cli INFO stats | grep "keyspace_misses:" | cut -d: -f2 | tr -d '\r')
        if [ "$hits" != "" ] && [ "$misses" != "" ] && [ "$hits" != "0" ]; then
            local total=$((hits + misses))
            local hit_rate=$((hits * 100 / total))
            if [ $hit_rate -lt 80 ]; then
                echo -e "${YELLOW}⚠ [$name] Cache hit rate low (${hit_rate}%) - Review caching strategy${NC}"
            else
                echo -e "${GREEN}✓ [$name] Cache hit rate good (${hit_rate}%)${NC}"
            fi
        fi
    done
    
    echo ""
}

# Main execution
main() {
    # Check each Redis instance
    for name in "${!REDIS_INSTANCES[@]}"; do
        check_redis_health "$name" "${REDIS_INSTANCES[$name]}"
    done
    
    # Check system health
    check_system_health
    
    # Generate recommendations
    generate_recommendations
    
    echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║   Health Check Complete                                   ║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo "Run this script with --watch for continuous monitoring:"
    echo "  watch -n 10 $0"
}

# Run main function
main "$@"

