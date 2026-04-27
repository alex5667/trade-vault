#!/bin/bash
# Monitor Multi-Symbol OrderFlow Service
# Мониторинг всех handlers в реальном времени

set -e

CONTAINER="scanner-multi-orderflow"
REDIS_HOST="localhost"
REDIS_PORT="6379"

# Цвета
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

clear

echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}   Multi-Symbol OrderFlow - Live Monitoring${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo ""

# Функция для получения метрики из Redis
get_redis_metric() {
    local key=$1
    redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" GET "$key" 2>/dev/null || echo "0"
}

# Функция для получения длины stream
get_stream_length() {
    local stream=$1
    redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" XLEN "$stream" 2>/dev/null || echo "0"
}

# Главный цикл мониторинга
while true; do
    clear
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}   Multi-Symbol OrderFlow - Live Monitoring${NC}"
    echo -e "${BLUE}   $(date '+%Y-%m-%d %H:%M:%S')${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
    
    # 1. Container Status
    echo -e "${YELLOW}📦 CONTAINER STATUS:${NC}"
    if docker ps | grep -q "$CONTAINER"; then
        status=$(docker inspect "$CONTAINER" --format='{{.State.Status}}')
        health=$(docker inspect "$CONTAINER" --format='{{.State.Health.Status}}' 2>/dev/null || echo "none")
        restarts=$(docker inspect "$CONTAINER" --format='{{.RestartCount}}')
        
        echo -e "   Status: ${GREEN}$status${NC}"
        if [ "$health" == "healthy" ]; then
            echo -e "   Health: ${GREEN}$health${NC}"
        else
            echo -e "   Health: ${YELLOW}$health${NC}"
        fi
        echo -e "   Restarts: ${restarts}"
    else
        echo -e "   ${RED}❌ Container not running${NC}"
    fi
    
    echo ""
    
    # 2. Resource Usage
    echo -e "${YELLOW}⚡ RESOURCE USAGE:${NC}"
    if docker ps | grep -q "$CONTAINER"; then
        stats=$(docker stats "$CONTAINER" --no-stream --format "{{.CPUPerc}}|{{.MemUsage}}" 2>/dev/null || echo "0%|0B / 0B")
        cpu=$(echo "$stats" | cut -d'|' -f1)
        mem=$(echo "$stats" | cut -d'|' -f2)
        
        echo "   CPU:    $cpu"
        echo "   Memory: $mem"
    fi
    
    echo ""
    
    # 3. Signals per Symbol
    echo -e "${YELLOW}📊 SIGNALS (последний час):${NC}"
    
    for symbol in XAUUSD BTCUSD ETHUSD BNBUSD; do
        stream="signals:orderflow:$symbol"
        count=$(get_stream_length "$stream")
        
        if [ "$count" -gt 0 ]; then
            # Получаем последний сигнал
            last_signal=$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" XREVRANGE "$stream" + - COUNT 1 2>/dev/null | grep -o 'LONG\|SHORT' | head -1 || echo "N/A")
            echo -e "   ${symbol:0:6}: ${count} signals | Last: ${last_signal}"
        fi
    done
    
    echo ""
    
    # 4. Recent Signals (последние 5)
    echo -e "${YELLOW}📤 RECENT SIGNALS (последние 5):${NC}"
    docker logs "$CONTAINER" --since 5m 2>&1 | grep "Сигнал опубликован" | tail -5 | while read line; do
        echo "   $line"
    done
    
    echo ""
    
    # 5. Errors (последние 3)
    echo -e "${YELLOW}❌ RECENT ERRORS (последние 3):${NC}"
    errors=$(docker logs "$CONTAINER" --since 5m 2>&1 | grep "❌" | tail -3)
    if [ -n "$errors" ]; then
        echo "$errors" | while read line; do
            echo -e "   ${RED}$line${NC}"
        done
    else
        echo -e "   ${GREEN}Нет ошибок${NC}"
    fi
    
    echo ""
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo "Press Ctrl+C to exit | Refreshing every 5 seconds..."
    
    sleep 5
done

