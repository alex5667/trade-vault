#!/bin/bash
# Quick Logs Viewer - Oct 31, 2025
# Fast access to service logs with filtering

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

cd "$(dirname "$0")/.."

show_menu() {
    echo -e "${CYAN}╔════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║          Quick Logs Viewer Menu                ║${NC}"
    echo -e "${CYAN}╚════════════════════════════════════════════════╝${NC}"
    echo ""
    echo "Select a service:"
    echo ""
    echo -e "${GREEN}Gateway & Core:${NC}"
    echo "  1) Go Gateway"
    echo "  2) Python Worker"
    echo "  3) Aggregated Hub V2"
    echo ""
    echo -e "${GREEN}Trading:${NC}"
    echo "  4) Signal Generator"
    echo "  5) Paper Executor"
    echo ""
    echo -e "${GREEN}Notifications:${NC}"
    echo "  6) Telegram Worker"
    echo ""
    echo -e "${GREEN}Infrastructure:${NC}"
    echo "  7) Redis Main"
    echo "  8) Redis Worker 1"
    echo "  9) Redis Worker 2"
    echo ""
    echo -e "${GREEN}Multi-view:${NC}"
    echo "  10) All Errors (last 5 min)"
    echo "  11) All Services (follow)"
    echo "  12) Signal Flow"
    echo ""
    echo "  0) Exit"
    echo ""
    echo -n "Enter choice: "
}

view_logs() {
    local service=$1
    local name=$2
    local follow=${3:-true}
    
    echo -e "\n${BLUE}═══ $name Logs ═══${NC}"
    echo "Press Ctrl+C to return to menu"
    echo ""
    
    if [ "$follow" = "true" ]; then
        docker-compose logs -f --tail=100 "$service"
    else
        docker-compose logs --tail=100 "$service"
    fi
}

view_errors() {
    echo -e "\n${RED}═══ All Errors (Last 5 minutes) ═══${NC}"
    docker-compose logs --since=5m 2>&1 | grep -i "error\|exception\|failed" | tail -50
    echo ""
    echo "Press Enter to continue..."
    read
}

view_all() {
    echo -e "\n${BLUE}═══ All Services (Following) ═══${NC}"
    echo "Press Ctrl+C to return to menu"
    echo ""
    docker-compose logs -f --tail=50
}

view_signal_flow() {
    echo -e "\n${CYAN}═══ Signal Flow Analysis ═══${NC}"
    echo ""
    
    echo -e "${BLUE}Recent Signals (last 5):${NC}"
    docker-compose exec -T redis redis-cli XREVRANGE "stream:signals:XAUUSD" + - COUNT 5 2>/dev/null || echo "No signals found"
    
    echo ""
    echo -e "${BLUE}Recent Ticks (last 3):${NC}"
    docker-compose exec -T redis redis-cli XREVRANGE "stream:tick_XAUUSD" + - COUNT 3 2>/dev/null || echo "No ticks found"
    
    echo ""
    echo -e "${BLUE}Paper Orders:${NC}"
    docker-compose exec -T redis redis-cli XREVRANGE "paper:orders" + - COUNT 3 2>/dev/null || echo "No orders found"
    
    echo ""
    echo "Press Enter to continue..."
    read
}

while true; do
    clear
    show_menu
    read choice
    
    case $choice in
        1) view_logs "go-gateway" "Go Gateway" ;;
        2) view_logs "python-worker" "Python Worker" ;;
        3) view_logs "aggregated-hub-v2" "Aggregated Hub V2" ;;
        4) view_logs "signal-generator" "Signal Generator" ;;
        5) view_logs "paper-executor" "Paper Executor" ;;
        6) view_logs "telegram-worker" "Telegram Worker" ;;
        7) view_logs "redis" "Redis Main" ;;
        8) view_logs "redis-worker-1" "Redis Worker 1" ;;
        9) view_logs "redis-worker-2" "Redis Worker 2" ;;
        10) view_errors ;;
        11) view_all ;;
        12) view_signal_flow ;;
        0) 
            echo -e "\n${GREEN}Goodbye!${NC}\n"
            exit 0
            ;;
        *)
            echo -e "\n${RED}Invalid choice. Press Enter to continue...${NC}"
            read
            ;;
    esac
done

