#!/bin/bash
# Trading System Manager - Oct 31, 2025
# Comprehensive system management tool

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m'

cd "$(dirname "$0")/.."

show_banner() {
    clear
    echo -e "${CYAN}╔════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║    Trading System Manager - Oct 31, 2025       ║${NC}"
    echo -e "${CYAN}║    Senior Dev Tools v2.0                       ║${NC}"
    echo -e "${CYAN}╚════════════════════════════════════════════════╝${NC}"
    echo ""
}

show_menu() {
    show_banner
    echo -e "${GREEN}System Management:${NC}"
    echo "  1) Start All Services"
    echo "  2) Stop All Services"
    echo "  3) Restart All Services"
    echo "  4) Rebuild & Restart (Full)"
    echo ""
    echo -e "${GREEN}Monitoring:${NC}"
    echo "  5) Health Check"
    echo "  6) View Logs (Interactive)"
    echo "  7) Real-time Stats"
    echo ""
    echo -e "${GREEN}Service Management:${NC}"
    echo "  8) Restart Single Service"
    echo "  9) Rebuild Single Service"
    echo ""
    echo -e "${GREEN}Maintenance:${NC}"
    echo "  10) Clean Up (Prune)"
    echo "  11) Reset Redis Data"
    echo "  12) View Service Status"
    echo ""
    echo -e "${GREEN}Fixes & Tools:${NC}"
    echo "  13) Apply Paper Executor Fix"
    echo "  14) Check All Redis Connections"
    echo ""
    echo "  0) Exit"
    echo ""
    echo -n "Enter choice: "
}

start_all() {
    echo -e "\n${BLUE}═══ Starting All Services ═══${NC}"
    docker-compose up -d
    echo -e "${GREEN}✓ All services started${NC}"
    echo ""
    echo "Press Enter to continue..."
    read
}

stop_all() {
    echo -e "\n${BLUE}═══ Stopping All Services ═══${NC}"
    docker-compose stop
    echo -e "${GREEN}✓ All services stopped${NC}"
    echo ""
    echo "Press Enter to continue..."
    read
}

restart_all() {
    echo -e "\n${BLUE}═══ Restarting All Services ═══${NC}"
    docker-compose restart
    echo -e "${GREEN}✓ All services restarted${NC}"
    echo ""
    echo "Press Enter to continue..."
    read
}

rebuild_all() {
    echo -e "\n${RED}═══ Full Rebuild & Restart ═══${NC}"
    echo -e "${YELLOW}This will stop all services, rebuild images, and restart.${NC}"
    echo -n "Are you sure? (y/N): "
    read confirm
    
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
        echo -e "\n${BLUE}Stopping services...${NC}"
        docker-compose down
        
        echo -e "${BLUE}Building images...${NC}"
        docker-compose build
        
        echo -e "${BLUE}Starting services...${NC}"
        docker-compose up -d
        
        echo -e "${GREEN}✓ Full rebuild complete${NC}"
    else
        echo -e "${YELLOW}Cancelled${NC}"
    fi
    echo ""
    echo "Press Enter to continue..."
    read
}

health_check() {
    echo -e "\n${BLUE}═══ Running Health Check ═══${NC}"
    ./scripts/health_check.sh
    echo ""
    echo "Press Enter to continue..."
    read
}

view_logs() {
    ./scripts/quick_logs.sh
}

real_time_stats() {
    echo -e "\n${BLUE}═══ Real-time Container Stats ═══${NC}"
    echo "Press Ctrl+C to return to menu"
    echo ""
    docker stats $(docker-compose ps -q)
}

restart_service() {
    echo -e "\n${BLUE}═══ Restart Single Service ═══${NC}"
    echo ""
    echo "Available services:"
    docker-compose ps --services | nl
    echo ""
    echo -n "Enter service name: "
    read service
    
    if [ -n "$service" ]; then
        echo -e "${BLUE}Restarting $service...${NC}"
        docker-compose restart "$service"
        echo -e "${GREEN}✓ $service restarted${NC}"
        
        echo ""
        echo "View logs? (y/N): "
        read view_logs_choice
        if [ "$view_logs_choice" = "y" ]; then
            docker-compose logs -f --tail=50 "$service"
        fi
    fi
    echo ""
    echo "Press Enter to continue..."
    read
}

rebuild_service() {
    echo -e "\n${BLUE}═══ Rebuild Single Service ═══${NC}"
    echo ""
    echo "Available services:"
    docker-compose ps --services | nl
    echo ""
    echo -n "Enter service name: "
    read service
    
    if [ -n "$service" ]; then
        echo -e "${BLUE}Stopping $service...${NC}"
        docker-compose stop "$service"
        
        echo -e "${BLUE}Rebuilding $service...${NC}"
        docker-compose build "$service"
        
        echo -e "${BLUE}Starting $service...${NC}"
        docker-compose up -d "$service"
        
        echo -e "${GREEN}✓ $service rebuilt and started${NC}"
        
        echo ""
        echo "View logs? (y/N): "
        read view_logs_choice
        if [ "$view_logs_choice" = "y" ]; then
            sleep 2
            docker-compose logs -f --tail=50 "$service"
        fi
    fi
    echo ""
    echo "Press Enter to continue..."
    read
}

cleanup() {
    echo -e "\n${YELLOW}═══ System Cleanup ═══${NC}"
    echo "This will remove:"
    echo "  - Stopped containers"
    echo "  - Unused networks"
    echo "  - Dangling images"
    echo ""
    echo -n "Continue? (y/N): "
    read confirm
    
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
        echo -e "${BLUE}Cleaning up...${NC}"
        docker system prune -f
        echo -e "${GREEN}✓ Cleanup complete${NC}"
    else
        echo -e "${YELLOW}Cancelled${NC}"
    fi
    echo ""
    echo "Press Enter to continue..."
    read
}

reset_redis() {
    echo -e "\n${RED}═══ Reset Redis Data ═══${NC}"
    echo -e "${YELLOW}WARNING: This will delete ALL data in Redis!${NC}"
    echo -n "Are you ABSOLUTELY sure? (type 'yes'): "
    read confirm
    
    if [ "$confirm" = "yes" ]; then
        echo -e "${BLUE}Flushing Redis main...${NC}"
        docker-compose exec -T redis redis-cli FLUSHALL
        
        echo -e "${BLUE}Flushing Redis worker 1...${NC}"
        docker-compose exec -T redis-worker-1 redis-cli FLUSHALL 2>/dev/null || true
        
        echo -e "${BLUE}Flushing Redis worker 2...${NC}"
        docker-compose exec -T redis-worker-2 redis-cli FLUSHALL 2>/dev/null || true
        
        echo -e "${GREEN}✓ All Redis data cleared${NC}"
    else
        echo -e "${YELLOW}Cancelled${NC}"
    fi
    echo ""
    echo "Press Enter to continue..."
    read
}

service_status() {
    echo -e "\n${BLUE}═══ Service Status ═══${NC}"
    docker-compose ps
    echo ""
    echo "Press Enter to continue..."
    read
}

apply_paper_fix() {
    echo -e "\n${BLUE}═══ Applying Paper Executor Fix ═══${NC}"
    ./scripts/fix_paper_executor.sh
    echo ""
    echo "Press Enter to continue..."
    read
}

check_redis_connections() {
    echo -e "\n${BLUE}═══ Checking Redis Connections ═══${NC}"
    echo ""
    
    services=("go-gateway" "python-worker" "aggregated-hub-v2" "signal-generator" "paper-executor" "telegram-worker")
    
    for service in "${services[@]}"; do
        echo -e "${CYAN}Checking $service...${NC}"
        if docker-compose logs --tail=50 "$service" 2>/dev/null | grep -q "Successfully connected to Redis\|Подключение.*успешно\|Connected to Redis"; then
            echo -e "  ${GREEN}✓ Connected to Redis${NC}"
        elif docker-compose logs --tail=50 "$service" 2>/dev/null | grep -qi "redis.*error\|connection.*failed"; then
            echo -e "  ${RED}✗ Redis Connection Error${NC}"
        else
            echo -e "  ${YELLOW}? Status Unknown${NC}"
        fi
    done
    
    echo ""
    echo "Press Enter to continue..."
    read
}

# Main loop
while true; do
    show_menu
    read choice
    
    case $choice in
        1) start_all ;;
        2) stop_all ;;
        3) restart_all ;;
        4) rebuild_all ;;
        5) health_check ;;
        6) view_logs ;;
        7) real_time_stats ;;
        8) restart_service ;;
        9) rebuild_service ;;
        10) cleanup ;;
        11) reset_redis ;;
        12) service_status ;;
        13) apply_paper_fix ;;
        14) check_redis_connections ;;
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

