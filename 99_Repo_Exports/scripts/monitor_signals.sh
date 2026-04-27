#!/bin/bash
# ==============================================================================
# XAUUSD Signal Monitoring Tool
# ==============================================================================
# Удобный инструмент для мониторинга сигналов в режиме реального времени
#
# Использование:
#   ./monitor_signals.sh [опция]
#
# Опции:
#   logs        - Смотреть все логи системы
#   gateway     - Логи Go Gateway (входящие тики)
#   python      - Логи Python Worker (обработка и генерация сигналов)
#   obi         - Логи OBI Service (Order Book Imbalance)
#   dashboard   - Открыть ROC/AUC Dashboard в браузере
#   obi-chart   - Открыть OBI график в браузере
#   redis       - Мониторить Redis Streams напрямую
#   stats       - Показать статистику системы
#   all         - Запустить все инструменты мониторинга
# ==============================================================================

set -e

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Функция для красивого заголовка
print_header() {
    echo -e "\n${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}$1${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}\n"
}

# Функция для проверки работы сервисов
check_services() {
    print_header "📊 Проверка сервисов"
    
    echo -e "${YELLOW}Проверяю Docker контейнеры...${NC}"
    docker compose ps | grep -E "scanner-go-gateway|scanner-python-worker|scanner-py-obi" || true
    echo
    
    # Проверка Go Gateway
    if curl -s http://localhost:8088/healthz > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Go Gateway (8088): работает${NC}"
    else
        echo -e "${RED}❌ Go Gateway (8088): не отвечает${NC}"
    fi
    
    # Проверка Python Worker
    if curl -s http://localhost:8088/tick > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Tick Ingest Server (8088): работает${NC}"
    else
        echo -e "${RED}❌ Tick Ingest Server (8088): не отвечает${NC}"
    fi
    
    # Проверка OBI Service
    if curl -s http://localhost:8090/healthz > /dev/null 2>&1; then
        echo -e "${GREEN}✅ OBI Service (8090): работает${NC}"
        obi_health=$(curl -s http://localhost:8090/healthz)
        echo -e "   ${CYAN}$obi_health${NC}"
    else
        echo -e "${RED}❌ OBI Service (8090): не отвечает${NC}"
    fi
    
    # Проверка Dashboard
    if curl -s http://localhost:8091/healthz > /dev/null 2>&1; then
        echo -e "${GREEN}✅ ROC Dashboard (8091): работает${NC}"
    else
        echo -e "${YELLOW}⚠️  ROC Dashboard (8091): не запущен${NC}"
        echo -e "   Запустите: docker exec -it scanner-python-worker python3 -m dashboard.app"
    fi
    
    echo
}

# Функция для мониторинга логов всех сервисов
monitor_all_logs() {
    print_header "📋 Мониторинг всех логов"
    echo -e "${YELLOW}Нажмите Ctrl+C для выхода${NC}\n"
    docker compose logs -f --tail=50
}

# Функция для мониторинга Go Gateway
monitor_gateway() {
    print_header "🚀 Go Gateway - Входящие тики и события"
    echo -e "${YELLOW}Здесь видны:${NC}"
    echo -e "  • POST /tick - входящие тики от MT5"
    echo -e "  • POST /notify - OBI события"
    echo -e "  • POST /orders/enqueue - постановка ордеров в очередь"
    echo -e "  • GET /orders/poll - получение ордеров MT5 EA\n"
    echo -e "${YELLOW}Нажмите Ctrl+C для выхода${NC}\n"
    docker compose logs -f --tail=100 scanner-go-gateway
}

# Функция для мониторинга Python Worker
monitor_python() {
    print_header "🐍 Python Worker - Обработка и генерация сигналов"
    echo -e "${YELLOW}Здесь видны:${NC}"
    echo -e "  • Обработка тиков"
    echo -e "  • Вычисление Delta Z-score, OBI, Weak Progress"
    echo -e "  • Генерация сигналов (LONG/SHORT)"
    echo -e "  • Постановка ордеров в очередь\n"
    echo -e "${YELLOW}Нажмите Ctrl+C для выхода${NC}\n"
    docker compose logs -f --tail=100 scanner-python-worker
}

# Функция для мониторинга OBI Service
monitor_obi() {
    print_header "📊 OBI Service - Order Book Imbalance"
    echo -e "${YELLOW}Здесь видны:${NC}"
    echo -e "  • POST /book - входящие снэпшоты DOM"
    echo -e "  • Расчет OBI (bid/ask дисбаланс)"
    echo -e "  • OBI события (sustain up/down)\n"
    echo -e "${YELLOW}Нажмите Ctrl+C для выхода${NC}\n"
    docker compose logs -f --tail=100 scanner-py-obi
}

# Функция для открытия Dashboard
open_dashboard() {
    print_header "📈 ROC/AUC Dashboard"
    
    # Проверяем, запущен ли dashboard
    if ! curl -s http://localhost:8091/healthz > /dev/null 2>&1; then
        echo -e "${YELLOW}⚠️  Dashboard не запущен. Запускаю...${NC}\n"
        docker exec -d scanner-python-worker python3 -m dashboard.app
        sleep 3
    fi
    
    echo -e "${GREEN}✅ Dashboard доступен по адресу:${NC}"
    echo -e "   ${CYAN}http://localhost:8091${NC}\n"
    echo -e "${YELLOW}Открываю в браузере...${NC}"
    
    # Пытаемся открыть в браузере
    if command -v xdg-open > /dev/null; then
        xdg-open "http://localhost:8091" 2>/dev/null &
    elif command -v open > /dev/null; then
        open "http://localhost:8091" 2>/dev/null &
    else
        echo -e "${YELLOW}Откройте вручную: http://localhost:8091${NC}"
    fi
}

# Функция для открытия OBI графика
open_obi_chart() {
    print_header "📊 OBI Timeline Chart"
    
    if ! curl -s http://localhost:8090/healthz > /dev/null 2>&1; then
        echo -e "${RED}❌ OBI Service не запущен${NC}"
        return 1
    fi
    
    echo -e "${GREEN}✅ OBI графики доступны:${NC}"
    echo -e "   ${CYAN}OBI Timeline:  http://localhost:8090/render/obi.png?symbol=XAUUSD&last=300${NC}"
    echo -e "   ${CYAN}Depth Profile: http://localhost:8090/render/depth.png?symbol=XAUUSD${NC}\n"
    echo -e "${YELLOW}Открываю OBI Timeline...${NC}"
    
    # Пытаемся открыть в браузере
    if command -v xdg-open > /dev/null; then
        xdg-open "http://localhost:8090/render/obi.png?symbol=XAUUSD&last=300" 2>/dev/null &
    elif command -v open > /dev/null; then
        open "http://localhost:8090/render/obi.png?symbol=XAUUSD&last=300" 2>/dev/null &
    else
        echo -e "${YELLOW}Откройте вручную: http://localhost:8090/render/obi.png?symbol=XAUUSD&last=300${NC}"
    fi
}

# Функция для мониторинга Redis Streams
monitor_redis() {
    print_header "💾 Redis Streams - Прямой мониторинг"
    echo -e "${YELLOW}Доступные streams:${NC}"
    echo -e "  • stream:tick_XAUUSD - входящие тики"
    echo -e "  • stream:signal_xau - генерируемые сигналы"
    echo -e "  • stream:of-spike - Order Flow спайки"
    echo -e "  • stream:of-bar - Order Flow бары\n"
    
    echo -e "${YELLOW}Выберите stream для мониторинга:${NC}"
    echo -e "  ${CYAN}1${NC} - stream:tick_XAUUSD (тики)"
    echo -e "  ${CYAN}2${NC} - stream:signal_xau (сигналы)"
    echo -e "  ${CYAN}3${NC} - stream:of-spike (спайки)"
    echo -e "  ${CYAN}4${NC} - Статистика всех streams"
    echo -e "  ${CYAN}0${NC} - Выход\n"
    
    read -p "Ваш выбор: " choice
    
    case $choice in
        1)
            echo -e "\n${CYAN}Мониторинг stream:tick_XAUUSD (последние 10, затем real-time)${NC}"
            echo -e "${YELLOW}Нажмите Ctrl+C для выхода${NC}\n"
            docker exec -it scanner-redis redis-cli XREAD COUNT 10 BLOCK 0 STREAMS stream:tick_XAUUSD 0
            ;;
        2)
            echo -e "\n${CYAN}Мониторинг stream:signal_xau (последние 10, затем real-time)${NC}"
            echo -e "${YELLOW}Нажмите Ctrl+C для выхода${NC}\n"
            docker exec -it scanner-redis redis-cli XREAD COUNT 10 BLOCK 0 STREAMS stream:signal_xau 0
            ;;
        3)
            echo -e "\n${CYAN}Мониторинг stream:of-spike (последние 10, затем real-time)${NC}"
            echo -e "${YELLOW}Нажмите Ctrl+C для выхода${NC}\n"
            docker exec -it scanner-redis redis-cli XREAD COUNT 10 BLOCK 0 STREAMS stream:of-spike 0
            ;;
        4)
            echo -e "\n${CYAN}Статистика всех streams:${NC}\n"
            docker exec -it scanner-redis redis-cli --scan --pattern "stream:*" | while read stream; do
                length=$(docker exec -it scanner-redis redis-cli XLEN "$stream" | tr -d '\r')
                echo -e "${GREEN}$stream${NC}: $length messages"
            done
            ;;
        0)
            echo -e "${YELLOW}Выход...${NC}"
            ;;
        *)
            echo -e "${RED}Неверный выбор${NC}"
            ;;
    esac
}

# Функция для показа статистики
show_stats() {
    print_header "📊 Статистика системы"
    
    # Статистика контейнеров
    echo -e "${CYAN}=== Docker контейнеры ===${NC}"
    docker compose ps | grep scanner
    echo
    
    # Статистика Redis
    echo -e "${CYAN}=== Redis статистика ===${NC}"
    docker exec scanner-redis redis-cli INFO stats | grep -E "total_connections_received|total_commands_processed|instantaneous_ops_per_sec"
    echo
    
    # Статистика Streams
    echo -e "${CYAN}=== Redis Streams ===${NC}"
    for stream in "stream:tick_XAUUSD" "stream:signal_xau" "stream:of-spike" "stream:of-bar"; do
        length=$(docker exec scanner-redis redis-cli XLEN "$stream" 2>/dev/null | tr -d '\r' || echo "0")
        echo -e "${GREEN}$stream${NC}: $length messages"
    done
    echo
    
    # API статистика
    echo -e "${CYAN}=== API Health Checks ===${NC}"
    
    # Go Gateway
    gw_health=$(curl -s http://localhost:8088/healthz 2>/dev/null || echo '{"ok":false}')
    echo -e "Go Gateway:    $gw_health"
    
    # OBI Service
    obi_health=$(curl -s http://localhost:8090/healthz 2>/dev/null || echo '{"ok":false}')
    echo -e "OBI Service:   $obi_health"
    
    # Dashboard
    dash_health=$(curl -s http://localhost:8091/healthz 2>/dev/null || echo '{"ok":false}')
    echo -e "ROC Dashboard: $dash_health"
    echo
    
    # Последние сигналы
    echo -e "${CYAN}=== Последние 3 сигнала ===${NC}"
    docker exec scanner-redis redis-cli XREVRANGE stream:signal_xau + - COUNT 3 2>/dev/null || echo "Нет данных"
    echo
}

# Функция для открытия всех инструментов мониторинга
monitor_all() {
    print_header "🚀 Запуск всех инструментов мониторинга"
    
    # Проверка сервисов
    check_services
    
    # Открываем Dashboard
    open_dashboard
    sleep 2
    
    # Открываем OBI Chart
    open_obi_chart
    sleep 2
    
    # Показываем статистику
    show_stats
    
    echo
    echo -e "${GREEN}✅ Все инструменты мониторинга запущены${NC}"
    echo -e "\n${CYAN}Доступные URL:${NC}"
    echo -e "  • ROC Dashboard: ${YELLOW}http://localhost:8091${NC}"
    echo -e "  • OBI Timeline:  ${YELLOW}http://localhost:8090/render/obi.png?symbol=XAUUSD&last=300${NC}"
    echo -e "  • OBI API:       ${YELLOW}http://localhost:8090/features/obi?symbol=XAUUSD&last=200${NC}"
    echo -e "  • Go Gateway:    ${YELLOW}http://localhost:8088/healthz${NC}"
    
    echo
    echo -e "${YELLOW}Для просмотра логов используйте:${NC}"
    echo -e "  ./monitor_signals.sh logs        # Все логи"
    echo -e "  ./monitor_signals.sh gateway     # Логи Go Gateway"
    echo -e "  ./monitor_signals.sh python      # Логи Python Worker"
    echo -e "  ./monitor_signals.sh obi         # Логи OBI Service"
}

# Главное меню
show_menu() {
    clear
    print_header "🎯 XAUUSD Signal Monitoring Tool"
    
    echo -e "${CYAN}Выберите режим мониторинга:${NC}\n"
    echo -e "  ${GREEN}1${NC} - 📋 Все логи (все сервисы)"
    echo -e "  ${GREEN}2${NC} - 🚀 Go Gateway (входящие тики и события)"
    echo -e "  ${GREEN}3${NC} - 🐍 Python Worker (обработка и сигналы)"
    echo -e "  ${GREEN}4${NC} - 📊 OBI Service (Order Book Imbalance)"
    echo -e "  ${GREEN}5${NC} - 📈 ROC/AUC Dashboard (веб интерфейс)"
    echo -e "  ${GREEN}6${NC} - 📉 OBI Timeline Chart (веб интерфейс)"
    echo -e "  ${GREEN}7${NC} - 💾 Redis Streams (прямой мониторинг)"
    echo -e "  ${GREEN}8${NC} - 📊 Статистика системы"
    echo -e "  ${GREEN}9${NC} - 🔍 Проверка сервисов"
    echo -e "  ${GREEN}0${NC} - 🚀 Запустить ВСЕ инструменты\n"
    echo -e "  ${RED}q${NC} - Выход\n"
    
    read -p "Ваш выбор: " choice
    
    case $choice in
        1) monitor_all_logs ;;
        2) monitor_gateway ;;
        3) monitor_python ;;
        4) monitor_obi ;;
        5) open_dashboard ;;
        6) open_obi_chart ;;
        7) monitor_redis ;;
        8) show_stats ;;
        9) check_services ;;
        0) monitor_all ;;
        q|Q) echo -e "\n${YELLOW}До свидания!${NC}\n"; exit 0 ;;
        *) echo -e "\n${RED}Неверный выбор. Попробуйте снова.${NC}\n"; sleep 2; show_menu ;;
    esac
}

# Точка входа
main() {
    # Если передан аргумент, выполняем напрямую
    if [ $# -gt 0 ]; then
        case $1 in
            logs) monitor_all_logs ;;
            gateway) monitor_gateway ;;
            python) monitor_python ;;
            obi) monitor_obi ;;
            dashboard) open_dashboard ;;
            obi-chart) open_obi_chart ;;
            redis) monitor_redis ;;
            stats) show_stats ;;
            check) check_services ;;
            all) monitor_all ;;
            help|--help|-h)
                echo "Использование: $0 [опция]"
                echo
                echo "Опции:"
                echo "  logs        - Все логи системы"
                echo "  gateway     - Логи Go Gateway"
                echo "  python      - Логи Python Worker"
                echo "  obi         - Логи OBI Service"
                echo "  dashboard   - ROC/AUC Dashboard"
                echo "  obi-chart   - OBI Timeline Chart"
                echo "  redis       - Redis Streams мониторинг"
                echo "  stats       - Статистика системы"
                echo "  check       - Проверка сервисов"
                echo "  all         - Все инструменты"
                echo
                ;;
            *)
                echo -e "${RED}Неизвестная опция: $1${NC}"
                echo "Используйте: $0 --help"
                exit 1
                ;;
        esac
    else
        # Без аргументов - показываем интерактивное меню
        show_menu
    fi
}

# Запуск
main "$@"

