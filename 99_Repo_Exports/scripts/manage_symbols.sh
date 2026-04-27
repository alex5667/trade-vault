#!/bin/bash
# Symbol Management CLI - Управление символами через Redis stream

REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PORT="${REDIS_PORT:-6379}"
STREAM="config:symbols"

# Цвета
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

# Функция для публикации команды в Redis
publish_command() {
    local action=$1
    shift
    local symbols="$@"
    
    # Создаем JSON
    local symbols_json=$(printf '%s\n' "${symbols[@]}" | jq -R . | jq -s .)
    local command="{\"action\":\"$action\",\"symbols\":$symbols_json,\"ts\":$(date +%s)000}"
    
    # Публикуем в Redis stream
    redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" XADD "$STREAM" "*" data "$command" >/dev/null
    
    echo -e "${GREEN}✅ Command published: $action $symbols${NC}"
}

# Функция для получения текущего списка
get_current_symbols() {
    local current=$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" GET "config:symbols:current" 2>/dev/null)
    if [ -n "$current" ]; then
        echo "$current" | jq -r '.[]' 2>/dev/null
    fi
}

# Команды
case "${1:-help}" in
    "add")
        shift
        if [ $# -eq 0 ]; then
            echo -e "${RED}Error: No symbols specified${NC}"
            echo "Usage: $0 add BTCUSD ETHUSD"
            exit 1
        fi
        
        echo -e "${YELLOW}Adding symbols: $@${NC}"
        publish_command "add" "$@"
        ;;
    
    "remove")
        shift
        if [ $# -eq 0 ]; then
            echo -e "${RED}Error: No symbols specified${NC}"
            echo "Usage: $0 remove BTCUSD"
            exit 1
        fi
        
        echo -e "${YELLOW}Removing symbols: $@${NC}"
        publish_command "remove" "$@"
        ;;
    
    "set")
        shift
        if [ $# -eq 0 ]; then
            echo -e "${RED}Error: No symbols specified${NC}"
            echo "Usage: $0 set XAUUSD BTCUSD ETHUSD"
            exit 1
        fi
        
        echo -e "${YELLOW}Setting symbols: $@${NC}"
        publish_command "set" "$@"
        ;;
    
    "list")
        echo -e "${BLUE}Current symbols:${NC}"
        current=$(get_current_symbols)
        if [ -n "$current" ]; then
            echo "$current" | while read symbol; do
                echo -e "  ${GREEN}✓${NC} $symbol"
            done
        else
            echo -e "  ${YELLOW}No symbols configured${NC}"
        fi
        ;;
    
    "status")
        echo -e "${BLUE}Checking multi-symbol-orderflow logs...${NC}"
        docker logs scanner-multi-orderflow 2>&1 | grep -E "Handler for .* started|Symbol .* (added|removed)" | tail -10
        ;;
    
    *)
        echo "Symbol Management CLI"
        echo ""
        echo "Usage: $0 {add|remove|set|list|status} [symbols...]"
        echo ""
        echo "Commands:"
        echo "  add SYMBOL...      Добавить символы (создаст handlers)"
        echo "  remove SYMBOL...   Удалить символы (остановит handlers)"
        echo "  set SYMBOL...      Установить список (заменит текущий)"
        echo "  list               Показать текущие символы"
        echo "  status             Статус handlers из логов"
        echo ""
        echo "Examples:"
        echo "  $0 add BTCUSD ETHUSD         # Добавить Bitcoin и Ethereum"
        echo "  $0 remove BTCUSD             # Удалить Bitcoin"
        echo "  $0 set XAUUSD BTCUSD         # Оставить только Gold и Bitcoin"
        echo "  $0 list                      # Показать активные"
        echo ""
        echo "Environment:"
        echo "  REDIS_HOST=${REDIS_HOST}"
        echo "  REDIS_PORT=${REDIS_PORT}"
        echo "  STREAM=${STREAM}"
        ;;
esac

