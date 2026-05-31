#!/bin/bash

# Детальная карта Redis клиентов с анализом контейнеров
# Показывает полную картину подключений

REDIS_HOST=${REDIS_HOST:-localhost}
REDIS_PORT=${REDIS_PORT:-6379}
REDIS_CLI="redis-cli -h $REDIS_HOST -p $REDIS_PORT"

# Цвета
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
NC='\033[0m'

echo -e "${PURPLE}🗺️  Детальная карта Redis клиентов${NC}"
echo -e "${PURPLE}===================================${NC}"
echo

# Получаем информацию о контейнерах
echo -e "${CYAN}🐳 Анализ Docker контейнеров:${NC}"
docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}" | grep -E "(redis|worker|scanner)" | while read line; do
    echo -e "  ${GREEN}$line${NC}"
done
echo

# Получаем сетевую информацию
echo -e "${CYAN}🌐 Сетевая карта Docker:${NC}"
docker network inspect scanner-network 2>/dev/null | jq -r '.[0].Containers | to_entries[] | "\(.value.Name): \(.value.IPv4Address)"' 2>/dev/null || echo "  Не удалось получить сетевую информацию"
echo

# Анализируем клиентов по контейнерам
echo -e "${CYAN}📊 Клиенты по контейнерам:${NC}"

# Получаем список клиентов
CLIENTS_DATA=$($REDIS_CLI client list)

# Создаем маппинг IP -> контейнер
declare -A IP_TO_CONTAINER
IP_TO_CONTAINER["172.18.0.4"]="scanner-redis"
IP_TO_CONTAINER["172.18.0.5"]="scanner-redis-worker-1"
IP_TO_CONTAINER["172.18.0.6"]="scanner-python-worker"
IP_TO_CONTAINER["172.18.0.7"]="scanner-redis-worker-2"
IP_TO_CONTAINER["172.18.0.8"]="scanner-redis-cleanup"
IP_TO_CONTAINER["172.18.0.9"]="scanner-go-worker"
IP_TO_CONTAINER["172.18.0.10"]="scanner-telegram-worker"
IP_TO_CONTAINER["172.18.0.11"]="scanner-signal-parser-worker"
IP_TO_CONTAINER["172.18.0.12"]="scanner-notify-worker"
IP_TO_CONTAINER["172.18.0.1"]="Docker Gateway"

# Анализируем клиентов
echo "$CLIENTS_DATA" | awk '{print $2}' | sed 's/addr=//' | cut -d: -f1 | sort | uniq -c | sort -nr | while read count ip; do
    container=${IP_TO_CONTAINER[$ip]}
    if [ -z "$container" ]; then
        container="Неизвестный ($ip)"
    fi
    
    if [ "$ip" = "172.18.0.9" ]; then
        echo -e "  ${YELLOW}$container${NC}: $count клиентов"
    elif [ "$ip" = "172.18.0.6" ]; then
        echo -e "  ${BLUE}$container${NC}: $count клиентов"
    elif [ "$ip" = "172.18.0.10" ]; then
        echo -e "  ${PURPLE}$container${NC}: $count клиентов"
    elif [ "$ip" = "172.18.0.11" ]; then
        echo -e "  ${CYAN}$container${NC}: $count клиентов"
    elif [ "$ip" = "172.18.0.12" ]; then
        echo -e "  ${WHITE}$container${NC}: $count клиентов"
    elif [ "$ip" = "172.18.0.5" ] || [ "$ip" = "172.18.0.7" ]; then
        echo -e "  ${GREEN}$container${NC}: $count клиентов"
    else
        echo -e "  ${RED}$container${NC}: $count клиентов"
    fi
done
echo

# Анализируем активность по контейнерам
echo -e "${CYAN}⚡ Активность по контейнерам:${NC}"
echo "$CLIENTS_DATA" | while read line; do
    ip=$(echo "$line" | grep -o 'addr=[^ ]*' | sed 's/addr=//' | cut -d: -f1)
    idle=$(echo "$line" | grep -o 'idle=[0-9]*' | sed 's/idle=//')
    cmd=$(echo "$line" | grep -o 'cmd=[^ ]*' | sed 's/cmd=//')
    
    container=${IP_TO_CONTAINER[$ip]}
    if [ -z "$container" ]; then
        container="Неизвестный"
    fi
    
    if [ "$idle" -lt 60 ] && [ "$cmd" != "NULL" ]; then
        echo -e "  ${GREEN}$container${NC}: активен (idle: ${idle}s, cmd: $cmd)"
    fi
done | sort | uniq -c | sort -nr
echo

# Показываем заблокированные соединения
echo -e "${CYAN}🚫 Заблокированные соединения:${NC}"
echo "$CLIENTS_DATA" | grep 'flags=b' | while read line; do
    ip=$(echo "$line" | grep -o 'addr=[^ ]*' | sed 's/addr=//' | cut -d: -f1)
    idle=$(echo "$line" | grep -o 'idle=[0-9]*' | sed 's/idle=//')
    age=$(echo "$line" | grep -o 'age=[0-9]*' | sed 's/age=//')
    
    container=${IP_TO_CONTAINER[$ip]}
    if [ -z "$container" ]; then
        container="Неизвестный"
    fi
    
    echo -e "  ${RED}$container${NC}: заблокирован (idle: ${idle}s, age: ${age}s)"
done
echo

# Создаем ASCII диаграмму
echo -e "${CYAN}🎨 ASCII диаграмма подключений:${NC}"
echo
echo -e "  ${GREEN}┌─────────────────────────────────────────────────────────────┐${NC}"
echo -e "  ${GREEN}│                    Redis Server (6379)                      │${NC}"
echo -e "  ${GREEN}│                  scanner-redis:6379                         │${NC}"
echo -e "  ${GREEN}└─────────────────┬───────────────────────────────────────────┘${NC}"
echo -e "                        ${GREEN}│${NC}"
echo -e "        ${GREEN}┌─────────────┼─────────────┐${NC}"
echo -e "        ${GREEN}│             │             │${NC}"

# Показываем основные подключения
echo "$CLIENTS_DATA" | awk '{print $2}' | sed 's/addr=//' | cut -d: -f1 | sort | uniq -c | sort -nr | head -6 | while read count ip; do
    container=${IP_TO_CONTAINER[$ip]}
    if [ -z "$container" ]; then
        container="Неизвестный"
    fi
    
    if [ "$ip" = "172.18.0.9" ]; then
        echo -e "        ${YELLOW}│${NC}  ${YELLOW}scanner-go-worker${NC}     ${GREEN}│${NC}  $count клиентов"
    elif [ "$ip" = "172.18.0.6" ]; then
        echo -e "        ${BLUE}│${NC}  ${BLUE}scanner-python-worker${NC}  ${GREEN}│${NC}  $count клиентов"
    elif [ "$ip" = "172.18.0.10" ]; then
        echo -e "        ${PURPLE}│${NC}  ${PURPLE}scanner-telegram-worker${NC} ${GREEN}│${NC}  $count клиентов"
    elif [ "$ip" = "172.18.0.11" ]; then
        echo -e "        ${CYAN}│${NC}  ${CYAN}scanner-signal-parser${NC}   ${GREEN}│${NC}  $count клиентов"
    elif [ "$ip" = "172.18.0.12" ]; then
        echo -e "        ${WHITE}│${NC}  ${WHITE}scanner-notify-worker${NC}  ${GREEN}│${NC}  $count клиентов"
    elif [ "$ip" = "172.18.0.5" ] || [ "$ip" = "172.18.0.7" ]; then
        echo -e "        ${GREEN}│${NC}  ${GREEN}$container${NC}        ${GREEN}│${NC}  $count клиентов"
    fi
done

echo -e "        ${GREEN}│             │             │${NC}"
echo -e "        ${GREEN}└─────────────┴─────────────┘${NC}"
echo

# Анализ производительности
echo -e "${CYAN}📈 Анализ производительности:${NC}"
TOTAL_CLIENTS=$(echo "$CLIENTS_DATA" | wc -l)
ACTIVE_CLIENTS=$(echo "$CLIENTS_DATA" | awk '{print $6}' | sed 's/idle=//' | awk '$1 < 60' | wc -l)
IDLE_CLIENTS=$((TOTAL_CLIENTS - ACTIVE_CLIENTS))
BLOCKED_CLIENTS=$(echo "$CLIENTS_DATA" | grep -c 'flags=b')

echo -e "  Всего клиентов: ${GREEN}$TOTAL_CLIENTS${NC}"
echo -e "  Активных (< 1 мин): ${GREEN}$ACTIVE_CLIENTS${NC}"
echo -e "  Простаивающих (> 1 мин): ${YELLOW}$IDLE_CLIENTS${NC}"
echo -e "  Заблокированных: ${RED}$BLOCKED_CLIENTS${NC}"
echo

# Рекомендации
echo -e "${CYAN}💡 Рекомендации:${NC}"
if [ "$IDLE_CLIENTS" -gt 3000 ]; then
    echo -e "  ${YELLOW}⚠️  Критически много простаивающих клиентов ($IDLE_CLIENTS)${NC}"
    echo -e "     Рекомендация: Уменьшите размер пула соединений в Go клиенте"
fi

if [ "$BLOCKED_CLIENTS" -gt 5 ]; then
    echo -e "  ${YELLOW}⚠️  Много заблокированных клиентов ($BLOCKED_CLIENTS)${NC}"
    echo -e "     Рекомендация: Проверьте операции XREAD и XREADGROUP"
fi

if [ "$TOTAL_CLIENTS" -gt 5000 ]; then
    echo -e "  ${YELLOW}⚠️  Высокое общее количество клиентов ($TOTAL_CLIENTS)${NC}"
    echo -e "     Рекомендация: Рассмотрите увеличение maxclients в Redis"
fi

echo -e "  ${GREEN}✅ Детальная карта создана успешно!${NC}"
