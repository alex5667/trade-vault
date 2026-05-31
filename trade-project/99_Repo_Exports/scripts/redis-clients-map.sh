#!/bin/bash

# Redis Clients Map - карта подключений клиентов
# Показывает кто и куда подключен к Redis

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

echo -e "${PURPLE}🗺️  Redis Clients Map${NC}"
echo -e "${PURPLE}====================${NC}"
echo

# Получаем список всех клиентов
CLIENTS_DATA=$($REDIS_CLI client list)

# Подсчитываем общую статистику
TOTAL_CLIENTS=$(echo "$CLIENTS_DATA" | wc -l)
echo -e "${CYAN}📊 Общая статистика:${NC}"
echo -e "  Всего клиентов: ${GREEN}$TOTAL_CLIENTS${NC}"
echo

# Анализируем по IP адресам
echo -e "${CYAN}🌐 Клиенты по IP адресам:${NC}"
echo "$CLIENTS_DATA" | awk '{print $2}' | sed 's/addr=//' | cut -d: -f1 | sort | uniq -c | sort -nr | while read count ip; do
    if [ "$ip" = "172.18.0.4" ]; then
        echo -e "  ${GREEN}$ip${NC}: $count клиентов (Redis сервер)"
    elif [ "$ip" = "172.18.0.6" ]; then
        echo -e "  ${BLUE}$ip${NC}: $count клиентов (scanner-python-worker)"
    elif [ "$ip" = "172.18.0.9" ]; then
        echo -e "  ${YELLOW}$ip${NC}: $count клиентов (scanner-go-worker)"
    elif [ "$ip" = "172.18.0.10" ]; then
        echo -e "  ${PURPLE}$ip${NC}: $count клиентов (scanner-telegram-worker)"
    elif [ "$ip" = "172.18.0.11" ]; then
        echo -e "  ${CYAN}$ip${NC}: $count клиентов (scanner-signal-parser-worker)"
    elif [ "$ip" = "172.18.0.12" ]; then
        echo -e "  ${WHITE}$ip${NC}: $count клиентов (scanner-notify-worker)"
    else
        echo -e "  ${RED}$ip${NC}: $count клиентов (неизвестный)"
    fi
done
echo

# Анализируем по библиотекам
echo -e "${CYAN}📚 Клиенты по библиотекам:${NC}"
echo "$CLIENTS_DATA" | grep -o 'lib-name=[^ ]*' | sed 's/lib-name=//' | sort | uniq -c | sort -nr | while read count lib; do
    if [ "$lib" = "redis-py" ]; then
        echo -e "  ${GREEN}redis-py${NC}: $count клиентов (Python)"
    elif [ "$lib" = "go-redis" ]; then
        echo -e "  ${BLUE}go-redis${NC}: $count клиентов (Go)"
    elif [ "$lib" = "ioredis" ]; then
        echo -e "  ${YELLOW}ioredis${NC}: $count клиентов (Node.js)"
    elif [ "$lib" = "" ]; then
        echo -e "  ${PURPLE}Неизвестная${NC}: $count клиентов"
    else
        echo -e "  ${CYAN}$lib${NC}: $count клиентов"
    fi
done
echo

# Анализируем по командам
echo -e "${CYAN}⚡ Активные команды:${NC}"
echo "$CLIENTS_DATA" | grep -o 'cmd=[^ ]*' | sed 's/cmd=//' | sort | uniq -c | sort -nr | head -10 | while read count cmd; do
    if [ "$cmd" = "xadd" ]; then
        echo -e "  ${GREEN}XADD${NC}: $count клиентов (добавление в стримы)"
    elif [ "$cmd" = "xread" ]; then
        echo -e "  ${BLUE}XREAD${NC}: $count клиентов (чтение стримов)"
    elif [ "$cmd" = "ping" ]; then
        echo -e "  ${YELLOW}PING${NC}: $count клиентов (проверка соединения)"
    elif [ "$cmd" = "set" ]; then
        echo -e "  ${PURPLE}SET${NC}: $count клиентов (установка значений)"
    elif [ "$cmd" = "get" ]; then
        echo -e "  ${CYAN}GET${NC}: $count клиентов (получение значений)"
    elif [ "$cmd" = "NULL" ]; then
        echo -e "  ${WHITE}IDLE${NC}: $count клиентов (простой)"
    else
        echo -e "  ${RED}$cmd${NC}: $count клиентов"
    fi
done
echo

# Анализируем по времени простоя
echo -e "${CYAN}⏰ Клиенты по времени простоя:${NC}"
IDLE_0_10=$(echo "$CLIENTS_DATA" | awk '{print $6}' | sed 's/idle=//' | awk '$1 <= 10' | wc -l)
IDLE_10_60=$(echo "$CLIENTS_DATA" | awk '{print $6}' | sed 's/idle=//' | awk '$1 > 10 && $1 <= 60' | wc -l)
IDLE_60_300=$(echo "$CLIENTS_DATA" | awk '{print $6}' | sed 's/idle=//' | awk '$1 > 60 && $1 <= 300' | wc -l)
IDLE_300_PLUS=$(echo "$CLIENTS_DATA" | awk '{print $6}' | sed 's/idle=//' | awk '$1 > 300' | wc -l)

echo -e "  ${GREEN}0-10 сек${NC}: $IDLE_0_10 клиентов (активные)"
echo -e "  ${YELLOW}10-60 сек${NC}: $IDLE_10_60 клиентов (умеренно активные)"
echo -e "  ${BLUE}1-5 мин${NC}: $IDLE_60_300 клиентов (малоактивные)"
echo -e "  ${RED}5+ мин${NC}: $IDLE_300_PLUS клиентов (простаивающие)"
echo

# Анализируем по базам данных
echo -e "${CYAN}🗄️  Клиенты по базам данных:${NC}"
echo "$CLIENTS_DATA" | grep -o 'db=[0-9]*' | sed 's/db=//' | sort | uniq -c | sort -nr | while read count db; do
    echo -e "  ${GREEN}DB $db${NC}: $count клиентов"
done
echo

# Показываем топ-10 самых активных клиентов
echo -e "${CYAN}🔥 Топ-10 самых активных клиентов:${NC}"
echo "$CLIENTS_DATA" | awk '{print $2, $6, $7, $8}' | sed 's/addr=//; s/idle=//; s/flags=//; s/age=//' | sort -k2 -n | head -10 | while read ip idle flags age; do
    if [ "$idle" -lt 10 ]; then
        status="${GREEN}Очень активный${NC}"
    elif [ "$idle" -lt 60 ]; then
        status="${YELLOW}Активный${NC}"
    elif [ "$idle" -lt 300 ]; then
        status="${BLUE}Умеренно активный${NC}"
    else
        status="${RED}Простаивающий${NC}"
    fi
    echo -e "  ${ip}:${NC} $status (idle: ${idle}s, age: ${age}s)"
done
echo

# Показываем заблокированные клиенты
BLOCKED_COUNT=$(echo "$CLIENTS_DATA" | grep -c 'flags=b')
if [ "$BLOCKED_COUNT" -gt 0 ]; then
    echo -e "${CYAN}🚫 Заблокированные клиенты:${NC}"
    echo "$CLIENTS_DATA" | grep 'flags=b' | awk '{print $2, $6, $7}' | sed 's/addr=//; s/idle=//; s/flags=//' | while read ip idle flags; do
        echo -e "  ${RED}$ip${NC}: заблокирован (idle: ${idle}s)"
    done
    echo
fi

# Создаем визуальную карту
echo -e "${CYAN}🗺️  Визуальная карта подключений:${NC}"
echo
echo -e "  ${GREEN}┌─────────────────────────────────────────────────────────────┐${NC}"
echo -e "  ${GREEN}│                    Redis Server (6379)                      │${NC}"
echo -e "  ${GREEN}└─────────────────┬───────────────────────────────────────────┘${NC}"
echo -e "                        ${GREEN}│${NC}"
echo -e "        ${GREEN}┌─────────────┼─────────────┐${NC}"
echo -e "        ${GREEN}│             │             │${NC}"

# Показываем основные подключения
echo "$CLIENTS_DATA" | awk '{print $2}' | sed 's/addr=//' | cut -d: -f1 | sort | uniq -c | sort -nr | head -5 | while read count ip; do
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
    fi
done

echo -e "        ${GREEN}│             │             │${NC}"
echo -e "        ${GREEN}└─────────────┴─────────────┘${NC}"
echo

# Финальная сводка
echo -e "${CYAN}📋 Сводка:${NC}"
echo -e "  • Всего клиентов: ${GREEN}$TOTAL_CLIENTS${NC}"
echo -e "  • Активных (idle < 10s): ${GREEN}$IDLE_0_10${NC}"
echo -e "  • Простаивающих (idle > 5min): ${RED}$IDLE_300_PLUS${NC}"
echo -e "  • Заблокированных: ${RED}$BLOCKED_COUNT${NC}"
echo

echo -e "${PURPLE}🎯 Рекомендации:${NC}"
if [ "$IDLE_300_PLUS" -gt 100 ]; then
    echo -e "  ${YELLOW}⚠️  Много простаивающих клиентов - рассмотрите оптимизацию пула соединений${NC}"
fi
if [ "$BLOCKED_COUNT" -gt 10 ]; then
    echo -e "  ${YELLOW}⚠️  Много заблокированных клиентов - проверьте операции XREAD${NC}"
fi
if [ "$TOTAL_CLIENTS" -gt 5000 ]; then
    echo -e "  ${YELLOW}⚠️  Высокое количество клиентов - рассмотрите увеличение maxclients${NC}"
fi

echo -e "  ${GREEN}✅ Карта клиентов создана успешно!${NC}"
