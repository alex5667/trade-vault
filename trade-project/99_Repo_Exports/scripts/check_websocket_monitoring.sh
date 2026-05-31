#!/bin/bash

# Скрипт для проверки системы мониторинга WebSocket соединений

set -e

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Функция для вывода заголовка
print_header() {
    echo -e "\n${BLUE}=== $1 ===${NC}"
}

# Функция для проверки статуса
check_status() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✅ $2${NC}"
    else
        echo -e "${RED}❌ $2${NC}"
        return 1
    fi
}

# Проверяем, что Docker контейнеры запущены
print_header "Проверка Docker контейнеров"

echo "Проверяем статус контейнеров..."
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep scanner

# Проверяем доступность метрик Prometheus
print_header "Проверка метрик Prometheus"

echo "Проверяем метрики go-worker..."
if curl -s http://localhost:2112/metrics | grep -q "websocket_"; then
    check_status 0 "Метрики WebSocket доступны"
else
    check_status 1 "Метрики WebSocket недоступны"
fi

# Проверяем HTTP endpoints мониторинга
print_header "Проверка HTTP endpoints мониторинга"

echo "Проверяем /monitoring/websocket/stats..."
if curl -s http://localhost:2112/monitoring/websocket/stats | jq . > /dev/null 2>&1; then
    check_status 0 "Endpoint /monitoring/websocket/stats работает"
else
    check_status 1 "Endpoint /monitoring/websocket/stats недоступен"
fi

echo "Проверяем /monitoring/websocket/alerts..."
if curl -s http://localhost:2112/monitoring/websocket/alerts | jq . > /dev/null 2>&1; then
    check_status 0 "Endpoint /monitoring/websocket/alerts работает"
else
    check_status 1 "Endpoint /monitoring/websocket/alerts недоступен"
fi

echo "Проверяем /monitoring/websocket/health..."
if curl -s http://localhost:2112/monitoring/websocket/health | jq . > /dev/null 2>&1; then
    check_status 0 "Endpoint /monitoring/websocket/health работает"
else
    check_status 1 "Endpoint /monitoring/websocket/health недоступен"
fi

# Проверяем доступность Prometheus
print_header "Проверка Prometheus"

echo "Проверяем доступность Prometheus..."
if curl -s http://localhost:9090/api/v1/status/config | jq . > /dev/null 2>&1; then
    check_status 0 "Prometheus доступен"
else
    check_status 1 "Prometheus недоступен"
fi

# Проверяем доступность Grafana
print_header "Проверка Grafana"

echo "Проверяем доступность Grafana..."
if curl -s http://localhost:3001/api/health | jq . > /dev/null 2>&1; then
    check_status 0 "Grafana доступен"
else
    check_status 1 "Grafana недоступен"
fi

# Показываем статистику WebSocket соединений
print_header "Статистика WebSocket соединений"

echo "Получаем статистику..."
STATS=$(curl -s http://localhost:2112/monitoring/websocket/stats)
echo "$STATS" | jq '.stats | to_entries | length' | xargs echo "Количество отслеживаемых соединений:"

echo -e "\n${YELLOW}Детальная статистика:${NC}"
echo "$STATS" | jq -r '.stats | to_entries[] | "\(.key): подключен=\(.value.is_connected), сообщений=\(.value.message_count), ошибок=\(.value.error_count)"'

# Показываем активные алерты
print_header "Активные алерты"

echo "Получаем алерты..."
ALERTS=$(curl -s http://localhost:2112/monitoring/websocket/alerts)
ALERT_COUNT=$(echo "$ALERTS" | jq '.count')

if [ "$ALERT_COUNT" -gt 0 ]; then
    echo -e "${RED}⚠️  Найдено $ALERT_COUNT активных алертов:${NC}"
    echo "$ALERTS" | jq -r '.alerts[] | "\(.severity): \(.type) - \(.symbol)@\(.timeframe)"'
else
    echo -e "${GREEN}✅ Активных алертов нет${NC}"
fi

# Показываем метрики Prometheus
print_header "Ключевые метрики Prometheus"

echo "Проверяем метрики WebSocket..."
curl -s http://localhost:9090/api/v1/query?query=websocket_connection_status | jq -r '.data.result[] | "\(.metric.symbol)@\(.metric.timeframe): \(.value[1])"'

echo -e "\n${YELLOW}Метрики сообщений:${NC}"
curl -s http://localhost:9090/api/v1/query?query=websocket_messages_received_total | jq -r '.data.result[] | "\(.metric.symbol)@\(.metric.timeframe): \(.value[1]) сообщений"' | tail -5

echo -e "\n${YELLOW}Метрики ошибок:${NC}"
curl -s http://localhost:9090/api/v1/query?query=websocket_errors_total | jq -r '.data.result[] | "\(.metric.symbol)@\(.metric.timeframe): \(.value[1]) ошибок"' | tail -5

print_header "Проверка завершена"

echo -e "${GREEN}🎉 Система мониторинга WebSocket соединений готова к работе!${NC}"
echo -e "${BLUE}📊 Доступные интерфейсы:${NC}"
echo -e "  • Prometheus: http://localhost:9090"
echo -e "  • Grafana: http://localhost:3001 (admin/admin)"
echo -e "  • Метрики: http://localhost:2112/metrics"
echo -e "  • Статистика: http://localhost:2112/monitoring/websocket/stats"
echo -e "  • Алерты: http://localhost:2112/monitoring/websocket/alerts"
echo -e "  • Здоровье: http://localhost:2112/monitoring/websocket/health" 