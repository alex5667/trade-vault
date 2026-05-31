#!/bin/bash
# Мониторинг системы сигналов в реальном времени
# Senior Developer + Trading Analyst

echo "🎯 МОНИТОРИНГ СИСТЕМЫ СИГНАЛОВ"
echo "================================"
echo ""

# Цвета
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 1. Статус сервисов
echo -e "${BLUE}1️⃣ СТАТУС СЕРВИСОВ${NC}"
echo "-------------------"
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "(aggregated-hub|signal-generator|notify-worker|redis-ticks)" || echo "  ⚠️ Нет запущенных сервисов"
echo ""

# 2. Streams статистика
echo -e "${BLUE}2️⃣ STREAMS СТАТИСТИКА${NC}"
echo "----------------------"
echo -n "signals:aggregated:XAUUSD:  "
docker exec redis-worker-1 redis-cli XLEN signals:aggregated:XAUUSD 2>/dev/null || echo "0"
echo -n "signals:orderflow:XAUUSD:   "
docker exec redis-worker-1 redis-cli XLEN signals:orderflow:XAUUSD 2>/dev/null || echo "0"
echo -n "signals:ta:XAUUSD:          "
docker exec redis-worker-1 redis-cli XLEN signals:ta:XAUUSD 2>/dev/null || echo "0"
echo -n "notify:telegram:            "
docker exec redis-worker-1 redis-cli XLEN notify:telegram 2>/dev/null || echo "0"
echo ""

# 3. Consumer Groups
echo -e "${BLUE}3️⃣ CONSUMER GROUPS${NC}"
echo "-------------------"
for stream in signals:aggregated:XAUUSD signals:orderflow:XAUUSD signals:ta:XAUUSD notify:telegram; do
    echo "Stream: $stream"
    docker exec redis-worker-1 redis-cli XINFO GROUPS $stream 2>/dev/null | grep -E "(name|pending|entries-read)" | head -6 | paste - - - | awk '{print "  Group:", $2, "| Pending:", $4, "| Read:", $6}'
    echo ""
done

# 4. Последние сигналы
echo -e "${BLUE}4️⃣ ПОСЛЕДНИЕ СИГНАЛЫ${NC}"
echo "---------------------"
echo "aggregated-hub:"
docker logs --tail 3 scanner-aggregated-hub 2>&1 | grep "Signal #" | tail -1 | sed 's/.*| /  /'
echo ""
echo "signal-generator:"
docker logs --tail 3 scanner-signal-generator 2>&1 | grep "🔔" | tail -1 | sed 's/.*| /  /'
echo ""
echo "notify-worker:"
docker logs --tail 3 scanner-notify-worker 2>&1 | grep "отправлен" | tail -1 | sed 's/^/  /'
echo ""

# 5. Redis Ticks Health
echo -e "${BLUE}5️⃣ REDIS-TICKS HEALTH${NC}"
echo "----------------------"
docker exec scanner-redis-ticks redis-cli INFO stats 2>/dev/null | grep -E "total_connections_received|total_commands_processed" | sed 's/^/  /'
echo ""

# 6. Команды для детального мониторинга
echo -e "${YELLOW}📋 КОМАНДЫ ДЛЯ МОНИТОРИНГА:${NC}"
echo "--------------------------------"
echo "  # Логи в реальном времени:"
echo "  docker-compose logs -f aggregated-hub signal-generator notify-worker"
echo ""
echo "  # Проверка consumer groups:"
echo "  docker exec redis-worker-1 redis-cli XINFO GROUPS signals:aggregated:XAUUSD"
echo ""
echo "  # Последние сигналы в stream:"
echo "  docker exec redis-worker-1 redis-cli XREVRANGE signals:aggregated:XAUUSD + - COUNT 5"
echo ""
echo "  # Проверка Telegram отправки:"
echo "  docker logs --tail 50 scanner-notify-worker | grep -E '(отправлен|direction)'"
echo ""

echo -e "${GREEN}✅ Мониторинг завершен${NC}"
echo ""

