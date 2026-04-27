#!/bin/bash
# Быстрая проверка статуса ATR Integration

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${YELLOW}════════════════════════════════════════════════════${NC}"
echo -e "${YELLOW}  ATR Integration Status Check${NC}"
echo -e "${YELLOW}════════════════════════════════════════════════════${NC}"
echo ""

# 1. Контейнеры
echo -e "${YELLOW}📦 Контейнеры:${NC}"
ATR_STATUS=$(docker ps --format "{{.Names}}" | grep scanner-atr-worker)
if [ -n "$ATR_STATUS" ]; then
    echo -e "  ✅ atr-worker: ${GREEN}RUNNING${NC}"
else
    echo -e "  ❌ atr-worker: ${RED}NOT RUNNING${NC}"
fi

REDIS_STATUS=$(docker ps --format "{{.Names}}" | grep scanner-redis-worker-1)
if [ -n "$REDIS_STATUS" ]; then
    echo -e "  ✅ redis-worker-1: ${GREEN}RUNNING${NC}"
else
    echo -e "  ❌ redis-worker-1: ${RED}NOT RUNNING${NC}"
fi

echo ""

# 2. ATR в Redis
echo -e "${YELLOW}📊 ATR в Redis:${NC}"
ATR_VALUE=$(docker exec scanner-redis-worker-1 redis-cli GET ta:last:atr:XAUUSD 2>/dev/null)
if [ -n "$ATR_VALUE" ]; then
    echo -e "  ✅ Ключ ta:last:atr:XAUUSD: ${GREEN}EXISTS${NC}"
    echo "  Значение:"
    echo "$ATR_VALUE" | python3 -m json.tool 2>/dev/null || echo "$ATR_VALUE"
else
    echo -e "  ⚠️  Ключ ta:last:atr:XAUUSD: ${YELLOW}NOT FOUND${NC}"
fi

echo ""

# 3. Свечи в stream
echo -e "${YELLOW}📈 Свечи:${NC}"
CANDLES_COUNT=$(docker exec scanner-redis-worker-1 redis-cli XLEN candles:data 2>/dev/null)
echo "  candles:data: $CANDLES_COUNT свечей"

echo ""

# 4. Consumer group
echo -e "${YELLOW}👥 Consumer Group:${NC}"
GROUP_INFO=$(docker exec scanner-redis-worker-1 redis-cli XINFO GROUPS candles:data 2>/dev/null | grep -A 20 "atr-worker-group" | head -20)
if [ -n "$GROUP_INFO" ]; then
    echo -e "  ✅ atr-worker-group: ${GREEN}EXISTS${NC}"
    LAG=$(echo "$GROUP_INFO" | grep -A 1 "^lag$" | tail -1)
    echo "  Lag: $LAG"
else
    echo -e "  ⚠️  atr-worker-group: ${YELLOW}NOT FOUND${NC}"
fi

echo ""

# 5. Последние логи atr-worker
echo -e "${YELLOW}📝 Последние логи atr-worker:${NC}"
docker logs --tail=5 scanner-atr-worker 2>&1

echo ""

# 6. Изменения в git
echo -e "${YELLOW}📝 Git статус:${NC}"
cd /home/alex/front/trade/scanner_infra 2>/dev/null
MODIFIED=$(git status --short | grep "^ M" | wc -l)
UNTRACKED=$(git status --short | grep "^??" | wc -l)
echo "  Измененных файлов: $MODIFIED"
echo "  Новых файлов: $UNTRACKED"

echo ""
echo -e "${GREEN}════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  Проверка завершена${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════${NC}"

