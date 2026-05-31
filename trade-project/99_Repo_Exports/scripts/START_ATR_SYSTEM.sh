#!/bin/bash
# Запуск системы с исправленным ATR Integration
# October 31, 2025

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${YELLOW}════════════════════════════════════════════════════${NC}"
echo -e "${YELLOW}  Запуск системы с ATR Integration${NC}"
echo -e "${YELLOW}════════════════════════════════════════════════════${NC}"
echo ""

# Проверка docker-compose.yml
if [ ! -f "docker-compose.yml" ]; then
    echo -e "${RED}❌ docker-compose.yml не найден${NC}"
    exit 1
fi

echo -e "${YELLOW}📋 Шаг 1: Остановка и очистка${NC}"
docker-compose down 2>/dev/null || true

echo ""
echo -e "${YELLOW}📋 Шаг 2: Пересборка образов${NC}"
docker-compose build python-worker

echo ""
echo -e "${YELLOW}📋 Шаг 3: Запуск базовых сервисов (Redis)${NC}"
docker-compose up -d redis redis-worker-1 redis-worker-2

echo ""
echo -e "${YELLOW}⏳ Ожидание готовности Redis (10 секунд)${NC}"
sleep 10

echo ""
echo -e "${YELLOW}📋 Шаг 4: Запуск go-worker-1m (генерация свечей)${NC}"
docker-compose up -d go-worker-1m

echo ""
echo -e "${YELLOW}⏳ Ожидание первых свечей (15 секунд)${NC}"
sleep 15

echo ""
echo -e "${YELLOW}📋 Шаг 5: Запуск atr-worker (вычисление ATR)${NC}"
docker-compose up -d atr-worker

echo ""
echo -e "${YELLOW}⏳ Ожидание инициализации atr-worker (10 секунд)${NC}"
sleep 10

echo ""
echo -e "${YELLOW}📋 Шаг 6: Запуск сигнальных сервисов${NC}"
docker-compose up -d python-worker scanner-aggregated-hub scanner-signal-hub

echo ""
echo -e "${YELLOW}⏳ Ожидание инициализации (20 секунд)${NC}"
sleep 20

echo ""
echo -e "${GREEN}✅ Система запущена!${NC}"
echo ""

# Проверка статуса
echo -e "${YELLOW}📊 Статус сервисов:${NC}"
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "(atr-worker|redis|python-worker|aggregated-hub|signal-hub|go-worker-1m)" | head -20

echo ""
echo -e "${YELLOW}📊 ATR в Redis:${NC}"
ATR=$(docker exec scanner-redis-worker-1 redis-cli GET ta:last:atr:XAUUSD 2>/dev/null)
if [ -n "$ATR" ]; then
    echo -e "  ${GREEN}✅ ta:last:atr:XAUUSD присутствует${NC}"
    echo "$ATR" | python3 -m json.tool 2>/dev/null | head -10 || echo "$ATR"
else
    echo -e "  ${YELLOW}⚠️  ta:last:atr:XAUUSD пока не создан (подождите 1-2 минуты)${NC}"
fi

echo ""
echo -e "${YELLOW}📊 Свечи в stream:${NC}"
CANDLES=$(docker exec scanner-redis-worker-1 redis-cli XLEN candles:data 2>/dev/null)
echo "  candles:data: $CANDLES свечей"

echo ""
echo -e "${GREEN}════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  Команды для мониторинга:${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════${NC}"
echo ""
echo "  # Логи atr-worker"
echo "  docker logs -f scanner-atr-worker"
echo ""
echo "  # ATR в реальном времени"
echo "  watch -n 2 'docker exec scanner-redis-worker-1 redis-cli GET ta:last:atr:XAUUSD | python3 -m json.tool'"
echo ""
echo "  # Все сервисы используют Redis ATR"
echo "  docker-compose logs -f | grep \"ATR from Redis\""
echo ""
echo "  # Статус системы"
echo "  ./check_atr_status.sh"
echo ""
echo -e "${GREEN}🎉 Готово! ATR Integration активен!${NC}"

