#!/bin/bash
# Verification script for OHLC Aggregator & Go Gateway fixes
# Date: October 31, 2025

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🔍 Проверка исправлений OHLC Aggregator & Go Gateway"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if services are running
echo "1️⃣  Проверка статуса сервисов..."
echo ""

if docker ps | grep -q "scanner-ohlc-aggregator"; then
    echo -e "${GREEN}✅ scanner-ohlc-aggregator запущен${NC}"
else
    echo -e "${RED}❌ scanner-ohlc-aggregator НЕ запущен${NC}"
fi

if docker ps | grep -q "scanner-go-gateway"; then
    echo -e "${GREEN}✅ scanner-go-gateway запущен${NC}"
else
    echo -e "${RED}❌ scanner-go-gateway НЕ запущен${NC}"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "2️⃣  Проверка логов OHLC Aggregator (последние 20 строк)..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
docker logs --tail 20 scanner-ohlc-aggregator 2>&1 | tail -20
echo ""

# Check for None/N/A errors
if docker logs --tail 50 scanner-ohlc-aggregator 2>&1 | grep -q "Текущий день: None"; then
    echo -e "${RED}❌ ОШИБКА: Найдено 'Текущий день: None' в логах!${NC}"
else
    echo -e "${GREEN}✅ Нет ошибок 'None' в логах OHLC Aggregator${NC}"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "3️⃣  Проверка логов Go Gateway (последние 30 строк)..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
docker logs --tail 30 scanner-go-gateway 2>&1 | tail -30
echo ""

# Check for truncated logs
if docker logs --tail 50 scanner-go-gateway 2>&1 | grep -E "^2025/[0-9]$"; then
    echo -e "${RED}❌ ОШИБКА: Найдены обрезанные логи!${NC}"
else
    echo -e "${GREEN}✅ Нет обрезанных логов в Go Gateway${NC}"
fi

# Check for successful initialization
if docker logs --tail 50 scanner-go-gateway 2>&1 | grep -q "All systems initialized successfully"; then
    echo -e "${GREEN}✅ Go Gateway успешно инициализирован${NC}"
else
    echo -e "${YELLOW}⚠️  Не найдено сообщение 'All systems initialized successfully'${NC}"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "4️⃣  Проверка Redis данных OHLC..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

PIVOTS_DATA=$(docker exec scanner-redis redis-cli GET "pivots:latest" 2>/dev/null)
if [ -n "$PIVOTS_DATA" ] && [ "$PIVOTS_DATA" != "(nil)" ]; then
    echo -e "${GREEN}✅ Данные pivots:latest найдены в Redis:${NC}"
    echo "$PIVOTS_DATA" | python3 -m json.tool 2>/dev/null || echo "$PIVOTS_DATA"
else
    echo -e "${YELLOW}⚠️  Данные pivots:latest отсутствуют (нормально если тики еще не поступали)${NC}"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "5️⃣  Проверка Go Gateway endpoints..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Health check
HEALTH_RESPONSE=$(curl -s http://localhost:8090/healthz 2>/dev/null)
if echo "$HEALTH_RESPONSE" | grep -q "ok"; then
    echo -e "${GREEN}✅ /healthz endpoint работает${NC}"
    echo "   Response: $HEALTH_RESPONSE"
else
    echo -e "${RED}❌ /healthz endpoint не отвечает${NC}"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "6️⃣  Проверка памяти и CPU..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}" | grep -E "(scanner-ohlc-aggregator|scanner-go-gateway|NAME)"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ Проверка завершена!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📋 Для детального мониторинга используйте:"
echo "   docker logs -f scanner-ohlc-aggregator"
echo "   docker logs -f scanner-go-gateway"
echo ""

