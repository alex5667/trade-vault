#!/bin/bash
# Скрипт для проверки потока данных regime

echo "🔍 ПРОВЕРКА ПОТОКА ДАННЫХ REGIME"
echo "=================================="
echo ""

# Цвета для вывода
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Функция проверки Redis
check_redis() {
    local container=$1
    local port=$2
    local stream=$3
    
    echo -e "${YELLOW}Проверка $container (порт $port)...${NC}"
    
    # Проверяем доступность
    if docker exec $container redis-cli -p 6379 ping > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Redis доступен${NC}"
        
        # Проверяем наличие stream
        local count=$(docker exec $container redis-cli -p 6379 XLEN $stream 2>/dev/null || echo "0")
        echo -e "   Stream: $stream"
        echo -e "   Сообщений: $count"
        
        if [ "$count" -gt 0 ]; then
            echo -e "${GREEN}   ✅ Данные присутствуют${NC}"
            # Показываем последнее сообщение
            echo -e "${YELLOW}   Последнее сообщение:${NC}"
            docker exec $container redis-cli -p 6379 XREVRANGE $stream + - COUNT 1 | head -20
        else
            echo -e "${RED}   ⚠️ Данных нет${NC}"
        fi
    else
        echo -e "${RED}❌ Redis недоступен${NC}"
    fi
    echo ""
}

# Проверяем основной Redis (источник kline данных)
echo "1️⃣ ИСТОЧНИК ДАННЫХ (redis:6379)"
echo "--------------------------------"
check_redis "scanner-redis" "6379" "stream:kline_1m"

# Проверяем redis-worker-1 (приемник regime данных)
echo "2️⃣ ПРИЕМНИК ДАННЫХ (redis-worker-1:6380)"
echo "----------------------------------------"
check_redis "scanner-redis-worker-1" "6380" "stream:regime"

# Проверяем статус regime-worker
echo "3️⃣ СТАТУС REGIME-WORKER"
echo "------------------------"
if docker ps | grep -q scanner-regime-worker; then
    echo -e "${GREEN}✅ Контейнер запущен${NC}"
    echo ""
    echo -e "${YELLOW}Последние логи:${NC}"
    docker logs scanner-regime-worker --tail 20
else
    echo -e "${RED}❌ Контейнер не запущен${NC}"
fi
echo ""

# Итоговая информация
echo "4️⃣ АРХИТЕКТУРА ПОТОКА"
echo "---------------------"
echo "📊 go-worker → redis:6379 (stream:kline_*)"
echo "📊 redis:6379 → regime-worker (читает kline)"
echo "📊 regime-worker → redis-worker-1:6379 (пишет stream:regime)"
echo "📊 redis-worker-1:6379 → trade_back:6380 (читает regime)"
echo ""
echo "💡 Порты:"
echo "   - redis:6379 (основной, внутри docker)"
echo "   - redis-worker-1:6379 (внутри docker) → localhost:6380 (снаружи)"
echo ""

echo "✅ Проверка завершена"

