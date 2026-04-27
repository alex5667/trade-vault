#!/bin/bash
# Скрипт для правильного перезапуска всех сервисов с учетом зависимостей

echo "🔄 ПРАВИЛЬНЫЙ ПЕРЕЗАПУСК ВСЕХ СЕРВИСОВ"
echo "======================================"
echo ""

# Цвета
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Останавливаем все
echo -e "${YELLOW}1️⃣ Остановка всех сервисов...${NC}"
docker-compose down
echo ""

# Пересоздаем с новой конфигурацией
echo -e "${YELLOW}2️⃣ Пересоздание сервисов...${NC}"
docker-compose up -d --force-recreate
echo ""

echo -e "${YELLOW}3️⃣ Ожидание инициализации...${NC}"
echo "   0:00 - Redis контейнеры запускаются..."
sleep 15

echo "   0:15 - Redis готовы..."
sleep 5

echo "   0:20 - go-worker запускается..."
sleep 10

echo "   0:30 - python-worker и regime-worker запускаются..."
sleep 10

echo "   0:40 - telegram и signal-parser запускаются..."
sleep 10

echo "   0:50 - notify-worker запускается..."
sleep 10

echo ""
echo -e "${GREEN}4️⃣ Загрузка Telegram каналов...${NC}"
./load_channels_docker.sh
echo ""

echo -e "${GREEN}5️⃣ Проверка статуса сервисов...${NC}"
docker ps --format "table {{.Names}}\t{{.Status}}" | grep scanner-
echo ""

echo -e "${GREEN}6️⃣ Проверка данных на 6380...${NC}"
docker exec scanner-redis-worker-1 redis-cli -p 6379 --scan --pattern "stream:*" | \
while read s; do 
    count=$(docker exec scanner-redis-worker-1 redis-cli -p 6379 XLEN "$s" 2>/dev/null)
    if [ "$count" != "0" ]; then
        echo "  $s: $count"
    fi
done
echo ""

echo -e "${GREEN}✅ Перезапуск завершен!${NC}"
echo ""
echo "📊 Мониторинг:"
echo "  docker-compose logs -f --tail=50"
echo ""
echo "🔍 Проверка:"
echo "  ./check_regime_flow.sh"

