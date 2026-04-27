#!/bin/bash
# Применение исправлений ATR Integration - October 31, 2025

set -e

echo "🔧 Применение исправлений ATR Integration..."
echo ""

# Цвета для вывода
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Проверка, что мы в правильной директории
if [ ! -f "docker-compose.yml" ]; then
    echo -e "${RED}❌ Ошибка: docker-compose.yml не найден${NC}"
    echo "Запустите скрипт из корня проекта scanner_infra"
    exit 1
fi

echo -e "${YELLOW}📋 Шаг 1: Остановка затронутых контейнеров${NC}"
docker-compose stop scanner-python-worker scanner-aggregated-hub scanner-signal-hub || true

echo ""
echo -e "${YELLOW}📋 Шаг 2: Пересборка образов${NC}"
docker-compose build python-worker

echo ""
echo -e "${YELLOW}📋 Шаг 3: Запуск ATR worker${NC}"
docker-compose up -d atr-worker

echo ""
echo -e "${YELLOW}📋 Шаг 4: Запуск обновлённых контейнеров${NC}"
docker-compose up -d python-worker scanner-aggregated-hub scanner-signal-hub

echo ""
echo -e "${GREEN}✅ Контейнеры перезапущены${NC}"

echo ""
echo -e "${YELLOW}📋 Шаг 5: Ожидание инициализации (30 секунд)${NC}"
sleep 30

echo ""
echo -e "${YELLOW}📋 Шаг 6: Проверка работы ATR worker${NC}"
docker-compose logs --tail=20 atr-worker

echo ""
echo -e "${YELLOW}📋 Шаг 7: Проверка ATR в Redis${NC}"
echo "Проверяем ключ ta:last:atr:XAUUSD:"
docker exec scanner-redis-worker-1 redis-cli --raw GET ta:last:atr:XAUUSD 2>/dev/null || echo "⚠️  Ключ пока не создан, подождите ~1 минуту"

echo ""
echo -e "${GREEN}✅ Применение завершено!${NC}"
echo ""
echo "📊 Команды для мониторинга:"
echo ""
echo "  # Логи ATR worker"
echo "  docker-compose logs -f atr-worker"
echo ""
echo "  # Логи aggregated-hub (фильтр ATR)"
echo "  docker-compose logs -f scanner-aggregated-hub | grep ATR"
echo ""
echo "  # Проверить ATR в реальном времени"
echo "  watch -n 2 'docker exec scanner-redis-worker-1 redis-cli --raw GET ta:last:atr:XAUUSD | jq .'"
echo ""
echo "  # Проверить что fallback больше не используется"
echo "  docker-compose logs scanner-aggregated-hub | grep 'fallback ATR'"
echo ""
echo -e "${GREEN}🎉 Теперь ATR должен быть ~3-4 вместо 1.20!${NC}"

