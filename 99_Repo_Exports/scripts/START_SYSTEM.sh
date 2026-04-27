#!/bin/bash

# ═══════════════════════════════════════════════════════════════════
#  SCANNER_INFRA - Startup Script
#  Автоматический запуск всей системы
# ═══════════════════════════════════════════════════════════════════

set -e

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                                                                ║"
echo "║     🚀 ЗАПУСК SCANNER_INFRA TRADING SYSTEM                    ║"
echo "║                                                                ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Цвета для вывода
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Функция для вывода статуса
print_status() {
    echo -e "${GREEN}✅${NC} $1"
}

print_info() {
    echo -e "${BLUE}ℹ️${NC}  $1"
}

print_wait() {
    echo -e "${YELLOW}⏳${NC} $1"
}

# 1. Остановка существующих контейнеров
print_info "Остановка существующих контейнеров..."
docker-compose down 2>/dev/null || true
print_status "Контейнеры остановлены"
echo ""

# 2. Запуск Redis кластера
print_info "Запуск Redis кластера..."
docker-compose up -d redis redis-worker-1 redis-worker-2
print_wait "Ожидание готовности Redis (15 сек)..."
sleep 15
print_status "Redis кластер запущен"
echo ""

# 3. Запуск OBI сервиса
print_info "Запуск OBI сервиса..."
docker-compose up -d py-obi-service
print_wait "Ожидание готовности OBI (15 сек)..."
sleep 15
print_status "OBI сервис запущен"
echo ""

# 4. Запуск Go Gateway
print_info "Запуск Go Gateway..."
docker-compose up -d go-gateway
print_wait "Ожидание готовности Gateway (10 сек)..."
sleep 10
print_status "Go Gateway запущен"
echo ""

# 5. Запуск Go Workers (OHLC aggregation)
print_info "Запуск Go Workers..."
docker-compose up -d --no-recreate \
    go-worker-1m \
    go-worker-5m \
    go-worker-15m \
    go-worker-1h \
    go-worker-4h \
    go-worker-1d \
    go-worker-1w \
    go-worker-1month
print_wait "Ожидание готовности Workers (5 сек)..."
sleep 5
print_status "Go Workers запущены"
echo ""

# 6. Запуск Signal Generator
print_info "Запуск Signal Generator..."
docker-compose up -d --no-recreate signal-generator
print_status "Signal Generator запущен"
echo ""

# 7. Запуск Python Worker
print_info "Запуск Python Worker (Orderflow Handler)..."
docker-compose up -d --no-recreate python-worker
print_status "Python Worker запущен"
echo ""

# 8. Запуск Aggregated Hub
print_info "Запуск Aggregated Hub..."
docker-compose up -d --no-recreate aggregated-hub
print_status "Aggregated Hub запущен"
echo ""

# 9. Проверка статуса
print_info "Проверка статуса сервисов..."
echo ""

# Подсчет healthy контейнеров
HEALTHY_COUNT=$(docker ps --filter "health=healthy" --format "{{.Names}}" | grep scanner | wc -l)
RUNNING_COUNT=$(docker ps --format "{{.Names}}" | grep scanner | wc -l)

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                    📊 СТАТУС СИСТЕМЫ                           ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "  Запущенных контейнеров: ${RUNNING_COUNT}"
echo "  Healthy контейнеров:     ${HEALTHY_COUNT}"
echo ""

# Проверка ключевых сервисов
print_info "Проверка ключевых endpoints..."
echo ""

# Go Gateway Health
if curl -s -f http://localhost:8090/healthz > /dev/null 2>&1; then
    print_status "Go Gateway:    http://localhost:8090 ✓"
else
    echo "  ❌ Go Gateway: недоступен"
fi

# OBI Service Health  
if curl -s -f http://localhost:8088/healthz > /dev/null 2>&1; then
    print_status "OBI Service:   http://localhost:8088 ✓"
else
    echo "  ❌ OBI Service: недоступен"
fi

# Paper Trading
if curl -s -f http://localhost:8090/paper/status > /dev/null 2>&1; then
    print_status "Paper Trading: активен ✓"
else
    echo "  ❌ Paper Trading: недоступен"
fi

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                                                                ║"
echo "║  ✨ СИСТЕМА ЗАПУЩЕНА И ГОТОВА К РАБОТЕ! ✨                    ║"
echo "║                                                                ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

print_info "Полезные команды:"
echo ""
echo "  # Просмотр логов:"
echo "  docker logs -f scanner-go-gateway"
echo "  docker logs -f scanner-aggregated-hub"
echo ""
echo "  # Проверка статуса:"
echo "  docker ps | grep scanner"
echo ""
echo "  # Тестовый ордер:"
echo "  curl -X POST http://localhost:8090/orders/enqueue \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"sid\":\"TEST\",\"symbol\":\"XAUUSD\",\"side\":\"LONG\",\"lot\":0.01}'"
echo ""
echo "  # Paper Trading сводка:"
echo "  curl http://localhost:8090/paper/summary"
echo ""
echo "  # Остановка системы:"
echo "  docker-compose down"
echo ""

print_status "Готово! 🚀"
echo ""

