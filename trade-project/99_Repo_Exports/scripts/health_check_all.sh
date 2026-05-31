#!/bin/bash

# Цвета
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo "╔════════════════════════════════════════════════════════════╗"
echo "║         🏥 Комплексная проверка здоровья системы          ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""
echo "Время проверки: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# Функция для проверки сервиса
check_service() {
    local service=$1
    local status=$(docker inspect -f '{{.State.Status}}' "$service" 2>/dev/null)
    
    if [ "$status" = "running" ]; then
        echo -e "${GREEN}✅${NC} $service"
        return 0
    elif [ "$status" = "exited" ]; then
        echo -e "${RED}❌${NC} $service (exited)"
        return 1
    else
        echo -e "${YELLOW}⚠️${NC}  $service ($status)"
        return 2
    fi
}

# 1. Проверка Docker сервисов
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "1️⃣  Docker Services"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

CRITICAL_SERVICES=(
    "scanner-redis-worker-1"
    "scanner-redis-worker-2"
    "scanner-go-worker"
    "scanner-python-worker"
    "scanner-telegram-worker"
    "scanner-signal-parser-worker"
    "scanner-notify-worker"
    "scanner-regime-worker"
)

failed=0
for service in "${CRITICAL_SERVICES[@]}"; do
    check_service "$service" || ((failed++))
done

echo ""
echo "Статус: $((${#CRITICAL_SERVICES[@]} - failed))/${#CRITICAL_SERVICES[@]} сервисов работают"

# 2. Проверка Redis
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "2️⃣  Redis Health"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

for redis in "scanner-redis-worker-1" "scanner-redis-worker-2"; do
    echo ""
    echo "📊 $redis:"
    
    if docker exec $redis redis-cli PING 2>/dev/null | grep -q PONG; then
        echo -e "   ${GREEN}✅${NC} PING: PONG"
        
        clients=$(docker exec $redis redis-cli INFO clients 2>/dev/null | grep connected_clients | cut -d: -f2 | tr -d '\r')
        echo "   👥 Клиентов: $clients"
        
        memory=$(docker exec $redis redis-cli INFO memory 2>/dev/null | grep used_memory_human | cut -d: -f2 | tr -d '\r')
        echo "   💾 Память: $memory"
        
        keys=$(docker exec $redis redis-cli DBSIZE 2>/dev/null | grep -oE '[0-9]+')
        echo "   🔑 Ключей: $keys"
    else
        echo -e "   ${RED}❌${NC} Не отвечает"
    fi
done

# 3. Проверка Redis конфигураций
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "3️⃣  Redis Configurations"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

SERVICES_TO_CHECK=(
    "scanner-go-worker"
    "scanner-python-worker"
    "scanner-telegram-worker"
    "scanner-signal-parser-worker"
    "scanner-notify-worker"
)

echo ""
wrong_config=0
for service in "${SERVICES_TO_CHECK[@]}"; do
    config=$(docker inspect "$service" 2>/dev/null | grep -E "REDIS_HOST=|REDIS_URL=" | head -1)
    
    if echo "$config" | grep -q "scanner-redis-worker-1"; then
        echo -e "${GREEN}✅${NC} $service → scanner-redis-worker-1"
    else
        echo -e "${RED}❌${NC} $service → НЕПРАВИЛЬНАЯ КОНФИГУРАЦИЯ"
        ((wrong_config++))
    fi
done

# 4. Проверка ошибок в логах
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "4️⃣  Error Logs (last 10 minutes)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo ""
redis_errors=$(docker-compose logs --tail=200 --since=10m 2>&1 | grep -i "error.*redis\|connecting to redis\|connection.*refused" | wc -l)

if [ "$redis_errors" -eq 0 ]; then
    echo -e "${GREEN}✅${NC} Ошибок Redis не найдено"
else
    echo -e "${YELLOW}⚠️${NC}  Найдено $redis_errors ошибок Redis"
    echo ""
    echo "Последние 5 ошибок:"
    docker-compose logs --tail=200 --since=10m 2>&1 | grep -i "error.*redis\|connecting to redis" | tail -5
fi

# 5. Проверка CPU и Memory
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "5️⃣  Resource Usage"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo ""
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}" | grep scanner | head -10

# 6. Финальная сводка
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📊 Summary"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

total_issues=$((failed + wrong_config + (redis_errors > 0 ? 1 : 0)))

if [ $total_issues -eq 0 ]; then
    echo -e "${GREEN}✅ Система работает идеально!${NC}"
    echo "   • Все сервисы запущены"
    echo "   • Redis работает стабильно"
    echo "   • Конфигурации правильные"
    echo "   • Ошибок не обнаружено"
else
    echo -e "${YELLOW}⚠️  Обнаружено проблем: $total_issues${NC}"
    [ $failed -gt 0 ] && echo "   • Не работает сервисов: $failed"
    [ $wrong_config -gt 0 ] && echo "   • Неправильных конфигураций: $wrong_config"
    [ $redis_errors -gt 0 ] && echo "   • Ошибок Redis в логах: $redis_errors"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Проверка завершена: $(date '+%H:%M:%S')"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

exit $total_issues

