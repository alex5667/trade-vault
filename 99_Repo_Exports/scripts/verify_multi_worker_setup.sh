#!/bin/bash

# Скрипт проверки Multi-Worker Setup для Scanner Infrastructure
# Автор: Senior Go/Python Developer + Senior Trading Systems Analyst
# Дата: 2025-10-18

set -e

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Счетчики
PASSED=0
FAILED=0
WARNINGS=0

echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}   🔍 Проверка Multi-Worker Setup для Scanner Infrastructure${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo ""

# Функция для вывода результата
check_result() {
    local test_name="$1"
    local result="$2"
    local details="$3"
    
    if [ "$result" = "PASS" ]; then
        echo -e "${GREEN}✅ PASS${NC}: $test_name"
        [ -n "$details" ] && echo -e "   ${details}"
        ((PASSED++))
    elif [ "$result" = "FAIL" ]; then
        echo -e "${RED}❌ FAIL${NC}: $test_name"
        [ -n "$details" ] && echo -e "   ${RED}${details}${NC}"
        ((FAILED++))
    elif [ "$result" = "WARN" ]; then
        echo -e "${YELLOW}⚠️  WARN${NC}: $test_name"
        [ -n "$details" ] && echo -e "   ${YELLOW}${details}${NC}"
        ((WARNINGS++))
    fi
}

echo -e "${BLUE}1. Проверка docker-compose.yml${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Проверка наличия файла
if [ -f "docker-compose.yml" ]; then
    check_result "docker-compose.yml существует" "PASS"
else
    check_result "docker-compose.yml существует" "FAIL" "Файл не найден!"
    exit 1
fi

# Проверка наличия всех 10 воркеров
WORKERS=("go-worker-1m" "go-worker-5m" "go-worker-15m" "go-worker-1h" "go-worker-4h" "go-worker-1d" "go-worker-1w" "go-worker-1M" "go-worker-3M" "go-worker-1y")
MISSING_WORKERS=()

for worker in "${WORKERS[@]}"; do
    if grep -q "$worker:" docker-compose.yml; then
        check_result "Воркер $worker определен" "PASS"
    else
        MISSING_WORKERS+=("$worker")
        check_result "Воркер $worker определен" "FAIL" "Воркер не найден в docker-compose.yml"
    fi
done

if [ ${#MISSING_WORKERS[@]} -eq 0 ]; then
    check_result "Все 10 воркеров определены" "PASS" "Найдено: ${#WORKERS[@]} воркеров"
else
    check_result "Все 10 воркеров определены" "FAIL" "Отсутствуют: ${MISSING_WORKERS[*]}"
fi

echo ""
echo -e "${BLUE}2. Проверка переменных окружения${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Проверка BINANCE_WS_TIMEFRAME для каждого воркера
TIMEFRAMES=("kline_1m" "kline_5m" "kline_15m" "kline_1h" "kline_4h" "kline_1d" "kline_1w" "kline_1M" "kline_3M" "kline_1y")
for i in "${!WORKERS[@]}"; do
    worker="${WORKERS[$i]}"
    timeframe="${TIMEFRAMES[$i]}"
    
    if grep -A 20 "$worker:" docker-compose.yml | grep -q "BINANCE_WS_TIMEFRAME=$timeframe"; then
        check_result "$worker: BINANCE_WS_TIMEFRAME=$timeframe" "PASS"
    else
        check_result "$worker: BINANCE_WS_TIMEFRAME=$timeframe" "FAIL" "Неверный или отсутствующий таймфрейм"
    fi
done

# Проверка PROMETHEUS_PORT для каждого воркера
PORTS=(2112 2113 2114 2115 2116 2117 2118 2119 2120 2121)
for i in "${!WORKERS[@]}"; do
    worker="${WORKERS[$i]}"
    port="${PORTS[$i]}"
    
    if grep -A 20 "$worker:" docker-compose.yml | grep -q "PROMETHEUS_PORT=$port"; then
        check_result "$worker: PROMETHEUS_PORT=$port" "PASS"
    else
        check_result "$worker: PROMETHEUS_PORT=$port" "FAIL" "Неверный или отсутствующий порт"
    fi
done

echo ""
echo -e "${BLUE}3. Проверка prometheus.yml${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ -f "prometheus.yml" ]; then
    check_result "prometheus.yml существует" "PASS"
    
    # Проверка наличия всех таргетов
    for i in "${!WORKERS[@]}"; do
        worker="${WORKERS[$i]}"
        port="${PORTS[$i]}"
        target="${worker}:${port}"
        
        if grep -q "$target" prometheus.yml; then
            check_result "Prometheus target: $target" "PASS"
        else
            check_result "Prometheus target: $target" "FAIL" "Target не найден в prometheus.yml"
        fi
    done
else
    check_result "prometheus.yml существует" "FAIL" "Файл не найден!"
fi

echo ""
echo -e "${BLUE}4. Проверка Go кода${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Проверка наличия функции getTimeframesFromEnv
if [ -f "go-worker/binance/multiplexed_manager.go" ]; then
    if grep -q "getTimeframesFromEnv" go-worker/binance/multiplexed_manager.go; then
        check_result "Функция getTimeframesFromEnv() существует" "PASS"
    else
        check_result "Функция getTimeframesFromEnv() существует" "FAIL" "Функция не найдена в multiplexed_manager.go"
    fi
    
    # Проверка import "os"
    if grep -A 10 'import (' go-worker/binance/multiplexed_manager.go | grep -q '"os"'; then
        check_result "Import 'os' добавлен в multiplexed_manager.go" "PASS"
    else
        check_result "Import 'os' добавлен в multiplexed_manager.go" "FAIL" "Import не найден"
    fi
else
    check_result "go-worker/binance/multiplexed_manager.go существует" "FAIL" "Файл не найден!"
fi

# Проверка PROMETHEUS_PORT в init.go
if [ -f "go-worker/internal/app/init.go" ]; then
    if grep -q "PROMETHEUS_PORT" go-worker/internal/app/init.go; then
        check_result "Поддержка PROMETHEUS_PORT в init.go" "PASS"
    else
        check_result "Поддержка PROMETHEUS_PORT в init.go" "FAIL" "Переменная не используется"
    fi
else
    check_result "go-worker/internal/app/init.go существует" "FAIL" "Файл не найден!"
fi

echo ""
echo -e "${BLUE}5. Проверка балансировки Redis${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Подсчет воркеров на redis-worker-1 и redis-worker-2
REDIS1_COUNT=0
REDIS2_COUNT=0

for worker in "${WORKERS[@]}"; do
    if grep -A 5 "$worker:" docker-compose.yml | grep -q "REDIS_HOST=redis-worker-1"; then
        ((REDIS1_COUNT++))
    elif grep -A 5 "$worker:" docker-compose.yml | grep -q "REDIS_HOST=redis-worker-2"; then
        ((REDIS2_COUNT++))
    fi
done

if [ $REDIS1_COUNT -eq 5 ] && [ $REDIS2_COUNT -eq 5 ]; then
    check_result "Балансировка Redis" "PASS" "redis-worker-1: $REDIS1_COUNT, redis-worker-2: $REDIS2_COUNT"
else
    check_result "Балансировка Redis" "WARN" "redis-worker-1: $REDIS1_COUNT, redis-worker-2: $REDIS2_COUNT (ожидается 5:5)"
fi

echo ""
echo -e "${BLUE}6. Проверка зависимостей${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Проверка зависимостей python-worker
if grep -A 30 "python-worker:" docker-compose.yml | grep -q "go-worker-1m:"; then
    check_result "python-worker зависит от go-workers" "PASS"
else
    check_result "python-worker зависит от go-workers" "FAIL" "Зависимости не обновлены"
fi

# Проверка зависимостей prometheus
if grep -A 20 "prometheus:" docker-compose.yml | grep -q "go-worker-1m"; then
    check_result "prometheus зависит от go-workers" "PASS"
else
    check_result "prometheus зависит от go-workers" "FAIL" "Зависимости не обновлены"
fi

# Проверка зависимостей regime-worker
if grep -A 30 "regime-worker:" docker-compose.yml | grep -q "go-worker-1m:"; then
    check_result "regime-worker зависит от go-workers" "PASS"
else
    check_result "regime-worker зависит от go-workers" "FAIL" "Зависимости не обновлены"
fi

echo ""
echo -e "${BLUE}7. Проверка документации${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ -f "docs/MULTI_WORKER_SETUP.md" ]; then
    check_result "Документация MULTI_WORKER_SETUP.md" "PASS"
else
    check_result "Документация MULTI_WORKER_SETUP.md" "WARN" "Файл не найден"
fi

if [ -f "docs/WORKERS_QUICK_REFERENCE.md" ]; then
    check_result "Документация WORKERS_QUICK_REFERENCE.md" "PASS"
else
    check_result "Документация WORKERS_QUICK_REFERENCE.md" "WARN" "Файл не найден"
fi

echo ""
echo -e "${BLUE}8. Проверка запущенных контейнеров (опционально)${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if command -v docker &> /dev/null; then
    RUNNING_WORKERS=0
    for worker in "${WORKERS[@]}"; do
        container="scanner-$worker"
        if docker ps --format '{{.Names}}' | grep -q "^$container$"; then
            ((RUNNING_WORKERS++))
            status=$(docker inspect $container | jq -r '.[0].State.Status' 2>/dev/null || echo "unknown")
            check_result "Контейнер $container запущен" "PASS" "Статус: $status"
        fi
    done
    
    if [ $RUNNING_WORKERS -eq 0 ]; then
        check_result "Воркеры запущены" "WARN" "Контейнеры не запущены (это нормально, если еще не был выполнен docker-compose up)"
    elif [ $RUNNING_WORKERS -eq 10 ]; then
        check_result "Все воркеры запущены" "PASS" "Запущено: $RUNNING_WORKERS/10"
    else
        check_result "Все воркеры запущены" "WARN" "Запущено только: $RUNNING_WORKERS/10"
    fi
else
    check_result "Docker доступен" "WARN" "Docker не установлен или недоступен"
fi

echo ""
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}   📊 Итоговая статистика${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${GREEN}✅ Пройдено:${NC}      $PASSED"
echo -e "${YELLOW}⚠️  Предупреждений:${NC} $WARNINGS"
echo -e "${RED}❌ Провалено:${NC}     $FAILED"
echo ""

TOTAL=$((PASSED + FAILED + WARNINGS))
SUCCESS_RATE=$((PASSED * 100 / TOTAL))

if [ $FAILED -eq 0 ] && [ $WARNINGS -eq 0 ]; then
    echo -e "${GREEN}🎉 Отлично! Все проверки пройдены успешно!${NC}"
    echo -e "${GREEN}   Система готова к запуску: docker-compose up -d${NC}"
    exit 0
elif [ $FAILED -eq 0 ]; then
    echo -e "${YELLOW}⚠️  Хорошо! Критических ошибок нет, но есть предупреждения.${NC}"
    echo -e "${YELLOW}   Проверьте предупреждения выше и запустите: docker-compose up -d${NC}"
    exit 0
else
    echo -e "${RED}❌ Обнаружены критические ошибки!${NC}"
    echo -e "${RED}   Исправьте ошибки перед запуском системы.${NC}"
    echo ""
    echo -e "${BLUE}📖 Документация:${NC}"
    echo -e "   - docs/MULTI_WORKER_SETUP.md - Полное описание архитектуры"
    echo -e "   - docs/WORKERS_QUICK_REFERENCE.md - Быстрая справка по командам"
    exit 1
fi

