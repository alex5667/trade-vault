#!/bin/bash
# Проверка всех 3 сервисов, обрабатывающих тики по XAUUSD

set -e

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║   Проверка сервисов обработки XAUUSD                           ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Функция проверки контейнера
check_container() {
    local container_name=$1
    local service_name=$2
    
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "🔍 Проверка: $service_name"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Проверка запущен ли контейнер
    if docker ps --format '{{.Names}}' | grep -q "^${container_name}$"; then
        echo -e "${GREEN}✅ Контейнер запущен${NC}"
        
        # Статус
        status=$(docker inspect --format='{{.State.Status}}' $container_name 2>/dev/null)
        echo "   Статус: $status"
        
        # Health
        health=$(docker inspect --format='{{.State.Health.Status}}' $container_name 2>/dev/null || echo "none")
        if [ "$health" = "healthy" ]; then
            echo -e "   Health: ${GREEN}$health${NC}"
        elif [ "$health" = "unhealthy" ]; then
            echo -e "   Health: ${RED}$health${NC}"
        else
            echo "   Health: $health"
        fi
        
        # Uptime
        started=$(docker inspect --format='{{.State.StartedAt}}' $container_name 2>/dev/null)
        echo "   Started: $started"
        
        # Restart count
        restarts=$(docker inspect --format='{{.RestartCount}}' $container_name 2>/dev/null)
        if [ "$restarts" -gt 0 ]; then
            echo -e "   Restarts: ${YELLOW}$restarts${NC}"
        else
            echo "   Restarts: $restarts"
        fi
        
        # Последние 5 строк логов
        echo ""
        echo "   📋 Последние логи:"
        docker logs --tail=5 $container_name 2>&1 | sed 's/^/      /'
        
    else
        echo -e "${RED}❌ Контейнер НЕ запущен${NC}"
    fi
    
    echo ""
}

# Функция проверки Redis stream
check_redis_stream() {
    local stream_name=$1
    local description=$2
    
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "🔍 Проверка Redis Stream: $description"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Проверка длины stream
    length=$(docker exec scanner-redis redis-cli XLEN "$stream_name" 2>/dev/null || echo "0")
    
    if [ "$length" -gt 0 ]; then
        echo -e "${GREEN}✅ Stream существует${NC}"
        echo "   Длина: $length сообщений"
        
        # Последнее сообщение
        echo ""
        echo "   📨 Последнее сообщение:"
        docker exec scanner-redis redis-cli XREVRANGE "$stream_name" + - COUNT 1 2>/dev/null | sed 's/^/      /' || echo "      Не удалось получить"
    else
        echo -e "${RED}❌ Stream пустой или не существует${NC}"
        echo "   Длина: $length"
    fi
    
    echo ""
}

echo "═══════════════════════════════════════════════════════════════"
echo "  ЧАСТЬ 1: Проверка контейнеров"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# Проверка 3 основных сервисов обработки XAUUSD
check_container "scanner-multi-orderflow" "Multi-Symbol OrderFlow Handler"
check_container "scanner-aggregated-hub" "Aggregated Hub V2"
check_container "scanner-signal-tracker" "Signal Performance Tracker"

# Дополнительно - tick ingest
check_container "scanner-tick-ingest" "Tick Ingest Server"

echo "═══════════════════════════════════════════════════════════════"
echo "  ЧАСТЬ 2: Проверка Redis Streams"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# Проверка входных данных
check_redis_stream "stream:tick_XAUUSD" "Тики XAUUSD"
check_redis_stream "candles:data" "Свечи от Binance"

# Проверка сигналов
check_redis_stream "signals:orderflow:XAUUSD" "OrderFlow сигналы"
check_redis_stream "signals:ta:XAUUSD" "TA сигналы"
check_redis_stream "signals:aggregated-hub:XAUUSD" "Aggregated Hub сигналы"

# Проверка уведомлений
check_redis_stream "notify:telegram" "Telegram уведомления"

echo "═══════════════════════════════════════════════════════════════"
echo "  ЧАСТЬ 3: Проверка Redis Keys"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# ATR
echo "🔍 ATR для XAUUSD:"
atr=$(docker exec scanner-redis redis-cli GET "ta:last:atr:XAUUSD:1m" 2>/dev/null || echo "не найден")
if [ "$atr" != "не найден" ]; then
    echo -e "${GREEN}✅ ATR найден${NC}"
    echo "   Значение: $atr" | head -c 200
else
    echo -e "${RED}❌ ATR не найден${NC}"
fi
echo ""

# Order Book
echo "🔍 Order Book для XAUUSD:"
book_fields=$(docker exec scanner-redis redis-cli HLEN "book:levels:XAUUSD" 2>/dev/null || echo "0")
if [ "$book_fields" -gt 0 ]; then
    echo -e "${GREEN}✅ Order Book найден${NC}"
    echo "   Полей: $book_fields"
else
    echo -e "${RED}❌ Order Book не найден${NC}"
fi
echo ""

# Статистика из StatsAggregator
echo "🔍 Статистика по стратегиям:"
strategies=$(docker exec scanner-redis redis-cli KEYS "stats:*" 2>/dev/null | wc -l || echo "0")
if [ "$strategies" -gt 0 ]; then
    echo -e "${GREEN}✅ Статистика найдена${NC}"
    echo "   Ключей статистики: $strategies"
    
    # Примеры ключей
    echo "   Примеры:"
    docker exec scanner-redis redis-cli KEYS "stats:*" 2>/dev/null | head -5 | sed 's/^/      /' || echo "      Не удалось получить"
else
    echo -e "${YELLOW}⚠️  Статистика пока не собрана${NC}"
    echo "   (это нормально, если трекер только запущен)"
fi
echo ""

echo "═══════════════════════════════════════════════════════════════"
echo "  ЧАСТЬ 4: Тест отправки в Telegram"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# Проверка Telegram credentials
echo "🔍 Telegram credentials:"
bot_token=$(docker exec scanner-signal-tracker env 2>/dev/null | grep TELEGRAM_BOT_TOKEN | cut -d'=' -f2)
chat_id=$(docker exec scanner-signal-tracker env 2>/dev/null | grep TELEGRAM_CHAT_ID | cut -d'=' -f2)

if [ -n "$bot_token" ] && [ "$bot_token" != "None" ]; then
    echo -e "${GREEN}✅ TELEGRAM_BOT_TOKEN установлен${NC}"
    echo "   Token: ${bot_token:0:20}..."
else
    echo -e "${RED}❌ TELEGRAM_BOT_TOKEN не установлен${NC}"
fi

if [ -n "$chat_id" ] && [ "$chat_id" != "None" ]; then
    echo -e "${GREEN}✅ TELEGRAM_CHAT_ID установлен${NC}"
    echo "   Chat ID: $chat_id"
else
    echo -e "${RED}❌ TELEGRAM_CHAT_ID не установлен${NC}"
fi
echo ""

echo "═══════════════════════════════════════════════════════════════"
echo "  ИТОГОВАЯ СВОДКА"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# Подсчет запущенных сервисов
running=0
total=4

for container in "scanner-multi-orderflow" "scanner-aggregated-hub" "scanner-signal-tracker" "scanner-tick-ingest"; do
    if docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
        ((running++))
    fi
done

echo "📊 Запущено сервисов: $running из $total"

if [ "$running" -eq "$total" ]; then
    echo -e "${GREEN}✅ Все сервисы работают!${NC}"
else
    echo -e "${YELLOW}⚠️  Некоторые сервисы не запущены${NC}"
    echo ""
    echo "Запустите систему: make up"
fi
echo ""

# Проверка наличия данных
data_ok=true

if [ "$(docker exec scanner-redis redis-cli XLEN stream:tick_XAUUSD 2>/dev/null || echo 0)" -eq 0 ]; then
    echo -e "${YELLOW}⚠️  Нет тиков XAUUSD${NC}"
    data_ok=false
fi

if [ "$(docker exec scanner-redis redis-cli XLEN signals:orderflow:XAUUSD 2>/dev/null || echo 0)" -eq 0 ]; then
    echo -e "${YELLOW}⚠️  Нет OrderFlow сигналов${NC}"
    data_ok=false
fi

if [ "$data_ok" = true ]; then
    echo -e "${GREEN}✅ Данные поступают корректно!${NC}"
    echo ""
    echo "📊 Периодическая статистика будет отправлена через 3 часа"
    echo "📊 Ежедневная сводка будет отправлена в 00:00 UTC"
else
    echo ""
    echo "Проверьте, что:"
    echo "  1. MT5 отправляет тики (make tick-ingest-status)"
    echo "  2. OrderFlow handler работает (make orderflow-status)"
    echo "  3. Aggregated Hub работает (make hub-status)"
fi

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "Дополнительные команды:"
echo "  make tracker-status    - Детальный статус трекера"
echo "  make tracker-logs      - Логи трекера в реальном времени"
echo "  make tracker-restart   - Перезапуск трекера"
echo ""

