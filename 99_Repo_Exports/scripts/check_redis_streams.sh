#!/bin/bash
# Проверка всех Redis Streams в системе

set -e

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║   Проверка Redis Streams - Полная диагностика                  ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Цвета
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Проверка Redis подключения
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}🔍 Проверка Redis подключения${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

if docker exec scanner-redis redis-cli ping >/dev/null 2>&1; then
    echo -e "${GREEN}✅ Redis Main (6379) - доступен${NC}"
else
    echo -e "${RED}❌ Redis Main недоступен!${NC}"
    echo "Запустите систему: make up"
    exit 1
fi

# Memory usage
mem_used=$(docker exec scanner-redis redis-cli INFO memory | grep used_memory_human | cut -d: -f2 | tr -d '\r')
mem_max=$(docker exec scanner-redis redis-cli CONFIG GET maxmemory | tail -1)
mem_max_gb=$(echo "scale=2; $mem_max / 1024 / 1024 / 1024" | bc 2>/dev/null || echo "?")

echo "   Memory: $mem_used / ${mem_max_gb}GB"

# Connected clients
clients=$(docker exec scanner-redis redis-cli INFO clients | grep connected_clients | cut -d: -f2 | tr -d '\r')
echo "   Clients: $clients"

echo ""

# Функция проверки stream
check_stream() {
    local stream=$1
    local description=$2
    local expected_min=$3
    
    length=$(docker exec scanner-redis redis-cli XLEN "$stream" 2>/dev/null || echo "0")
    
    if [ "$length" -gt 0 ]; then
        if [ -n "$expected_min" ] && [ "$length" -lt "$expected_min" ]; then
            echo -e "${YELLOW}⚠️  $description${NC}"
            echo -e "   Stream: $stream"
            echo -e "   Length: $length (ожидается >$expected_min)"
        else
            echo -e "${GREEN}✅ $description${NC}"
            echo "   Stream: $stream"
            echo "   Length: $length"
        fi
        
        # Показываем последнее сообщение (первые 2 поля)
        last_msg=$(docker exec scanner-redis redis-cli XREVRANGE "$stream" + - COUNT 1 2>/dev/null | head -3 | tail -2)
        if [ -n "$last_msg" ]; then
            echo "   Last: $(echo "$last_msg" | tr '\n' ' ' | cut -c1-60)..."
        fi
    else
        echo -e "${RED}❌ $description${NC}"
        echo "   Stream: $stream"
        echo "   Length: 0 (пустой или не существует)"
    fi
    
    echo ""
}

echo "═══════════════════════════════════════════════════════════════"
echo "  ЧАСТЬ 1: Input Streams (входящие данные)"
echo "═══════════════════════════════════════════════════════════════"
echo ""

check_stream "candles:data" "Candles от Binance" 100
check_stream "stream:tick_XAUUSD" "Тики XAUUSD" 50
check_stream "stream:book_XAUUSD" "Order Book XAUUSD"

echo "═══════════════════════════════════════════════════════════════"
echo "  ЧАСТЬ 2: Signal Streams (генерируемые сигналы)"
echo "═══════════════════════════════════════════════════════════════"
echo ""

check_stream "signals:orderflow:XAUUSD" "OrderFlow сигналы"
check_stream "signals:ta:XAUUSD" "Technical Analysis сигналы"
check_stream "signals:aggregated-hub:XAUUSD" "Aggregated Hub сигналы"
check_stream "signal:telegram:raw" "Telegram сырые сигналы"

echo "═══════════════════════════════════════════════════════════════"
echo "  ЧАСТЬ 3: Output Streams (уведомления и ордера)"
echo "═══════════════════════════════════════════════════════════════"
echo ""

check_stream "notify:telegram" "Telegram уведомления"
check_stream "orders:queue" "Очередь ордеров"

echo "═══════════════════════════════════════════════════════════════"
echo "  ЧАСТЬ 4: Auxiliary Streams (вспомогательные)"
echo "═══════════════════════════════════════════════════════════════"
echo ""

check_stream "trades:prints_XAUUSD" "Prints XAUUSD"
check_stream "config:symbols" "Конфигурация символов"

echo "═══════════════════════════════════════════════════════════════"
echo "  ЧАСТЬ 5: Consumer Groups"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# Функция проверки consumer groups
check_consumer_groups() {
    local stream=$1
    local description=$2
    
    echo -e "${CYAN}🔍 $description (stream: $stream)${NC}"
    
    groups=$(docker exec scanner-redis redis-cli XINFO GROUPS "$stream" 2>/dev/null || echo "")
    
    if [ -n "$groups" ]; then
        # Парсим вывод XINFO GROUPS
        group_count=$(echo "$groups" | grep -c "name" || echo "0")
        echo -e "${GREEN}✅ Consumer groups: $group_count${NC}"
        
        # Показываем первые 3 группы
        echo "$groups" | grep "name\|pending\|consumers" | head -9 | sed 's/^/   /'
    else
        echo -e "${YELLOW}⚠️  Consumer groups не найдены${NC}"
    fi
    
    echo ""
}

check_consumer_groups "candles:data" "Candles"
check_consumer_groups "signals:orderflow:XAUUSD" "OrderFlow сигналы"
check_consumer_groups "stream:tick_XAUUSD" "Тики XAUUSD"
check_consumer_groups "notify:telegram" "Telegram уведомления"

echo "═══════════════════════════════════════════════════════════════"
echo "  ЧАСТЬ 6: Ключи в Redis"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# Статистика
echo "📊 Статистика (stats:*):"
stats_count=$(docker exec scanner-redis redis-cli KEYS "stats:*" 2>/dev/null | wc -l)
if [ "$stats_count" -gt 0 ]; then
    echo -e "${GREEN}✅ Найдено ключей: $stats_count${NC}"
    docker exec scanner-redis redis-cli KEYS "stats:*" 2>/dev/null | head -5 | sed 's/^/   /'
    if [ "$stats_count" -gt 5 ]; then
        echo "   ... и еще $(($stats_count - 5))"
    fi
else
    echo -e "${YELLOW}⚠️  Статистика еще не собрана${NC}"
fi
echo ""

# ATR
echo "📊 ATR значения (ta:last:atr:*):"
atr_count=$(docker exec scanner-redis redis-cli KEYS "ta:last:atr:*" 2>/dev/null | wc -l)
if [ "$atr_count" -gt 0 ]; then
    echo -e "${GREEN}✅ Найдено ключей: $atr_count${NC}"
    
    # Показываем первый ATR
    first_atr_key=$(docker exec scanner-redis redis-cli KEYS "ta:last:atr:*" 2>/dev/null | head -1)
    if [ -n "$first_atr_key" ]; then
        atr_value=$(docker exec scanner-redis redis-cli GET "$first_atr_key" 2>/dev/null)
        echo "   Пример: $first_atr_key"
        echo "   Значение: $(echo "$atr_value" | head -c 100)..."
    fi
else
    echo -e "${YELLOW}⚠️  ATR значения не найдены${NC}"
fi
echo ""

# Order Book
echo "📊 Order Book (book:levels:*):"
book_count=$(docker exec scanner-redis redis-cli KEYS "book:levels:*" 2>/dev/null | wc -l)
if [ "$book_count" -gt 0 ]; then
    echo -e "${GREEN}✅ Найдено ключей: $book_count${NC}"
    docker exec scanner-redis redis-cli KEYS "book:levels:*" 2>/dev/null | sed 's/^/   /'
else
    echo -e "${YELLOW}⚠️  Order Book не найден${NC}"
fi
echo ""

# Symbol specs
echo "📊 Symbol specs (symbol_specs:*):"
specs_count=$(docker exec scanner-redis redis-cli KEYS "symbol_specs:*" 2>/dev/null | wc -l)
if [ "$specs_count" -gt 0 ]; then
    echo -e "${GREEN}✅ Найдено ключей: $specs_count${NC}"
    docker exec scanner-redis redis-cli KEYS "symbol_specs:*" 2>/dev/null | sed 's/^/   /'
else
    echo -e "${YELLOW}⚠️  Symbol specs не найдены${NC}"
fi
echo ""

echo "═══════════════════════════════════════════════════════════════"
echo "  ИТОГОВАЯ СВОДКА"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# Подсчет активных streams
active_streams=0
total_streams=10

# Проверяем ключевые streams
for stream in "candles:data" "stream:tick_XAUUSD" "signals:orderflow:XAUUSD" "signals:ta:XAUUSD" "notify:telegram"; do
    len=$(docker exec scanner-redis redis-cli XLEN "$stream" 2>/dev/null || echo "0")
    if [ "$len" -gt 0 ]; then
        active_streams=$((active_streams + 1))
    fi
done

echo "📊 Активных streams: $active_streams / 5 (ключевых)"

if [ "$active_streams" -ge 4 ]; then
    echo -e "${GREEN}✅ Система работает нормально!${NC}"
    echo ""
    echo "Рекомендации:"
    echo "  - Все ключевые потоки активны"
    echo "  - Данные поступают корректно"
    echo "  - Система готова к работе"
elif [ "$active_streams" -ge 2 ]; then
    echo -e "${YELLOW}⚠️  Система частично работает${NC}"
    echo ""
    echo "Проверьте:"
    echo "  - Не все потоки активны"
    echo "  - Возможно, некоторые сервисы не запущены"
    echo "  - Запустите: make full-status"
else
    echo -e "${RED}❌ Система НЕ работает корректно!${NC}"
    echo ""
    echo "Проблемы:"
    echo "  - Большинство потоков пусты"
    echo "  - Данные не поступают"
    echo ""
    echo "Решение:"
    echo "  1. Проверьте статус: make status"
    echo "  2. Проверьте логи: make logs"
    echo "  3. Перезапустите: make restart"
fi

echo ""

echo "Дополнительные команды:"
echo "  make redis-stats           - Полная статистика Redis"
echo "  make check-xauusd-services - Проверка сервисов XAUUSD"
echo "  make check-telegram        - Проверка Telegram интеграции"

echo ""

