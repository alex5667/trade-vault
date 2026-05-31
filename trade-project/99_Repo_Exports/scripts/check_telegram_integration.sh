#!/bin/bash
# Комплексная проверка Telegram интеграции во всех сервисах

set -e

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║   Проверка Telegram интеграции во всех сервисах                ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Цвета
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Счетчики
total_services=0
ok_services=0

# Функция проверки Telegram credentials в контейнере
check_telegram_credentials() {
    local container=$1
    local service_name=$2
    
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}🔍 $service_name${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    total_services=$((total_services + 1))
    
    # Проверка, что контейнер запущен
    if ! docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
        echo -e "${RED}❌ Контейнер не запущен${NC}"
        echo ""
        return 1
    fi
    
    echo -e "${GREEN}✅ Контейнер запущен${NC}"
    
    # Проверка TELEGRAM_BOT_TOKEN
    token=$(docker exec $container env 2>/dev/null | grep TELEGRAM_BOT_TOKEN | cut -d'=' -f2 || echo "")
    
    if [ -z "$token" ] || [ "$token" = "None" ] || [ "$token" = "" ]; then
        echo -e "${RED}❌ TELEGRAM_BOT_TOKEN не установлен${NC}"
    else
        echo -e "${GREEN}✅ TELEGRAM_BOT_TOKEN установлен${NC}"
        echo "   Token: ${token:0:25}..."
    fi
    
    # Проверка TELEGRAM_CHAT_ID
    chat_id=$(docker exec $container env 2>/dev/null | grep TELEGRAM_CHAT_ID | cut -d'=' -f2 || echo "")
    
    if [ -z "$chat_id" ] || [ "$chat_id" = "None" ] || [ "$chat_id" = "" ]; then
        echo -e "${RED}❌ TELEGRAM_CHAT_ID не установлен${NC}"
    else
        echo -e "${GREEN}✅ TELEGRAM_CHAT_ID установлен${NC}"
        echo "   Chat ID: $chat_id"
    fi
    
    # Если оба параметра установлены
    if [ -n "$token" ] && [ "$token" != "None" ] && [ -n "$chat_id" ] && [ "$chat_id" != "None" ]; then
        ok_services=$((ok_services + 1))
        echo -e "${GREEN}✅ Telegram полностью настроен${NC}"
    else
        echo -e "${YELLOW}⚠️  Telegram не полностью настроен${NC}"
    fi
    
    echo ""
}

echo "═══════════════════════════════════════════════════════════════"
echo "  Проверка Telegram credentials во всех сервисах"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# Проверка всех сервисов с Telegram интеграцией

check_telegram_credentials "scanner-go-gateway" "Go Gateway"
check_telegram_credentials "scanner-signal-tracker" "Signal Performance Tracker"

# Проверка bot-nest (если запущен)
if docker ps --format '{{.Names}}' | grep -q "bot-nest"; then
    check_telegram_credentials "bot-nest" "Bot Nest (Node.js)"
fi

# Проверка notify-worker (если запущен)
if docker ps --format '{{.Names}}' | grep -q "notify-worker"; then
    check_telegram_credentials "notify-worker" "Notify Worker"
fi

echo "═══════════════════════════════════════════════════════════════"
echo "  Проверка Redis streams для Telegram"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# Проверка notify:telegram stream
echo "🔍 Проверка stream notify:telegram:"
notify_len=$(docker exec scanner-redis redis-cli XLEN "notify:telegram" 2>/dev/null || echo "0")

if [ "$notify_len" -gt 0 ]; then
    echo -e "${GREEN}✅ Stream существует${NC}"
    echo "   Длина: $notify_len сообщений"
    
    # Последнее сообщение
    echo "   Последнее сообщение:"
    docker exec scanner-redis redis-cli XREVRANGE "notify:telegram" + - COUNT 1 2>/dev/null | head -10 | sed 's/^/      /'
else
    echo -e "${YELLOW}⚠️  Stream пустой (это нормально, если не было уведомлений)${NC}"
    echo "   Длина: $notify_len"
fi
echo ""

# Проверка consumer groups для notify:telegram
echo "🔍 Проверка consumer groups для notify:telegram:"
groups=$(docker exec scanner-redis redis-cli XINFO GROUPS "notify:telegram" 2>/dev/null || echo "")

if [ -n "$groups" ]; then
    echo -e "${GREEN}✅ Consumer groups найдены${NC}"
    echo "$groups" | sed 's/^/   /'
else
    echo -e "${YELLOW}⚠️  Consumer groups не найдены${NC}"
fi
echo ""

echo "═══════════════════════════════════════════════════════════════"
echo "  Тест отправки в Telegram API"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# Получаем credentials из любого контейнера
token=$(docker exec scanner-go-gateway env 2>/dev/null | grep TELEGRAM_BOT_TOKEN | cut -d'=' -f2 || echo "")
chat_id=$(docker exec scanner-go-gateway env 2>/dev/null | grep TELEGRAM_CHAT_ID | cut -d'=' -f2 || echo "")

if [ -z "$token" ] || [ "$token" = "None" ]; then
    # Попробуем из signal-tracker
    token=$(docker exec scanner-signal-tracker env 2>/dev/null | grep TELEGRAM_BOT_TOKEN | cut -d'=' -f2 || echo "")
    chat_id=$(docker exec scanner-signal-tracker env 2>/dev/null | grep TELEGRAM_CHAT_ID | cut -d'=' -f2 || echo "")
fi

if [ -n "$token" ] && [ "$token" != "None" ] && [ -n "$chat_id" ] && [ "$chat_id" != "None" ]; then
    echo "Отправка тестового сообщения..."
    
    test_message="🧪 Тест Telegram интеграции Scanner Infrastructure%0A%0A✅ Все сервисы проверены%0A⏰ $(date '+%Y-%m-%d %H:%M:%S')"
    
    response=$(curl -s -w "\n%{http_code}" -X POST \
        "https://api.telegram.org/bot${token}/sendMessage" \
        -d "chat_id=${chat_id}" \
        -d "text=${test_message}" \
        2>/dev/null || echo "error\n000")
    
    http_code=$(echo "$response" | tail -1)
    
    if [ "$http_code" = "200" ]; then
        echo -e "${GREEN}✅ Тестовое сообщение успешно отправлено!${NC}"
        echo "   Проверьте Telegram"
    else
        echo -e "${RED}❌ Ошибка отправки (HTTP $http_code)${NC}"
        echo "   Response: $(echo "$response" | head -1)"
    fi
else
    echo -e "${YELLOW}⚠️  Credentials не найдены, пропускаем тест отправки${NC}"
fi

echo ""

echo "═══════════════════════════════════════════════════════════════"
echo "  ИТОГОВАЯ СВОДКА"
echo "═══════════════════════════════════════════════════════════════"
echo ""

echo "📊 Сервисов с Telegram: $total_services"
echo "✅ Настроены корректно: $ok_services"

if [ "$ok_services" -eq "$total_services" ] && [ "$total_services" -gt 0 ]; then
    echo ""
    echo -e "${GREEN}✅ Все сервисы настроены правильно!${NC}"
    echo ""
    echo "Telegram уведомления будут работать:"
    echo "  📊 Go Gateway → Уведомления о сигналах"
    echo "  📊 Signal Tracker → Статистика каждые 3 часа"
    echo "  📊 Bot Nest → Обработка команд и callbacks"
elif [ "$ok_services" -gt 0 ]; then
    echo ""
    echo -e "${YELLOW}⚠️  Некоторые сервисы не настроены${NC}"
    echo ""
    echo "Настроенные: $ok_services из $total_services"
    echo ""
    echo "Для исправления:"
    echo "  1. Создайте .env файл с credentials"
    echo "  2. Или добавьте в docker-compose.yml:"
    echo "     environment:"
    echo "       - TELEGRAM_BOT_TOKEN=your_token"
    echo "       - TELEGRAM_CHAT_ID=your_chat_id"
    echo "  3. Перезапустите: make down && make up-bg"
else
    echo ""
    echo -e "${RED}❌ Ни один сервис не настроен!${NC}"
    echo ""
    echo "КРИТИЧНО: Telegram уведомления НЕ БУДУТ РАБОТАТЬ"
    echo ""
    echo "Решение:"
    echo "  1. Получите Telegram Bot Token от @BotFather"
    echo "  2. Получите Chat ID (напишите боту /start и используйте @userinfobot)"
    echo "  3. Создайте .env файл:"
    echo "     cat > .env << EOF"
    echo "     TELEGRAM_BOT_TOKEN=your_token"
    echo "     TELEGRAM_CHAT_ID=your_chat_id"
    echo "     EOF"
    echo "  4. Перезапустите систему: make down && make up-bg"
fi

echo ""
echo "═══════════════════════════════════════════════════════════════"

