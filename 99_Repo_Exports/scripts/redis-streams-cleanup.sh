#!/bin/bash

# Скрипт настройки очистки Redis Streams
# Устанавливает TTL и максимальное количество записей для автоматической очистки

set -e

# Цвета
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

# Конфигурация
REDIS_HOST=${REDIS_HOST:-localhost}
REDIS_PORT=${REDIS_PORT:-6379}
REDIS_CLI="redis-cli -h $REDIS_HOST -p $REDIS_PORT"

# Настройки очистки по умолчанию
DEFAULT_MAXLEN=1000        # Максимальное количество записей в стриме
DEFAULT_TTL_HOURS=24       # TTL в часах (24 часа = 1 день)
DEFAULT_TTL_MINUTES=1440   # TTL в минутах

# Логирование в Docker
LOG_PREFIX="[$(date '+%Y-%m-%d %H:%M:%S')] Redis Cleanup:"

echo -e "${BLUE}🧹 Настройка очистки Redis Streams${NC}"
echo -e "${BLUE}========================================${NC}"

# Проверка подключения к Redis
check_redis_connection() {
    echo -e "${YELLOW}🔍 Проверка подключения к Redis...${NC}"
    
    if $REDIS_CLI ping > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Redis доступен на $REDIS_HOST:$REDIS_PORT${NC}"
        return 0
    else
        echo -e "${RED}❌ Redis недоступен на $REDIS_HOST:$REDIS_PORT${NC}"
        return 1
    fi
}

# Настройка очистки для конкретного стрима
setup_stream_cleanup() {
    local stream_name="$1"
    local maxlen="${2:-$DEFAULT_MAXLEN}"
    local ttl_hours="${3:-$DEFAULT_TTL_HOURS}"
    
    echo -e "\n${YELLOW}🔧 Настройка очистки для стрима: ${stream_name}${NC}"
    
    # Проверяем, существует ли стрим
    local stream_length=$($REDIS_CLI xlen "$stream_name" 2>/dev/null || echo "0")
    
    if [ "$stream_length" = "0" ]; then
        echo -e "  ${YELLOW}⏸️ Стрим пустой, пропускаем${NC}"
        return
    fi
    
    echo -e "  📊 Текущая длина: ${GREEN}$stream_length${NC}"
    
    # Устанавливаем максимальную длину стрима
    echo -e "  📏 Устанавливаем максимальную длину: ${GREEN}$maxlen${NC}"
    
    # Используем XTRIM для установки максимальной длины
    echo -e "  🧹 Применяем ограничение длины..."
    $REDIS_CLI xtrim "$stream_name" maxlen "$maxlen" > /dev/null 2>&1 || true
    
    # Логируем в Docker
    echo "$LOG_PREFIX Стрим $stream_name обрезан до $maxlen записей (было $stream_length)"
    
    echo -e "  ${GREEN}✅ Настройки применены${NC}"
}

# Настройка очистки для всех основных стримов
setup_all_streams_cleanup() {
    echo -e "\n${BLUE}📡 Настройка очистки для всех стримов${NC}"
    
    # Основные стримы системы scanner-infra
    local streams=(
        "stream:symbol-to-redis:1000:168"      # Символы: 1000 записей, 7 дней
        "stream:kline_1m:10000:24"             # 1m свечи: 10000 записей, 1 день
        "stream:kline_5m:3000:72"             # 5m свечи: 3000 записей, 3 дня
        "stream:kline_15m:2000:168"           # 15m свечи: 2000 записей, 7 дней
        "stream:kline_30m:1500:168"           # 30m свечи: 1500 записей, 7 дней
        "stream:kline_1h:1000:720"            # 1h свечи: 1000 записей, 30 дней
        "stream:kline_4h:500:1440"            # 4h свечи: 500 записей, 60 дней
        "stream:kline_1d:100:4320"            # 1d свечи: 100 записей, 180 дней
        "signal:telegram:raw:2000:168"        # Telegram сырые: 2000 записей, 7 дней
        "signal:telegram:parsed:1000:720"     # Telegram парсинг: 1000 записей, 30 дней
        "notify:telegram:500:168"             # Уведомления: 500 записей, 7 дней
        "stream:volatility:1000:168"          # Волатильность: 1000 записей, 7 дней
        "stream:top-gainers:500:168"          # Топ растущих: 500 записей, 7 дней
        "stream:top-losers:500:168"           # Топ падающих: 500 записей, 7 дней
    )
    
    # Добавляем функцию для поиска существующих стримов
    echo -e "${YELLOW}🔍 Поиск существующих стримов...${NC}"
    local existing_streams=()
    
    # Проверяем каждый стрим из списка
    for stream_config in "${streams[@]}"; do
        # Разбираем строку по первому двоеточию (только для maxlen)
        local stream_name=$(echo "$stream_config" | cut -d: -f1-3)
        local maxlen=$(echo "$stream_config" | cut -d: -f4)
        local ttl_hours=$(echo "$stream_config" | cut -d: -f5)
        
        echo -e "  ${BLUE}🔍 Проверяем стрим: $stream_name${NC}"
        
        # Проверяем длину стрима
        local stream_length=$($REDIS_CLI xlen "$stream_name" 2>/dev/null || echo "ERROR")
        echo -e "    ${BLUE}Длина стрима $stream_name: $stream_length${NC}"
        
        if [ "$stream_length" != "ERROR" ] && [ "$stream_length" -ge 0 ]; then
            existing_streams+=("$stream_config")
            echo -e "  ${GREEN}✅ Найден стрим: $stream_name (длина: $stream_length)${NC}"
        else
            echo -e "  ${YELLOW}⏸️ Стрим не найден: $stream_name${NC}"
        fi
    done
    
    # Обрабатываем только существующие стримы
    for stream_config in "${existing_streams[@]}"; do
        # Разбираем строку по первому двоеточию (только для maxlen)
        local stream_name=$(echo "$stream_config" | cut -d: -f1-3)
        local maxlen=$(echo "$stream_config" | cut -d: -f4)
        local ttl_hours=$(echo "$stream_config" | cut -d: -f5)
        setup_stream_cleanup "$stream_name" "$maxlen" "$ttl_hours"
    done
}

# Настройка автоматической очистки по расписанию
setup_scheduled_cleanup() {
    echo -e "\n${BLUE}⏰ Настройка автоматической очистки${NC}"
    
    # Создаем cron задачу для ежедневной очистки
    local cron_job="0 2 * * * /usr/bin/docker exec scanner-redis redis-cli --eval /tmp/cleanup.lua"
    
    echo -e "${YELLOW}Рекомендуемый cron для автоматической очистки:${NC}"
    echo -e "  ${GREEN}$cron_job${NC}"
    echo -e "  ${YELLOW}Очистка будет выполняться каждый день в 2:00 утра${NC}"
}

# Создание Lua скрипта для очистки
create_cleanup_lua() {
    echo -e "\n${BLUE}📝 Создание Lua скрипта для очистки${NC}"
    
    cat > cleanup.lua << 'EOF'
-- Lua скрипт для очистки Redis Streams
-- Удаляет старые записи и поддерживает максимальную длину

local streams = {
    {name = "stream:symbol-to-redis", maxlen = 1000},
    {name = "stream:kline_1m", maxlen = 5000},
    {name = "stream:kline_5m", maxlen = 3000},
    {name = "stream:kline_15m", maxlen = 2000},
    {name = "stream:kline_30m", maxlen = 1500},
    {name = "stream:kline_1h", maxlen = 1000},
    {name = "stream:kline_4h", maxlen = 500},
    {name = "stream:kline_1d", maxlen = 100},
    {name = "signal:telegram:raw", maxlen = 2000},
    {name = "signal:telegram:parsed", maxlen = 1000},
    {name = "notify:telegram", maxlen = 500},
    {name = "stream:volatility", maxlen = 1000},
    {name = "stream:top-gainers", maxlen = 500},
    {name = "stream:top-losers", maxlen = 500}
}

local cleaned_count = 0

for _, stream in ipairs(streams) do
    local length = redis.call("XLEN", stream.name)
    if length > stream.maxlen then
        local to_remove = length - stream.maxlen
        redis.call("XTRIM", stream.name, "MAXLEN", stream.maxlen)
        cleaned_count = cleaned_count + to_remove
        print("Очищен стрим " .. stream.name .. ": удалено " .. to_remove .. " записей")
    end
end

print("Всего удалено записей: " .. cleaned_count)
return cleaned_count
EOF

    echo -e "${GREEN}✅ Lua скрипт cleanup.lua создан${NC}"
    echo -e "${YELLOW}Для использования: redis-cli --eval cleanup.lua${NC}"
}

# Мониторинг текущего состояния стримов
monitor_streams_status() {
    echo -e "\n${BLUE}📊 Текущее состояние стримов${NC}"
    
    local streams=(
        "stream:symbol-to-redis"
        "stream:kline_1m"
        "stream:kline_5m"
        "stream:kline_15m"
        "stream:kline_30m"
        "stream:kline_1h"
        "stream:kline_4h"
        "stream:kline_1d"
        "signal:telegram:raw"
        "signal:telegram:parsed"
        "notify:telegram"
        "stream:volatility"
        "stream:top-gainers"
        "stream:top-losers"
    )
    
    echo -e "${YELLOW}Название стрима                    | Длина | Статус${NC}"
    echo -e "${YELLOW}-----------------------------------|-------|--------${NC}"
    
    for stream in "${streams[@]}"; do
        local length=$($REDIS_CLI xlen "$stream" 2>/dev/null || echo "0")
        local status=""
        
        if [ "$length" = "0" ]; then
            status="⏸️ Пустой"
        elif [ "$length" -lt 100 ]; then
            status="🟢 Низкая"
        elif [ "$length" -lt 1000 ]; then
            status="🟡 Средняя"
        else
            status="🔴 Высокая"
        fi
        
        printf "%-35s | %5s | %s\n" "$stream" "$length" "$status"
    done
}

# Основная функция
main() {
    case "${1:-all}" in
        "setup")
            check_redis_connection && setup_all_streams_cleanup
            ;;
        "monitor")
            check_redis_connection && monitor_streams_status
            ;;
        "lua")
            create_cleanup_lua
            ;;
        "cron")
            setup_scheduled_cleanup
            ;;
        "all")
            check_redis_connection && {
                setup_all_streams_cleanup
                echo
                monitor_streams_status
                echo
                setup_scheduled_cleanup
                echo
                create_cleanup_lua
            }
            ;;
        "help"|*)
            echo -e "${BLUE}Использование: $0 [команда]${NC}"
            echo
            echo "Команды:"
            echo "  setup   - Настроить очистку для всех стримов"
            echo "  monitor - Показать текущее состояние стримов"
            echo "  lua     - Создать Lua скрипт для очистки"
            echo "  cron    - Показать настройки cron для автоматической очистки"
            echo "  all     - Выполнить все настройки (по умолчанию)"
            echo "  help    - Показать эту справку"
            ;;
    esac
}

# Запуск скрипта
main "$@" 