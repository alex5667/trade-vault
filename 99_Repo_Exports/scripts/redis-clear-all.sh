#!/bin/bash

# Скрипт полной очистки Redis
# Удаляет все ключи, стримы и данные

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

echo -e "${RED}🧹 ПОЛНАЯ ОЧИСТКА REDIS${NC}"
echo -e "${RED}========================${NC}"
echo -e "${YELLOW}⚠️  ВНИМАНИЕ: Это удалит ВСЕ данные из Redis!${NC}"
echo

# Подтверждение
read -p "Вы уверены, что хотите удалить ВСЕ данные? (yes/no): " confirm
if [ "$confirm" != "yes" ]; then
    echo -e "${YELLOW}❌ Очистка отменена${NC}"
    exit 0
fi

echo -e "${BLUE}🔍 Проверка подключения к Redis...${NC}"
if ! $REDIS_CLI ping > /dev/null 2>&1; then
    echo -e "${RED}❌ Redis недоступен на $REDIS_HOST:$REDIS_PORT${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Redis доступен${NC}"

# Получаем общее количество ключей
echo -e "${BLUE}📊 Подсчет общего количества ключей...${NC}"
total_keys=$($REDIS_CLI dbsize)
echo -e "${YELLOW}Всего ключей: $total_keys${NC}"

if [ "$total_keys" -eq 0 ]; then
    echo -e "${GREEN}✅ Redis уже пуст${NC}"
    exit 0
fi

# Очищаем по паттернам
echo -e "${BLUE}🧹 Очистка по паттернам...${NC}"

# 1. Очищаем все стримы
echo -e "${YELLOW}📡 Очистка стримов...${NC}"
stream_keys=$($REDIS_CLI --scan --pattern "*stream*" | wc -l)
echo -e "  Найдено стримов: $stream_keys"
if [ "$stream_keys" -gt 0 ]; then
    $REDIS_CLI --scan --pattern "*stream*" | xargs -I {} $REDIS_CLI del {}
    echo -e "  ${GREEN}✅ Стримы очищены${NC}"
fi

# 2. Очищаем все сигналы
echo -e "${YELLOW}📡 Очистка сигналов...${NC}"
signal_keys=$($REDIS_CLI --scan --pattern "*signal*" | wc -l)
echo -e "  Найдено сигналов: $signal_keys"
if [ "$signal_keys" -gt 0 ]; then
    $REDIS_CLI --scan --pattern "*signal*" | xargs -I {} $REDIS_CLI del {}
    echo -e "  ${GREEN}✅ Сигналы очищены${NC}"
fi

# 3. Очищаем все уведомления
echo -e "${YELLOW}📡 Очистка уведомлений...${NC}"
notify_keys=$($REDIS_CLI --scan --pattern "*notify*" | wc -l)
echo -e "  Найдено уведомлений: $notify_keys"
if [ "$notify_keys" -gt 0 ]; then
    $REDIS_CLI --scan --pattern "*notify*" | xargs -I {} $REDIS_CLI del {}
    echo -e "  ${GREEN}✅ Уведомления очищены${NC}"
fi

# 4. Очищаем все свечи
echo -e "${YELLOW}📊 Очистка свечей...${NC}"
kline_keys=$($REDIS_CLI --scan --pattern "*kline*" | wc -l)
echo -e "  Найдено свечей: $kline_keys"
if [ "$kline_keys" -gt 0 ]; then
    $REDIS_CLI --scan --pattern "*kline*" | xargs -I {} $REDIS_CLI del {}
    echo -e "  ${GREEN}✅ Свечи очищены${NC}"
fi

# 5. Очищаем все тикеры
echo -e "${YELLOW}📈 Очистка тикеров...${NC}"
ticker_keys=$($REDIS_CLI --scan --pattern "*ticker*" | wc -l)
echo -e "  Найдено тикеров: $ticker_keys"
if [ "$ticker_keys" -gt 0 ]; then
    $REDIS_CLI --scan --pattern "*ticker*" | xargs -I {} $REDIS_CLI del {}
    echo -e "  ${GREEN}✅ Тикеры очищены${NC}"
fi

# 6. Очищаем все ATR
echo -e "${YELLOW}📊 Очистка ATR...${NC}"
atr_keys=$($REDIS_CLI --scan --pattern "*ATR*" | wc -l)
echo -e "  Найдено ATR: $atr_keys"
if [ "$atr_keys" -gt 0 ]; then
    $REDIS_CLI --scan --pattern "*ATR*" | xargs -I {} $REDIS_CLI del {}
    echo -e "  ${GREEN}✅ ATR очищены${NC}"
fi

# 7. Очищаем все символы
echo -e "${YELLOW}🔤 Очистка символов...${NC}"
symbol_keys=$($REDIS_CLI --scan --pattern "*symbol*" | wc -l)
echo -e "  Найдено символов: $symbol_keys"
if [ "$symbol_keys" -gt 0 ]; then
    $REDIS_CLI --scan --pattern "*symbol*" | xargs -I {} $REDIS_CLI del {}
    echo -e "  ${GREEN}✅ Символы очищены${NC}"
fi

# 8. Очищаем все Binance данные
echo -e "${YELLOW}🏦 Очистка Binance данных...${NC}"
binance_keys=$($REDIS_CLI --scan --pattern "*binance*" | wc -l)
echo -e "  Найдено Binance данных: $binance_keys"
if [ "$binance_keys" -gt 0 ]; then
    $REDIS_CLI --scan --pattern "*binance*" | xargs -I {} $REDIS_CLI del {}
    echo -e "  ${GREEN}✅ Binance данные очищены${NC}"
fi

# 9. Очищаем все dedup
echo -e "${YELLOW}🔄 Очистка dedup...${NC}"
dedup_keys=$($REDIS_CLI --scan --pattern "*dedup*" | wc -l)
echo -e "  Найдено dedup: $dedup_keys"
if [ "$dedup_keys" -gt 0 ]; then
    $REDIS_CLI --scan --pattern "*dedup*" | xargs -I {} $REDIS_CLI del {}
    echo -e "  ${GREEN}✅ Dedup очищены${NC}"
fi

    # 10. Очищаем все остальные ключи
    echo -e "${YELLOW}🧹 Очистка остальных ключей...${NC}"
    remaining_keys=$($REDIS_CLI dbsize)
    echo -e "  Осталось ключей: $remaining_keys"
    
    if [ "$remaining_keys" -gt 0 ]; then
        echo -e "  ${YELLOW}Удаление оставшихся ключей...${NC}"
        # Используем более безопасный способ удаления
        $REDIS_CLI --scan | while read -r key; do
            if [ -n "$key" ]; then
                $REDIS_CLI del "$key" > /dev/null 2>&1 || true
            fi
        done
        echo -e "  ${GREEN}✅ Остальные ключи очищены${NC}"
    fi

# Проверяем результат
echo -e "${BLUE}📊 Проверка результата...${NC}"
final_keys=$($REDIS_CLI dbsize)
echo -e "${GREEN}✅ Очистка завершена!${NC}"
echo -e "${BLUE}Ключей до очистки: $total_keys${NC}"
echo -e "${BLUE}Ключей после очистки: $final_keys${NC}"

# Очищаем память
echo -e "${BLUE}🧹 Очистка памяти...${NC}"
$REDIS_CLI memory purge > /dev/null 2>&1 || true
echo -e "${GREEN}✅ Память очищена${NC}"

echo -e "\n${GREEN}🎉 Redis полностью очищен!${NC}" 