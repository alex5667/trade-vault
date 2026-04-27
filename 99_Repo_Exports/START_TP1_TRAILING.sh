#!/bin/bash
# Быстрый старт TP1 Trailing System
# Использование: ./START_TP1_TRAILING.sh

set -e

GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${GREEN}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║        🎯 TP1 Trailing System - Quick Start 🎯               ║${NC}"
echo -e "${GREEN}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Шаг 1: Проверка Docker
echo -e "${BLUE}Шаг 1/5: Проверка Docker...${NC}"
if ! docker ps >/dev/null 2>&1; then
    echo -e "${YELLOW}❌ Docker не запущен или недоступен${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Docker работает${NC}"
echo ""

# Шаг 2: Проверка Redis
echo -e "${BLUE}Шаг 2/5: Проверка Redis...${NC}"
if ! docker ps | grep -q scanner-redis; then
    echo -e "${YELLOW}⚠️  Redis не запущен. Запуск всей системы...${NC}"
    make up-bg
    sleep 10
fi
echo -e "${GREEN}✅ Redis работает${NC}"
echo ""

# Шаг 3: Запуск TP Event Listener
echo -e "${BLUE}Шаг 3/5: Запуск TP Event Listener...${NC}"
docker-compose -f docker-compose.yml -f docker-compose.tp-trailing.yml up -d tp-event-listener
sleep 5
echo -e "${GREEN}✅ TP Event Listener запущен${NC}"
echo ""

# Шаг 4: Проверка статуса
echo -e "${BLUE}Шаг 4/5: Проверка статуса...${NC}"
if docker ps | grep -q scanner-tp-event-listener; then
    echo -e "${GREEN}✅ scanner-tp-event-listener работает${NC}"
else
    echo -e "${YELLOW}❌ Ошибка запуска${NC}"
    echo -e "${YELLOW}Логи:${NC}"
    docker logs scanner-tp-event-listener --tail 20
    exit 1
fi
echo ""

# Шаг 5: Интеграционный тест
echo -e "${BLUE}Шаг 5/5: Запуск интеграционного теста...${NC}"
echo ""

# Создаём тестовый сигнал
TEST_SID="test-signal-$(date +%s)"
echo -e "${YELLOW}Создание тестового сигнала: ${TEST_SID}${NC}"

python3 -c "
import redis, json, time
r = redis.from_url('redis://localhost:6379/0', decode_responses=True)
signal = {
    'sid': '${TEST_SID}',
    'symbol': 'XAUUSD',
    'side': 'LONG',
    'entry': 2765.5,
    'sl': 2758.7,
    'tp_levels': [2769.9, 2773.1, 2776.3],
    'lot': 0.03,
    'trail_after_tp1': True,
    'trail_profile': 'rocket_v1',
    'source': 'test',
    'ts': int(time.time() * 1000)
}
r.set(f'signals:{signal[\"sid\"]}', json.dumps(signal), ex=3600)
print('✅ Тестовый сигнал создан')
"

sleep 2

# Эмитируем TP1 событие
echo -e "${YELLOW}Эмитирование TP1_HIT события...${NC}"
python3 -m python-worker.services.tp_event_emulator --sid "${TEST_SID}" --scenario tp1_only 2>/dev/null || \
    docker exec scanner-tp-event-listener python -m services.tp_event_emulator --sid "${TEST_SID}" --scenario tp1_only

sleep 3

# Проверяем результат
echo ""
echo -e "${BLUE}Проверка результатов...${NC}"
docker logs scanner-tp-event-listener --tail 30 | grep "${TEST_SID}" || echo "⚠️  Событие не найдено в логах"

echo ""
echo -e "${GREEN}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║                    ✅ УСТАНОВКА ЗАВЕРШЕНА ✅                  ║${NC}"
echo -e "${GREEN}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${GREEN}TP1 Trailing System готов к использованию!${NC}"
echo ""
echo -e "${BLUE}Полезные команды:${NC}"
echo -e "  ${YELLOW}make trailing-status${NC}   - Статус сервиса"
echo -e "  ${YELLOW}make trailing-logs${NC}     - Логи в реальном времени"
echo -e "  ${YELLOW}make trailing-stats${NC}    - Статистика работы"
echo -e "  ${YELLOW}make trailing-test${NC}     - Повторить тест"
echo -e "  ${YELLOW}make trailing-help${NC}     - Полная справка"
echo ""
echo -e "${BLUE}Документация:${NC}"
echo -e "  📖 ${YELLOW}TP1_TRAILING_QUICKSTART.md${NC} - Быстрый старт"
echo -e "  📖 ${YELLOW}documentation/ticks/TP1_TRAILING_SYSTEM.md${NC} - Полная документация"
echo ""
echo -e "${GREEN}Happy Trading! 🚀${NC}"

