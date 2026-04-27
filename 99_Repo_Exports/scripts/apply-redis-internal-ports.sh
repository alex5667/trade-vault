#!/bin/bash

# Скрипт для применения новой Redis архитектуры
# Внутри контейнеров: обмен через 6380, 6381
# Снаружи: доступ через 6379

set -e

# Цвета
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m'

echo -e "${PURPLE}🔧 Применение новой Redis архитектуры${NC}"
echo -e "${PURPLE}====================================${NC}"
echo
echo "Архитектура:"
echo "  • Внутри контейнеров: worker Redis (6380, 6381)"
echo "  • Снаружи: основной Redis (6379)"
echo

# Функция для проверки успешности операции
check_success() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ $1${NC}"
    else
        echo -e "${RED}❌ $1${NC}"
        exit 1
    fi
}

# Шаг 1: Остановка сервисов
echo -e "${BLUE}1. Остановка сервисов...${NC}"
docker-compose down
check_success "Сервисы остановлены"

# Шаг 2: Пересборка всех образов
echo -e "${BLUE}2. Пересборка образов...${NC}"
docker-compose build go-worker telegram-worker python-worker notify-worker
check_success "Образы пересобраны"

# Шаг 3: Запуск сервисов
echo -e "${BLUE}3. Запуск сервисов с новой архитектурой...${NC}"
docker-compose up -d
check_success "Сервисы запущены"

# Шаг 4: Ожидание готовности всех Redis
echo -e "${BLUE}4. Ожидание готовности Redis сервисов...${NC}"
echo "  ⏳ Основной Redis (6379)..."
for i in {1..60}; do
    if redis-cli -h localhost -p 6379 ping > /dev/null 2>&1; then
        echo -e "${GREEN}  ✅ Redis (6379) готов${NC}"
        break
    fi
    if [ $i -eq 60 ]; then
        echo -e "${RED}  ❌ Redis (6379) не готов${NC}"
        exit 1
    fi
    sleep 1
done

echo "  ⏳ Worker-1 Redis (6380)..."
for i in {1..60}; do
    if redis-cli -h localhost -p 6380 ping > /dev/null 2>&1; then
        echo -e "${GREEN}  ✅ Worker-1 Redis (6380) готов${NC}"
        break
    fi
    if [ $i -eq 60 ]; then
        echo -e "${RED}  ❌ Worker-1 Redis (6380) не готов${NC}"
        exit 1
    fi
    sleep 1
done

echo "  ⏳ Worker-2 Redis (6381)..."
for i in {1..60}; do
    if redis-cli -h localhost -p 6381 ping > /dev/null 2>&1; then
        echo -e "${GREEN}  ✅ Worker-2 Redis (6381) готов${NC}"
        break
    fi
    if [ $i -eq 60 ]; then
        echo -e "${RED}  ❌ Worker-2 Redis (6381) не готов${NC}"
        exit 1
    fi
    sleep 1
done

# Шаг 5: Проверка архитектуры
echo -e "${BLUE}5. Проверка новой архитектуры...${NC}"
sleep 30

# Проверяем внешний доступ (только 6379)
echo "  📊 Внешний доступ (6379):"
EXTERNAL_CLIENTS=$(redis-cli -h localhost -p 6379 info clients | grep "connected_clients:" | cut -d: -f2 | tr -d '\r')
echo "    Подключенных клиентов: $EXTERNAL_CLIENTS"

# Проверяем внутренние worker порты
echo "  📊 Внутренние worker порты:"
WORKER1_CLIENTS=$(redis-cli -h localhost -p 6380 info clients | grep "connected_clients:" | cut -d: -f2 | tr -d '\r')
WORKER2_CLIENTS=$(redis-cli -h localhost -p 6381 info clients | grep "connected_clients:" | cut -d: -f2 | tr -d '\r')
echo "    Worker-1 (6380): $WORKER1_CLIENTS клиентов"
echo "    Worker-2 (6381): $WORKER2_CLIENTS клиентов"

# Проверяем логи go-worker
echo "  📊 Проверка логов go-worker:"
docker logs scanner-go-worker --tail 10 | grep "Redis.*подключение установлено" || echo "    Ожидание подключений..."

echo
echo -e "${GREEN}=== Новая Redis архитектура применена! ===${NC}"
echo
echo "�� Результаты:"
echo "  • Внешний доступ (6379): $EXTERNAL_CLIENTS клиентов"
echo "  • Worker-1 (6380): $WORKER1_CLIENTS клиентов"  
echo "  • Worker-2 (6381): $WORKER2_CLIENTS клиентов"
echo
echo "🔍 Мониторинг:"
echo "  • Внешний: redis-cli -h localhost -p 6379 info"
echo "  • Worker-1: redis-cli -h localhost -p 6380 info"
echo "  • Worker-2: redis-cli -h localhost -p 6381 info"
echo
