#!/bin/bash
# Production Migration Plan - Пошаговая миграция с rollback
# Usage: ./scripts/migration_plan.sh [step]

set -e

COMPOSE_FILE="docker-compose.yml"
OLD_SERVICE="python-worker"
NEW_SERVICE="multi-symbol-orderflow"
BACKUP_DIR="./migration_backups/$(date +%Y%m%d_%H%M%S)"

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}   PRODUCTION MIGRATION: python-worker → multi-symbol-orderflow${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo ""

# Функция для создания бэкапа
backup_state() {
    echo -e "${YELLOW}📦 Создание бэкапа...${NC}"
    mkdir -p "$BACKUP_DIR"
    
    # Бэкап docker-compose.yml
    cp "$COMPOSE_FILE" "$BACKUP_DIR/docker-compose.yml.backup"
    
    # Бэкап логов старого сервиса
    docker logs "$OLD_SERVICE" > "$BACKUP_DIR/${OLD_SERVICE}.log" 2>&1 || true
    
    # Бэкап конфигурации
    docker inspect "$OLD_SERVICE" > "$BACKUP_DIR/${OLD_SERVICE}.inspect.json" 2>&1 || true
    
    echo -e "${GREEN}✅ Бэкап создан: $BACKUP_DIR${NC}"
}

# Шаг 1: Pre-flight проверки
step1_preflight() {
    echo -e "${YELLOW}🔍 Шаг 1: Pre-flight проверки${NC}"
    echo ""
    
    # Проверка что старый сервис запущен
    if ! docker ps | grep -q "$OLD_SERVICE"; then
        echo -e "${RED}❌ $OLD_SERVICE не запущен!${NC}"
        exit 1
    fi
    echo -e "${GREEN}✅ $OLD_SERVICE запущен${NC}"
    
    # Проверка Redis
    if ! docker ps | grep -q "scanner-redis"; then
        echo -e "${RED}❌ Redis не запущен!${NC}"
        exit 1
    fi
    echo -e "${GREEN}✅ Redis запущен${NC}"
    
    # Проверка что есть сигналы
    signal_count=$(docker logs "$OLD_SERVICE" --since 1h 2>&1 | grep -c "Сигнал опубликован" || true)
    if [ "$signal_count" -eq 0 ]; then
        echo -e "${YELLOW}⚠️  Нет сигналов за последний час (проверьте что система работает)${NC}"
    else
        echo -e "${GREEN}✅ Сигналы генерируются: $signal_count за последний час${NC}"
    fi
    
    # Проверка disk space
    disk_usage=$(df -h / | awk 'NR==2 {print $5}' | sed 's/%//')
    if [ "$disk_usage" -gt 90 ]; then
        echo -e "${YELLOW}⚠️  Disk usage: ${disk_usage}% (рекомендуется <90%)${NC}"
    else
        echo -e "${GREEN}✅ Disk usage: ${disk_usage}%${NC}"
    fi
    
    echo ""
    echo -e "${GREEN}✅ Все pre-flight проверки пройдены${NC}"
}

# Шаг 2: Запуск нового сервиса (параллельно)
step2_start_new() {
    echo -e "${YELLOW}🚀 Шаг 2: Запуск нового сервиса (параллельно со старым)${NC}"
    echo ""
    
    backup_state
    
    # Запускаем новый сервис
    docker-compose up -d "$NEW_SERVICE"
    
    # Ждем запуска
    echo "⏳ Ожидание запуска нового сервиса (30 секунд)..."
    sleep 30
    
    # Проверка что запустился
    if ! docker ps | grep -q "$NEW_SERVICE"; then
        echo -e "${RED}❌ $NEW_SERVICE не запустился!${NC}"
        echo "Логи:"
        docker logs "$NEW_SERVICE" --tail 50
        exit 1
    fi
    
    echo -e "${GREEN}✅ $NEW_SERVICE запущен${NC}"
    
    # Проверка health
    health=$(docker inspect "$NEW_SERVICE" --format='{{.State.Health.Status}}' 2>/dev/null || echo "none")
    echo "Health status: $health"
    
    echo ""
    echo -e "${GREEN}✅ Новый сервис запущен параллельно${NC}"
    echo -e "${YELLOW}📊 Теперь запустите A/B тестирование: python scripts/ab_testing_compare.py --duration 24${NC}"
}

# Шаг 3: Проверка результатов A/B тестирования
step3_check_ab() {
    echo -e "${YELLOW}📊 Шаг 3: Проверка результатов A/B тестирования${NC}"
    echo ""
    
    if [ ! -f "ab_test_report.json" ]; then
        echo -e "${YELLOW}⚠️  Файл ab_test_report.json не найден${NC}"
        echo "Запустите: python scripts/ab_testing_compare.py --duration 24 --output ab_test_report.json"
        exit 1
    fi
    
    # Парсим результат
    ready=$(cat ab_test_report.json | grep -o '"ready_for_migration":[^,]*' | cut -d':' -f2 | tr -d ' ')
    
    if [ "$ready" == "true" ]; then
        echo -e "${GREEN}✅ A/B тестирование пройдено успешно!${NC}"
        echo "Готово к миграции."
    else
        echo -e "${RED}❌ A/B тестирование НЕ пройдено!${NC}"
        echo "Детали в ab_test_report.json"
        exit 1
    fi
}

# Шаг 4: Остановка старого сервиса
step4_stop_old() {
    echo -e "${YELLOW}⏹️  Шаг 4: Остановка старого сервиса${NC}"
    echo ""
    
    read -p "Вы уверены что хотите остановить $OLD_SERVICE? (yes/no): " confirm
    if [ "$confirm" != "yes" ]; then
        echo "Отменено"
        exit 0
    fi
    
    backup_state
    
    # Останавливаем старый сервис
    docker-compose stop "$OLD_SERVICE"
    
    echo -e "${GREEN}✅ $OLD_SERVICE остановлен${NC}"
    echo -e "${YELLOW}📊 Мониторинг нового сервиса: docker-compose logs -f $NEW_SERVICE${NC}"
    echo ""
    echo -e "${YELLOW}⚠️  Если возникнут проблемы, запустите rollback: $0 rollback${NC}"
}

# Шаг 5: Финализация (удаление старого из docker-compose.yml)
step5_finalize() {
    echo -e "${YELLOW}🏁 Шаг 5: Финализация миграции${NC}"
    echo ""
    
    read -p "Вы уверены что хотите финализировать миграцию? (yes/no): " confirm
    if [ "$confirm" != "yes" ]; then
        echo "Отменено"
        exit 0
    fi
    
    backup_state
    
    echo "Закомментируйте старый сервис в docker-compose.yml вручную или:"
    echo "  # $OLD_SERVICE - DEPRECATED, используйте $NEW_SERVICE"
    echo ""
    echo -e "${GREEN}✅ Миграция завершена!${NC}"
    echo ""
    echo "Следующие шаги:"
    echo "  1. Мониторить новый сервис 24-48 часов"
    echo "  2. Если стабильно - удалить старый код"
    echo "  3. Обновить документацию"
}

# Rollback - откат к старому сервису
rollback() {
    echo -e "${RED}🔙 ROLLBACK: Возврат к старому сервису${NC}"
    echo ""
    
    # Останавливаем новый
    docker-compose stop "$NEW_SERVICE"
    
    # Запускаем старый
    docker-compose up -d "$OLD_SERVICE"
    
    # Ждем запуска
    sleep 15
    
    # Проверка
    if docker ps | grep -q "$OLD_SERVICE"; then
        echo -e "${GREEN}✅ Rollback выполнен успешно, $OLD_SERVICE запущен${NC}"
    else
        echo -e "${RED}❌ Ошибка rollback!${NC}"
        exit 1
    fi
    
    echo ""
    echo "Логи последнего бэкапа: $BACKUP_DIR"
}

# Main
case "${1:-help}" in
    "step1"|"1")
        step1_preflight
        ;;
    "step2"|"2")
        step2_start_new
        ;;
    "step3"|"3")
        step3_check_ab
        ;;
    "step4"|"4")
        step4_stop_old
        ;;
    "step5"|"5")
        step5_finalize
        ;;
    "rollback")
        rollback
        ;;
    "all")
        step1_preflight
        echo ""
        step2_start_new
        echo ""
        echo -e "${YELLOW}⏸️  Пауза для A/B тестирования${NC}"
        echo "Запустите A/B тестирование (24-48 часов), затем:"
        echo "  $0 step3  # Проверка результатов"
        echo "  $0 step4  # Остановка старого"
        echo "  $0 step5  # Финализация"
        ;;
    *)
        echo "Usage: $0 {step1|step2|step3|step4|step5|all|rollback}"
        echo ""
        echo "Шаги миграции:"
        echo "  step1 - Pre-flight проверки"
        echo "  step2 - Запуск нового сервиса (параллельно)"
        echo "  step3 - Проверка A/B тестирования"
        echo "  step4 - Остановка старого сервиса"
        echo "  step5 - Финализация"
        echo "  all   - Запустить step1+step2 (затем вручную step3-5)"
        echo "  rollback - Откат к старому сервису"
        ;;
esac

