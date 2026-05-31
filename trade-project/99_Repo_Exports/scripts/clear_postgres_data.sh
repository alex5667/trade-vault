#!/bin/bash

# Скрипт полной очистки всех данных PostgreSQL
# Удаляет ВСЕ данные из баз данных trade и scanner_analytics

set -e

# Цвета
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

# Конфигурация PostgreSQL
POSTGRES_CONTAINER="scanner-postgres"
DATABASES=("trade" "scanner_analytics")

# Проверка флага --yes
AUTO_CONFIRM=false
if [ "$1" = "--yes" ] || [ "$1" = "-y" ]; then
    AUTO_CONFIRM=true
fi

echo -e "${RED}🗃️ ПОЛНАЯ ОЧИСТКА ВСЕХ ДАННЫХ POSTGRESQL${NC}"
echo -e "${RED}========================================${NC}"
echo -e "${YELLOW}⚠️  ВНИМАНИЕ: Это удалит ВСЕ данные из PostgreSQL!${NC}"
echo -e "${YELLOW}   Включая все таблицы, индексы, данные и т.д.${NC}"
echo ""

# Подтверждение
if [ "$AUTO_CONFIRM" = false ]; then
    read -p "Вы уверены, что хотите удалить ВСЕ данные? (yes/no): " confirm
    if [ "$confirm" != "yes" ]; then
        echo -e "${YELLOW}❌ Очистка отменена${NC}"
        exit 0
    fi
else
    echo -e "${BLUE}Автоматическое подтверждение (--yes)${NC}"
fi

# Функция для выполнения команд PostgreSQL
postgres_cmd() {
    local db=$1
    shift
    docker exec "$POSTGRES_CONTAINER" psql -U postgres -d "$db" -c "$*" 2>/dev/null
}

# Проверка доступности контейнера
echo -e "${BLUE}🔍 Проверка доступности PostgreSQL контейнера...${NC}"

if ! docker ps --format '{{.Names}}' | grep -q "^${POSTGRES_CONTAINER}$"; then
    echo -e "${RED}❌ Контейнер $POSTGRES_CONTAINER не запущен${NC}"
    exit 1
fi

if ! docker exec "$POSTGRES_CONTAINER" pg_isready -U postgres > /dev/null 2>&1; then
    echo -e "${RED}❌ PostgreSQL не отвечает${NC}"
    exit 1
fi

echo -e "  ${GREEN}✅ PostgreSQL доступен${NC}"

echo ""
echo -e "${BLUE}📊 Подсчет общего количества данных...${NC}"

for db in "${DATABASES[@]}"; do
    echo -e "${YELLOW}$db:${NC}"

    # Получаем список всех таблиц
    tables=$(postgres_cmd "$db" "SELECT tablename FROM pg_tables WHERE schemaname = 'public';" | grep -v "tablename\|--\|(" | grep -v "^$" | tr -d ' ')

    if [ -z "$tables" ]; then
        echo -e "  Таблиц: 0"
        continue
    fi

    table_count=$(echo "$tables" | wc -l)
    echo -e "  Таблиц: $table_count"

    total_rows=0
    for table in $tables; do
        row_count=$(postgres_cmd "$db" "SELECT COUNT(*) FROM \"$table\";" | grep -v "count\|--\|(" | tr -d ' ')
        total_rows=$((total_rows + row_count))
        echo -e "    $table: $row_count записей"
    done

    echo -e "  Всего записей: $total_rows"
done

echo ""
echo -e "${BLUE}🗃️ Начало полной очистки...${NC}"

TOTAL_DELETED=0

for db in "${DATABASES[@]}"; do
    echo -e "${YELLOW}Очистка базы данных $db...${NC}"

    # Получаем список всех таблиц
    tables=$(postgres_cmd "$db" "SELECT tablename FROM pg_tables WHERE schemaname = 'public';" | grep -v "tablename\|--\|(" | grep -v "^$" | tr -d ' ')

    if [ -z "$tables" ]; then
        echo -e "  ${GREEN}✅ База данных $db уже пуста${NC}"
        continue
    fi

    # Удаляем все данные из таблиц
    for table in $tables; do
        echo -e "  Очистка таблицы $table..."

        # Получаем количество записей перед удалением
        row_count=$(postgres_cmd "$db" "SELECT COUNT(*) FROM \"$table\";" | grep -v "count\|--\|(" | tr -d ' ')
        if [ "$row_count" -gt 0 ]; then
            postgres_cmd "$db" "TRUNCATE TABLE \"$table\" CASCADE;" > /dev/null 2>&1
            TOTAL_DELETED=$((TOTAL_DELETED + row_count))
            echo -e "    ${GREEN}✅ Удалено $row_count записей${NC}"
        else
            echo -e "    ${BLUE}ℹ️  Таблица уже пуста${NC}"
        fi
    done
done

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}✅ ПОЛНАЯ ОЧИСТКА POSTGRESQL ЗАВЕРШЕНА${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "${BLUE}Всего удалено записей: $TOTAL_DELETED${NC}"
echo -e "${GREEN}========================================${NC}"

# Финальная проверка
echo ""
echo -e "${BLUE}📊 Финальная проверка...${NC}"
for db in "${DATABASES[@]}"; do
    tables=$(postgres_cmd "$db" "SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public';" | grep -v "count\|--\|(" | tr -d ' ')
    echo -e "${YELLOW}$db:${NC} $tables таблиц осталось"

    if [ "$tables" -gt 0 ]; then
        total_rows=0
        table_list=$(postgres_cmd "$db" "SELECT tablename FROM pg_tables WHERE schemaname = 'public';" | grep -v "tablename\|--\|(" | grep -v "^$" | tr -d ' ')
        for table in $table_list; do
            row_count=$(postgres_cmd "$db" "SELECT COUNT(*) FROM \"$table\";" | grep -v "count\|--\|(" | tr -d ' ')
            total_rows=$((total_rows + row_count))
        done
        echo -e "  Записей в таблицах: $total_rows"
    fi
done

