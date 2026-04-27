#!/bin/bash

# Redis External Access для scanner-infra
# Внешний доступ к Redis через Docker

set -e

# Цвета
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

REDIS_CONTAINER="scanner-redis"

# Функция показа справки
show_help() {
    echo -e "${BLUE}Redis External Access для scanner-infra${NC}"
    echo
    echo "Использование: $0 [команда] [аргументы...]"
    echo
    echo "Команды:"
    echo "  ping                    - Проверка подключения"
    echo "  info [секция]           - Информация о Redis"
    echo "  memory                  - Информация о памяти"
    echo "  keys [паттерн]          - Поиск ключей"
    echo "  get <ключ>              - Получить значение ключа"
    echo "  set <ключ> <значение>   - Установить значение ключа"
    echo "  del <ключ>              - Удалить ключ"
    echo "  dbsize                  - Размер базы данных"
    echo "  flushdb                 - Очистить базу данных"
    echo "  monitor                 - Мониторинг команд"
    echo "  cli                     - Интерактивный режим"
    echo "  help                    - Показать эту справку"
    echo
    echo "Примеры:"
    echo "  $0 ping"
    echo "  $0 info memory"
    echo "  $0 keys '*'"
    echo "  $0 get mykey"
    echo "  $0 set mykey myvalue"
    echo "  $0 cli"
}

# Функция проверки подключения
check_connection() {
    if ! docker ps | grep -q "$REDIS_CONTAINER"; then
        echo -e "${RED}❌ Контейнер $REDIS_CONTAINER не запущен${NC}"
        exit 1
    fi
}

# Функция выполнения команды Redis
execute_redis_command() {
    local command="$1"
    shift
    
    case "$command" in
        "ping")
            docker exec $REDIS_CONTAINER redis-cli ping
            ;;
        "info")
            if [ -n "$1" ]; then
                docker exec $REDIS_CONTAINER redis-cli info "$1"
            else
                docker exec $REDIS_CONTAINER redis-cli info
            fi
            ;;
        "memory")
            docker exec $REDIS_CONTAINER redis-cli info memory
            ;;
        "keys")
            if [ -n "$1" ]; then
                docker exec $REDIS_CONTAINER redis-cli keys "$1"
            else
                echo -e "${YELLOW}⚠️  Укажите паттерн для поиска ключей${NC}"
                echo "Пример: $0 keys '*'"
            fi
            ;;
        "get")
            if [ -n "$1" ]; then
                docker exec $REDIS_CONTAINER redis-cli get "$1"
            else
                echo -e "${YELLOW}⚠️  Укажите ключ${NC}"
                echo "Пример: $0 get mykey"
            fi
            ;;
        "set")
            if [ -n "$1" ] && [ -n "$2" ]; then
                docker exec $REDIS_CONTAINER redis-cli set "$1" "$2"
            else
                echo -e "${YELLOW}⚠️  Укажите ключ и значение${NC}"
                echo "Пример: $0 set mykey myvalue"
            fi
            ;;
        "del")
            if [ -n "$1" ]; then
                docker exec $REDIS_CONTAINER redis-cli del "$1"
            else
                echo -e "${YELLOW}⚠️  Укажите ключ${NC}"
                echo "Пример: $0 del mykey"
            fi
            ;;
        "dbsize")
            docker exec $REDIS_CONTAINER redis-cli dbsize
            ;;
        "flushdb")
            echo -e "${YELLOW}⚠️  Вы уверены, что хотите очистить базу данных? (y/N)${NC}"
            read -r -n 1 -p "Подтвердите: " confirm
            echo
            if [[ $confirm =~ ^[Yy]$ ]]; then
                docker exec $REDIS_CONTAINER redis-cli flushdb
                echo -e "${GREEN}✅ База данных очищена${NC}"
            else
                echo -e "${BLUE}❌ Операция отменена${NC}"
            fi
            ;;
        "monitor")
            echo -e "${BLUE}🔍 Мониторинг команд Redis (нажмите Ctrl+C для остановки)${NC}"
            docker exec $REDIS_CONTAINER redis-cli monitor
            ;;
        "cli")
            echo -e "${BLUE}🔗 Подключение к Redis CLI${NC}"
            docker exec -it $REDIS_CONTAINER redis-cli
            ;;
        "help"|"-h"|"--help")
            show_help
            ;;
        *)
            echo -e "${RED}❌ Неизвестная команда: $command${NC}"
            show_help
            exit 1
            ;;
    esac
}

# Основная логика
main() {
    if [ $# -eq 0 ]; then
        show_help
        exit 0
    fi
    
    check_connection
    execute_redis_command "$@"
}

# Запуск
main "$@"
