#!/bin/bash

# Redis External Access Setup для scanner-infra
# Настройка внешних подключений к Redis

set -e

# Цвета
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

# Конфигурация
REDIS_CONTAINER="scanner-redis"
REDIS_CONF_PATH="/usr/local/etc/redis/redis.conf"
HOST_CONF_PATH="./redis-simple.conf"

echo -e "${BLUE}🔧 Redis External Access Setup для scanner-infra${NC}"
echo -e "${BLUE}===============================================${NC}"

# Функция проверки статуса контейнера
check_container_status() {
    if ! docker ps | grep -q "$REDIS_CONTAINER"; then
        echo -e "${RED}❌ Контейнер $REDIS_CONTAINER не запущен${NC}"
        exit 1
    fi
    echo -e "${GREEN}✅ Контейнер $REDIS_CONTAINER запущен${NC}"
}

# Функция установки необходимых утилит
install_utilities() {
    echo -e "${YELLOW}📦 Установка необходимых утилит в Redis контейнер...${NC}"
    
    # Обновляем пакеты и устанавливаем необходимые утилиты
    docker exec $REDIS_CONTAINER sh -c "
        apt-get update && \
        apt-get install -y net-tools procps lsof curl wget && \
        apt-get clean && \
        rm -rf /var/lib/apt/lists/*
    " || {
        echo -e "${YELLOW}⚠️  Не удалось установить утилиты через apt-get, пробуем альтернативный способ...${NC}"
        
        # Альтернативный способ - копируем утилиты из другого контейнера
        docker run --rm -v /tmp:/host alpine:latest sh -c "
            apk add --no-cache net-tools procps lsof curl wget && \
            cp /bin/netstat /host/netstat && \
            cp /bin/ss /host/ss && \
            cp /bin/ps /host/ps && \
            cp /bin/lsof /host/lsof
        " || echo -e "${YELLOW}⚠️  Альтернативная установка не удалась${NC}"
    }
    
    echo -e "${GREEN}✅ Утилиты установлены${NC}"
}

# Функция проверки сетевых настроек
check_network_settings() {
    echo -e "${YELLOW}🔍 Проверка сетевых настроек...${NC}"
    
    # Проверяем, какие интерфейсы слушает Redis
    echo -e "${BLUE}📡 Интерфейсы, которые слушает Redis:${NC}"
    docker exec $REDIS_CONTAINER netstat -tlnp 2>/dev/null | grep 6379 || \
    docker exec $REDIS_CONTAINER ss -tlnp 2>/dev/null | grep 6379 || \
    echo -e "${YELLOW}⚠️  Не удалось получить информацию о сетевых интерфейсах${NC}"
    
    # Проверяем процессы Redis
    echo -e "${BLUE}🔄 Процессы Redis:${NC}"
    docker exec $REDIS_CONTAINER ps aux 2>/dev/null | grep redis || \
    docker exec $REDIS_CONTAINER ps -ef 2>/dev/null | grep redis || \
    echo -e "${YELLOW}⚠️  Не удалось получить информацию о процессах${NC}"
}

# Функция настройки Redis для внешних подключений
configure_redis_external() {
    echo -e "${YELLOW}⚙️  Настройка Redis для внешних подключений...${NC}"
    
    # Создаем резервную копию конфигурации
    docker exec $REDIS_CONTAINER cp $REDIS_CONF_PATH ${REDIS_CONF_PATH}.backup
    
    # Настраиваем Redis для внешних подключений
    docker exec $REDIS_CONTAINER redis-cli config set bind "0.0.0.0" || {
        echo -e "${YELLOW}⚠️  Не удалось установить bind через CONFIG SET${NC}"
    }
    
    docker exec $REDIS_CONTAINER redis-cli config set protected-mode "no" || {
        echo -e "${YELLOW}⚠️  Не удалось отключить protected-mode через CONFIG SET${NC}"
    }
    
    # Проверяем текущие настройки
    echo -e "${BLUE}�� Текущие настройки Redis:${NC}"
    echo -e "  Bind: $(docker exec $REDIS_CONTAINER redis-cli config get bind | tail -1)"
    echo -e "  Protected Mode: $(docker exec $REDIS_CONTAINER redis-cli config get protected-mode | tail -1)"
    echo -e "  Port: $(docker exec $REDIS_CONTAINER redis-cli config get port | tail -1)"
}

# Функция проверки внешних подключений
test_external_connections() {
    echo -e "${YELLOW}🧪 Тестирование внешних подключений...${NC}"
    
    # Тест 1: Подключение через localhost
    echo -e "${BLUE}🔍 Тест 1: Подключение через localhost:6379${NC}"
    if timeout 5 redis-cli -h localhost -p 6379 ping 2>/dev/null | grep -q "PONG"; then
        echo -e "${GREEN}✅ Подключение через localhost работает${NC}"
    else
        echo -e "${RED}❌ Подключение через localhost не работает${NC}"
    fi
    
    # Тест 2: Подключение через 127.0.0.1
    echo -e "${BLUE}🔍 Тест 2: Подключение через 127.0.0.1:6379${NC}"
    if timeout 5 redis-cli -h 127.0.0.1 -p 6379 ping 2>/dev/null | grep -q "PONG"; then
        echo -e "${GREEN}✅ Подключение через 127.0.0.1 работает${NC}"
    else
        echo -e "${RED}❌ Подключение через 127.0.0.1 не работает${NC}"
    fi
    
    # Тест 3: Подключение через Docker network
    echo -e "${BLUE}🔍 Тест 3: Подключение через Docker network${NC}"
    if docker run --rm --network scanner_infra_scanner-network redis:7 redis-cli -h redis -p 6379 ping 2>/dev/null | grep -q "PONG"; then
        echo -e "${GREEN}✅ Подключение через Docker network работает${NC}"
    else
        echo -e "${RED}❌ Подключение через Docker network не работает${NC}"
    fi
}

# Функция диагностики проблем
diagnose_issues() {
    echo -e "${YELLOW}🔍 Диагностика проблем с внешними подключениями...${NC}"
    
    # Проверяем, слушает ли Redis на всех интерфейсах
    echo -e "${BLUE}📡 Проверка сетевых интерфейсов:${NC}"
    docker exec $REDIS_CONTAINER netstat -tlnp 2>/dev/null | grep 6379 || \
    docker exec $REDIS_CONTAINER ss -tlnp 2>/dev/null | grep 6379 || \
    echo -e "${YELLOW}⚠️  Не удалось получить информацию о сетевых интерфейсах${NC}"
    
    # Проверяем логи Redis
    echo -e "${BLUE}📋 Последние записи в логах Redis:${NC}"
    docker logs $REDIS_CONTAINER --tail 10 | grep -i "bind\|listen\|accept\|connection" || \
    echo -e "${YELLOW}⚠️  Нет релевантных записей в логах${NC}"
    
    # Проверяем Docker port mapping
    echo -e "${BLUE}🐳 Docker port mapping:${NC}"
    docker port $REDIS_CONTAINER | grep 6379 || \
    echo -e "${RED}❌ Порт 6379 не проброшен${NC}"
}

# Функция создания wrapper скрипта для подключения
create_redis_wrapper() {
    echo -e "${YELLOW}📝 Создание wrapper скрипта для подключения к Redis...${NC}"
    
    cat > redis-connect.sh << 'WRAPPER_EOF'
#!/bin/bash

# Redis Connect Wrapper для scanner-infra
# Автоматический выбор способа подключения к Redis

REDIS_CONTAINER="scanner-redis"

# Функция подключения через Docker
connect_via_docker() {
    docker exec $REDIS_CONTAINER redis-cli "$@"
}

# Функция подключения через localhost
connect_via_localhost() {
    redis-cli -h localhost -p 6379 "$@"
}

# Функция подключения через Docker network
connect_via_network() {
    docker run --rm --network scanner_infra_scanner-network redis:7 redis-cli -h redis -p 6379 "$@"
}

# Определяем лучший способ подключения
if docker exec $REDIS_CONTAINER redis-cli ping > /dev/null 2>&1; then
    echo "🔗 Подключение через Docker exec..."
    connect_via_docker "$@"
elif timeout 3 redis-cli -h localhost -p 6379 ping > /dev/null 2>&1; then
    echo "🔗 Подключение через localhost..."
    connect_via_localhost "$@"
else
    echo "🔗 Подключение через Docker network..."
    connect_via_network "$@"
fi
WRAPPER_EOF

    chmod +x redis-connect.sh
    echo -e "${GREEN}✅ Wrapper скрипт создан: ./redis-connect.sh${NC}"
}

# Функция показа справки
show_help() {
    echo -e "${BLUE}Redis External Access Setup для scanner-infra${NC}"
    echo
    echo "Использование: $0 [команда]"
    echo
    echo "Команды:"
    echo "  setup      - Полная настройка внешних подключений (по умолчанию)"
    echo "  install    - Установка утилит в контейнер"
    echo "  configure  - Настройка Redis конфигурации"
    echo "  test       - Тестирование подключений"
    echo "  diagnose   - Диагностика проблем"
    echo "  wrapper    - Создание wrapper скрипта"
    echo "  help       - Показать эту справку"
    echo
    echo "Примеры:"
    echo "  $0 setup"
    echo "  $0 test"
    echo "  $0 diagnose"
}

# Основная функция настройки
setup_external_access() {
    echo -e "${BLUE}🚀 Запуск полной настройки внешних подключений${NC}"
    
    check_container_status
    install_utilities
    configure_redis_external
    check_network_settings
    test_external_connections
    create_redis_wrapper
    
    echo
    echo -e "${GREEN}🎉 Настройка завершена!${NC}"
    echo -e "${BLUE}📋 Доступные способы подключения:${NC}"
    echo -e "  • ./redis-connect.sh ping"
    echo -e "  • docker exec $REDIS_CONTAINER redis-cli ping"
    echo -e "  • redis-cli -h localhost -p 6379 ping (если работает)"
}

# Основная логика
case "${1:-setup}" in
    "setup")
        setup_external_access
        ;;
    "install")
        check_container_status
        install_utilities
        ;;
    "configure")
        check_container_status
        configure_redis_external
        ;;
    "test")
        test_external_connections
        ;;
    "diagnose")
        diagnose_issues
        ;;
    "wrapper")
        create_redis_wrapper
        ;;
    "help"|"-h"|"--help")
        show_help
        ;;
    *)
        echo -e "${RED}❌ Неизвестная команда: $1${NC}"
        show_help
        exit 1
        ;;
esac
