#!/bin/bash

# Optimize Redis Resources для scanner-infra
# Оптимизация ресурсов Redis и создание воркеров

set -e

# Цвета
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

# Конфигурация
REDIS_CONTAINER="scanner-redis"
REDIS_CONF_PATH="./redis-simple.conf"
DOCKER_COMPOSE_PATH="./docker-compose.yml"

echo -e "${BLUE}🚀 Optimize Redis Resources для scanner-infra${NC}"
echo -e "${BLUE}============================================${NC}"

# Функция анализа системы
analyze_system() {
    echo -e "${YELLOW}🔍 Анализ системы...${NC}"
    
    # Получаем информацию о системе
    local total_memory=$(free -m | awk 'NR==2{printf "%.0f", $2}')
    local available_memory=$(free -m | awk 'NR==2{printf "%.0f", $7}')
    local cpu_cores=$(nproc)
    
    echo -e "${BLUE}📊 Системные ресурсы:${NC}"
    echo -e "  💾 Общая память: ${GREEN}${total_memory}MB${NC}"
    echo -e "  💾 Доступная память: ${GREEN}${available_memory}MB${NC}"
    echo -e "  🔧 CPU ядер: ${GREEN}${cpu_cores}${NC}"
    
    # Рекомендации по ресурсам
    local recommended_redis_memory=$((total_memory * 20 / 100))  # 20% от общей памяти
    local recommended_workers=$((cpu_cores / 2))  # Половина CPU ядер
    
    echo -e "${BLUE}📋 Рекомендации:${NC}"
    echo -e "  💾 Память для Redis: ${GREEN}${recommended_redis_memory}MB${NC}"
    echo -e "  🔧 Количество воркеров: ${GREEN}${recommended_workers}${NC}"
    
    # Сохраняем рекомендации
    echo "$recommended_redis_memory" > /tmp/redis_memory_mb
    echo "$recommended_workers" > /tmp/redis_workers
}

# Функция создания оптимизированной конфигурации Redis
create_optimized_redis_config() {
    echo -e "${YELLOW}⚙️  Создание оптимизированной конфигурации Redis...${NC}"
    
    local redis_memory=$(cat /tmp/redis_memory_mb)
    local redis_memory_gb=$((redis_memory / 1024))
    
    # Создаем оптимизированную конфигурацию Redis
    cat > "$REDIS_CONF_PATH" << REDIS_EOF
# Оптимизированная конфигурация Redis для scanner-infra
# Выделено ${redis_memory}MB памяти и оптимизированы настройки

# Сетевые настройки
port 6379
bind 0.0.0.0
timeout 0
tcp-keepalive 300
tcp-backlog 511

# Клиенты
maxclients 50000

# Память - выделяем ${redis_memory}MB
maxmemory ${redis_memory}mb
maxmemory-policy allkeys-lru
maxmemory-samples 10

# Настройки для высокой производительности
# Увеличиваем лимиты буферов для клиентов
client-output-buffer-limit normal 0 0 0
client-output-buffer-limit replica 512mb 128mb 60
client-output-buffer-limit pubsub 64mb 16mb 60

# Настройки для стабильности подключений
tcp-keepalive 300
timeout 0

# Настройки для высокой производительности
# Увеличиваем лимиты для медленных клиентов
slowlog-log-slower-than 10000
slowlog-max-len 128

# Настройки для предотвращения блокировок
lua-time-limit 5000

# Persistence - оптимизированные настройки
save ""
appendonly yes
appendfilename "appendonly.aof"
appendfsync everysec
no-appendfsync-on-rewrite no
auto-aof-rewrite-percentage 100
auto-aof-rewrite-min-size 64mb

# Логирование
loglevel notice
logfile ""

# Безопасность
protected-mode no
rename-command FLUSHDB ""
rename-command FLUSHALL ""
rename-command KEYS ""

# Производительность - оптимизированные настройки
hz 20
dynamic-hz yes

# Настройки для высокой производительности
# Увеличиваем лимиты для внешних подключений
client-output-buffer-limit normal 0 0 0
client-output-buffer-limit replica 512mb 128mb 60
client-output-buffer-limit pubsub 64mb 16mb 60

# Настройки для предотвращения разрывов соединений
tcp-keepalive 300
timeout 0

# Настройки для высокой производительности
# Увеличиваем лимиты для медленных клиентов
slowlog-log-slower-than 10000
slowlog-max-len 128

# Настройки для предотвращения блокировок
lua-time-limit 5000

# ========================================
# НАСТРОЙКИ САМООЧИЩЕНИЯ И TTL
# ========================================

# Автоматическая очистка ключей с TTL
# Проверка каждые 10 секунд (уже настроено выше)

# Автоматическая очистка устаревших ключей
# Удаление ключей с истекшим TTL
active-expire-effort 10

# Автоматическая очистка памяти
# Удаление неиспользуемых страниц памяти
activedefrag yes
active-defrag-ignore-bytes 100mb
active-defrag-threshold-lower 10
active-defrag-threshold-upper 100
active-defrag-cycle-min 5
active-defrag-cycle-max 75

# Дополнительные настройки для высокой производительности
# Увеличиваем лимиты для внешних подключений
client-output-buffer-limit normal 0 0 0
client-output-buffer-limit replica 512mb 128mb 60
client-output-buffer-limit pubsub 64mb 16mb 60

# Настройки для предотвращения разрывов соединений
tcp-keepalive 300
timeout 0

# Настройки для высокой производительности
# Увеличиваем лимиты для медленных клиентов
slowlog-log-slower-than 10000
slowlog-max-len 128

# Настройки для предотвращения блокировок
lua-time-limit 5000

# Настройки для высокой производительности
# Увеличиваем лимиты для внешних подключений
client-output-buffer-limit normal 0 0 0
client-output-buffer-limit replica 512mb 128mb 60
client-output-buffer-limit pubsub 64mb 16mb 60

# Настройки для предотвращения разрывов соединений
tcp-keepalive 300
timeout 0

# Настройки для высокой производительности
# Увеличиваем лимиты для медленных клиентов
slowlog-log-slower-than 10000
slowlog-max-len 128

# Настройки для предотвращения блокировок
lua-time-limit 5000
REDIS_EOF

    echo -e "${GREEN}✅ Оптимизированная конфигурация Redis создана${NC}"
}

# Функция создания Docker Compose с оптимизированными ресурсами
create_optimized_docker_compose() {
    echo -e "${YELLOW}🐳 Создание оптимизированного Docker Compose...${NC}"
    
    local redis_memory=$(cat /tmp/redis_memory_mb)
    local redis_workers=$(cat /tmp/redis_workers)
    
    # Создаем резервную копию
    cp "$DOCKER_COMPOSE_PATH" "${DOCKER_COMPOSE_PATH}.backup.$(date +%Y%m%d_%H%M%S)"
    
    # Создаем оптимизированный Docker Compose
    cat > "$DOCKER_COMPOSE_PATH" << DOCKER_EOF
# Docker Compose для scanner-infra системы с оптимизированными ресурсами

services:
  # Redis для scanner-infra системы с оптимизированными ресурсами
  redis:
    image: redis:7
    container_name: scanner-redis
    ports:
      - '6379:6379'
    volumes:
      - scanner-redis-data:/data
      - ./redis-simple.conf:/usr/local/etc/redis/redis.conf
    command: redis-server /usr/local/etc/redis/redis.conf
    networks:
      - scanner-network
    healthcheck:
      test: ['CMD', 'redis-cli', 'ping']
      interval: 5s
      timeout: 3s
      retries: 5
    restart: unless-stopped
    # Оптимизированные ресурсы
    deploy:
      resources:
        limits:
          memory: ${redis_memory}M
          cpus: '4.0'
        reservations:
          memory: ${redis_memory}M
          cpus: '2.0'
    # Настройки для высокой производительности
    ulimits:
      nofile:
        soft: 65536
        hard: 65536
    # Настройки для стабильности
    sysctls:
      - net.core.somaxconn=65535
      - net.ipv4.tcp_max_syn_backlog=65535
    # Настройки для производительности
    shm_size: 1gb

  # Go Worker для сбора данных с Binance
  go-worker:
    build:
      context: ./go-worker
      dockerfile: Dockerfile
    container_name: scanner-go-worker
    environment:
      - REDIS_HOST=redis
      - REDIS_PORT=6379
      - BINANCE_WS_TIMEFRAME=kline_1m
    depends_on:
      redis:
        condition: service_healthy
    networks:
      - scanner-network
    restart: unless-stopped
    # Оптимизированные ресурсы для воркера
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: '2.0'
        reservations:
          memory: 1G
          cpus: '1.0'
    # Улучшенные настройки для сетевой стабильности
    dns:
      - 8.8.8.8
      - 8.8.4.4
    # Увеличиваем лимиты для сетевых соединений
    ulimits:
      nofile:
        soft: 65536
        hard: 65536
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3

  # Дополнительные Redis воркеры (если нужно)
  redis-worker-1:
    image: redis:7
    container_name: scanner-redis-worker-1
    ports:
      - '6380:6379'
    volumes:
      - scanner-redis-worker-1-data:/data
      - ./redis-simple.conf:/usr/local/etc/redis/redis.conf
    command: redis-server /usr/local/etc/redis/redis.conf
    networks:
      - scanner-network
    healthcheck:
      test: ['CMD', 'redis-cli', 'ping']
      interval: 5s
      timeout: 3s
      retries: 5
    restart: unless-stopped
    # Ресурсы для воркера
    deploy:
      resources:
        limits:
          memory: 1G
          cpus: '1.0'
        reservations:
          memory: 512M
          cpus: '0.5'
    ulimits:
      nofile:
        soft: 32768
        hard: 32768

  redis-worker-2:
    image: redis:7
    container_name: scanner-redis-worker-2
    ports:
      - '6381:6379'
    volumes:
      - scanner-redis-worker-2-data:/data
      - ./redis-simple.conf:/usr/local/etc/redis/redis.conf
    command: redis-server /usr/local/etc/redis/redis.conf
    networks:
      - scanner-network
    healthcheck:
      test: ['CMD', 'redis-cli', 'ping']
      interval: 5s
      timeout: 3s
      retries: 5
    restart: unless-stopped
    # Ресурсы для воркера
    deploy:
      resources:
        limits:
          memory: 1G
          cpus: '1.0'
        reservations:
          memory: 512M
          cpus: '0.5'
    ulimits:
      nofile:
        soft: 32768
        hard: 32768

  # Redis Cluster (опционально)
  redis-cluster-init:
    image: redis:7
    container_name: scanner-redis-cluster-init
    depends_on:
      - redis
      - redis-worker-1
      - redis-worker-2
    networks:
      - scanner-network
    command: |
      sh -c '
        sleep 10
        redis-cli -h redis --cluster create redis:6379 redis-worker-1:6379 redis-worker-2:6379 --cluster-replicas 0 --cluster-yes
      '
    restart: "no"

networks:
  scanner-network:
    driver: bridge
    ipam:
      config:
        - subnet: 172.20.0.0/16

volumes:
  scanner-redis-data:
    driver: local
  scanner-redis-worker-1-data:
    driver: local
  scanner-redis-worker-2-data:
    driver: local
DOCKER_EOF

    echo -e "${GREEN}✅ Оптимизированный Docker Compose создан${NC}"
}

# Функция создания скрипта для управления воркерами
create_worker_management_script() {
    echo -e "${YELLOW}📝 Создание скрипта управления воркерами...${NC}"
    
    cat > manage-redis-workers.sh << 'WORKER_EOF'
#!/bin/bash

# Управление Redis воркерами для scanner-infra

set -e

# Цвета
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

# Функция показа справки
show_help() {
    echo -e "${BLUE}Управление Redis воркерами для scanner-infra${NC}"
    echo
    echo "Использование: $0 [команда]"
    echo
    echo "Команды:"
    echo "  start-all     - Запустить все Redis воркеры"
    echo "  stop-all      - Остановить все Redis воркеры"
    echo "  restart-all   - Перезапустить все Redis воркеры"
    echo "  status        - Показать статус всех воркеров"
    echo "  logs          - Показать логи всех воркеров"
    echo "  stats         - Показать статистику ресурсов"
    echo "  test          - Тестировать все воркеры"
    echo "  help          - Показать эту справку"
    echo
    echo "Примеры:"
    echo "  $0 start-all"
    echo "  $0 status"
    echo "  $0 test"
}

# Функция запуска всех воркеров
start_all_workers() {
    echo -e "${YELLOW}🚀 Запуск всех Redis воркеров...${NC}"
    
    docker-compose up -d redis redis-worker-1 redis-worker-2
    
    echo -e "${GREEN}✅ Все воркеры запущены${NC}"
}

# Функция остановки всех воркеров
stop_all_workers() {
    echo -e "${YELLOW}⏹️  Остановка всех Redis воркеров...${NC}"
    
    docker-compose stop redis redis-worker-1 redis-worker-2
    
    echo -e "${GREEN}✅ Все воркеры остановлены${NC}"
}

# Функция перезапуска всех воркеров
restart_all_workers() {
    echo -e "${YELLOW}🔄 Перезапуск всех Redis воркеров...${NC}"
    
    stop_all_workers
    sleep 5
    start_all_workers
    
    echo -e "${GREEN}✅ Все воркеры перезапущены${NC}"
}

# Функция показа статуса
show_status() {
    echo -e "${BLUE}📊 Статус Redis воркеров${NC}"
    echo -e "${BLUE}========================${NC}"
    
    # Основной Redis
    echo -e "${YELLOW}🔧 Основной Redis (порт 6379):${NC}"
    if docker ps | grep -q "scanner-redis"; then
        echo -e "${GREEN}✅ Запущен${NC}"
        docker exec scanner-redis redis-cli ping 2>/dev/null | grep -q "PONG" && echo -e "${GREEN}✅ Отвечает${NC}" || echo -e "${RED}❌ Не отвечает${NC}"
    else
        echo -e "${RED}❌ Остановлен${NC}"
    fi
    
    # Воркер 1
    echo -e "${YELLOW}🔧 Воркер 1 (порт 6380):${NC}"
    if docker ps | grep -q "scanner-redis-worker-1"; then
        echo -e "${GREEN}✅ Запущен${NC}"
        docker exec scanner-redis-worker-1 redis-cli ping 2>/dev/null | grep -q "PONG" && echo -e "${GREEN}✅ Отвечает${NC}" || echo -e "${RED}❌ Не отвечает${NC}"
    else
        echo -e "${RED}❌ Остановлен${NC}"
    fi
    
    # Воркер 2
    echo -e "${YELLOW}🔧 Воркер 2 (порт 6381):${NC}"
    if docker ps | grep -q "scanner-redis-worker-2"; then
        echo -e "${GREEN}✅ Запущен${NC}"
        docker exec scanner-redis-worker-2 redis-cli ping 2>/dev/null | grep -q "PONG" && echo -e "${GREEN}✅ Отвечает${NC}" || echo -e "${RED}❌ Не отвечает${NC}"
    else
        echo -e "${RED}❌ Остановлен${NC}"
    fi
}

# Функция показа логов
show_logs() {
    echo -e "${BLUE}📋 Логи Redis воркеров${NC}"
    echo -e "${BLUE}====================${NC}"
    
    echo -e "${YELLOW}🔧 Основной Redis:${NC}"
    docker logs scanner-redis --tail 10
    
    echo -e "${YELLOW}🔧 Воркер 1:${NC}"
    docker logs scanner-redis-worker-1 --tail 10
    
    echo -e "${YELLOW}🔧 Воркер 2:${NC}"
    docker logs scanner-redis-worker-2 --tail 10
}

# Функция показа статистики
show_stats() {
    echo -e "${BLUE}📊 Статистика ресурсов Redis воркеров${NC}"
    echo -e "${BLUE}=====================================${NC}"
    
    docker stats scanner-redis scanner-redis-worker-1 scanner-redis-worker-2 --no-stream
}

# Функция тестирования
test_workers() {
    echo -e "${BLUE}🧪 Тестирование Redis воркеров${NC}"
    echo -e "${BLUE}==============================${NC}"
    
    # Тест основного Redis
    echo -e "${YELLOW}🔍 Тест основного Redis (localhost:6379):${NC}"
    if timeout 5 redis-cli -h localhost -p 6379 ping 2>/dev/null | grep -q "PONG"; then
        echo -e "${GREEN}✅ Основной Redis работает${NC}"
    else
        echo -e "${RED}❌ Основной Redis не работает${NC}"
    fi
    
    # Тест воркера 1
    echo -e "${YELLOW}🔍 Тест воркера 1 (localhost:6380):${NC}"
    if timeout 5 redis-cli -h localhost -p 6380 ping 2>/dev/null | grep -q "PONG"; then
        echo -e "${GREEN}✅ Воркер 1 работает${NC}"
    else
        echo -e "${RED}❌ Воркер 1 не работает${NC}"
    fi
    
    # Тест воркера 2
    echo -e "${YELLOW}🔍 Тест воркера 2 (localhost:6381):${NC}"
    if timeout 5 redis-cli -h localhost -p 6381 ping 2>/dev/null | grep -q "PONG"; then
        echo -e "${GREEN}✅ Воркер 2 работает${NC}"
    else
        echo -e "${RED}❌ Воркер 2 не работает${NC}"
    fi
}

# Основная логика
case "${1:-help}" in
    "start-all")
        start_all_workers
        ;;
    "stop-all")
        stop_all_workers
        ;;
    "restart-all")
        restart_all_workers
        ;;
    "status")
        show_status
        ;;
    "logs")
        show_logs
        ;;
    "stats")
        show_stats
        ;;
    "test")
        test_workers
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
WORKER_EOF

    chmod +x manage-redis-workers.sh
    echo -e "${GREEN}✅ Скрипт управления воркерами создан: ./manage-redis-workers.sh${NC}"
}

# Функция перезапуска с оптимизированными ресурсами
restart_with_optimized_resources() {
    echo -e "${YELLOW}🔄 Перезапуск с оптимизированными ресурсами...${NC}"
    
    # Останавливаем текущий контейнер
    docker stop $REDIS_CONTAINER
    
    # Запускаем с новой конфигурацией
    docker-compose up -d redis
    
    # Ждем, пока контейнер запустится
    echo -e "${BLUE}⏳ Ожидание запуска контейнера...${NC}"
    sleep 15
    
    # Проверяем, что контейнер запустился
    if docker ps | grep -q "$REDIS_CONTAINER"; then
        echo -e "${GREEN}✅ Контейнер перезапущен с оптимизированными ресурсами${NC}"
    else
        echo -e "${RED}❌ Не удалось перезапустить контейнер${NC}"
        exit 1
    fi
}

# Функция тестирования оптимизированного Redis
test_optimized_redis() {
    echo -e "${YELLOW}🧪 Тестирование оптимизированного Redis...${NC}"
    
    # Ждем, пока Redis полностью запустится
    sleep 5
    
    # Тест 1: Базовое подключение
    echo -e "${BLUE}🔍 Тест 1: Базовое подключение${NC}"
    if timeout 10 redis-cli -h localhost -p 6379 ping 2>/dev/null | grep -q "PONG"; then
        echo -e "${GREEN}✅ Базовое подключение работает${NC}"
    else
        echo -e "${RED}❌ Базовое подключение не работает${NC}"
        return 1
    fi
    
    # Тест 2: Информация о памяти
    echo -e "${BLUE}🔍 Тест 2: Информация о памяти${NC}"
    local memory_info=$(redis-cli -h localhost -p 6379 info memory 2>/dev/null)
    local max_memory=$(echo "$memory_info" | grep "maxmemory:" | cut -d: -f2 | tr -d '\r')
    local max_memory_mb=$((max_memory / 1024 / 1024))
    echo -e "  💾 Максимальная память: ${GREEN}${max_memory_mb}MB${NC}"
    
    # Тест 3: Производительность
    echo -e "${BLUE}🔍 Тест 3: Тест производительности${NC}"
    local start_time=$(date +%s%N)
    for i in {1..100}; do
        redis-cli -h localhost -p 6379 set "test_key_$i" "test_value_$i" > /dev/null 2>&1
    done
    local end_time=$(date +%s%N)
    local duration=$(( (end_time - start_time) / 1000000 ))
    echo -e "  ⚡ 100 операций SET: ${GREEN}${duration}ms${NC}"
    
    # Очистка тестовых ключей
    for i in {1..100}; do
        redis-cli -h localhost -p 6379 del "test_key_$i" > /dev/null 2>&1
    done
    
    echo -e "${GREEN}✅ Тестирование завершено${NC}"
}

# Основная функция
main() {
    echo -e "${BLUE}�� Запуск оптимизации ресурсов Redis${NC}"
    
    analyze_system
    create_optimized_redis_config
    create_optimized_docker_compose
    create_worker_management_script
    restart_with_optimized_resources
    test_optimized_redis
    
    echo
    echo -e "${GREEN}🎉 Оптимизация ресурсов завершена!${NC}"
    echo -e "${BLUE}📋 Доступные инструменты:${NC}"
    echo -e "  • ./manage-redis-workers.sh - Управление воркерами"
    echo -e "  • docker-compose up -d - Запуск всех сервисов"
    echo -e "  • redis-cli -h localhost -p 6379 ping - Основной Redis"
    echo -e "  • redis-cli -h localhost -p 6380 ping - Воркер 1"
    echo -e "  • redis-cli -h localhost -p 6381 ping - Воркер 2"
    
    echo
    echo -e "${BLUE}📋 Рекомендации:${NC}"
    echo -e "  • Используйте основной Redis для критических операций"
    echo -e "  • Используйте воркеры для распределения нагрузки"
    echo -e "  • Мониторьте использование ресурсов"
    echo -e "  • Настройте балансировку нагрузки между воркерами"
}

# Запуск
main "$@"
