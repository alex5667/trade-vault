#!/bin/bash

# Redis Load Balancer для scanner-infra
# Балансировка нагрузки между Redis воркерами

set -e

# Цвета
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

# Конфигурация воркеров
REDIS_WORKERS=(
    "localhost:6379"  # Основной Redis
    "localhost:6380"  # Воркер 1
    "localhost:6381"  # Воркер 2
)

# Функция получения случайного воркера
get_random_worker() {
    local workers=("${REDIS_WORKERS[@]}")
    local random_index=$((RANDOM % ${#workers[@]}))
    echo "${workers[$random_index]}"
}

# Функция получения воркера по хешу ключа
get_worker_by_hash() {
    local key="$1"
    local workers=("${REDIS_WORKERS[@]}")
    local hash=$(echo -n "$key" | md5sum | cut -d' ' -f1)
    local hash_int=$((0x${hash:0:8}))
    local index=$((hash_int % ${#workers[@]}))
    echo "${workers[$index]}"
}

# Функция выполнения команды на воркере
execute_on_worker() {
    local worker="$1"
    local command="$2"
    shift 2
    
    local host=$(echo "$worker" | cut -d: -f1)
    local port=$(echo "$worker" | cut -d: -f2)
    
    redis-cli -h "$host" -p "$port" "$command" "$@"
}

# Функция тестирования всех воркеров
test_all_workers() {
    echo -e "${BLUE}�� Тестирование всех Redis воркеров${NC}"
    echo -e "${BLUE}====================================${NC}"
    
    for i in "${!REDIS_WORKERS[@]}"; do
        local worker="${REDIS_WORKERS[$i]}"
        local host=$(echo "$worker" | cut -d: -f1)
        local port=$(echo "$worker" | cut -d: -f2)
        
        echo -e "${YELLOW}🔍 Тест воркера $((i+1)) ($worker):${NC}"
        
        if timeout 5 redis-cli -h "$host" -p "$port" ping 2>/dev/null | grep -q "PONG"; then
            echo -e "${GREEN}✅ Воркер $((i+1)) работает${NC}"
        else
            echo -e "${RED}❌ Воркер $((i+1)) не работает${NC}"
        fi
    done
}

# Функция распределения нагрузки
distribute_load() {
    echo -e "${BLUE}⚖️  Распределение нагрузки между воркерами${NC}"
    echo -e "${BLUE}==========================================${NC}"
    
    local test_keys=("key1" "key2" "key3" "key4" "key5")
    
    for key in "${test_keys[@]}"; do
        local worker=$(get_worker_by_hash "$key")
        local host=$(echo "$worker" | cut -d: -f1)
        local port=$(echo "$worker" | cut -d: -f2)
        
        echo -e "${YELLOW}🔑 Ключ '$key' -> Воркер $worker${NC}"
        
        # Устанавливаем значение
        if execute_on_worker "$worker" "set" "$key" "value_$key" > /dev/null 2>&1; then
            echo -e "${GREEN}✅ SET успешно${NC}"
        else
            echo -e "${RED}❌ SET неудачно${NC}"
        fi
        
        # Получаем значение
        local value=$(execute_on_worker "$worker" "get" "$key" 2>/dev/null)
        if [ "$value" = "value_$key" ]; then
            echo -e "${GREEN}✅ GET успешно: $value${NC}"
        else
            echo -e "${RED}❌ GET неудачно${NC}"
        fi
        
        echo
    done
}

# Функция мониторинга производительности
monitor_performance() {
    echo -e "${BLUE}📊 Мониторинг производительности воркеров${NC}"
    echo -e "${BLUE}==========================================${NC}"
    
    for i in "${!REDIS_WORKERS[@]}"; do
        local worker="${REDIS_WORKERS[$i]}"
        local host=$(echo "$worker" | cut -d: -f1)
        local port=$(echo "$worker" | cut -d: -f2)
        
        echo -e "${YELLOW}🔍 Воркер $((i+1)) ($worker):${NC}"
        
        # Информация о памяти
        local memory_info=$(execute_on_worker "$worker" "info" "memory" 2>/dev/null)
        local used_memory=$(echo "$memory_info" | grep "used_memory_human:" | cut -d: -f2 | tr -d '\r')
        local max_memory=$(echo "$memory_info" | grep "maxmemory_human:" | cut -d: -f2 | tr -d '\r')
        
        echo -e "  �� Память: $used_memory / $max_memory"
        
        # Количество ключей
        local key_count=$(execute_on_worker "$worker" "dbsize" 2>/dev/null)
        echo -e "  🔑 Ключей: $key_count"
        
        # Операции в секунду
        local stats_info=$(execute_on_worker "$worker" "info" "stats" 2>/dev/null)
        local ops_per_sec=$(echo "$stats_info" | grep "instantaneous_ops_per_sec:" | cut -d: -f2 | tr -d '\r')
        echo -e "  ⚡ Операций/сек: $ops_per_sec"
        
        echo
    done
}

# Функция создания конфигурации для бэкенда
create_backend_config() {
    echo -e "${YELLOW}📝 Создание конфигурации для бэкенда...${NC}"
    
    cat > redis-backend-config.json << 'CONFIG_EOF'
{
  "redis": {
    "workers": [
      {
        "name": "primary",
        "host": "localhost",
        "port": 6379,
        "role": "primary",
        "weight": 3
      },
      {
        "name": "worker1",
        "host": "localhost",
        "port": 6380,
        "role": "worker",
        "weight": 1
      },
      {
        "name": "worker2",
        "host": "localhost",
        "port": 6381,
        "role": "worker",
        "weight": 1
      }
    ],
    "loadBalancing": {
      "strategy": "round_robin",
      "healthCheckInterval": 30000,
      "maxRetries": 3,
      "retryDelay": 1000
    },
    "connection": {
      "timeout": 10000,
      "commandTimeout": 5000,
      "keepAlive": 30000,
      "maxRetries": 3
    }
  }
}
CONFIG_EOF

    echo -e "${GREEN}✅ Конфигурация для бэкенда создана: redis-backend-config.json${NC}"
}

# Функция показа справки
show_help() {
    echo -e "${BLUE}Redis Load Balancer для scanner-infra${NC}"
    echo
    echo "Использование: $0 [команда]"
    echo
    echo "Команды:"
    echo "  test          - Тестирование всех воркеров"
    echo "  distribute    - Распределение нагрузки"
    echo "  monitor       - Мониторинг производительности"
    echo "  config        - Создание конфигурации для бэкенда"
    echo "  random        - Получить случайный воркер"
    echo "  hash <key>    - Получить воркер по хешу ключа"
    echo "  help          - Показать эту справку"
    echo
    echo "Примеры:"
    echo "  $0 test"
    echo "  $0 distribute"
    echo "  $0 hash mykey"
}

# Основная логика
case "${1:-help}" in
    "test")
        test_all_workers
        ;;
    "distribute")
        distribute_load
        ;;
    "monitor")
        monitor_performance
        ;;
    "config")
        create_backend_config
        ;;
    "random")
        echo "Случайный воркер: $(get_random_worker)"
        ;;
    "hash")
        if [ -n "$2" ]; then
            echo "Воркер для ключа '$2': $(get_worker_by_hash "$2")"
        else
            echo -e "${RED}❌ Укажите ключ${NC}"
            echo "Пример: $0 hash mykey"
        fi
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
