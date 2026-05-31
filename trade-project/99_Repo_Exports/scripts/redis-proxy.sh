#!/bin/bash

# Redis Proxy для scanner-infra
# Проксирование внешних подключений к Redis

REDIS_CONTAINER="scanner-redis"
PROXY_PORT=${PROXY_PORT:-6380}

echo "🔗 Запуск Redis Proxy на порту $PROXY_PORT"
echo "Подключение: redis-cli -h localhost -p $PROXY_PORT"

# Создаем именованный пайп для коммуникации
PIPE="/tmp/redis-proxy-pipe"
rm -f "$PIPE"
mkfifo "$PIPE"

# Функция обработки подключений
handle_connection() {
    local client_fd=$1
    
    # Читаем команду от клиента
    while read -r line <&$client_fd; do
        if [ -n "$line" ]; then
            # Передаем команду в Redis контейнер
            echo "$line" | docker exec -i $REDIS_CONTAINER redis-cli
        fi
    done
    
    # Закрываем соединение
    exec $client_fd<&-
}

# Запускаем сервер
echo "🚀 Redis Proxy запущен на порту $PROXY_PORT"
echo "Нажмите Ctrl+C для остановки"

# Простой TCP сервер
while true; do
    # Слушаем подключения
    nc -l -p $PROXY_PORT -e /bin/bash -c '
        # Обрабатываем подключение
        while read -r line; do
            if [ -n "$line" ]; then
                echo "$line" | docker exec -i scanner-redis redis-cli
            fi
        done
    '
done
