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
