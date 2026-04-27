#!/bin/bash

# Быстрый подсчет общего количества записей в Redis kline streams
# Выводит только число для использования в других скриптах

set -e

# Конфигурация Redis
REDIS_HOST=${REDIS_HOST:-localhost}
REDIS_PORT=${REDIS_PORT:-6379}
REDIS_CLI="redis-cli -h $REDIS_HOST -p $REDIS_PORT"

# Проверка подключения
if ! $REDIS_CLI ping > /dev/null 2>&1; then
    echo "0"
    exit 1
fi

# Поиск kline streams и подсчет записей
cursor=0
total_records=0

while true; do
    result=$($REDIS_CLI scan $cursor match "*" count 1000 2>/dev/null)
    if [ $? -ne 0 ]; then
        echo "0"
        exit 1
    fi
    
    cursor=$(echo "$result" | head -1)
    keys=$(echo "$result" | tail -n +2 | grep "stream:kline")
    
    if [ -n "$keys" ]; then
        for stream in $keys; do
            if [ -n "$stream" ]; then
                length=$($REDIS_CLI xlen "$stream" 2>/dev/null || echo "0")
                if [[ "$length" =~ ^[0-9]+$ ]]; then
                    total_records=$((total_records + length))
                fi
            fi
        done
    fi
    
    if [ "$cursor" = "0" ]; then
        break
    fi
done

echo "$total_records" 