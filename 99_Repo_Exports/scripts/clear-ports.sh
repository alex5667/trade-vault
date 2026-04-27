#!/bin/bash

# Скрипт для очистки портов перед запуском контейнеров
# Очищает порты: 6379 (Redis), 9090 (Prometheus), 3001 (Grafana)

echo "🧹 Очистка портов перед запуском контейнеров..."

# Функция для очистки порта
clear_port() {
    local port=$1
    local service_name=$2
    
    echo "🔍 Проверяем порт $port ($service_name)..."
    
    # Проверяем, используется ли порт
    if sudo lsof -i :$port >/dev/null 2>&1; then
        echo "⚠️  Порт $port ($service_name) занят, освобождаем..."
        
        # Убиваем процессы, использующие порт
        sudo fuser -k $port/tcp >/dev/null 2>&1
        
        # Дополнительная проверка и очистка через lsof
        local pids=$(sudo lsof -t -i :$port 2>/dev/null)
        if [ ! -z "$pids" ]; then
            echo "🔫 Завершаем процессы: $pids"
            echo $pids | xargs sudo kill -9 >/dev/null 2>&1
        fi
        
        # Ждем немного для освобождения порта
        sleep 2
        
        # Проверяем, что порт освобожден
        if sudo lsof -i :$port >/dev/null 2>&1; then
            echo "❌ Не удалось освободить порт $port"
            return 1
        else
            echo "✅ Порт $port ($service_name) успешно освобожден"
        fi
    else
        echo "✅ Порт $port ($service_name) свободен"
    fi
}

# Очищаем порты
clear_port 6379 "Redis"
clear_port 9090 "Prometheus" 
clear_port 3001 "Grafana"

echo "🎉 Очистка портов завершена!"
echo "🚀 Теперь можно запускать контейнеры"
