#!/bin/bash

# Скрипт для очистки портов БЕЗ sudo
# Очищает порты: 6379 (Redis), 9090 (Prometheus), 3001 (Grafana)

echo "🧹 Очистка портов перед запуском контейнеров..."

# Функция для проверки порта без sudo
check_port_no_sudo() {
    local port=$1
    # Используем ss вместо lsof, так как ss не требует sudo для чтения
    ss -tuln | grep -q ":$port "
    return $?
}

# Функция для очистки порта без sudo
clear_port_no_sudo() {
    local port=$1
    local service_name=$2
    
    echo "🔍 Проверяем порт $port ($service_name)..."
    
    # Проверяем, используется ли порт (без sudo)
    if check_port_no_sudo $port; then
        echo "⚠️  Порт $port ($service_name) занят, пытаемся освободить..."
        
        # Пытаемся найти и убить процессы через Docker
        echo "🐳 Проверяем Docker контейнеры..."
        local containers=$(docker ps -a --format "table {{.Names}}\t{{.Ports}}" | grep ":$port->" | awk '{print $1}')
        
        if [ ! -z "$containers" ]; then
            echo "🔫 Останавливаем Docker контейнеры: $containers"
            echo $containers | xargs docker stop >/dev/null 2>&1
            echo $containers | xargs docker rm >/dev/null 2>&1
        fi
        
        # Ждем немного
        sleep 2
        
        # Проверяем снова
        if check_port_no_sudo $port; then
            echo "⚠️  Порт $port все еще занят, но это может быть системный процесс"
            echo "💡 Попробуйте запустить: sudo ./clear-ports.sh"
            return 1
        else
            echo "✅ Порт $port ($service_name) успешно освобожден"
        fi
    else
        echo "✅ Порт $port ($service_name) свободен"
    fi
}

# Очищаем порты
clear_port_no_sudo 6379 "Redis"
clear_port_no_sudo 9090 "Prometheus" 
clear_port_no_sudo 3001 "Grafana"

echo "🎉 Очистка портов завершена!"
echo "🚀 Теперь можно запускать контейнеры"
