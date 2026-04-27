#!/bin/bash

# Умный скрипт для очистки портов с минимальным использованием sudo
# Очищает порты: 6379 (Redis), 9090 (Prometheus), 3001 (Grafana)

echo "🧹 Очистка портов перед запуском контейнеров..."

# Флаг для проверки sudo
SUDO_AVAILABLE=false

# Проверяем, есть ли sudo и можем ли мы его использовать
check_sudo() {
    if command -v sudo >/dev/null 2>&1; then
        # Проверяем, можем ли использовать sudo без пароля
        if sudo -n true 2>/dev/null; then
            SUDO_AVAILABLE=true
            echo "✅ Sudo доступен без пароля"
        else
            echo "⚠️  Sudo требует пароль, попробуем без него"
        fi
    else
        echo "⚠️  Sudo не установлен, работаем без него"
    fi
}

# Функция для проверки порта
check_port() {
    local port=$1
    if [ "$SUDO_AVAILABLE" = true ]; then
        sudo lsof -i :$port >/dev/null 2>&1
    else
        ss -tuln | grep -q ":$port "
    fi
}

# Функция для очистки порта
clear_port() {
    local port=$1
    local service_name=$2
    
    echo "🔍 Проверяем порт $port ($service_name)..."
    
    if check_port $port; then
        echo "⚠️  Порт $port ($service_name) занят, освобождаем..."
        
        # Сначала пытаемся через Docker
        echo "🐳 Проверяем Docker контейнеры..."
        local containers=$(docker ps -a --format "table {{.Names}}\t{{.Ports}}" | grep ":$port->" | awk '{print $1}')
        
        if [ ! -z "$containers" ]; then
            echo "🔫 Останавливаем Docker контейнеры: $containers"
            echo $containers | xargs docker stop >/dev/null 2>&1
            echo $containers | xargs docker rm >/dev/null 2>&1
        fi
        
        # Если sudo доступен, используем его для системных процессов
        if [ "$SUDO_AVAILABLE" = true ]; then
            echo "🔫 Завершаем системные процессы..."
            sudo fuser -k $port/tcp >/dev/null 2>&1
            
            local pids=$(sudo lsof -t -i :$port 2>/dev/null)
            if [ ! -z "$pids" ]; then
                echo "🔫 Завершаем процессы: $pids"
                echo $pids | xargs sudo kill -9 >/dev/null 2>&1
            fi
        fi
        
        sleep 2
        
        if check_port $port; then
            echo "❌ Не удалось освободить порт $port"
            if [ "$SUDO_AVAILABLE" = false ]; then
                echo "💡 Попробуйте запустить: sudo ./clear-ports.sh"
            fi
            return 1
        else
            echo "✅ Порт $port ($service_name) успешно освобожден"
        fi
    else
        echo "✅ Порт $port ($service_name) свободен"
    fi
}

# Проверяем sudo
check_sudo

# Очищаем порты
clear_port 6379 "Redis"
clear_port 9090 "Prometheus" 
clear_port 3001 "Grafana"

echo "🎉 Очистка портов завершена!"
echo "🚀 Теперь можно запускать контейнеры"
