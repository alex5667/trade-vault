#!/bin/bash

# Скрипт для очистки портов 6379, 9090, 3001, 3000
# Включает поддержку Docker контейнеров и системных процессов
# Автор: AI Assistant

echo "🔧 Скрипт очистки портов запущен..."
echo "Очищаем порты: 6379, 9090, 3001, 3000"
echo "=========================================="

# Функция для очистки конкретного порта
clear_port() {
    local port=$1
    echo "🔍 Проверяем порт $port..."
    
    # Проверяем, занят ли порт
    if ! ss -tulpn | grep -q ":$port "; then
        echo "✅ Порт $port свободен"
        return 0
    fi
    
    echo "⚠️  Порт $port занят. Ищем процессы..."
    
    # Пробуем найти процессы через lsof
    local pids=$(lsof -ti:$port 2>/dev/null)
    
    if [ -n "$pids" ]; then
        echo "📋 Найдены процессы через lsof:"
        lsof -i:$port 2>/dev/null
        
        echo "🛑 Завершаем процессы на порту $port..."
        for pid in $pids; do
            echo "   Завершаем процесс PID: $pid"
            kill -9 $pid 2>/dev/null
            if [ $? -eq 0 ]; then
                echo "   ✅ Процесс $pid успешно завершен"
            else
                echo "   ❌ Не удалось завершить процесс $pid"
            fi
        done
    else
        echo "⚠️  Процессы не найдены через lsof, пробуем fuser..."
        
        # Используем fuser для поиска и завершения процессов
        if command -v fuser >/dev/null 2>&1; then
            local fuser_pids=$(fuser $port/tcp 2>/dev/null)
            if [ -n "$fuser_pids" ]; then
                echo "📋 Найдены процессы через fuser: $fuser_pids"
                echo "🛑 Завершаем процессы через fuser..."
                sudo fuser -k $port/tcp 2>/dev/null
                if [ $? -eq 0 ]; then
                    echo "   ✅ Процессы завершены через fuser"
                else
                    echo "   ❌ Не удалось завершить процессы через fuser"
                fi
            else
                echo "   ⚠️  Процессы не найдены через fuser"
            fi
        fi
    fi
    
    # Проверяем Docker контейнеры
    echo "🐳 Проверяем Docker контейнеры..."
    local docker_containers=$(docker ps -a --format "table {{.Names}}\t{{.Ports}}" | grep ":$port" | awk '{print $1}')
    
    if [ -n "$docker_containers" ]; then
        echo "📋 Найдены Docker контейнеры, использующие порт $port:"
        for container in $docker_containers; do
            echo "   Контейнер: $container"
            echo "   🛑 Останавливаем контейнер $container..."
            docker stop $container 2>/dev/null
            if [ $? -eq 0 ]; then
                echo "   ✅ Контейнер $container остановлен"
            else
                echo "   ❌ Не удалось остановить контейнер $container"
            fi
            
            echo "   🗑️  Удаляем контейнер $container..."
            docker rm $container 2>/dev/null
            if [ $? -eq 0 ]; then
                echo "   ✅ Контейнер $container удален"
            else
                echo "   ❌ Не удалось удалить контейнер $container"
            fi
        done
    fi
    
    # Проверяем, что порт действительно освобожден
    sleep 1
    if ! ss -tulpn | grep -q ":$port "; then
        echo "✅ Порт $port успешно освобожден"
    else
        echo "❌ Порт $port все еще занят"
        echo "🔍 Текущее состояние порта:"
        ss -tulpn | grep ":$port "
        echo "⚠️  Возможно, потребуется перезагрузка системы"
    fi
    echo ""
}

# Очищаем каждый порт
clear_port 6379
clear_port 9090
clear_port 3001
clear_port 3000

echo "=========================================="
echo "🏁 Очистка портов завершена!"
echo ""

# Финальная проверка всех портов
echo "📊 Финальная проверка портов:"
for port in 6379 9090 3001 3000; do
    if ss -tulpn | grep -q ":$port "; then
        echo "❌ Порт $port все еще занят"
        ss -tulpn | grep ":$port "
    else
        echo "✅ Порт $port свободен"
    fi
done

echo ""
echo "💡 Если порты все еще заняты, попробуйте:"
echo "   - Перезагрузить систему"
echo "   - Проверить системные службы: systemctl status"
echo "   - Очистить Docker: docker system prune -a"
echo "   - Ручная очистка: sudo fuser -k <port>/tcp"
