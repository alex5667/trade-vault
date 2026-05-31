#!/bin/bash

# Агрессивный скрипт для очистки портов 6379, 9090, 3001, 3000
# Требует права root
# Автор: AI Assistant

if [ "$EUID" -ne 0 ]; then
    echo "❌ Этот скрипт должен запускаться с правами root"
    echo "Используйте: sudo ./clear_ports_root.sh"
    exit 1
fi

echo "🔧 Агрессивный скрипт очистки портов запущен (root)"
echo "Очищаем порты: 6379, 9090, 3001, 3000"
echo "=========================================="

# Функция для агрессивной очистки порта
aggressive_clear_port() {
    local port=$1
    echo "🔍 Агрессивно очищаем порт $port..."
    
    # Проверяем, занят ли порт
    if ! ss -tulpn | grep -q ":$port "; then
        echo "✅ Порт $port уже свободен"
        return 0
    fi
    
    echo "⚠️  Порт $port занят. Применяем агрессивные методы..."
    
    # 1. fuser - самый эффективный метод
    echo "🛑 Используем fuser для завершения процессов..."
    fuser -k $port/tcp 2>/dev/null
    if [ $? -eq 0 ]; then
        echo "   ✅ Процессы завершены через fuser"
    else
        echo "   ⚠️  fuser не нашел процессы"
    fi
    
    # 2. lsof + kill
    echo "🔍 Ищем процессы через lsof..."
    local pids=$(lsof -ti:$port 2>/dev/null)
    if [ -n "$pids" ]; then
        echo "�� Найдены процессы: $pids"
        for pid in $pids; do
            echo "   🛑 Завершаем PID: $pid"
            kill -9 $pid 2>/dev/null
        done
    fi
    
    # 3. netstat + kill (если доступен)
    if command -v netstat >/dev/null 2>&1; then
        echo "🔍 Проверяем через netstat..."
        local netstat_pids=$(netstat -tulpn | grep ":$port " | awk '{print $7}' | cut -d'/' -f1 | grep -v '-' | sort -u)
        if [ -n "$netstat_pids" ]; then
            echo "📋 Найдены процессы через netstat: $netstat_pids"
            for pid in $netstat_pids; do
                echo "   🛑 Завершаем PID: $pid"
                kill -9 $pid 2>/dev/null
            done
        fi
    fi
    
    # 4. Проверяем Docker
    echo "🐳 Проверяем Docker контейнеры..."
    docker ps -a --format "table {{.Names}}\t{{.Ports}}" | grep ":$port" | awk '{print $1}' | while read container; do
        if [ -n "$container" ] && [ "$container" != "NAMES" ]; then
            echo "   🛑 Останавливаем контейнер: $container"
            docker stop $container 2>/dev/null
            docker rm $container 2>/dev/null
        fi
    done
    
    # 5. Проверяем iptables
    echo "🔧 Проверяем iptables правила..."
    iptables -t nat -L | grep ":$port" && echo "   ⚠️  Найдены iptables правила для порта $port"
    
    # 6. Финальная проверка
    sleep 2
    if ! ss -tulpn | grep -q ":$port "; then
        echo "✅ Порт $port успешно освобожден"
    else
        echo "❌ Порт $port все еще занят"
        echo "🔍 Текущее состояние:"
        ss -tulpn | grep ":$port "
        echo "⚠️  Возможно, порт занят системным процессом"
    fi
    echo ""
}

# Очищаем каждый порт агрессивно
aggressive_clear_port 6379
aggressive_clear_port 9090
aggressive_clear_port 3001
aggressive_clear_port 3000

echo "=========================================="
echo "🏁 Агрессивная очистка портов завершена!"
echo ""

# Финальная проверка
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
echo "🐳 Очистка всех scanner Docker контейнеров..."
scanner_containers=$(docker ps -a --filter "name=scanner" --format "{{.Names}}" 2>/dev/null)
if [ -n "$scanner_containers" ]; then
    echo "📋 Найдены scanner контейнеры:"
    echo "$scanner_containers"
    echo ""
    for container in $scanner_containers; do
        echo "   🛑 Останавливаем и удаляем: $container"
        docker stop $container 2>/dev/null
        docker rm -f $container 2>/dev/null
    done
    echo "✅ Все scanner контейнеры удалены"
else
    echo "✅ Нет активных scanner контейнеров"
fi

echo ""
echo "🐳 Очистка Docker сетей с активными endpoints..."
if docker network ls | grep -q "scanner"; then
    # Находим все сети scanner
    docker network ls --filter "name=scanner" --format "{{.Name}}" | while read network; do
        echo "🔍 Проверяем сеть: $network"
        # Получаем контейнеры, подключенные к сети
        containers=$(docker network inspect $network --format '{{range .Containers}}{{.Name}} {{end}}' 2>/dev/null)
        if [ -n "$containers" ]; then
            echo "   📋 Найдены активные endpoints: $containers"
            for container in $containers; do
                echo "   🛑 Останавливаем и удаляем: $container"
                docker stop $container 2>/dev/null
                docker rm -f $container 2>/dev/null
            done
        fi
        echo "   🗑️  Удаляем сеть: $network"
        docker network rm $network 2>/dev/null && echo "   ✅ Сеть удалена" || echo "   ⚠️  Не удалось удалить сеть"
    done
else
    echo "✅ Нет активных scanner сетей"
fi

echo ""
echo "💡 Если порты все еще заняты:"
echo "   - Перезагрузите систему"
echo "   - Проверьте системные службы"
echo "   - Очистите Docker: docker system prune -a"
