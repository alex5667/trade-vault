#!/bin/bash
# Скрипт для настройки DNS в Docker daemon
# Выполните: bash apply_docker_dns_fix.sh

set -e

echo "🔧 Настройка DNS для Docker daemon..."
echo ""

# Создать директорию если не существует
sudo mkdir -p /etc/docker

# Создать резервную копию если файл существует
if [ -f /etc/docker/daemon.json ]; then
    echo "📋 Создание резервной копии существующего daemon.json..."
    sudo cp /etc/docker/daemon.json /etc/docker/daemon.json.backup.$(date +%Y%m%d_%H%M%S)
    echo "✅ Резервная копия создана"
fi

# Создать/обновить daemon.json с DNS настройками
echo "📝 Настройка DNS серверов (8.8.8.8, 8.8.4.4, 1.1.1.1)..."
echo '{"dns": ["8.8.8.8", "8.8.4.4", "1.1.1.1"]}' | sudo tee /etc/docker/daemon.json > /dev/null

echo "✅ Конфигурация сохранена в /etc/docker/daemon.json"
echo ""
echo "📄 Содержимое файла:"
sudo cat /etc/docker/daemon.json
echo ""

# Перезапустить Docker
echo "🔄 Перезапуск Docker daemon..."
sudo systemctl restart docker

echo ""
echo "⏳ Ожидание запуска Docker (3 секунды)..."
sleep 3

# Проверить статус Docker
echo ""
echo "🧪 Проверка статуса Docker..."
if systemctl is-active --quiet docker; then
    echo "✅ Docker daemon запущен"
else
    echo "⚠️  Docker daemon не запущен, проверьте: sudo systemctl status docker"
    exit 1
fi

# Тест DNS разрешения
echo ""
echo "🧪 Тест DNS разрешения..."
if docker run --rm alpine:latest nslookup registry-1.docker.io 2>&1 | grep -q "registry-1.docker.io"; then
    echo "✅ DNS разрешение работает!"
else
    echo "⚠️  DNS тест не прошел, но это может быть нормально"
fi

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "✅ Настройка DNS для Docker завершена!"
echo ""
echo "📝 Теперь вы можете запустить: make up"
echo "═══════════════════════════════════════════════════════════"



