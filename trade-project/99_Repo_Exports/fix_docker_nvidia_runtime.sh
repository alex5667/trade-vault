#!/bin/bash
# Скрипт для настройки NVIDIA runtime в Docker
# Выполните: bash fix_docker_nvidia_runtime.sh

set -e

echo "🔧 Настройка NVIDIA runtime для Docker..."
echo ""

# Проверить наличие nvidia-container-toolkit
if ! dpkg -l | grep -q nvidia-container-toolkit; then
    echo "❌ nvidia-container-toolkit не установлен!"
    echo "   Установите: sudo apt-get install -y nvidia-container-toolkit"
    exit 1
fi

echo "✅ nvidia-container-toolkit установлен"

# Создать директорию если не существует
sudo mkdir -p /etc/docker

# Создать резервную копию если файл существует
if [ -f /etc/docker/daemon.json ]; then
    echo "📋 Создание резервной копии существующего daemon.json..."
    sudo cp /etc/docker/daemon.json /etc/docker/daemon.json.backup.$(date +%Y%m%d_%H%M%S)
    echo "✅ Резервная копия создана"
fi

# Создать/обновить конфигурацию с DNS и NVIDIA runtime
echo "📝 Настройка Docker daemon (DNS + NVIDIA runtime)..."
sudo tee /etc/docker/daemon.json > /dev/null << 'EOF'
{
  "dns": ["8.8.8.8", "8.8.4.4", "1.1.1.1"],
  "runtimes": {
    "nvidia": {
      "path": "nvidia-container-runtime",
      "runtimeArgs": []
    }
  },
  "default-runtime": "runc"
}
EOF

echo "✅ Конфигурация сохранена"
echo ""
echo "📄 Содержимое файла:"
sudo cat /etc/docker/daemon.json
echo ""

# Перезапустить Docker
echo "🔄 Перезапуск Docker daemon..."
sudo systemctl restart docker

echo ""
echo "⏳ Ожидание запуска Docker (5 секунд)..."
sleep 5

# Проверить статус Docker
echo ""
echo "🧪 Проверка статуса Docker..."
if systemctl is-active --quiet docker; then
    echo "✅ Docker daemon запущен"
else
    echo "⚠️  Docker daemon не запущен, проверьте: sudo systemctl status docker"
    exit 1
fi

# Проверить наличие nvidia runtime
echo ""
echo "🧪 Проверка NVIDIA runtime..."
if docker info 2>&1 | grep -i "nvidia" || docker info 2>&1 | grep -i "runtime" | grep -i "nvidia"; then
    echo "✅ NVIDIA runtime обнаружен"
else
    echo "ℹ️  Проверяем runtimes вручную..."
    docker info 2>&1 | grep -A 5 -i "runtimes" || echo "   Выполните: docker info | grep -i runtime"
fi

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "✅ Настройка завершена!"
echo ""
echo "📝 Теперь вы можете запустить: make up"
echo "═══════════════════════════════════════════════════════════"
