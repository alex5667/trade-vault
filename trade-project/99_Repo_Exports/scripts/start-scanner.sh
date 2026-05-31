#!/bin/bash

# Скрипт для запуска scanner-infra с автоматической очисткой портов

echo "🚀 Запуск Scanner Infrastructure..."

# Переходим в директорию скрипта
cd "$(dirname "$0")"

# Очищаем порты перед запуском
echo "🧹 Очистка портов..."
./clear-ports.sh

# Проверяем результат очистки
if [ $? -ne 0 ]; then
    echo "❌ Ошибка при очистке портов. Остановка."
    exit 1
fi

echo ""
echo "🐳 Запуск Docker Compose..."

# Запускаем docker-compose
docker-compose up -d

# Проверяем статус
if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Scanner Infrastructure успешно запущен!"
    echo ""
    echo "📊 Доступные сервисы:"
    echo "   • Redis: localhost:6379"
    echo "   • Prometheus: http://localhost:9090"
    echo "   • Grafana: http://localhost:3001 (admin/admin)"
    echo ""
    echo "📋 Статус контейнеров:"
    docker-compose ps
else
    echo "❌ Ошибка при запуске Docker Compose"
    exit 1
fi
