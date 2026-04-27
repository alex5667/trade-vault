#!/bin/bash

# Скрипт для остановки scanner-infra с очисткой портов

echo "🛑 Остановка Scanner Infrastructure..."

# Переходим в директорию скрипта
cd "$(dirname "$0")"

# Останавливаем docker-compose
echo "🐳 Остановка Docker Compose..."
docker-compose down

# Очищаем порты после остановки
echo "🧹 Очистка портов после остановки..."
./clear-ports.sh

echo "✅ Scanner Infrastructure остановлен и порты очищены!"
