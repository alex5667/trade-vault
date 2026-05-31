#!/bin/bash

# Скрипт для перезапуска Redis при проблемах с подключением trade_back
# Используется для исправления ECONNRESET ошибок

echo "🔄 Перезапуск Redis для исправления проблем с подключением trade_back..."

# Останавливаем Redis
echo "⏹️ Остановка Redis..."
docker-compose stop redis

# Ждем полной остановки
sleep 3

# Очищаем порт 6379
echo "🧹 Очистка порта 6379..."
sudo fuser -k 6379/tcp 2>/dev/null || true

# Ждем освобождения порта
sleep 2

# Запускаем Redis
echo "▶️ Запуск Redis..."
docker-compose up -d redis

# Ждем готовности Redis
echo "⏳ Ожидание готовности Redis..."
for i in {1..30}; do
    if docker-compose exec -T redis redis-cli ping >/dev/null 2>&1; then
        echo "✅ Redis готов к работе!"
        break
    fi
    echo "⏳ Попытка $i/30..."
    sleep 2
done

# Проверяем статус
echo "📊 Статус Redis:"
docker-compose ps redis

echo "🎉 Redis перезапущен успешно!"
echo "💡 Теперь можно запускать trade_back: npm run start:dev"
