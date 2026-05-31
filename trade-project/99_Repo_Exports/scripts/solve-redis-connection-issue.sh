#!/bin/bash

# Комплексное решение проблемы с Redis подключением в trade_back
# Решает проблемы с ECONNRESET и переподключением

echo "🔧 Комплексное решение проблемы с Redis подключением в trade_back..."

# 1. Останавливаем все сервисы
echo "⏹️ Остановка всех сервисов..."
docker-compose down

# 2. Очищаем порты
echo "🧹 Очистка портов..."
sudo fuser -k 6379/tcp 2>/dev/null || true
sudo fuser -k 6380/tcp 2>/dev/null || true
sudo fuser -k 6381/tcp 2>/dev/null || true
sleep 2

# 3. Применяем исправления к trade_back
echo "🔄 Применение исправлений к trade_back..."
if [ -d "/home/alex/front/trade/trade_back" ]; then
    cd /home/alex/front/trade/trade_back/src/redis
    
    # Создаем резервные копии
    cp createRedis.ts createRedis.ts.backup.$(date +%Y%m%d_%H%M%S)
    cp redis.module.ts redis.module.ts.backup.$(date +%Y%m%d_%H%M%S)
    
    # Копируем исправленную версию
    cp /home/alex/front/trade/scanner_infra/trade_back_fixed/createRedisFixed.ts ./createRedisFixed.ts
    
    # Заменяем createRedis на исправленную версию
    cp createRedisFixed.ts createRedis.ts
    
    # Обновляем redis.module.ts
    sed -i "s/createRedisLocal as createRedis, createRedisSubscriberLocal as createRedisSubscriber/createRedisFixed as createRedis, createRedisSubscriberFixed as createRedisSubscriber/" redis.module.ts
    sed -i "s/from '.\/createRedisLocal'/from '.\/createRedisFixed'/" redis.module.ts
    
    echo "✅ Исправления trade_back применены!"
else
    echo "⚠️ Директория trade_back не найдена, пропускаем исправления"
fi

# 4. Возвращаемся в корневую директорию
cd /home/alex/front/trade/scanner_infra

# 5. Запускаем Redis с новой конфигурацией
echo "🚀 Запуск Redis с оптимизированной конфигурацией..."
docker-compose up -d redis

# 6. Ждем готовности Redis
echo "⏳ Ожидание готовности Redis..."
for i in {1..30}; do
    if docker-compose exec -T redis redis-cli ping >/dev/null 2>&1; then
        echo "✅ Redis готов к работе!"
        break
    fi
    echo "⏳ Попытка $i/30..."
    sleep 2
done

# 7. Запускаем остальные сервисы
echo "🚀 Запуск остальных сервисов..."
docker-compose up -d

# 8. Проверяем статус
echo "📊 Статус сервисов:"
docker-compose ps

# 9. Показываем логи Redis
echo "📝 Последние логи Redis:"
docker logs scanner-redis --tail=20

echo "🎉 Решение применено!"
echo "💡 Теперь можно запускать trade_back: npm run start:dev"
echo "📊 Для мониторинга используйте: docker logs scanner-redis -f"

