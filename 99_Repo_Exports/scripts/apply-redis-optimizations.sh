#!/bin/bash

echo "🚀 Применение оптимизаций Redis для больших нагрузок..."

# Останавливаем текущие контейнеры
echo "⏹️  Остановка текущих контейнеров..."
docker-compose down

# Запускаем оптимизированную конфигурацию
echo "🔄 Запуск оптимизированной конфигурации..."
docker-compose -f docker-compose-optimized.yml up -d

# Ждем запуска
echo "⏳ Ожидание запуска Redis..."
sleep 15

# Проверяем статус
echo "📊 Статус Redis контейнеров:"
docker ps | grep redis

echo ""
echo "🔍 Проверка подключения к Redis:"
docker exec scanner-redis redis-cli ping

echo ""
echo "📈 Проверка конфигурации Redis:"
echo "Max memory: $(docker exec scanner-redis redis-cli config get maxmemory | tail -1)"
echo "Max clients: $(docker exec scanner-redis redis-cli config get maxclients | tail -1)"
echo "TCP backlog: $(docker exec scanner-redis redis-cli config get tcp-backlog | tail -1)"

echo ""
echo "✅ ОПТИМИЗАЦИИ ПРИМЕНЕНЫ:"
echo "================================"
echo "1. Увеличена память Redis до 16GB"
echo "2. Увеличен лимит клиентов до 50,000"
echo "3. Увеличен TCP backlog до 65,535"
echo "4. Оптимизированы настройки сети"
echo "5. Увеличены ресурсы контейнеров"
echo "6. Оптимизированы настройки AOF и RDB"
echo "7. Отключены медленные логи"
echo "8. Увеличены лимиты для стримов"
echo "================================"

echo ""
echo "🌐 ОТКРЫТЫЕ ПОРТЫ REDIS:"
echo "  - 0.0.0.0:6379   (основной Redis)"
echo "  - 0.0.0.0:6380   (Redis Worker 1)"
echo "  - 0.0.0.0:6381   (Redis Worker 2)"
echo "  - 0.0.0.0:16379  (sentinel порт)"
echo "  - 0.0.0.0:16380  (sentinel порт)"
echo "  - 0.0.0.0:16381  (sentinel порт)"
echo "  - 0.0.0.0:26379  (cluster порт)"
echo "  - 0.0.0.0:26380  (cluster порт)"
echo "  - 0.0.0.0:26381  (cluster порт)"

echo ""
echo "🔗 ПОДКЛЮЧЕНИЕ:"
echo "  redis://localhost:6379"
echo "  redis://localhost:6380"
echo "  redis://localhost:6381"

echo ""
echo "📊 МОНИТОРИНГ:"
echo "  Prometheus: http://localhost:9090"
echo "  Grafana: http://localhost:3001"
echo "  Логи Redis: docker logs scanner-redis"
