#!/bin/bash

echo "📝 Коммит миграции REDIS_HOST → scanner-redis-worker-1"
echo ""

# Добавляем измененные файлы
git add \
    docker-compose.yml \
    Dockerfile.cleanup \
    go-worker/infra/redisclient/client.go \
    python-worker/core/redis_client.py \
    redis-monitor.sh \
    redis-health-check.sh \
    redis-stress-monitor.sh \
    SYMBOL_EXCHANGE_PROTOCOL.md \
    REDIS_HOST_MIGRATION.md \
    REDIS_HOST_QUICK_REF.md

echo "✅ Файлы добавлены в staging"
echo ""

# Показываем что будет закоммичено
echo "📋 Изменения для коммита:"
git status --short | grep "^[AM]"
echo ""

# Коммитим
git commit -m "🔄 Migrate all services to scanner-redis-worker-1

Changes:
- Updated docker-compose.yml for 6 services
  * go-worker, python-worker, redis-cleanup: REDIS_HOST
  * telegram-worker, signal-parser-worker, notify-worker: REDIS_URL
  
- Updated default values in source code
  * go-worker/infra/redisclient/client.go
  * python-worker/core/redis_client.py
  
- Updated Dockerfile.cleanup ENV
- Updated monitoring scripts (redis-*.sh)
- Updated documentation

Fixed Issue:
- Resolved 'Error -3 connecting to redis:6379' in signal-parser-worker
- All services now successfully connect to scanner-redis-worker-1

Status:
✅ All 6 services tested and working
✅ Redis: 112 clients, 3751 keys
✅ No connection errors in logs

Documentation: REDIS_HOST_MIGRATION.md"

echo ""
echo "✅ Коммит выполнен!"
echo ""
echo "Для отправки в репозиторий выполните:"
echo "  git push origin main"

