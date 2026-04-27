#!/bin/bash
# Wrapper для запуска ML диагностики через Docker

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REDIS_URL="${REDIS_URL:-redis://redis-worker-1:6379/0}"

echo "=========================================="
echo "ML DIAGNOSTICS RUNNER"
echo "=========================================="
echo "Redis URL: $REDIS_URL"
echo ""

# Проверяем доступность Redis
if ! docker exec redis-worker-1 redis-cli PING > /dev/null 2>&1; then
    echo "❌ Redis недоступен. Убедитесь, что контейнер redis-worker-1 запущен."
    exit 1
fi

# Запускаем скрипты через docker exec в контейнере с Python
# Используем контейнер, который имеет доступ к Redis сети

CONTAINER="scanner-signal-parser-worker"

if ! docker exec $CONTAINER python3 -c "import redis" > /dev/null 2>&1; then
    echo "❌ Python redis не установлен в контейнере $CONTAINER"
    echo "Попробуем использовать другой контейнер..."
    CONTAINER="scanner-python-worker"
fi

echo "Используем контейнер: $CONTAINER"
echo ""

# Копируем скрипты во временную директорию контейнера
echo "📋 Шаг 1/5: Проверка конфигурации ML..."
docker cp "$SCRIPT_DIR/python-worker/tools/ml_check_config.py" $CONTAINER:/tmp/ml_check_config.py 2>/dev/null || true
docker exec -e REDIS_URL="$REDIS_URL" $CONTAINER python3 /tmp/ml_check_config.py --redis-url "$REDIS_URL" 2>&1 || echo "⚠️  Ошибка при проверке конфигурации"

echo ""
echo "📊 Шаг 2/5: Диагностика ошибок..."
docker cp "$SCRIPT_DIR/python-worker/tools/ml_diagnose_errors.py" $CONTAINER:/tmp/ml_diagnose_errors.py 2>/dev/null || true
docker exec -e REDIS_URL="$REDIS_URL" $CONTAINER python3 /tmp/ml_diagnose_errors.py --redis-url "$REDIS_URL" --window-min 60 2>&1 || echo "⚠️  Ошибка при диагностике ошибок"

echo ""
echo "⏱️  Шаг 3/5: Диагностика латентности..."
docker cp "$SCRIPT_DIR/python-worker/tools/ml_diagnose_latency.py" $CONTAINER:/tmp/ml_diagnose_latency.py 2>/dev/null || true
docker exec -e REDIS_URL="$REDIS_URL" $CONTAINER python3 /tmp/ml_diagnose_latency.py --redis-url "$REDIS_URL" --window-min 60 2>&1 || echo "⚠️  Ошибка при диагностике латентности"

echo ""
echo "📋 Шаг 4/5: Список pending предложений guard..."
docker cp "$SCRIPT_DIR/python-worker/tools/ml_guard_approve.py" $CONTAINER:/tmp/ml_guard_approve.py 2>/dev/null || true
docker exec -e REDIS_URL="$REDIS_URL" $CONTAINER python3 /tmp/ml_guard_approve.py --redis-url "$REDIS_URL" --action list 2>&1 || echo "⚠️  Ошибка при получении списка предложений"

echo ""
echo "=========================================="
echo "ДИАГНОСТИКА ЗАВЕРШЕНА"
echo "=========================================="

