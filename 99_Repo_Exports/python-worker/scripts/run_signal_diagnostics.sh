#!/bin/bash
# Скрипт для запуска диагностики сигналов CryptoOrderFlow

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PYTHON_WORKER_DIR="$PROJECT_ROOT/python-worker"

echo "🔍 Запуск диагностики CryptoOrderFlow сигналов..."
echo ""

# Проверяем, что мы в правильной директории
if [ ! -f "$PYTHON_WORKER_DIR/services/crypto_orderflow_service.py" ]; then
    echo "❌ Ошибка: не найдена директория python-worker"
    exit 1
fi

# Запускаем диагностический скрипт
cd "$PYTHON_WORKER_DIR"

# Используем переменные окружения из docker-compose или дефолтные
export REDIS_URL="${REDIS_URL:-redis://scanner-redis-worker-1:6379/0}"
export REDIS_TICKS_URL="${REDIS_TICKS_URL:-redis://redis-ticks:6379/0}"
export CRYPTO_NOTIFY_REDIS_URL="${CRYPTO_NOTIFY_REDIS_URL:-$REDIS_URL}"
export CRYPTO_NOTIFY_STREAM="${CRYPTO_NOTIFY_STREAM:-notify:telegram}"
export CRYPTO_RAW_STREAM="${CRYPTO_RAW_STREAM:-signals:crypto:raw}"
export CRYPTO_ORDERFLOW_SIGNAL_STREAM="${CRYPTO_ORDERFLOW_SIGNAL_STREAM:-signals:cryptoorderflow:{symbol}}"

python3 scripts/check_crypto_signals.py

