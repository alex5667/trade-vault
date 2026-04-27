#!/bin/bash
# Скрипт для запуска оффлайн-джоба расчета baseline

set -e

echo "🚀 Starting baseline calculation job..."

# Переменные окружения
export DATABASE_URL="${DATABASE_URL:-postgresql://postgres:password@localhost:5432/trade}"
export BASELINE_LOOKBACK_DAYS="${BASELINE_LOOKBACK_DAYS:-60}"
export BASELINE_MIN_SIGNALS="${BASELINE_MIN_SIGNALS:-200}"
export BASELINE_MIN_TRADES="${BASELINE_MIN_TRADES:-50}"
export BASELINE_YAML_PATH="${BASELINE_YAML_PATH:-crypto_conf_scorer_baseline.yaml}"
export BASELINE_INSERT_DB="${BASELINE_INSERT_DB:-1}"

echo "Configuration:"
echo "  DATABASE_URL: $DATABASE_URL"
echo "  LOOKBACK_DAYS: $BASELINE_LOOKBACK_DAYS"
echo "  MIN_SIGNALS: $BASELINE_MIN_SIGNALS"
echo "  MIN_TRADES: $BASELINE_MIN_TRADES"
echo "  YAML_PATH: $BASELINE_YAML_PATH"
echo "  INSERT_DB: $BASELINE_INSERT_DB"

# Запуск джоба
cd /app
python -m regime.baseline_job

echo "✅ Baseline job completed successfully!"
