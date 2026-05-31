#!/bin/bash
# Setup and run local calibration
# This script helps configure PG_DSN and run the calibration process

set -euo pipefail

echo "🚀 Настройка и запуск локальной калибровки"
echo "=========================================="
echo

# Check if PG_DSN is configured
DEFAULT_PG_DSN="postgresql://postgres:12345@localhost:5434/scanner_analytics"
export PG_DSN=${PG_DSN:-"$DEFAULT_PG_DSN"}

if [[ "$PG_DSN" == "$DEFAULT_PG_DSN" ]]; then
    echo "⚠️  Используется PG_DSN по умолчанию: $PG_DSN"
else
    echo "✅ Используется PG_DSN из окружения: $PG_DSN"
fi

echo
echo "📊 Текущие параметры калибровки:"
echo "   PG_DSN: ${PG_DSN:-'не установлен'}"
echo "   CALIB_LOOKBACK_DAYS: ${CALIB_LOOKBACK_DAYS:-365}"
echo "   CALIB_MIN_TRADES_CLUSTER: ${CALIB_MIN_TRADES_CLUSTER:-300}"
echo "   CALIB_MIN_TRADES_BUCKET: ${CALIB_MIN_TRADES_BUCKET:-30}"
echo "   CALIB_MIN_MEAN_PNL_R: ${CALIB_MIN_MEAN_PNL_R:-0.0}"
echo

# Check database connection
echo "🔍 Проверка подключения к базе данных..."
if python3 -c "
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath('$0'))))

try:
    import psycopg2
    conn = psycopg2.connect('$PG_DSN', connect_timeout=5)
    conn.close()
    print('✅ Подключение к базе данных успешно')
except Exception as e:
    print(f'❌ Ошибка подключения: {e}')
    exit(1)
"; then
    echo
else
    echo "⚠️  Не удалось подключиться к базе данных (PG_DSN=$PG_DSN)."
    echo "   Калибровка пропущена — запустите вручную: make calibration-run"
    exit 0
fi

# Check if migration was applied
echo "📋 Проверка наличия таблицы signal_local_calibration..."
if python3 -c "
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath('$0'))))

try:
    import psycopg2
    conn = psycopg2.connect('$PG_DSN', connect_timeout=5)
    cur = conn.cursor()
    cur.execute(\"SELECT to_regclass('public.signal_local_calibration')\")
    result = cur.fetchone()[0]
    cur.close()
    conn.close()
    if result is None:
        print('❌ Таблица signal_local_calibration не найдена')
        print('   Сначала примените миграцию:')
        print('   psql -d <database> -f python-worker/migrations/001_add_local_calibration.sql')
        exit(1)
    print('✅ Таблица signal_local_calibration существует')
except Exception as e:
    print(f'❌ Ошибка проверки таблицы: {e}')
    exit(1)
"; then
    echo
else
    echo "⚠️  Таблица signal_local_calibration не существует — пропускаем калибровку."
    echo "   Примените миграцию: psql -d scanner_analytics -f python-worker/migrations/001_add_local_calibration.sql"
    exit 0
fi

# Run calibration
echo "🎯 Запуск калибровки..."
echo "   Это может занять несколько минут в зависимости от объема данных..."
echo

python3 python-worker/scripts/run_local_calibration.py

echo
echo "✅ Калибровка завершена!"
echo
echo "📊 Проверка результатов..."

# Check results
python3 python-worker/scripts/check_calibration_results.py

echo
echo "🎉 Настройка локальной калибровки завершена!"
echo
echo "💡 Рекомендации:"
echo "   - Добавьте запуск калибровки в cron для регулярного обновления:"
echo "     0 2 * * * cd /home/alex/front/trade/scanner_infra && make calibration-auto-start"
echo "   - Мониторьте качество калибровки с помощью check_calibration_results.py"
echo "   - При изменении данных перезапускайте калибровку"
