#!/bin/bash
# Скрипт для запуска автоматической калибровки TRAILING_TP1_OFFSET_ATR
# Добавьте в crontab для регулярного выполнения

set -e

# Переход в директорию проекта
cd "$(dirname "$0")/../.."

# Настройка переменных окружения
export PG_DSN_CALIBRATION="${PG_DSN_CALIBRATION:-postgresql://postgres:postgres@localhost:5432/scanner_analytics}"
export REDIS_URL="${REDIS_URL:-redis://localhost:6379/0}"

# Логирование
LOG_FILE="/var/log/trailing_tp1_calibration.log"
echo "$(date): Starting TRAILING_TP1_OFFSET_ATR calibration" >> "$LOG_FILE"

# Запуск калибровки
python -m services.auto_calibration_service >> "$LOG_FILE" 2>&1

# Проверка статуса
if [ $? -eq 0 ]; then
    echo "$(date): Calibration completed successfully" >> "$LOG_FILE"
else
    echo "$(date): Calibration failed with exit code $?" >> "$LOG_FILE"
    # Здесь можно добавить отправку алерта
fi
