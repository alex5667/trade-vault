#!/bin/bash

set -e

echo "🤖 Установка Auto Confidence Calibration SystemD Service"
echo "======================================================="

# Проверка прав sudo
if ! sudo -n true 2>/dev/null; then
    echo "❌ Требуются права sudo для установки systemd сервиса"
    echo "Запустите скрипт с sudo: sudo $0"
    exit 1
fi

# Проверка наличия файлов
if [[ ! -f "auto-train-conf-calibration.service" ]]; then
    echo "❌ Файл auto-train-conf-calibration.service не найден"
    exit 1
fi

if [[ ! -f "auto-train-conf-calibration.timer" ]]; then
    echo "❌ Файл auto-train-conf-calibration.timer не найден"
    exit 1
fi

# Копирование файлов
echo "📋 Копирование unit файлов..."
sudo cp auto-train-conf-calibration.service /etc/systemd/system/
sudo cp auto-train-conf-calibration.timer /etc/systemd/system/

# Перезагрузка systemd
echo "🔄 Перезагрузка systemd..."
sudo systemctl daemon-reload

# Проверка наличия env файла
if [[ ! -f "/etc/trade/conf_calibration.env" ]]; then
    echo "⚠️ ENV файл не найден: /etc/trade/conf_calibration.env"
    echo "Создайте его на основе примера из README.md"
    echo ""
    echo "Пример содержимого:"
    echo 'PERF_PG_DSN="postgresql://user:pass@host:5432/dbname?sslmode=require"'
    echo 'CONF_CAL_MODE="isotonic"'
    echo 'CONF_CAL_PATH="/home/alex/front/trade/scanner_infra/calibration/confidence_calibration.json"'
    echo 'CONF_CAL_MIN_SAMPLES="300"'
    echo 'CONF_CAL_RELOAD_SEC="30"'
    echo 'CONF_CAL_MIN_NEW_ELIGIBLE="300"'
    echo 'CONF_CAL_FORCE_AFTER_SEC="604800"'
    echo 'CONF_CAL_WINDOW_DAYS="365"'
    echo 'CONF_CAL_EPS_R="0.05"'
    echo 'CONF_CAL_STATE_PATH="/home/alex/front/trade/scanner_infra/calibration/confidence_calibration.state.json"'
    echo 'CONF_CAL_LOCK_PATH="/tmp/auto_train_conf_calibration.lock"'
    echo 'LOG_LEVEL="INFO"'
    echo ""
fi

# Включение и запуск таймера
echo "⏰ Включение таймера (запускается каждые 6 часов)..."
sudo systemctl enable --now auto-train-conf-calibration.timer

# Проверка статуса
echo ""
echo "📊 Проверка статуса..."
sleep 2

TIMER_STATUS=$(sudo systemctl is-active auto-train-conf-calibration.timer)
if [[ "$TIMER_STATUS" == "active" ]]; then
    echo "✅ Таймер успешно включен"
else
    echo "❌ Ошибка: таймер не активен"
    exit 1
fi

# Показать расписание
echo ""
echo "📅 Расписание запусков:"
sudo systemctl list-timers | grep auto-train-conf-calibration || true

echo ""
echo "🎉 Установка завершена!"
echo ""
echo "Команды управления:"
echo "  make calibration-auto-start  - Запустить один раз"
echo "  make calibration-auto-stop   - Остановить"
echo "  make calibration-auto-status - Статус"
echo "  make calibration-auto-enable - Включить таймер"
echo "  make calibration-auto-disable- Отключить таймер"
echo ""
echo "Логи:"
echo "  sudo journalctl -u auto-train-conf-calibration.service -f"
echo "  sudo journalctl -u auto-train-conf-calibration.timer -f"
