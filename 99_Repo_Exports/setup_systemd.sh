#!/bin/bash
echo "🤖 Установка Auto Confidence Calibration SystemD Service"
echo "======================================================="

# Проверка наличия файлов
if [[ ! -f "infra/systemd/auto-train-conf-calibration.service" ]]; then
    echo "❌ Service файл не найден"
    exit 1
fi

if [[ ! -f "infra/systemd/auto-train-conf-calibration.timer" ]]; then
    echo "❌ Timer файл не найден"
    exit 1
fi

echo "📋 Копирование unit файлов..."
sudo cp infra/systemd/auto-train-conf-calibration.service /etc/systemd/system/
sudo cp infra/systemd/auto-train-conf-calibration.timer /etc/systemd/system/

echo "🔄 Перезагрузка systemd..."
sudo systemctl daemon-reload

echo "⏰ Включение таймера (запускается каждые 6 часов)..."
sudo systemctl enable --now auto-train-conf-calibration.timer

echo "📊 Проверка статуса..."
sleep 2

TIMER_STATUS=$(sudo systemctl is-active auto-train-conf-calibration.timer)
if [[ "$TIMER_STATUS" == "active" ]]; then
    echo "✅ Таймер успешно включен"
else
    echo "❌ Ошибка: таймер не активен"
    exit 1
fi

echo ""
echo "📅 Расписание запусков:"
sudo systemctl list-timers | grep auto-train-conf-calibration || true

echo ""
echo "🎉 SystemD установка завершена!"
echo ""
echo "Теперь выполните создание ENV файла:"
echo "sudo mkdir -p /etc/trade"
echo "sudo cp etc/trade/conf_calibration.env.example /etc/trade/conf_calibration.env"
echo "sudo nano /etc/trade/conf_calibration.env  # Отредактируйте настройки БД"
