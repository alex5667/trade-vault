# Auto Confidence Calibration SystemD Setup

## Установка

```bash
# Копирование unit файлов
sudo cp auto-train-conf-calibration.service /etc/systemd/system/
sudo cp auto-train-conf-calibration.timer /etc/systemd/system/

# Перезагрузка systemd
sudo systemctl daemon-reload

# Включение таймера (запускается каждые 6 часов)
sudo systemctl enable --now auto-train-conf-calibration.timer

# Проверка статуса
sudo systemctl list-timers | grep auto-train-conf-calibration
```

## Команды управления

### Через Makefile (рекомендуется)
```bash
# Запуск один раз
make calibration-auto-start

# Остановка
make calibration-auto-stop

# Статус
make calibration-auto-status

# Включение таймера (автоматический запуск каждые 6 часов)
make calibration-auto-enable

# Отключение таймера
make calibration-auto-disable
```

### Через systemctl
```bash
# Запуск один раз
sudo systemctl start auto-train-conf-calibration.service

# Остановка
sudo systemctl stop auto-train-conf-calibration.service

# Статус
sudo systemctl status auto-train-conf-calibration.service

# Включение таймера
sudo systemctl enable --now auto-train-conf-calibration.timer

# Отключение таймера
sudo systemctl disable --now auto-train-conf-calibration.timer

# Логи
sudo journalctl -u auto-train-conf-calibration.service -n 50 --no-pager
```

## Конфигурация

Создайте файл `/etc/trade/conf_calibration.env`:

```bash
# Database connection (ВАЖНО: scanner_analytics база!)
PERF_PG_DSN="postgresql://trading:trading_password@postgres:5432/scanner_analytics?sslmode=require"

# Если запускаете вне Docker, используйте localhost:5434
# PERF_PG_DSN="postgresql://trading:trading_password@localhost:5434/scanner_analytics?sslmode=require"

# Runtime (уже используются)
CONF_CAL_MODE="isotonic"
CONF_CAL_PATH="/home/alex/front/trade/scanner_infra/calibration/confidence_calibration.json"
CONF_CAL_MIN_SAMPLES="300"
CONF_CAL_RELOAD_SEC="30"

# Auto-train
CONF_CAL_MIN_NEW_ELIGIBLE="300"     # Минимальное кол-во новых eligible исходов для запуска
CONF_CAL_FORCE_AFTER_SEC="604800"   # Принудительный запуск через 7 дней, даже если мало данных
CONF_CAL_WINDOW_DAYS="365"          # Обучаемся на окне 1 год
CONF_CAL_EPS_R="0.05"               # Нейтральная зона вокруг 0R
CONF_CAL_STATE_PATH="/home/alex/front/trade/scanner_infra/calibration/confidence_calibration.state.json"
CONF_CAL_LOCK_PATH="/tmp/auto_train_conf_calibration.lock"
LOG_LEVEL="INFO"
```

## Информация о базе данных

- **Пользователь:** `trading` (создается в `init-postgres.sql`)
- **Пароль:** `trading_password` (создается в `init-postgres.sql`)
- **База:** `scanner_analytics` (содержит таблицу `signal_performance`)
- **Хост:** `postgres` (внутри Docker) или `localhost` (снаружи)
- **Порт:** `5432` (внутри Docker) или `5434` (проброшен на host)

## Автоматический запуск

При выполнении `make up` или `make up-bg` сервис автоматически запускается после калибровки.

## Мониторинг

### State файл
Система ведет state файл с информацией о последнем запуске:
- `last_trained_at` - время последнего обучения
- `last_data_until` - дата последних данных
- `last_new_eligible` - количество новых eligible исходов
- `last_decision` - причина последнего запуска

### Логи
```bash
# Логи сервиса
sudo journalctl -u auto-train-conf-calibration.service -f

# Логи таймера
sudo journalctl -u auto-train-conf-calibration.timer -f
```

## Диагностика

### Проверка работы
```bash
# Статус таймера
sudo systemctl list-timers | grep auto-train-conf-calibration

# Последние логи
sudo journalctl -u auto-train-conf-calibration.service -n 20

# Проверка наличия файлов
ls -la calibration/confidence_calibration.json
ls -la calibration/confidence_calibration.state.json
```

### Ручной запуск для тестирования
```bash
# Принудительный запуск (игнорируя условия)
sudo systemctl start auto-train-conf-calibration.service

# Проверка результатов
make calibration-check
```

## Безопасность

- **Lockfile**: Предотвращает параллельные запуски
- **Atomic writes**: JSON файлы записываются атомарно
- **Fail-open**: Ошибки не ломают основную систему
- **Resource limits**: Nice=10, ограничения CPU/Memory

## Troubleshooting

### Сервис не запускается
```bash
# Проверить статус
sudo systemctl status auto-train-conf-calibration.service

# Проверить логи
sudo journalctl -u auto-train-conf-calibration.service

# Проверить конфигурацию
sudo systemctl cat auto-train-conf-calibration.service
```

### Проблемы с окружением
```bash
# Проверить наличие env файла
ls -la /etc/trade/conf_calibration.env

# Проверить переменные
cat /etc/trade/conf_calibration.env

# Проверить права
sudo -u alex env | grep CONF_CAL
```

### Проблемы с DB
```bash
# Проверить подключение к БД
PGPASSWORD=pass psql -h host -U user -d dbname -c "SELECT 1"

# Проверить наличие данных
PGPASSWORD=pass psql -h host -U user -d dbname -c "SELECT COUNT(*) FROM signal_performance WHERE ts_signal >= now() - interval '1 day'"
```
