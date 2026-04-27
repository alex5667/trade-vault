# Интеграция Локальной Калибровки в Makefile

## ✅ Что было сделано:

### 1. **Автоматический запуск при `make up`**
- Добавлен вызов `./python-worker/scripts/setup_and_run_calibration.sh` в конец цели `up`
- Добавлена задержка 10 секунд для полного запуска сервисов перед калибровкой

### 2. **Новые команды Makefile**
- `make calibration-run` - Запуск настройки калибровки
- `make calibration-check` - Проверка результатов калибровки
- `make calibration-help` - Справка по системе калибровки

### 3. **Обновление справки**
- Добавлен раздел "Local Calibration" в `make help`
- Информация о автоматическом запуске при `make up`

## 🚀 Использование:

### Автоматический запуск:
```bash
make up  # Запустит все сервисы + автоматически настроит калибровку
```

### Ручной запуск:
```bash
# Только калибровка
make calibration-run

# Проверка результатов
make calibration-check

# Справка
make calibration-help
```

## ⚙️ Настройка:

### PG_DSN
Убедитесь, что переменная `PG_DSN` настроена правильно в `docker-compose.yml` или через environment:

```bash
export PG_DSN="postgresql://username:password@host:port/database"
```

### Параметры калибровки
Настроены в `docker-compose.yml`:
- `CALIB_LOOKBACK_DAYS=365`
- `CALIB_MIN_TRADES_CLUSTER=300`
- `CALIB_MIN_TRADES_BUCKET=30`
- `CALIB_MIN_MEAN_PNL_R=0.0`

## 📊 Результат:

Теперь при каждом запуске системы через `make up` будет автоматически выполняться:
1. Запуск всех сервисов
2. Ожидание 10 секунд
3. Автоматическая настройка локальной калибровки
4. Проверка подключения к базе данных
5. Запуск калибровки с текущими параметрами

## 🔧 Диагностика:

Если что-то пойдет не так, проверьте:
```bash
make calibration-check  # Проверка результатов
make logs | grep calib  # Логи калибровки
```

## 📚 Документация:
- `python-worker/CALIBRATION_SETUP.md` - Полная настройка
- `python-worker/scripts/` - Скрипты калибровки
