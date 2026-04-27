# Настройка и Запуск Локальной Калибровки

Этот документ описывает процесс настройки и запуска системы локальной калибровки порогов сигналов.

## 📋 Предварительные Требования

1. **PostgreSQL база данных** с таблицей `signals`
2. **Python зависимости**: `psycopg2-binary`
3. **Примененная миграция**: `migrations/001_add_local_calibration.sql`

## 🔧 Настройка PG_DSN

### 1. Проверка текущего DSN
```bash
# В docker-compose.yml переменная PG_DSN имеет плейсхолдеры:
PG_DSN=postgresql://user:pass@postgres:5432/trade
```

### 2. Настройка правильного DSN
Замените плейсхолдеры на реальные credentials:

```bash
# Пример для локальной базы данных:
export PG_DSN="postgresql://myuser:mypass@localhost:5432/trading_db"

# Пример для Docker контейнера:
export PG_DSN="postgresql://user:password@postgres:5432/trade"

# Пример для production:
export PG_DSN="postgresql://app_user:secure_pass@db-host.internal:5432/prod_db"
```

### 3. Тестирование подключения
```bash
# Простая проверка:
python3 -c "import psycopg2; psycopg2.connect('$PG_DSN'); print('✅ OK')"

# Или через наш скрипт:
PG_DSN="postgresql://..." python3 scripts/check_calibration_results.py
```

## 🏗️ Применение Миграции

### 1. Подключитесь к базе данных
```bash
# Через psql:
psql -h localhost -U myuser -d trading_db

# Или через Docker:
docker exec -it postgres_container psql -U postgres -d trade
```

### 2. Примените миграцию
```bash
# Из корневой директории проекта:
psql -d your_database -f python-worker/migrations/001_add_local_calibration.sql

# Или через Docker:
docker exec -i postgres_container psql -U postgres -d trade < python-worker/migrations/001_add_local_calibration.sql
```

### 3. Проверьте результат
```sql
-- Проверьте создание таблицы:
\d signal_local_calibration

-- Проверьте заполнение session/regime:
SELECT session, regime, COUNT(*) FROM signals GROUP BY session, regime ORDER BY session, regime;
```

## 🚀 Запуск Калибровки

### Способ 1: Интерактивная настройка (рекомендуется)
```bash
cd /path/to/scanner_infra

# Запустите интерактивный скрипт настройки:
./python-worker/scripts/setup_and_run_calibration.sh
```

### Способ 2: Ручной запуск
```bash
cd /path/to/scanner_infra

# Настройте переменные окружения:
export PG_DSN="postgresql://user:pass@localhost:5432/trade"
export CALIB_LOOKBACK_DAYS=365
export CALIB_MIN_TRADES_CLUSTER=300
export CALIB_MIN_TRADES_BUCKET=30
export CALIB_MIN_MEAN_PNL_R=0.0

# Запустите калибровку:
python3 python-worker/scripts/run_local_calibration.py
```

### Способ 3: Через Docker
```bash
# Если сервисы уже запущены:
docker exec -it scanner_multi-symbol-orderflow_1 bash

# В контейнере:
cd /app
export PG_DSN="postgresql://user:pass@postgres:5432/trade"
python3 scripts/run_local_calibration.py
```

## 📊 Проверка Результатов

### 1. Базовая проверка
```bash
# Запустите скрипт проверки:
python3 python-worker/scripts/check_calibration_results.py

# Или через Docker:
docker exec scanner_multi-symbol-orderflow_1 python3 scripts/check_calibration_results.py
```

### 2. Ручная проверка в базе данных
```sql
-- Общее количество записей:
SELECT COUNT(*) FROM signal_local_calibration;

-- Статистика по символам:
SELECT symbol, COUNT(*) as entries, AVG(count_samples) as avg_samples
FROM signal_local_calibration
GROUP BY symbol ORDER BY symbol;

-- Статистика по метрикам:
SELECT metric, COUNT(*) as count, AVG(chosen_threshold) as avg_threshold
FROM signal_local_calibration
GROUP BY metric ORDER BY metric;

-- Примеры записей:
SELECT symbol, session, regime, metric, q90, q95, q98, chosen_threshold, count_samples
FROM signal_local_calibration
ORDER BY symbol, session, regime, metric
LIMIT 20;
```

### 3. Ожидаемые результаты
```
📈 Total calibration entries: 150
📊 Statistics by symbol:
  BTCUSDT: 45 entries (500 avg samples)
  ETHUSDT: 38 entries (450 avg samples)
  XAUUSD: 67 entries (320 avg samples)
```

## 🔄 Регулярное Обновление

### Настройка Cron
```bash
# Ежедневное обновление в 2:00 ночи:
crontab -e

# Добавьте строку:
0 2 * * * cd /path/to/scanner_infra && PG_DSN="postgresql://..." python3 python-worker/scripts/run_local_calibration.py >> /var/log/calibration.log 2>&1
```

### Мониторинг
```bash
# Проверяйте логи калибровки:
tail -f /var/log/calibration.log

# Регулярно проверяйте качество:
python3 python-worker/scripts/check_calibration_results.py
```

## ⚙️ Параметры Калибровки

| Параметр | Значение по умолчанию | Описание |
|----------|----------------------|----------|
| `CALIB_LOOKBACK_DAYS` | 365 | Период анализа истории (дни) |
| `CALIB_MIN_TRADES_CLUSTER` | 300 | Минимальное количество сделок на кластер |
| `CALIB_MIN_TRADES_BUCKET` | 30 | Минимальное количество сделок на бакет |
| `CALIB_MIN_MEAN_PNL_R` | 0.0 | Минимальный средний PnL на бакет |

## 🔍 Диагностика Проблем

### Ошибка подключения к БД
```
❌ Database error: ...
```
**Решение**: Проверьте PG_DSN и доступность базы данных.

### Таблица не существует
```
❌ Table 'signal_local_calibration' does not exist!
```
**Решение**: Примените миграцию `001_add_local_calibration.sql`.

### Нет данных для калибровки
```
Loaded 0 signals
```
**Решение**:
- Проверьте наличие данных в таблице `signals`
- Увеличьте `CALIB_LOOKBACK_DAYS`
- Проверьте фильтры по pnl_r

### Низкое качество калибровки
```
Low sample count (<100): 25 entries (16.7%)
```
**Решение**:
- Уменьшите `CALIB_MIN_TRADES_CLUSTER`
- Проверьте распределение данных по кластерам
- Рассмотрите объединение кластеров (fallback логика)

## 📈 Мониторинг Качества

### Ключевые метрики:
- **Количество записей**: Должно быть > 100 для хорошего покрытия
- **Средний размер выборки**: > 300 сделок на кластер
- **NULL значения**: < 5% для порогов
- **Последнее обновление**: Не старше 24 часов

### Алерты:
```bash
# Настройте мониторинг количества записей:
count=$(PG_DSN="..." python3 -c "
import psycopg2
conn = psycopg2.connect('$PG_DSN')
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM signal_local_calibration')
print(cur.fetchone()[0])
")
if [ "$count" -lt 50 ]; then
    echo "⚠️ Low calibration count: $count"
fi
```

## 🎯 Следующие Шаги

1. **Запустите начальную калибровку** на исторических данных
2. **Проверьте качество** калибровки
3. **Настройте регулярное обновление** через cron
4. **Мониторьте эффективность** локальных порогов vs глобальных
5. **Отрегулируйте параметры** при необходимости

Система локальной калибровки готова к использованию! 🚀
