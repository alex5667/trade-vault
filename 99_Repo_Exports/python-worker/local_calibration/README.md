# Local Calibration System

Система локальной калибровки порогов сигналов на основе кластеров (symbol, session, regime).

## Обзор

Система позволяет каждому кластеру (символ + сессия + режим рынка) иметь собственные пороги фильтрации, адаптированные к исторической эффективности сигналов в этом конкретном контексте.

## Архитектура

### 1. SQL Схема

#### Добавление полей в таблицу signals:
```sql
ALTER TABLE signals
    ADD COLUMN IF NOT EXISTS session TEXT,
    ADD COLUMN IF NOT EXISTS regime TEXT;
```

#### Таблица калибровки:
```sql
CREATE TABLE signal_local_calibration (
    symbol          TEXT NOT NULL,
    session         TEXT NOT NULL,
    regime          TEXT NOT NULL,
    metric          TEXT NOT NULL,
    q90             DOUBLE PRECISION,
    q95             DOUBLE PRECISION,
    q98             DOUBLE PRECISION,
    chosen_threshold DOUBLE PRECISION,
    count_samples   BIGINT NOT NULL,
    cdf_points      JSONB NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, session, regime, metric)
);
```

### 2. Оффлайн Калибратор

Скрипт `calibrate_local_thresholds.py` рассчитывает пороги на основе исторических данных.

#### Запуск:
```bash
# Установка переменных окружения
export PG_DSN="postgresql://user:pass@localhost:5432/trade"
export CALIB_LOOKBACK_DAYS=365
export CALIB_MIN_TRADES_CLUSTER=300

# Запуск калибровки
python -m local_calibration.calibrate_local_thresholds
```

#### Или через wrapper скрипт:
```bash
python scripts/run_local_calibration.py
```

### 3. Онлайн Store

Класс `LocalCalibrationStore` загружает калибровку в память для быстрого доступа.

#### Использование:
```python
from local_calibration.store import LocalCalibrationStore

store = LocalCalibrationStore()
store.load_from_db("postgresql://...")

# Получение конфигурации для метрики
cfg = store.get_metric_cfg("BTCUSDT", "us", "trend", "delta_spike_z")
if cfg:
    local_q = eval_local_quantile(cfg.cdf_points, signal_value)
    threshold = cfg.threshold
```

### 4. Интеграция в Handler

SignalContext расширен полями для локальной калибровки:

```python
@dataclass
class SignalContext:
    # ... существующие поля ...

    # Локальная калибровка
    session: str = ""
    regime: str = ""
    delta_spike_z_local_q: float = float("nan")
    delta_spike_z_local_thr: float = float("nan")
    # ... другие метрики ...
```

## Настройка

### Переменные окружения:

#### База данных:
```bash
PG_DSN=postgresql://user:pass@postgres:5432/trade
```

#### Параметры калибровки:
```bash
CALIB_LOOKBACK_DAYS=365        # Дней для анализа истории
CALIB_MIN_TRADES_CLUSTER=300   # Мин. сделок на кластер
CALIB_MIN_TRADES_BUCKET=30     # Мин. сделок на бакет
CALIB_MIN_MEAN_PNL_R=0.0       # Мин. средний PnL на бакет
```

#### Поддерживаемые метрики:
- `delta_spike_z` - основной Z-score сигнала
- `obi` - Order Book Imbalance
- `weak_progress` - Weak Progress
- `atr_quantile` - ATR quantile

## Алгоритм работы

### 1. Подготовка данных:
- Лейблинг сессий (Asia/Europe/US) по UTC времени
- Лейблинг режимов (trend/range/mixed) по техническим индикаторам
- Группировка сигналов по кластерам (symbol, session, regime)

### 2. Расчет калибровки:
- Вычисление квантилей (90%, 95%, 98%) для каждой метрики
- Построение эмпирической CDF (101 точка)
- Разбиение на бакеты по значению метрики
- Выбор оптимального порога по максимальному PnL в бакетах

### 3. Онлайн применение:
- Загрузка калибровки при старте сервиса
- Вычисление локального квантиля для каждого сигнала
- Фильтрация сигналов по локальным порогам
- Включение локальных метрик в payload

## Fallback стратегия

При отсутствии калибровки для конкретного кластера используется иерархический fallback:

1. `(symbol, session, regime)` - точная калибровка
2. `(symbol, session, "mixed")` - без учета режима
3. `(symbol, "mixed", "mixed")` - только по символу
4. Глобальные пороги - если ничего не найдено

## Мониторинг

### Метрики для отслеживания:
- Количество активных кластеров
- Средний размер выборки на кластер
- Эффективность фильтрации по локальным vs глобальным порогам
- Обновление timestamp калибровки

### Логи:
```
Loaded 150 calibration entries from database
Calibrated BTCUSDT asia trend delta_spike_z: 450 samples
```

## Запуск и обслуживание

### Инициализация:
```bash
# 1. Применить SQL миграции
psql -d trade -f migrations/001_add_local_calibration.sql

# 2. Запустить начальную калибровку
python scripts/run_local_calibration.py
```

### Регулярное обновление:
```bash
# Добавить в crontab для ежедневного обновления в 2:00
0 2 * * * /path/to/python scripts/run_local_calibration.py
```

### Мониторинг:
```bash
# Проверить статус калибровки
python -c "from local_calibration.store import LocalCalibrationStore; s = LocalCalibrationStore(); s.load_from_db('postgresql://...'); print(f'Loaded {len(s._cfg)} entries')"
```

## Производительность

### Оптимизации:
- Загрузка калибровки при старте сервиса (не на каждый сигнал)
- In-memory хранение для быстрого доступа
- Линейная интерполяция CDF (O(log N) для поиска)
- Минимальный overhead на сигнал (~0.1ms)

### Масштабируемость:
- Поддержка тысяч кластеров
- Эффективное хранение в PostgreSQL
- Возможность шардирования по символам

## Безопасность и надежность

### Валидация:
- Проверка корректности CDF (монотонность)
- Защита от экстремальных значений
- Graceful degradation при ошибках

### Резервные стратегии:
- Продолжение работы с глобальными порогами при сбое
- Логирование ошибок калибровки
- Возможность ручного override порогов
