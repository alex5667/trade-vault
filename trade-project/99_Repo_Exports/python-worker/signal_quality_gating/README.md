# Сервисы формирования ok_rate и soft_rate

## Обзор

Этот каталог содержит все файлы, участвующие в формировании метрик `ok_rate` и `soft_rate` для Order Flow Gate.

## Архитектура потока данных

```
scanner-crypto-orderflow (strategy.py)
    ↓
OFConfirmEngine (core/of_confirm_engine.py)
    ↓
MLConfirmGate (services/ml_confirm_gate.py) [опционально]
    ↓
Redis Stream: metrics:of_gate
    ↓
scanner-of-gate-sre (tools/of_gate_sre_monitor.py)
    ↓
ok_rate, soft_rate (вычисляются)
    ↓
scanner-notify-worker (telegram-worker/notify_worker.py)
    ↓
Telegram отчет
```

## Компоненты

### 1. scanner-crypto-orderflow (сервис стратегии)

**Файл:** `services/orderflow_strategy.py` (оригинал: `python-worker/services/orderflow/strategy.py`)

**Роль:** Основной источник данных. Выполняет логику подтверждения сигналов через `OFConfirmEngine` и записывает результаты (`ok` и `ok_soft`) в Redis Stream `metrics:of_gate`.

**Ключевые части:**
- Строки 1795-1840: Запись метрик в `metrics:of_gate`
- Строки 332-1900: Метод `process_tick()` - обработка тиков и вызов `OFConfirmEngine`
- Строки 1802-1803: Запись `ok` и `ok_soft` в метрики

**Зависимости:** 
- Множество модулей из `core/`, `services/`, `common/`
- Основные: `OFConfirmEngine`, `MLConfirmGate`, `AsyncSignalPublisher`

**Примечание:** Файл очень большой (4479 строк) и имеет множество зависимостей. Для работы с ok_rate/soft_rate критичны только строки 1795-1840 (запись метрик).

### 2. OFConfirmEngine (ядро логики)

**Файл:** `core/of_confirm_engine.py`

**Роль:** Ядро логики в составе сервиса стратегии. Рассчитывает количество подтверждений ("ног") и определяет статус сигнала.

**Ключевые методы:**
- `build()`: Основной метод, вычисляет `ok`, `have`, `need`, `score`, `ok_soft`
- `_resolve_now_ts()`: Определение временной метки
- `_load_meta_model()`: Загрузка мета-модели (опционально)

**Зависимости:**
- `core/book_evidence.py` - вычисление OBI/Iceberg/OFI флагов
- `core/of_evidence.py` - вычисление sweep/reclaim/absorption
- `core/strong_of_gate.py` - логика Strong Gate (reversal/continuation)
- `core/absorption_level_score.py` - оценка поглощения на уровне
- `core/of_confirm_contract.py` - контракты данных
- `core/cfg_merge.py` - слияние конфигураций
- `core/strong_need_policy.py` - политика Strong Need
- `core/fp_edge_evidence.py` - FP edge evidence
- `core/scenario_v4.py` - классификация сценариев v4
- `core/compat_utils.py` - утилиты совместимости
- `services/cancellation_spike_gate.py` - детектор отмены спайков
- `services/ml_confirm_gate.py` - ML фильтр
- `common/metrics_stage.py` - метрики

**Ключевые выходные данные:**
- `ok`: 1/0 - прошел ли сигнал жесткие проверки (Enforce)
- `ok_soft`: 1/0 - прошел ли сигнал по "мягкому" критерию (не хватило 1 подтверждения, но качество выше порога)
- `have`: количество подтверждений ("ног")
- `need`: необходимое количество подтверждений
- `score`: качественная оценка (0..1)

### 3. MLConfirmGate (ML-фильтр)

**Файл:** `services/ml_confirm_gate.py`

**Роль:** ML-фильтр, который может наложить вето на сигнал, что влияет на итоговый `ok_rate`.

**Ключевые методы:**
- `check()`: Проверка сигнала через ML модель
- `_decide_util_mh()`: Решение на основе utility model (v10.4)
- `_load_cfg_and_model()`: Загрузка конфигурации и модели из Redis

**Режимы работы:**
- `OFF`: не влияет на решение
- `SHADOW`: только логирует, не блокирует
- `ENFORCE`: блокирует сигналы с низкой вероятностью

**Зависимости:**
- `services/ml_calibration.py` - калибровка вероятностей (PlattLogitCalibrator)
- `redis` - для загрузки конфигурации и модели
- `joblib` - для загрузки ML модели

**Влияние на ok_rate:**
- В режиме `ENFORCE` может установить `ok=0` даже если `OFConfirmEngine` вернул `ok=1`
- Это уменьшает `ok_rate`, но повышает качество сигналов

### 4. scanner-of-gate-sre (мониторинговый сервис)

**Файл:** `tools/of_gate_sre_monitor.py`

**Роль:** Мониторинговый сервис, который считывает данные из Redis, вычисляет средние значения за окно (например, 60 мин) и формирует итоговые метрики `ok_rate` и `soft_rate`.

**Ключевые функции:**
- `compute_stats()`: Вычисление статистики из окна данных
- `build_alerts()`: Построение алертов на основе порогов
- `_read_stream_window()`: Чтение данных из Redis Stream за временное окно

**Вычисляемые метрики:**
- `ok_rate`: Доля сигналов с `ok=1` (строка 152)
- `soft_rate`: Доля сигналов с `ok_soft=1` (строка 153)
- `lat_p50/p95/p99_us`: Перцентили латентности
- `ml_lat_p50/p95/p99_us`: Перцентили латентности ML
- `exec_p50/p90/p99`: Перцентили execution risk
- `scenario_dist`: Распределение по сценариям
- `scenario_l1`: L1 расстояние распределения сценариев (drift detection)

**Зависимости:**
- `common/redis_errors.py` - retry логика для Redis операций
- `redis` - для чтения из Stream

**Пороги алертов (по умолчанию):**
- `ok_min`: 0.10 (ok_rate должен быть >= 10%)
- `soft_max`: 0.70 (soft_rate должен быть <= 70%)
- `lat_p99_us_max`: 25000 (p99 латентность <= 25ms)
- `ml_lat_p99_us_max`: 25000 (p99 ML латентность <= 25ms)
- `exec_p90_max`: 0.90 (p90 execution risk <= 0.90)

### 5. Redis (metrics:of_gate)

**Stream:** `metrics:of_gate`

**Роль:** Шина данных, через которую передаются сырые метрики от стратегии к монитору.

**Формат записи (из strategy.py, строки 1795-1834):**
```python
{
    "type": "of_gate",
    "ts_ms": "...",
    "symbol": "...",
    "direction": "LONG|SHORT",
    "scenario": "...",
    "scenario_v4": "...",
    "ok": "1|0",              # ← используется для ok_rate
    "ok_soft": "1|0",         # ← используется для soft_rate
    "have": "...",
    "need": "...",
    "score": "...",
    "reason": "...",
    "gate_bits": "...",
    "exec_risk_bps": "...",
    "exec_risk_norm": "...",
    "latency_us": "...",
    "ml_mode": "...",
    "ml_allow": "...",
    "ml_p_edge": "...",
    "ml_p_min": "...",
    "ml_latency_us": "...",
    # ... другие поля
}
```

### 6. scanner-notify-worker (доставка отчетов)

**Файл:** `telegram-worker/notify_worker.py`

**Роль:** Отвечает за доставку сформированного SRE-отчета с метриками `ok_rate` и `soft_rate` в Telegram.

**Ключевые функции:**
- `handle_message()`: Обработка сообщений из `notify:telegram` stream
- Поддержка типа `"report"` для отправки SRE отчетов

**Зависимости:**
- `telegram-worker/notifier.py` - обертка над Telegram API
- `telegram-worker/improved_notifier.py` - улучшенный notifier с rate limiting

## Краткая суть метрик

### ok_rate
**Определение:** Доля сигналов, прошедших все жесткие проверки (Enforce).

**Формула:**
```
ok_rate = count(ok == 1) / total_count
```

**Где вычисляется:**
- `tools/of_gate_sre_monitor.py`, строка 152

**Что влияет:**
- `OFConfirmEngine.build()`: вычисляет `ok` на основе `have >= need` и `score >= score_min`
- `MLConfirmGate.check()`: может установить `ok=0` в режиме `ENFORCE`
- `CancellationSpikeGate`: может установить `ok=0` при обнаружении спайка отмен

**Типичные значения:**
- Норма: 0.10 - 0.30 (10-30% сигналов проходят)
- Критично низко: < 0.10 (алерт)

### soft_rate
**Определение:** Доля сигналов, которые прошли по "мягкому" критерию (например, не хватило 1 подтверждения, но качество выше порога), они обычно помечаются как `is_virtual=1`.

**Формула:**
```
soft_rate = count(ok_soft == 1) / total_count
```

**Где вычисляется:**
- `tools/of_gate_sre_monitor.py`, строка 153

**Где устанавливается:**
- `core/of_confirm_engine.py`, строки 986-1007: логика soft-fail
- Условия:
  - `ok == 0` (не прошел жесткие проверки)
  - `have == need - 1` (не хватило 1 подтверждения)
  - `score >= soft_score_min` (обычно 0.60)
  - `exec_risk_norm <= soft_exec_max` (обычно 0.65)

**Типичные значения:**
- Норма: 0.20 - 0.50 (20-50% сигналов проходят по мягкому критерию)
- Критично высоко: > 0.70 (алерт)

## Структура каталога

```
ok_rate_soft_rate_services/
├── README.md                          # Этот файл
├── core/                              # Ядро логики
│   ├── of_confirm_engine.py          # OFConfirmEngine (главный)
│   ├── book_evidence.py               # OBI/Iceberg/OFI evidence
│   ├── of_evidence.py                 # Sweep/Reclaim/Absorption
│   ├── strong_of_gate.py              # Strong Gate логика
│   ├── absorption_level_score.py     # Absorption на уровне
│   ├── of_confirm_contract.py         # Контракты данных
│   ├── cfg_merge.py                   # Слияние конфигураций
│   ├── strong_need_policy.py          # Политика Strong Need
│   ├── fp_edge_evidence.py            # FP edge evidence
│   ├── scenario_v4.py                 # Классификация сценариев
│   └── compat_utils.py                # Утилиты совместимости
├── services/                          # Сервисы
│   ├── ml_confirm_gate.py             # ML фильтр
│   ├── ml_calibration.py              # Калибровка ML
│   ├── cancellation_spike_gate.py      # Детектор отмены спайков
│   └── orderflow_strategy.py          # Стратегия (запись метрик)
├── tools/                             # Инструменты
│   └── of_gate_sre_monitor.py         # SRE монитор (вычисление ok_rate/soft_rate)
├── telegram-worker/                   # Telegram worker
│   └── notify_worker.py               # Доставка отчетов
└── common/                            # Общие утилиты
    ├── metrics_stage.py               # Метрики
    └── redis_errors.py                # Retry логика Redis
```

## Зависимости между файлами

### OFConfirmEngine → зависимости
```
of_confirm_engine.py
├── book_evidence.py (OBI/Iceberg/OFI)
├── of_evidence.py (Sweep/Reclaim/Absorption)
├── strong_of_gate.py (Reversal/Continuation логика)
├── absorption_level_score.py (Absorption на уровне)
├── of_confirm_contract.py (Контракты)
├── cfg_merge.py (Конфигурация)
├── strong_need_policy.py (Need политика)
├── fp_edge_evidence.py (FP edge)
├── scenario_v4.py (Сценарии v4)
├── compat_utils.py (Совместимость)
├── cancellation_spike_gate.py (Спайк детектор)
├── ml_confirm_gate.py (ML фильтр)
└── metrics_stage.py (Метрики)
```

### MLConfirmGate → зависимости
```
ml_confirm_gate.py
└── ml_calibration.py (Калибровка)
```

### of_gate_sre_monitor → зависимости
```
of_gate_sre_monitor.py
└── redis_errors.py (Retry)
```

### strategy.py → зависимости
```
orderflow_strategy.py
├── of_confirm_engine.py (главный)
├── ml_confirm_gate.py (ML фильтр)
├── async_signal_publisher.py (публикация)
└── ... (множество других, см. импорты в файле)
```

## Как использовать

### 1. Запуск стратегии (запись метрик)
```bash
# В docker-compose или напрямую
python -m services.orderflow.strategy
# Метрики записываются в metrics:of_gate
```

### 2. Запуск SRE монитора (вычисление ok_rate/soft_rate)
```bash
python -m tools.of_gate_sre_monitor \
    --window-min 60 \
    --min-n 200 \
    --ok-min 0.10 \
    --soft-max 0.70
```

### 3. Просмотр метрик в Redis
```bash
# Чтение последних записей
redis-cli XREVRANGE metrics:of_gate + - COUNT 10

# Подсчет ok_rate вручную
redis-cli --eval count_ok_rate.lua metrics:of_gate
```

## Переменные окружения

### Стратегия (strategy.py)
- `OF_GATE_METRICS_STREAM`: Stream для метрик (по умолчанию: `metrics:of_gate`)
- `OF_GATE_METRICS_ENABLE`: Включить запись метрик (по умолчанию: `1`)
- `OF_GATE_METRICS_SAMPLE`: Процент выборки (по умолчанию: `0.10` = 10%)
- `OF_GATE_METRICS_MAXLEN`: Максимальная длина stream (по умолчанию: `200000`)

### SRE Монитор (of_gate_sre_monitor.py)
- `OF_GATE_METRICS_STREAM`: Stream для чтения (по умолчанию: `metrics:of_gate`)
- `NOTIFY_TELEGRAM_STREAM`: Stream для отправки отчетов (по умолчанию: `notify:telegram`)
- `SRE_OF_GATE_WINDOW_MIN`: Окно в минутах (по умолчанию: `60`)
- `SRE_OF_GATE_MIN_N`: Минимальное количество записей (по умолчанию: `200`)
- `SRE_OF_GATE_OK_MIN`: Минимальный ok_rate (по умолчанию: `0.10`)
- `SRE_OF_GATE_SOFT_MAX`: Максимальный soft_rate (по умолчанию: `0.70`)

### ML Gate (ml_confirm_gate.py)
- `ML_CONFIRM_MODE`: Режим работы (`OFF|SHADOW|ENFORCE`)
- `ML_CONFIRM_FAIL_POLICY`: Политика при ошибке (`OPEN|CLOSED`)
- `ML_CFG_CHAMPION_KEY`: Ключ конфигурации champion (по умолчанию: `cfg:ml_confirm:champion`)
- `ML_CFG_CHALLENGER_KEY`: Ключ конфигурации challenger (по умолчанию: `cfg:ml_confirm:challenger`)

## Тестирование

### Проверка записи метрик
```python
import redis
r = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)

# Читаем последние записи
records = r.xrevrange("metrics:of_gate", count=100)

# Подсчитываем ok_rate
ok_count = sum(1 for _, fields in records if fields.get("ok") == "1")
total = len(records)
ok_rate = ok_count / total if total > 0 else 0.0
print(f"ok_rate: {ok_rate:.3f}")
```

### Проверка SRE монитора
```bash
# Запуск с выводом в файл
python -m tools.of_gate_sre_monitor --out /tmp/sre_stats.json --always 1

# Проверка результата
cat /tmp/sre_stats.json | jq '.stats.ok_rate, .stats.soft_rate'
```

## Примечания

1. **strategy.py** - очень большой файл (4479 строк). Для работы с ok_rate/soft_rate критичны только строки 1795-1840 (запись метрик). Остальные зависимости могут быть не скопированы, но это не влияет на понимание потока данных.

2. **Зависимости strategy.py** - файл имеет множество зависимостей из `core/`, `services/`, `common/`. Полный список можно увидеть в начале файла (строки 1-150). Для работы с ok_rate/soft_rate большинство из них не критичны.

3. **Redis Stream** - данные в `metrics:of_gate` хранятся с ограничением длины (`maxlen=200000`), старые записи автоматически удаляются.

4. **Латентность** - SRE монитор также отслеживает латентность вычислений (`latency_us`, `ml_latency_us`) для выявления проблем производительности.

5. **Сценарии** - метрики разбиваются по сценариям (`scenario_v4`), что позволяет анализировать ok_rate/soft_rate отдельно для разных типов сигналов (reversal, continuation, range_meanrev, и т.д.).


