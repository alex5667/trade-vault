# Orderflow Services Reference

Эта папка содержит копии всех сервисов Orderflow и их зависимостей для формирования фичей и обработки микроструктурных метрик.

## Структура

```
orderflow_services/
├── services/
│   ├── crypto_orderflow_service.py    # Основной сервис формирования фичей (OBI, CVD, Delta-Z, Icebergs)
│   ├── ohlc_aggregator.py             # Real-time OHLC агрегатор на основе тиков
│   ├── crypto_htf_aggregator.py        # HTF контекст (старшие таймфреймы)
│   ├── trade_monitor.py                # Trade Monitor с OFConfirmEngine (Strong Gate)
│   └── orderflow/                      # Модули Orderflow сервиса
│       ├── configuration.py
│       ├── runtime.py
│       ├── strategy.py
│       ├── metrics.py
│       ├── utils.py
│       ├── calibration_repo.py
│       ├── calibration_service.py
│       └── ...
├── core/                               # Core модули (детекторы, метрики, логика)
│   ├── of_confirm_engine.py           # OFConfirmEngine - вычисление Strong Gate
│   ├── strong_of_gate.py              # Strong Gate логика (Reversal/Continuation)
│   ├── crypto_orderflow_detectors.py   # Детекторы OBI, Iceberg, Delta-Z
│   ├── book_evidence.py                # Вычисление OBI/Iceberg флагов
│   ├── of_evidence.py                 # Sweep/Reclaim/Absorption
│   ├── pressure_tracker.py
│   ├── burst_gate.py
│   └── ... (все зависимости)
├── geometry/
│   └── htf_zones_publisher.py         # Публикация зон поддержки/сопротивления
├── handlers/
│   └── crypto_orderflow/
│       └── utils/
│           └── log_sampler.py
└── common/                             # Общие утилиты
    ├── decision_trace.py
    ├── zone_store.py
    └── ...
```

## Основные сервисы

### 1. scanner-crypto-orderflow-service
**Файл:** `services/crypto_orderflow_service.py`

Ключевой сервис формирования фичей. Обрабатывает сырые тики и вычисляет микроструктурные метрики:

- **OBI (Order Book Imbalance)** - дисбаланс стакана
- **CVD (Cumulative Volume Delta)** - кумулятивная дельта объема
- **Delta-Z** - всплески дельты (z-score)
- **Icebergs** - детекция айсбергов в стакане

**Основные компоненты:**
- `services/orderflow/strategy.py` - стратегия обработки тиков
- `services/orderflow/runtime.py` - runtime для символов
- `services/orderflow/configuration.py` - конфигурация

### 2. scanner-ohlc-aggregator
**Файл:** `services/ohlc_aggregator.py`

Вычисляет real-time OHLC на основе тиков. Агрегирует дневные H/L/C для расчета Pivot уровней.

### 3. scanner-crypto-htf-aggregator
**Файл:** `services/crypto_htf_aggregator.py`

Формирует контекст старших таймфреймов (HTF):
- Previous Day High/Low/Middle
- Weekly High/Low
- Session opens (Asia/Europe/US)
- Order Block zones
- Fair Value Gap zones

### 4. scanner-htf-zones-publisher
**Файл:** `geometry/htf_zones_publisher.py`

Публикует зоны поддержки/сопротивления в Redis:
- `zones:htf:v1:{symbol}` - JSON с зонами
- Использует `HTFLevelsService` для вычисления уровней

### 5. Trade Monitor (scanner-python-worker)
**Файл:** `services/trade_monitor.py`

Внутри работает **OFConfirmEngine**, который вычисляет эвристический "Strong Gate".

**OFConfirmEngine** (`core/of_confirm_engine.py`):
- Проверяет выполнение условий (legs) для паттернов Reversal/Continuation
- Использует `strong_of_gate.py` для оценки:
  - **Reversal**: требует 2 из 3:
    - A) deltaSpikeZ + weakProgress
    - B) sweep + reclaim
    - C) obi_stable OR ofi_leg OR iceberg_strict OR fp_edge_absorb
  - **Continuation**: требует 2 из 3:
    - A) hidden_ctx_recent AND direction==trend_dir
    - B) obi_stable OR ofi_leg OR iceberg_strict OR fp_edge_absorb
    - C) cont_ctx_recent

## Зависимости

Все импортируемые модули скопированы в соответствующие папки:

- **core/** - детекторы, метрики, логика оценки
- **common/** - общие утилиты (decision_trace, zone_store, metrics_stage)
- **services/** - вспомогательные сервисы (signal_preprocess, persistence_manager, async_signal_publisher)
- **handlers/** - утилиты обработчиков (log_sampler)

## Использование

Все файлы сохранены с сохранением структуры импортов. Для использования в другом проекте может потребоваться:

1. Настроить пути импортов (sys.path или PYTHONPATH)
2. Убедиться, что все зависимости установлены
3. Настроить Redis подключения
4. Настроить переменные окружения

## Примечания

- Все файлы скопированы из `python-worker/` директории
- Структура импортов сохранена
- Включены все необходимые зависимости для работы сервисов

