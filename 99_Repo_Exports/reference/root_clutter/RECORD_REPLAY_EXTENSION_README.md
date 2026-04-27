# Record & Replay System Extension

## Обзор

Расширение системы record & replay добавляет:
- Безопасную сериализацию контекста (`ctx_export.py`)
- Гибкий recorder с env-контролем (`recorder.py`) 
- Интеграцию в BaseOrderFlowHandler для автоматической записи
- Стабильные signal_id для golden тестов
- CLI инструменты для создания golden файлов

## Новые возможности

### 1. Безопасная сериализация контекста

```python
from replay.ctx_export import export_ctx

# Автоматически фильтрует whitelist поля, чистит NaN/Inf
ctx_dict = export_ctx(ctx)
```

**Безопасность:**
- Только whitelist поля (core features + dq flags)
- NaN/Inf -> None
- Большие объекты игнорируются
- Компактные списки/дикт'ы сохраняются

### 2. Гибкий ReplayRecorder

```python
# Автоматически инициализируется в BaseOrderFlowHandler через env
recorder = ReplayRecorder()  # env-driven
recorder.record_ctx(ctx)
recorder.record_signal(payload)
```

**Env переменные:**
```bash
REPLAY_RECORD=1                    # Включить запись
REPLAY_RECORD_PATH=/tmp/record.jsonl  # Файл для записи
REPLAY_RECORD_TYPES=ctx,signal,tick  # Что записывать
REPLAY_RECORD_FLUSH=1               # Flush после каждой записи
REPLAY_RECORD_SAMPLE_EVERY=1        # Записывать каждый N-й event
```

### 3. Стабильные signal_id для golden тестов

```python
# Включается опционально для regression/golden
export REPLAY_STABLE_SIGNAL_ID=1

# Генерирует детерминированный ID по ключевым полям
signal_id = hashlib.sha1(key.encode()).hexdigest()
```

**Ключ:** `symbol|kind|side|ts_bucket|level_price_rounded|venue|timeframe`

## Интеграция

### BaseOrderFlowHandler

```python
# Автоматическая инициализация recorder
self._replay_recorder = ReplayRecorder()  # если env включён

# Запись тика (опционально)
self._replay_record_tick(tick_payload)

# Запись ctx на bucket boundary (КЛЮЧЕВОЙ момент)
self._replay_record_ctx(ctx)  # ctx полностью сформирован

# Генерация сигналов с записью
```

### CryptoOrderFlowHandler

```python
# Стабильный signal_id (опционально)
if env.REPLAY_STABLE_SIGNAL_ID:
    payload["signal_id"] = self._stable_signal_id(payload)

# Запись исходящих сигналов
rr.record_signal(payload)
```

## CLI инструменты

### make_golden.py

```bash
# Создать golden файл из записанных сигналов
python -m tools.make_golden \
  --in /tmp/recorded_signals.jsonl \
  --out golden.json \
  --samples 3 \
  --sample_step 10
```

**Выход:** JSON с counts, percentiles, control samples

## Использование

### 1. Запись в проде/локально

```bash
export REPLAY_RECORD=1
export REPLAY_RECORD_PATH=/tmp/replay_ctx.jsonl
export REPLAY_RECORD_TYPES=ctx,signal
export REPLAY_RECORD_FLUSH=1
# Для стабильных ID в golden
export REPLAY_STABLE_SIGNAL_ID=1

# Запустить воркер
```

### 2. Создание golden файла

```bash
# Из записанных сигналов
python -m tools.make_golden --in /tmp/replay_ctx.jsonl --out golden.json

# Или через replay + ручная курация
python -m tools.replay_local --factory my_factory --report_out report.json
# Затем вручную отобрать samples в golden.json
```

### 3. Replay для тестирования

```bash
# Используя существующую систему
python -m tools.replay_local \
  --in /tmp/replay_ctx.jsonl \
  --type ctx \
  --factory handlers.replay_factory:create_adapter
```

### 4. Регрессионные тесты

```bash
# С реальным handler (опционально)
export REPLAY_FACTORY="handlers.replay_factory:create_adapter"
export REPLAY_INPUT="/tmp/replay_ctx.jsonl"
export REPLAY_GOLDEN="golden.json"
pytest -k record_replay_real_optional
```

## Безопасность и производительность

### Fail-open дизайн
- Все компоненты fail-open
- Нет влияния на основной flow
- Логгирование ошибок без прерывания

### Производительность
- Ленивая инициализация recorder
- Sampling для снижения IO
- Компактная сериализация
- Thread-safe запись

### Безопасность данных
- Не записывает чувствительные данные
- Фильтрация больших объектов
- Санитизация чисел

## Тестирование

### Unit тесты
```bash
pytest tests/unit/test_ctx_export.py  # Сериализация ctx
```

### Integration тесты
```bash
pytest tests/integration/test_record_replay.py  # Демо система
pytest -k record_replay_real_optional  # Реальный handler (manual)
```

## Структура файлов

```
replay/
├── ctx_export.py      # Сериализация контекста
├── recorder.py        # ReplayRecorder класс
└── ...                # Существующие модули

tools/
├── make_golden.py     # CLI для golden файлов
└── ...                # Существующие tools

tests/
├── unit/test_ctx_export.py
└── integration/test_record_replay_real_optional.py
```

## Преимущества расширения

### Надежность
- Полная сериализация ctx на bucket boundary
- Детерминированные signal_id
- Компактные, безопасные JSONL файлы

### Гибкость
- Env-driven конфигурация
- Sampling для производительности
- Выборочные типы записей

### Тестируемость
- Golden файлы с control samples
- Regression detection
- Manual/real integration tests

### Производительность
- Минимальный overhead
- Fail-open дизайн
- Оптимизированная сериализация

## Troubleshooting

### Recorder не инициализируется
```
# Проверить env переменные
echo $REPLAY_RECORD $REPLAY_RECORD_PATH
```

### Golden файлы не совпадают
```
# Включить стабильные ID
export REPLAY_STABLE_SIGNAL_ID=1

# Проверить порядок samples в golden.json
```

### Большой размер JSONL
```
# Включить sampling
export REPLAY_RECORD_SAMPLE_EVERY=10

# Ограничить типы записей
export REPLAY_RECORD_TYPES=ctx
```

## Следующие шаги

1. **Подключить к реальному проекту**
   - Обновить `handlers/replay_factory.py`
   - Записать эталонные сессии
   - Создать golden файлы

2. **Настроить CI**
   - Добавить env переменные
   - Включить опциональные тесты

3. **Мониторинг**
   - Следить за размером JSONL файлов
   - Мониторить performance impact

**Расширение готово к продакшену и обеспечивает продвинутую интеграционную тестируемость!** 🚀
