# Record & Replay System

## Обзор

Система record & replay позволяет:
- Записывать входные данные (ticks/ctx) в JSONL файлы
- Детерминированно воспроизводить обработку
- Сравнивать результаты с golden files
- Проводить интеграционное тестирование без зависимостей от Redis

## Архитектура

### Основные компоненты

#### 1. `replay/jsonl.py` - JSONL утилиты
```python
@dataclass(slots=True)
class JsonlWriter:
    path: str
    flush: bool = True

def iter_jsonl(path: str) -> Iterator[dict[str, Any]]
def _safe_json(obj: Any) -> Any  # JSON-сериализация без ошибок
```

#### 2. `replay/report.py` - Отчеты и нормализация
```python
def normalize_signal_payload(p: Dict[str, Any]) -> Dict[str, Any]
def build_report(signals: List[Dict[str, Any]]) -> ReplayReport
```

#### 3. `replay/replay_runner.py` - Запуск replay
```python
class OutboxCollector:  # Подмена outbox для тестов
class HandlerAdapter:   # Адаптер к реальному handler
def replay_jsonl(...) -> OutboxCollector
```

## Использование

### Уровень 1: Запись и replay ticks

#### Запись Redis stream:
```bash
python -m tools.record_redis_stream \
  --redis redis://localhost:6379/0 \
  --stream binance:ticks:BTCUSDT \
  --minutes 3 \
  --out /tmp/replay_ticks.jsonl
```

#### Создание адаптера:
```python
# python-worker/handlers/replay_factory.py
from replay.replay_runner import HandlerAdapter, OutboxCollector

def create_adapter() -> HandlerAdapter:
    outbox = OutboxCollector()
    # Создать ваш реальный handler с подменой outbox
    handler = CryptoOrderFlowHandler(...)
    handler._emitter._outbox_pub = outbox  # Подмена
    
    return HandlerAdapter(
        process_tick=lambda payload: handler._process_tick(payload),
        process_ctx=None,  # Для tick replay
        _replay_outbox=outbox
    )
```

#### Запуск replay:
```bash
python -m tools.replay_local \
  --in /tmp/replay_ticks.jsonl \
  --type tick \
  --factory python_worker.handlers.replay_factory:create_adapter
```

### Уровень 2: "Жесткий" replay ctx

#### Запись ctx на bucket boundary:
```python
# В BaseOrderFlowHandler._process_bucket_boundary()
if os.getenv("RECORD_CTX_REPLAY"):
    ctx_dict = self._serialize_current_ctx()
    JsonlWriter("/tmp/ctx_record.jsonl").write({
        "type": "ctx",
        "ts_ms": current_ts,
        "payload": ctx_dict
    })
```

#### Replay с ctx:
```bash
python -m tools.replay_local \
  --in /tmp/ctx_record.jsonl \
  --type ctx \
  --factory python_worker.handlers.replay_factory:create_adapter_with_ctx_support
```

## Формат JSONL

### Tick запись:
```json
{
  "type": "tick",
  "ts_ms": 1700000000000,
  "redis_stream": "binance:ticks:BTCUSDT",
  "redis_id": "1700000000000-0",
  "payload": {
    "ts": 1700000000000,
    "bid": 100.0,
    "ask": 101.0,
    "last": 100.5,
    "volume": 2.0
  }
}
```

### Ctx запись:
```json
{
  "type": "ctx",
  "ts_ms": 1700000000000,
  "payload": {
    "ts": 1700000000000,
    "symbol": "BTCUSDT",
    "price": 43000.0,
    "z_delta": 3.5,
    "obi": 0.5,
    // ... полный ctx
  }
}
```

### Signal выход:
```json
{
  "kind": "breakout",
  "side": "buy", 
  "symbol": "BTCUSDT",
  "level_price": 42950.0,
  "raw_score": 1.2,
  "final_score": 0.84,
  "confidence": 42.0,
  "reason_code": "OK"
}
```

## Golden Files

### Структура golden файла:
```json
{
  "counts_by_kind": {
    "breakout": 3,
    "obi_spike": 2
  },
  "score_p50_by_kind": {
    "breakout": 0.84,
    "obi_spike": 0.69
  },
  "score_p95_by_kind": {
    "breakout": 1.09,
    "obi_spike": 0.72
  },
  "samples": [
    {
      "index": 0,
      "payload_norm": {
        "kind": "breakout",
        "side": "buy",
        "symbol": "BTCUSDT",
        // ... нормализованный payload
      }
    }
  ]
}
```

### Нормализация payload:
```python
def normalize_signal_payload(p: Dict[str, Any]) -> Dict[str, Any]:
    # Убирает: signal_id, ts (нестабильные)
    # Оставляет: kind, side, symbol, level_price, scores, reason_code
    # Добавляет: qf (quality flags)
```

## Тестирование

### Интеграционный тест:
```python
def test_replay_ctx_matches_golden_report_and_samples():
    adapter = create_demo_adapter()  # или реальный
    
    outbox = replay_jsonl(
        adapter=adapter,
        path="fixtures/replay/ctx_sample.jsonl", 
        type_filter="ctx"
    )
    
    report = build_report(outbox.items)
    # Сравнение с golden
```

### Запуск тестов:
```bash
pytest tests/integration/test_record_replay.py -v
```

## Преимущества

### Детерминизм:
- Тесты не зависят от Redis/external services
- Повторяемые результаты
- Быстрое выполнение

### Надежность:
- Fail-open сериализация
- Graceful handling corrupted records
- Thread-safe запись

### Гибкость:
- Поддержка разных типов событий (tick/ctx/signal)
- Configurable адаптеры
- Golden files для регрессионного тестирования

## Подключение к реальному проекту

### 1. Создать replay_factory.py:
```python
# handlers/replay_factory.py
def create_adapter() -> HandlerAdapter:
    # Создать handler с подменой outbox
    # Вернуть HandlerAdapter с _replay_outbox
```

### 2. Добавить запись ctx (опционально):
```python
# В BaseOrderFlowHandler
if RECORD_CTX:
    self._record_ctx_for_replay(ctx_dict)
```

### 3. Создать golden files:
- Записать эталонную сессию
- Сгенерировать golden report/samples
- Зафиксировать в git

### 4. Интеграционные тесты:
```python
@pytest.mark.manual  # Запускать отдельно
def test_real_handler_replay():
    adapter = create_real_adapter()
    # Replay и сравнение с golden
```

## Troubleshooting

### OutboxCollector не найден:
```
RuntimeError: OutboxCollector not found: adapter must expose _replay_outbox
```
**Решение**: Убедиться, что адаптер имеет `_replay_outbox = OutboxCollector()`

### Несоответствие golden:
- Проверить порядок сигналов
- Обновить percentiles после изменений логики
- Проверить нормализацию payload

### Проблемы с сериализацией:
- `_safe_json()` преобразует сложные объекты в str
- Для custom типов добавить `_safe_json` обработку

## Производительность

- JSONL запись: потокобезопасная с threading.Lock
- Чтение: итераторное, не грузит весь файл в память
- Replay: configurable `max_events` для быстрого тестирования
- Сериализация: `separators=(",", ":")` для компактности
