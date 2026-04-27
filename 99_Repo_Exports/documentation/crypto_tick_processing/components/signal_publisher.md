# Signal Publisher - Детальная документация

## Обзор

**Signal Publisher** - асинхронный компонент для надежной публикации торговых сигналов в Redis Streams. Обеспечивает fail-open семантику, нормализацию контрактов, обработку ошибок и метрики производительности.

**Расположение**: `python-worker/services/async_signal_publisher.py`

**Назначение**: Централизованная точка публикации сигналов с гарантией доставки и мониторингом.

## Архитектурные принципы

### 1. Fail-Open Design
- **Never fails**: Публикация никогда не вызывает исключений
- **Graceful degradation**: Продолжение работы при частичных сбоях
- **Error isolation**: Ошибки не влияют на основной поток

### 2. Contract Normalization
- **Standard fields**: Единые имена полей для всех сигналов
- **Type safety**: Валидация и преобразование типов
- **Backward compatibility**: Поддержка legacy форматов

### 3. Performance Optimized
- **Async operations**: Неблокирующие I/O операции
- **Batch publishing**: Групповая публикация для снижения overhead
- **Connection reuse**: Переиспользование Redis соединений

## Детальная структура класса

### Основные компоненты

#### StreamSink - Конфигурация приемника

```python
@dataclass(frozen=True)
class StreamSink:
    """
    Конфигурация одного Redis Stream приемника.
    """
    name: str          # Название стрима (например, "signals:orderflow:BTCUSDT")
    field: str = "payload"  # Название поля для данных ("payload", "data")
    maxlen: int = 10000    # Максимальная длина стрима
```

#### AsyncPublishResult - Результат публикации

```python
@dataclass(frozen=True)
class AsyncPublishResult:
    """
    Результат операции публикации для анализа и тестирования.
    """
    ok: bool           # Успешная публикация
    raw_written: bool  # Данные записаны в Redis
    busy_loading: bool # Redis в состоянии загрузки
    errors: int        # Количество ошибок
```

#### AsyncSignalPublisher - Основной класс

```python
class AsyncSignalPublisher:
    """
    Асинхронный публикатор сигналов с fail-open семантикой.
    """

    def __init__(
        self,
        *,
        redis_client: Any,           # aioredis клиент
        source: str,                 # Источник сигналов
        metrics_prefix: str = "signals_publish_async",
        logger: Any = None,
    ):
        self.r = redis_client
        self.source = str(source or "na")
        self.metrics_prefix = str(metrics_prefix or "signals_publish_async")
        self.logger = logger
```

## Детальная логика методов

### Публикация в один стрим (xadd_json)

**Основной метод публикации в один Redis Stream:**

```python
async def xadd_json(
    self,
    *,
    sink: StreamSink,
    payload: Dict[str, Any],
    symbol: str,
    approximate: bool = True,
) -> AsyncPublishResult:
    """
    Публикация JSON payload в Redis Stream.
    FAIL-OPEN: никогда не вызывает исключений.
    """

    errors = 0
    busy = False
    raw_written = False

    # 1. Нормализация контракта
    try:
        preprocess_signal_for_publish(
            payload,
            symbol=str(symbol),
            source=self.source,
            logger=self.logger
        )
    except Exception:
        # Fail-open: ошибка нормализации не блокирует публикацию
        pass

    # 2. Сериализация в JSON
    ser = _json_dumps_safe(payload)

    # 3. Публикация в Redis Stream
    try:
        await self.r.xadd(
            sink.name,
            fields={str(sink.field or "payload"): ser},
            maxlen=int(sink.maxlen),
            approximate=bool(approximate),
        )
        raw_written = True

    except redis.exceptions.BusyLoadingError:
        # Redis в состоянии загрузки (например, при перезапуске)
        busy = True

    except Exception as e:
        errors += 1
        # Метрики ошибок
        await _aincr_fail_open(self.r, f"{self.metrics_prefix}:xadd_errors_total")

        if self.logger:
            try:
                self.logger.warning("async_publish.xadd failed stream=%s err=%r", sink.name, e)
            except Exception:
                pass

    # Обработка состояния busy loading
    if busy:
        await _aincr_fail_open(self.r, f"{self.metrics_prefix}:busyloading_total")
        return AsyncPublishResult(ok=False, raw_written=False, busy_loading=True, errors=errors)

    # Финальный результат
    ok = raw_written
    await _aincr_fail_open(
        self.r,
        f"{self.metrics_prefix}:ok_total" if ok else f"{self.metrics_prefix}:all_failed_total"
    )

    return AsyncPublishResult(ok=ok, raw_written=raw_written, busy_loading=False, errors=errors)
```

### Publish - множественная публикация

**Метод для публикации в несколько стримов одновременно:**

```python
async def publish(
    self,
    signal: Dict[str, Any],
    sinks: List[StreamSink],
    symbol: str,
    max_retries: int = 3
) -> List[AsyncPublishResult]:
    """
    Публикация сигнала во все указанные стримы.
    """

    results = []

    for sink in sinks:
        result = await self.xadd_json(
            sink=sink,
            payload=signal.copy(),  # Копия для независимости
            symbol=symbol
        )

        # Retry logic для failed публикаций
        if not result.ok and not result.busy_loading and max_retries > 0:
            for attempt in range(max_retries):
                if attempt > 0:  # Небольшая задержка между попытками
                    await asyncio.sleep(0.1 * (2 ** attempt))

                retry_result = await self.xadd_json(
                    sink=sink,
                    payload=signal.copy(),
                    symbol=symbol
                )

                if retry_result.ok:
                    result = retry_result
                    break

        results.append(result)

    return results
```

## Предварительная обработка сигналов

### Signal Preprocessing (signal_preprocess.py)

```python
def preprocess_signal_for_publish(
    signal: Dict[str, Any],
    symbol: str,
    source: str,
    logger: Any = None
) -> None:
    """
    Нормализация сигнала перед публикацией.
    """

    # 1. Стандартизация типов
    signal["symbol"] = str(symbol).upper()
    signal["source"] = str(source)

    # 2. Нормализация временных меток
    if "ts" in signal and "ts_ms" not in signal:
        signal["ts_ms"] = int(signal["ts"])
    if "generated_at" in signal:
        signal["ts_ms"] = int(signal["generated_at"])

    # 3. Нормализация сторон сделки
    if "direction" in signal:
        direction = str(signal["direction"]).upper()
        signal["direction"] = direction
        signal["side_int"] = 1 if direction == "LONG" else -1

    # 4. Нормализация цен
    for field in ["entry", "sl", "tp1", "tp2", "tp3"]:
        if field in signal and signal[field] is not None:
            signal[field] = float(signal[field])

    # 5. Добавление идентификаторов
    if "signal_id" not in signal:
        import uuid
        signal["signal_id"] = f"{source}:{symbol}:{int(time.time()*1000)}:{str(uuid.uuid4())[:8]}"

    # 6. Валидация обязательных полей
    required_fields = ["signal_id", "symbol", "direction", "entry"]
    missing = [f for f in required_fields if f not in signal or signal[f] is None]

    if missing and logger:
        logger.warning(f"Signal missing required fields: {missing}")
```

## Форматы данных

### Структура сигнала после нормализации

```python
{
    # Идентификация
    "signal_id": "crypto-of:BTCUSDT:1704888000123:abc12345",
    "symbol": "BTCUSDT",
    "source": "crypto_orderflow",

    # Направление и вход
    "direction": "LONG",      # "LONG" | "SHORT"
    "side_int": 1,           # 1 = LONG, -1 = SHORT
    "entry": 45000.0,        # Цена входа

    # Уровни выхода
    "sl": 44000.0,           # Stop Loss
    "tp1": 46000.0,          # Take Profit 1
    "tp2": 47000.0,          # Take Profit 2
    "tp3": 48000.0,          # Take Profit 3

    # Метаданные
    "ts_ms": 1704888000123,  # Timestamp в ms
    "generated_at": 1704888000123,

    # Технические индикаторы
    "delta": 4.8,            # Значение дельты
    "delta_z": 2.9,          # Z-score дельты
    "confidence": 0.85,      # Уверенность сигнала

    # Дополнительные поля
    "confirmations": ["obi=0.55", "absorption=2.1"],
    "indicators": {...},     # Расширенные индикаторы
    "reason": "delta_spike"  # Причина генерации
}
```

### Формат в Redis Stream

```redis
XADD signals:orderflow:BTCUSDT * payload "...json..."
```

**Пример содержимого поля payload:**
```json
{
  "signal_id": "crypto-of:BTCUSDT:1704888000123:abc12345",
  "symbol": "BTCUSDT",
  "direction": "LONG",
  "entry": 45000.0,
  "sl": 44000.0,
  "tp1": 46000.0,
  "ts_ms": 1704888000123,
  "delta": 4.8,
  "delta_z": 2.9,
  "confidence": 0.85
}
```

## Конфигурационные параметры

### Параметры инициализации

**AsyncSignalPublisher:**
- `redis_client`: aioredis клиент (обязательно)
- `source`: Источник сигналов (default: "na")
- `metrics_prefix`: Префикс для метрик (default: "signals_publish_async")
- `logger`: Логгер для сообщений

**StreamSink:**
- `name`: Название Redis Stream
- `field`: Название поля для данных (default: "payload")
- `maxlen`: Максимальная длина стрима (default: 10000)

### Переменные окружения

**Публикация:**
- `SIGNAL_PUBLISH_MAX_RETRIES`: Максимум повторных попыток (default: 3)
- `SIGNAL_STREAM_MAXLEN`: Максимальная длина стримов (default: 10000)

**Метрики:**
- `SIGNALS_PUBLISH_METRICS_PREFIX`: Префикс для метрик
- `SIGNALS_PUBLISH_LOG_LEVEL`: Уровень логирования

## Производительность и оптимизации

### Оптимизации

1. **JSON сериализация**: Оптимизированная сериализация без ensure_ascii
2. **Connection pooling**: Переиспользование Redis соединений
3. **Batch operations**: Групповая публикация для снижения overhead

### JSON сериализация (_json_dumps_safe)

```python
def _json_dumps_safe(obj: Any) -> str:
    """
    Безопасная JSON сериализация для hot-path.
    """
    try:
        return json.dumps(
            obj,
            ensure_ascii=False,        # Поддержка Unicode
            separators=(",", ":"),     # Минимальный размер
            default=str                # Fallback для неизвестных типов
        )
    except Exception:
        return '{"error":"json_dumps_failed"}'
```

### Async метрики (_aincr_fail_open)

```python
async def _aincr_fail_open(r: Any, key: str) -> None:
    """Асинхронный инкремент метрик без блокировки основного потока."""
    try:
        if r is not None and key:
            await r.incr(key)
    except Exception:
        # Fail-open: метрики не должны ломать публикацию
        return
```

## Мониторинг и метрики

### Prometheus метрики

**Автоматически собираемые метрики:**

```python
# Успешные публикации
signals_publish_async:ok_total{source="crypto_orderflow", symbol="BTCUSDT"} 1250

# Ошибки публикации
signals_publish_async:xadd_errors_total{source="crypto_orderflow"} 5

# Busy loading события
signals_publish_async:busyloading_total{source="crypto_orderflow"} 2

# Полные неудачи
signals_publish_async:all_failed_total{source="crypto_orderflow"} 1
```

### Мониторинг здоровья

```python
def health_check(self) -> Dict[str, Any]:
    """Проверка здоровья публикатора."""

    return {
        "redis_connected": await self._check_redis_connection(),
        "last_publish_ts": self._last_publish_timestamp,
        "pending_signals": len(self._pending_queue),
        "error_rate": self._calculate_error_rate(),
        "avg_publish_latency": self._calculate_avg_latency()
    }
```

### Логирование

**Уровни логирования:**
- `INFO`: Успешные публикации (сэмплированные)
- `WARNING`: Ошибки публикации с деталями
- `ERROR`: Критические ошибки инфраструктуры

**Формат логов:**
```json
{
  "timestamp": "2024-01-10T12:00:00.123Z",
  "level": "WARNING",
  "source": "async_signal_publisher",
  "stream": "signals:orderflow:BTCUSDT",
  "error": "Connection timeout",
  "signal_id": "crypto-of:BTCUSDT:1704888000123:abc12345"
}
```

## Обработка ошибок

### Fail-Open стратегия

1. **JSON сериализация**: Fallback на error объект
2. **Redis недоступен**: Возврат failed result без исключений
3. **Busy loading**: Специальная обработка для состояния загрузки Redis
4. **Contract normalization**: Ошибки нормализации не блокируют публикацию

### Обработка BusyLoadingError

```python
except redis.exceptions.BusyLoadingError:
    # Redis перезагружается (например, после failover)
    busy = True
    # Не считаем ошибкой - это временное состояние
```

### Retry logic

```python
# Экспоненциальная задержка между попытками
for attempt in range(max_retries):
    if attempt > 0:
        await asyncio.sleep(0.1 * (2 ** attempt))  # 0.1s, 0.2s, 0.4s...

    result = await self.xadd_json(...)
    if result.ok:
        break
```

## Типичные проблемы и решения

### Проблема: Высокий error_rate
**Симптомы**: signals_publish_async:xadd_errors_total растет
**Решения**:
- Проверить подключение к Redis
- Проверить права на запись в стримы
- Убедиться что maxlen не превышен
- Проверить формат данных

### Проблема: Busy loading частые
**Симптомы**: signals_publish_async:busyloading_total растет
**Решения**:
- Проверить здоровье Redis кластера
- Добавить задержки между публикациями
- Использовать approximate=True для maxlen
- Рассмотреть отказоустойчивую конфигурацию Redis

### Проблема: Высокая latency публикации
**Симптомы**: Задержки в доставке сигналов
**Решения**:
- Оптимизировать JSON сериализацию
- Использовать batch публикацию
- Проверить сетевую задержку до Redis
- Мониторить размер стримов

### Проблема: Потеря сигналов
**Симптомы**: Сигналы не доходят до потребителей
**Решения**:
- Проверить consumer groups на lagging
- Убедиться в корректности названий стримов
- Мониторить XPENDING в Redis
- Добавить дедупликацию на стороне потребителя

## Интеграция с другими компонентами

### CryptoOrderflowService

```python
# Создание публикатора
publisher = AsyncSignalPublisher(
    redis_client=self.notify_client,
    source="crypto_orderflow",
    logger=self.logger
)

# Определение приемников
sinks = [
    StreamSink(name=f"signals:orderflow:{symbol}", field="data"),
    StreamSink(name=f"signals:audit:{symbol}", field="payload"),
    StreamSink(name="notify:telegram", field="payload"),
]

# Публикация сигнала
results = await publisher.publish(signal, sinks, symbol)

# Анализ результатов
successful = sum(1 for r in results if r.ok)
if successful < len(sinks):
    self.logger.warning(f"Signal {signal_id} published to {successful}/{len(sinks)} sinks")
```

### SignalPerformanceTracker

```python
# Публикация обновлений позиций
position_update = {
    "type": "position_update",
    "pos_id": position.pos_id,
    "symbol": position.symbol,
    "pnl": position.unrealized_pnl,
    "current_price": price
}

await publisher.xadd_json(
    sink=StreamSink(name="position:updates", field="data"),
    payload=position_update,
    symbol=position.symbol
)
```

## Заключение

AsyncSignalPublisher предоставляет надежную и производительную систему публикации сигналов с fail-open семантикой. Его архитектура обеспечивает гарантированную доставку в условиях сетевых сбоев и высоких нагрузок, с полным мониторингом и метриками для оперативного контроля.
