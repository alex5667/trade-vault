# ATR Cache - Детальная документация

## Обзор

**ATRCache** - высокопроизводительный кеш-слой для значений Average True Range (ATR), обеспечивающий быстрое получение волатильности для расчетов риск-менеджмента в торговых стратегиях. Поддерживает множественные источники данных и автоматический fallback.

**Расположение**: `python-worker/utils/atr_cache.py`

**Назначение**: Предоставление быстрого доступа к ATR значениям, необходимым для расчета стоп-лоссов, тейк-профитов и размеров позиций в реальном времени.

## Архитектурные принципы

### 1. Multi-Source Architecture
- **Primary**: Centralized ATR tracker (trade_back service)
- **Secondary**: Local worker calculations
- **Fallback**: Legacy compatibility keys
- **Cache**: In-memory with Redis backend

### 2. Fail-Open Design
- Graceful degradation при недоступности источников
- Оптимистическая обработка ошибок
- Возврат None вместо исключений

### 3. Performance Optimized
- TTL-based expiration
- Multiple key formats for compatibility
- Lazy loading и кеширование

## Детальная структура класса

### Основные атрибуты

#### Подключения
```python
self.redis_client: redis.Redis  # Redis клиент для кеша
self.ttl: int                   # Время жизни ключей (секунды)
```

## Детальная логика методов

### Инициализация (__init__)

```python
def __init__(self, ttl: int = 3600):
    """
    Инициализация ATR кеша.

    Args:
        ttl: Время жизни ключей в секундах (default: 1 час)
    """

    # Выбор Redis клиента
    url = os.getenv("ATR_REDIS_URL")
    if url:
        self.redis_client = redis.from_url(url, decode_responses=True)
    else:
        self.redis_client = get_redis()

    self.ttl = ttl
```

### Получение ATR (get)

**Основной метод получения ATR с многоуровневым fallback:**

```python
def get(self, symbol: str, timeframe: str) -> Optional[float]:
    """Получение ATR из всех доступных источников."""

    try:
        tf_upper = self._normalize_tracker_tf(timeframe)

        # Источник 1: Centralized tracker (trade_back)
        tracker_key = f"ATR:{symbol}:{tf_upper}"
        tracker_atr, _ = self.redis_client.hmget(tracker_key, "atr", "lastCloseTime")

        if tracker_atr:
            return float(tracker_atr)

        # Источник 2: Local worker (primary key)
        primary_key = f"atr:{symbol}:{timeframe}"
        value = self.redis_client.get(primary_key)

        # Источник 3: Legacy worker (val key)
        if value is None:
            value = self.redis_client.get(f"atr:val:{symbol}:{timeframe}")

        # Источник 4: Trade service hash
        if value is None:
            trade_key = f"trade:ATR:{symbol}:{tf_upper}"
            if self.redis_client.type(trade_key) == "hash":
                value = self.redis_client.hget(trade_key, "atr")

        # Источник 5: Legacy JSON format
        if value is None:
            legacy_key = f"ta:last:atr:{symbol}"
            legacy_payload = self.redis_client.get(legacy_key)
            if legacy_payload:
                try:
                    legacy_data = json.loads(legacy_payload)
                    value = legacy_data.get("atr")
                except (json.JSONDecodeError, TypeError, AttributeError):
                    pass

        return float(value) if value is not None else None

    except Exception as e:
        print(f"⚠️ ATRCache: Ошибка получения ATR для {symbol}:{timeframe}: {e}")
        return None
```

#### Стратегия источников

1. **ATR:{symbol}:{TF}** (Hash) - Centralized tracker
   - Формат: `{"atr": float, "lastCloseTime": timestamp}`
   - Преимущество: Свежие данные с timestamp проверки

2. **atr:{symbol}:{timeframe}** (String) - Local worker primary
   - Формат: `float`
   - Преимущество: Простой и быстрый

3. **atr:val:{symbol}:{timeframe}** (String) - Legacy compatibility
   - Формат: `float`
   - Для обратной совместимости

4. **trade:ATR:{symbol}:{TF}** (Hash) - Trade service
   - Формат: `{"atr": float}`
   - Резервный источник

5. **ta:last:atr:{symbol}** (String JSON) - Legacy format
   - Формат: `{"atr": float, ...}`
   - Последний fallback

### Сохранение ATR (set)

```python
def set(self, symbol: str, timeframe: str, atr_value: float) -> bool:
    """Сохранение ATR в кеш с TTL."""

    try:
        if atr_value <= 0:
            return False

        primary_key = f"atr:{symbol}:{timeframe}"
        self.redis_client.set(primary_key, str(atr_value), ex=self.ttl)

        # Legacy compatibility
        self.redis_client.set(f"atr:val:{symbol}:{timeframe}", str(atr_value), ex=self.ttl)

        return True

    except Exception as e:
        print(f"❌ ATRCache: Ошибка сохранения ATR для {symbol}:{timeframe}: {e}")
        return False
```

### Удаление ATR (delete)

```python
def delete(self, symbol: str, timeframe: str) -> bool:
    """Удаление ATR из кеша."""

    try:
        key = f"atr:{symbol}:{timeframe}"
        self.redis_client.delete(key)
        return True

    except Exception as e:
        print(f"❌ ATRCache: Ошибка удаления ATR для {symbol}:{timeframe}: {e}")
        return False
```

### Очистка всего кеша (clear_all)

```python
def clear_all(self) -> int:
    """Удаление всех ATR ключей."""

    try:
        # Поиск всех ATR ключей
        pattern = "atr:*"
        keys = self.redis_client.keys(pattern)

        if keys:
            deleted = self.redis_client.delete(*keys)
            print(f"🧹 ATRCache: Удалено {deleted} ATR ключей")
            return deleted

        return 0

    except Exception as e:
        print(f"❌ ATRCache: Ошибка очистки кеша: {e}")
        return 0
```

### Вспомогательные методы

#### Нормализация таймфрейма (_normalize_tracker_tf)

```python
def _normalize_tracker_tf(self, tf: str) -> str:
    """Нормализация таймфрейма для tracker'а."""

    # M1, M5, M15, M30, H1, H4, D1, W1, MN
    tf = tf.upper()

    # Преобразования
    mappings = {
        '1M': 'M1', '5M': 'M5', '15M': 'M15', '30M': 'M30',
        '1H': 'H1', '4H': 'H4', '1D': 'D1', '1W': 'W1', '1MN': 'MN'
    }

    return mappings.get(tf, tf)
```

## Форматы ключей Redis

### Основные форматы

| Формат | Тип | Описание | Пример |
|--------|-----|----------|---------|
| `atr:{symbol}:{tf}` | String | Primary worker cache | `atr:BTCUSDT:1m` |
| `atr:val:{symbol}:{tf}` | String | Legacy worker cache | `atr:val:BTCUSDT:1m` |
| `ATR:{symbol}:{TF}` | Hash | Centralized tracker | `ATR:BTCUSDT:M1` |
| `trade:ATR:{symbol}:{TF}` | Hash | Trade service | `trade:ATR:BTCUSDT:M1` |
| `ta:last:atr:{symbol}` | String JSON | Legacy format | `ta:last:atr:BTCUSDT` |

### Структура Hash ключей

#### ATR:{symbol}:{TF} (Centralized tracker)
```json
{
  "atr": 125.47,
  "lastCloseTime": 1704888000000,
  "symbol": "BTCUSDT",
  "timeframe": "M1"
}
```

#### trade:ATR:{symbol}:{TF} (Trade service)
```json
{
  "atr": 125.47,
  "timestamp": 1704888000000
}
```

#### ta:last:atr:{symbol} (Legacy JSON)
```json
{
  "atr": 125.47,
  "timestamp": 1704888000000,
  "symbol": "BTCUSDT"
}
```

## Конфигурационные параметры

### Переменные окружения

**Redis:**
- `ATR_REDIS_URL`: URL отдельного Redis для ATR (опционально)
- `ATR_CACHE_TTL`: TTL для ATR ключей в секундах (default: 3600)

**Источники:**
- `ATR_SOURCE`: Приоритетный источник ("tracker", "worker", "legacy")
- `ATR_FALLBACK_ENABLED`: Включить fallback источники (default: true)

### Программная конфигурация

```python
# Создание экземпляра с кастомным TTL
atr_cache = ATRCache(ttl=7200)  # 2 часа

# Использование
atr = atr_cache.get("BTCUSDT", "1m")
if atr:
    print(f"ATR BTCUSDT 1m: {atr}")
```

## Производительность и оптимизации

### Кеширование

1. **TTL Expiration**: Автоматическая очистка устаревших данных
2. **Multiple Keys**: Поддержка нескольких форматов для совместимости
3. **Lazy Loading**: Загрузка только по запросу

### Оптимизации запросов

```python
# Batch получение ATR для нескольких символов
def get_multiple_atr(self, symbols: List[str], timeframe: str) -> Dict[str, float]:
    """Batch получение ATR для оптимизации."""

    pipeline = self.redis_client.pipeline()
    for symbol in symbols:
        primary_key = f"atr:{symbol}:{timeframe}"
        pipeline.get(primary_key)

    results = pipeline.execute()

    return {
        symbol: float(atr) if atr else None
        for symbol, atr in zip(symbols, results)
    }
```

### Memory Management

- **TTL-based cleanup**: Redis автоматически удаляет устаревшие ключи
- **No in-memory cache**: Все данные в Redis для consistency
- **Connection pooling**: Переиспользование соединений

## Мониторинг и метрики

### Метрики использования

```python
def get_cache_stats(self) -> Dict[str, Any]:
    """Получение статистики использования кеша."""

    try:
        # Подсчет ключей
        pattern = "atr:*"
        keys = self.redis_client.keys(pattern)

        stats = {
            "total_keys": len(keys),
            "cache_hit_rate": self._calculate_hit_rate(),
            "avg_atr_age": self._calculate_avg_age(keys),
            "sources_usage": self._analyze_sources_usage()
        }

        return stats

    except Exception as e:
        print(f"❌ ATRCache: Ошибка получения статистики: {e}")
        return {}
```

### Health Checks

```python
def health_check(self) -> Dict[str, Any]:
    """Проверка здоровья ATR кеша."""

    health = {
        "redis_connected": False,
        "can_read": False,
        "can_write": False,
        "latency_ms": None
    }

    try:
        # Проверка подключения
        start_time = time.time()
        self.redis_client.ping()
        health["redis_connected"] = True

        # Проверка чтения
        test_key = "atr:health_check:test"
        self.redis_client.set(test_key, "1", ex=10)
        value = self.redis_client.get(test_key)
        health["can_read"] = value == "1"
        health["can_write"] = True

        # Замер latency
        health["latency_ms"] = (time.time() - start_time) * 1000

    except Exception as e:
        print(f"❌ ATRCache health check failed: {e}")

    return health
```

## Обработка ошибок

### Fail-Open стратегия

1. **Redis unavailable**: Возврат None, логирование
2. **Invalid data**: Попытка конвертации, возврат None при ошибке
3. **Multiple sources**: Последовательная попытка всех источников
4. **Legacy compatibility**: Поддержка старых форматов

### Валидация данных

```python
def _validate_atr_value(self, value: Any) -> Optional[float]:
    """Валидация ATR значения."""

    if value is None:
        return None

    try:
        float_value = float(value)
        if float_value > 0 and float_value < 1000000:  # Разумные границы
            return float_value
    except (ValueError, TypeError):
        pass

    return None
```

## Типичные проблемы и решения

### Проблема: ATR значения устарели
**Симптомы**: Использование старых значений ATR
**Решения**:
- Уменьшить TTL в ATRCache
- Проверить свежесть centralized tracker
- Добавить проверку lastCloseTime

### Проблема: Высокая latency при получении ATR
**Симптомы**: Задержки в расчетах позиций
**Решения**:
- Использовать локальный кеш в памяти
- Оптимизировать порядок источников (ближайшие сначала)
- Предварительно загружать ATR для активных символов

### Проблема: Несогласованность между источниками
**Симптомы**: Разные значения ATR из разных источников
**Решения**:
- Определить primary источник и использовать только его
- Добавить валидацию и reconciliation
- Логировать расхождения для анализа

### Проблема: Memory pressure от ATR ключей
**Симптомы**: Рост потребления памяти Redis
**Решения**:
- Настроить оптимальный TTL
- Регулярная очистка устаревших ключей
- Использовать compression для больших значений

## Интеграция с другими компонентами

### CryptoOrderflowService

```python
# Получение ATR для расчетов уровней
atr = self.atr_cache.get(symbol, self.config.get("atr_tf", "1m"))
if atr is None:
    # Fallback на локальный расчет
    atr = self._calculate_atr_from_ticks(runtime, window=60)
```

### TradeMonitorService

```python
# Использование ATR для позиций
symbol_spec = get_symbol_info(position.symbol)
atr = atr_cache.get(position.symbol, "1m")

if atr and symbol_spec:
    # Расчет SL/TP на основе ATR
    sl_distance = atr * self.sl_atr_multiplier
    # ...
```

### ReportingService

```python
# Включение ATR в отчеты
strategy_report = {
    "atr_values": {
        symbol: atr_cache.get(symbol, "1m")
        for symbol in symbols
    },
    # ... другие метрики
}
```

## Заключение

ATRCache предоставляет надежный и эффективный механизм кеширования ATR значений, критически важных для риск-менеджмента в торговых стратегиях. Его multi-source архитектура обеспечивает высокую доступность и совместимость с различными источниками данных.
