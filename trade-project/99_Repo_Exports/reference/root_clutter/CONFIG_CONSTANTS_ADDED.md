# Добавленные Константы в core/config.py

## Проблема
Ошибка импорта в контейнере multi-symbol-orderflow-1:
```
ImportError: cannot import name 'SCANNER_CONSUMER_GROUP' from 'core.config'
```

## Решение
Добавлены недостающие константы в `core/config.py` с поддержкой ENV переменных.

## Добавленные Константы

### Stream Consumer Configuration
```python
SCANNER_CONSUMER_GROUP: str = os.getenv("SCANNER_CONSUMER_GROUP", "scanner-consumer-group")
SCANNER_STREAMS: list = os.getenv("SCANNER_STREAMS", "stream:tick_XAUUSD,stream:book_XAUUSD").split(",")
SCANNER_READ_COUNT: int = int(os.getenv("SCANNER_READ_COUNT", "10"))
SCANNER_READ_BLOCK_MS: int = int(os.getenv("SCANNER_READ_BLOCK_MS", "5000"))
SCANNER_STATS_INTERVAL_SEC: int = int(os.getenv("SCANNER_STATS_INTERVAL_SEC", "60"))
```

### Binance Streams Configuration
```python
BINANCE_STREAMS: list = os.getenv("BINANCE_STREAMS", "stream:binance_tickers,stream:binance_funding,stream:binance_pairs").split(",")
```

### XAU/MT5 Configuration
```python
XAU_TICK_STREAM: str = os.getenv("XAU_TICK_STREAM", "stream:tick_XAUUSD")
XAU_TICK_STREAM_MAXLEN: int = int(os.getenv("XAU_TICK_STREAM_MAXLEN", "10000"))
XAU_HANDLER_ENABLED: bool = os.getenv("XAU_HANDLER_ENABLED", "true").lower() == "true"
```

### Metrics Scheduler Configuration
```python
METRICS_SCHEDULER_INTERVAL_SEC: int = int(os.getenv("METRICS_SCHEDULER_INTERVAL_SEC", "300"))
```

### Kline Data Handler Configuration
```python
SUBSCRIBE_STREAM: str = os.getenv("SUBSCRIBE_STREAM", "stream:subscribe")
KLINE_CONSUMER_GROUP: str = os.getenv("KLINE_CONSUMER_GROUP", "kline-consumer-group")
KLINE_PENDING_FETCH: int = int(os.getenv("KLINE_PENDING_FETCH", "100"))
KLINE_READ_COUNT: int = int(os.getenv("KLINE_READ_COUNT", "10"))
KLINE_READ_BLOCK_MS: int = int(os.getenv("KLINE_READ_BLOCK_MS", "5000"))
```

## Проверка

```bash
# Тест импорта всех констант
cd python-worker && python3 -c "from core.config import *; print('✅ OK')"

# Тест импорта main_multi_symbol_dynamic.py
cd python-worker && python3 -c "import main_multi_symbol_dynamic; print('✅ OK')"
```

## Значения по умолчанию

Все константы имеют разумные значения по умолчанию и могут быть переопределены через ENV переменные в `docker-compose.yml`.

## Результат

✅ Контейнер multi-symbol-orderflow-1 теперь запускается без ошибок импорта
✅ Все зависимости разрешены
✅ Система готова к работе
