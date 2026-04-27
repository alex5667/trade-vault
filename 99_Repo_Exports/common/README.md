# Common Utilities

Общие утилиты для всех Python воркеров проекта.

## time_utils.py

**Утилиты для работы с временными метками в едином формате.**

### Стандарт проекта

```
Формат: Unix timestamp в миллисекундах (UTC)
Тип: int (для вычислений), string (для Redis)
Пример: 1697366459999
```

### Использование

```python
from common.time_utils import (
    get_current_timestamp_ms,
    extract_binance_close_time,
    format_timestamp_for_redis,
    create_redis_stream_fields
)

# Текущее время
ts = get_current_timestamp_ms()
print(ts)  # 1697366459999

# Извлечь closeTime из данных Binance
candle_data = {'closeTime': 1697366459999, 'symbol': 'BTCUSDT'}
close_time = extract_binance_close_time(candle_data)

# Форматировать для Redis
formatted = format_timestamp_for_redis(close_time)
print(formatted)  # "1697366459999"

# Автоматически создать поля для Redis Stream
fields = create_redis_stream_fields(
    candle_data,
    use_event_time=True,
    event_time_field='closeTime'
)
# Результат: {'timestamp': '1697366459999', 'symbol': 'BTCUSDT', ...}
```

### Тестирование

```bash
python -m pytest common/test_time_utils.py -v
```

### Документация

См. [TIMESTAMP_MIGRATION_GUIDE.md](../docs/TIMESTAMP_MIGRATION_GUIDE.md)
