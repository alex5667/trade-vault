# Добавленные Константы Stream Publisher в core/config.py

## Проблема
Ошибка импорта в контейнере `multi-symbol-orderflow-1`:
```
ImportError: cannot import name 'STREAM_MAPPING' from 'core.config'
```

## Решение
Добавлены недостающие константы для Stream Publisher в `core/config.py`.

## Добавленные Константы

### STREAM_MAPPING
```python
STREAM_MAPPING: dict = {
    "signals": "stream:signals",
    "orders": "stream:orders",
    "alerts": "stream:alerts",
    "signal:crypto": "stream:signal_crypto",
    "signal:forex": "stream:signal_forex",
    "trigger:crypto": "stream:trigger_crypto",
    "trigger:forex": "stream:trigger_forex",
    "top:crypto": "stream:top_crypto",
    "top:forex": "stream:top_forex",
}
```
**Назначение**: Mapping имен каналов на Redis стримы для публикации сигналов.

### STREAM_MAX_LENGTH
```python
STREAM_MAX_LENGTH: int = int(os.getenv("STREAM_MAX_LENGTH", "10000"))
```
**Назначение**: Максимальная длина Redis стримов (автоматическая очистка старых сообщений).

### SIGNAL_DEDUP_TTL_SEC
```python
SIGNAL_DEDUP_TTL_SEC: int = int(os.getenv("SIGNAL_DEDUP_TTL_SEC", "300"))
```
**Назначение**: Время жизни (TTL) ключа дедупликации сигналов в секундах.

## Использование

### В StreamPublisher
```python
from core.config import STREAM_MAPPING, STREAM_MAX_LENGTH, SIGNAL_DEDUP_TTL_SEC

class StreamPublisher:
    def __init__(self):
        self.stream_mapping = STREAM_MAPPING

    def publish_to_stream(self, stream_name: str, data: Dict[str, Any],
                         max_length: int = STREAM_MAX_LENGTH) -> Optional[str]:
        # ...

    # Дедупликация сигналов
    was_set = main_redis.set(dedup_key, 1, ex=SIGNAL_DEDUP_TTL_SEC, nx=True)
```

## Настройка через ENV

```bash
# Максимальная длина стримов
STREAM_MAX_LENGTH=5000

# TTL для дедупликации (5 минут)
SIGNAL_DEDUP_TTL_SEC=300
```

## Проверка

```bash
# Тест импорта
cd python-worker && python3 -c "
from core.config import STREAM_MAPPING, STREAM_MAX_LENGTH, SIGNAL_DEDUP_TTL_SEC
print('✅ Constants imported successfully')
print(f'Mapping keys: {list(STREAM_MAPPING.keys())}')
print(f'Max length: {STREAM_MAX_LENGTH}')
print(f'Dedup TTL: {SIGNAL_DEDUP_TTL_SEC}')
"

# Тест publisher
cd python-worker && python3 -c "
from publisher.stream_publisher_impl import StreamPublisher
print('✅ StreamPublisher imported successfully')
"
```

## Результат

✅ Контейнер `multi-symbol-orderflow-1` теперь запускается без ошибок импорта
✅ Stream Publisher полностью функционален
✅ Все зависимости разрешены
✅ Система готова к публикации сигналов в Redis стримы
