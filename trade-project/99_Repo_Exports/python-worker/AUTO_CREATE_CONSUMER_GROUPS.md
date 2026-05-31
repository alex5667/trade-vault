# Auto-Create Consumer Groups - Graceful NOGROUP Handling

## Проблема

При использовании Redis Streams с consumer groups возникала ошибка:

```
redis.exceptions.ResponseError: NOGROUP No such key 'stream:tick_BTCUSDT' 
or consumer group 'btcusdt-signal-group'
```

**Причины:**
1. Stream еще не создан (нет данных)
2. Consumer group не создана для stream
3. При XAUTOCLAIM (claim pending) Redis требует существующую группу

**Последствия:**
- Ошибки в логах (хотя не критичные)
- Невозможность claim pending сообщений до создания группы
- Требуется ручное создание групп для новых streams

## Решение

Добавлено автоматическое создание consumer group при ошибке NOGROUP.

### 1. Обновлен `core/redis_stream_consumer.py`

**Метод `claim_pending()`:**

```python
def claim_pending(self, stream: str, min_idle_ms: int, ...) -> Tuple[str, List[StreamMsg]]:
    try:
        res = self.client.xautoclaim(...)
    except Exception as e:
        # ✅ Auto-create consumer group if NOGROUP error
        if "NOGROUP" in str(e):
            try:
                self.client.xgroup_create(stream, self.group, id='0', mkstream=True)
                logger.info(f"✅ Auto-created consumer group '{self.group}' for stream '{stream}'")
                
                # Retry claim after creating group
                res = self.client.xautoclaim(...)
            except Exception as create_err:
                if "BUSYGROUP" not in str(create_err):
                    logger.warning(f"⚠️ Failed to create consumer group: {create_err}")
                return "0-0", []  # Return empty result
```

**Особенности:**
- Автоматически создает группу при NOGROUP
- Retry claim после создания группы
- Игнорирует BUSYGROUP (группа уже существует)
- Graceful fallback: возвращает пустой результат при ошибке

### 2. Обновлен `services/stream_worker.py`

**Метод `_claim_orphan_pending()`:**

```python
def _claim_orphan_pending(self) -> int:
    for stream in self._streams:
        try:
            next_id, msgs, _deleted = self.client.xautoclaim(...)
            # Process messages...
        except Exception as e:
            # ✅ Auto-create consumer group if NOGROUP error
            if "NOGROUP" in str(e):
                try:
                    self.client.xgroup_create(stream, self.group, id='0', mkstream=True)
                    self.log.info("[%s] ✅ Auto-created consumer group for %s", self.name, stream)
                    
                    # Retry claim
                    next_id, msgs, _deleted = self.client.xautoclaim(...)
                    # Process messages...
                except Exception as create_err:
                    if "BUSYGROUP" not in str(create_err):
                        self.log.warning("[%s] ⚠️ Failed to create group: %s", self.name, create_err)
```

## Преимущества

### 1. Автоматическое восстановление
- ✅ Не требуется ручное создание групп
- ✅ Работает для динамически появляющихся streams
- ✅ Graceful handling без падения потоков

### 2. Меньше ошибок в логах
- ✅ NOGROUP обрабатывается корректно
- ✅ Логируется создание группы (INFO level)
- ✅ Только реальные ошибки в WARNING/ERROR

### 3. Production-ready
- ✅ Idempotent: BUSYGROUP игнорируется
- ✅ Retry после создания группы
- ✅ Fallback на пустой результат при ошибке

## Поведение

### До изменений
```
ERROR: NOGROUP No such key 'stream:tick_BTCUSDT' or consumer group 'btcusdt-signal-group'
ERROR: NOGROUP No such key 'stream:tick_BTCUSDT' or consumer group 'btcusdt-signal-group'
ERROR: NOGROUP No such key 'stream:tick_BTCUSDT' or consumer group 'btcusdt-signal-group'
... (повторяется каждую минуту)
```

### После изменений
```
INFO: ✅ Auto-created consumer group 'btcusdt-signal-group' for stream 'stream:tick_BTCUSDT'
... (больше нет ошибок NOGROUP для этого stream)
```

## Когда создается группа

**Автоматически при:**
1. Первом XAUTOCLAIM для нового stream
2. После удаления consumer group (если stream существует)
3. При добавлении нового символа в систему

**Параметры создания:**
- `id='0'` - начинаем с начала stream (для pending messages)
- `mkstream=True` - создать stream если не существует

## Совместимость

### Обратная совместимость
- ✅ Не меняет поведение для существующих групп
- ✅ Работает с существующим кодом
- ✅ Не требует миграции

### Redis версии
- ✅ Redis 6.2+ (XAUTOCLAIM поддерживается)
- ✅ Fallback для старых версий redis-py

## Тестирование

### 1. Тест на новый stream
```python
# 1. Удалить consumer group
redis-cli XGROUP DESTROY stream:tick_NEWCOIN my-group

# 2. Попробовать claim pending
# Ожидаемый результат: группа создается автоматически
```

### 2. Тест на несуществующий stream
```python
# 1. Попробовать claim pending для несуществующего stream
# Ожидаемый результат: группа создается, stream создается (mkstream=True)
```

### 3. Тест на BUSYGROUP
```python
# 1. Создать группу вручную
redis-cli XGROUP CREATE stream:tick_TEST my-group 0 MKSTREAM

# 2. Попробовать claim pending
# Ожидаемый результат: BUSYGROUP игнорируется, работает нормально
```

## Мониторинг

### Логи

**Успешное создание:**
```
INFO | [signals_listener] ✅ Auto-created consumer group for signals:ta:XAUUSD
```

**Ошибка создания:**
```
WARNING | [signals_listener] ⚠️ Failed to create consumer group for signals:ta:XAUUSD: <error>
```

**BUSYGROUP (игнорируется):**
```
(нет логов - это нормально)
```

### Redis команды для проверки

```bash
# Список consumer groups для stream
redis-cli XINFO GROUPS stream:tick_BTCUSDT

# Список consumers в группе
redis-cli XINFO CONSUMERS stream:tick_BTCUSDT my-group

# Pending messages
redis-cli XPENDING stream:tick_BTCUSDT my-group
```

## Рекомендации

### 1. Мониторинг создания групп
Следите за логами `Auto-created consumer group` - это нормально для новых streams, но если происходит часто для одних и тех же streams - возможна проблема.

### 2. TTL для streams
Если streams удаляются автоматически (MAXLEN, EXPIRE), consumer groups тоже удаляются. Auto-create решает эту проблему.

### 3. Graceful degradation
Если создание группы не удалось, система продолжает работу (возвращает пустой результат), но логирует предупреждение.

## Итоги

✅ **Автоматическое создание групп**: Не требуется ручное управление  
✅ **Graceful handling**: Нет падений при NOGROUP  
✅ **Production-ready**: Idempotent, с retry и fallback  
✅ **Меньше ошибок**: Только реальные проблемы в логах  

**Система теперь более устойчива к динамическим изменениям streams!** 🚀


