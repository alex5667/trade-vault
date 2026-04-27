# ✅ Исправление таймаута батча книг

## Проблема

1. **Неправильный таймаут**: `(ts - self._book_buffer_last_ts)` всегда давал 0, так как `_book_buffer_last_ts` обновлялся на каждом append
2. **Неправильный timestamp в батче**: Один и тот же `ts` передавался во все `_process_l2_snapshot()` внутри батча, что портило `self._l2_last_ts` и staleness

## Решение

### 1. Поля в `__init__`

**Было:**
```python
self._book_buffer_last_ts = 0
```

**Стало:**
```python
self._book_batch_start_ms = 0          # wall-clock, для batch-timeout
self._book_buffer_last_append_ts = 0   # опционально, только debug/метрики
```

### 2. Исправленный `_process_book`

**Основные изменения:**

1. **Разделение времени:**
   - `now_ms` - wall-clock время для таймаута
   - `book_ts` - event timestamp из книги для staleness

2. **Фиксация старта батча:**
   ```python
   if not self._book_buffer:
       self._book_batch_start_ms = now_ms
   ```

3. **Правильный таймаут:**
   ```python
   elapsed_ms = now_ms - (self._book_batch_start_ms or now_ms)
   should_process = (
       len(self._book_buffer) >= self._book_buffer_max or
       elapsed_ms >= self._book_buffer_timeout_ms
   )
   ```

4. **Правильный timestamp для каждой книги:**
   ```python
   # ВАЖНО: ts должен быть от конкретной книги, а не один общий
   for b, snap in zip(buf, snapshots):
       if not snap:
           continue
       ts_i = int(b.get("ts", 0)) or now_ms
       self._process_l2_snapshot(snap, ts_i)
   ```

## Результат

✅ **Таймаут работает корректно** - сравнивается wall-clock время с временем старта батча

✅ **Staleness корректна** - каждая книга передает свой timestamp в `_process_l2_snapshot()`

✅ **Нет аллокаций на горячем пути** - все вычисления O(1)

## Статус

✅ **ИСПРАВЛЕНО И ГОТОВО К ИСПОЛЬЗОВАНИЮ**

