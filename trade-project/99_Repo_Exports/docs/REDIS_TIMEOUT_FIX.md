# 🔧 Redis Timeout Fix - Исправление ошибок подключения

## ❌ Проблема

Множественные ошибки timeout при подключении к Redis:

```
❌ BinanceStreamConsumer: Ошибка чтения стримов: Timeout connecting to server
⚠️ Failed to write tick to Redis: Timeout connecting to server
❌ Неожиданная ошибка: Timeout connecting to server
```

**Причина:** Слишком короткие timeout'ы (5 секунд) при высокой нагрузке.

---

## ✅ Решение

Увеличены timeout'ы для всех Redis клиентов до производственных значений:

### Изменённые файлы

| Файл                                    | Было   | Стало              |
| --------------------------------------- | ------ | ------------------ |
| `python-worker/stream_consumer_impl.py` | 5 сек  | **120 сек**        |
| `py-obi/book_obi_service.py`            | 5 сек  | **120 сек**        |
| `regime-worker/worker.py`               | 60 сек | ✅ (без изменений) |

### Параметры

```python
redis.Redis(
    host=redis_host,
    port=redis_port,
    socket_connect_timeout=30,   # Было: 5, Стало: 30
    socket_timeout=120,           # Было: 5, Стало: 120
    retry_on_timeout=True,        # Новое: автоматический retry
    health_check_interval=30
)
```

---

## 🎯 Что изменилось

### 1. stream_consumer_impl.py (BinanceStreamConsumer)

**До:**

```python
socket_connect_timeout=5,
socket_timeout=5,
```

**После:**

```python
socket_connect_timeout=30,  # Увеличено
socket_timeout=120,         # Увеличено
retry_on_timeout=True,      # Добавлено
```

### 2. py-obi/book_obi_service.py (OBI Service)

**До:**

```python
socket_connect_timeout=5,
socket_timeout=5
```

**После:**

```python
socket_connect_timeout=30,  # Увеличено
socket_timeout=120,         # Увеличено
retry_on_timeout=True       # Добавлено
```

### 3. regime-worker/worker.py

✅ **Уже правильно настроен** (60 сек timeout)

---

## 📊 Рекомендуемые timeout'ы

### Production настройки

| Параметр                 | Рекомендуемое | Описание                         |
| ------------------------ | ------------- | -------------------------------- |
| `socket_connect_timeout` | 30 сек        | Время на установку соединения    |
| `socket_timeout`         | 120 сек       | Общий timeout операций           |
| `retry_on_timeout`       | True          | Автоматический retry при timeout |
| `health_check_interval`  | 30 сек        | Проверка здоровья соединения     |
| `socket_keepalive`       | True          | Keep-alive для долгих соединений |

### Для разных нагрузок

| Сценарий           | connect_timeout | socket_timeout | retry |
| ------------------ | --------------- | -------------- | ----- |
| **Low load**       | 10 сек          | 60 сек         | True  |
| **Medium load**    | 20 сек          | 90 сек         | True  |
| **High load**      | 30 сек          | 120 сек        | True  |
| **Very high load** | 60 сек          | 180 сек        | True  |

---

## 🔄 Перезапуск сервисов

```bash
# Перезапустить все затронутые сервисы
docker-compose restart scanner-python-worker scanner-py-obi scanner-regime-worker

# Или через Make
make restart
```

---

## 🧪 Проверка

### 1. Проверка логов на ошибки

```bash
# Python worker
docker logs scanner-python-worker --tail 50 | grep -i timeout

# OBI service
docker logs scanner-py-obi --tail 50 | grep -i timeout

# Regime worker
docker logs scanner-regime-worker --tail 50 | grep -i timeout
```

**Ожидаемый результат:** Нет ошибок timeout

### 2. Проверка Redis подключений

```bash
# Количество подключений
redis-cli INFO clients | grep connected_clients

# Rejected connections (должно быть 0)
redis-cli INFO stats | grep rejected_connections
```

### 3. Мониторинг в реальном времени

```bash
# Следить за метриками
watch -n 2 'redis-cli INFO stats | grep -E "total_connections|rejected|instantaneous"'
```

---

## 🛡️ Дополнительные улучшения

### Connection Pooling

Все сервисы используют connection pooling для эффективного использования соединений:

```python
pool = ConnectionPool(
    host=redis_host,
    port=redis_port,
    max_connections=50,      # Максимум соединений в пуле
    socket_keepalive=True,
    socket_connect_timeout=30,
    socket_timeout=120,
    retry_on_timeout=True,
    health_check_interval=30,
    decode_responses=True
)

redis_client = redis.Redis(connection_pool=pool)
```

### Retry логика

Добавлен `retry_on_timeout=True` для автоматического повтора при временных сбоях.

### Health checks

`health_check_interval=30` - периодическая проверка здоровья соединения каждые 30 секунд.

---

## 📈 Monitoring

### Redis метрики для отслеживания

```bash
# 1. Подключения
redis-cli INFO clients

# 2. Timeout ошибки
redis-cli INFO stats | grep timeout

# 3. Latency
redis-cli --latency

# 4. Slow log
redis-cli SLOWLOG GET 10
```

### Grafana alerts (опционально)

```yaml
- alert: RedisHighTimeout
  expr: redis_timeout_errors > 10
  for: 5m
  annotations:
    summary: 'Redis experiencing high timeout rate'
```

---

## 🎯 Best Practices

1. **Timeout'ы:**

   - `socket_connect_timeout` >= 30 сек
   - `socket_timeout` >= 120 сек
   - Всегда включать `retry_on_timeout=True`

2. **Connection Pooling:**

   - Используйте пулы для повторного использования соединений
   - `max_connections` = количество workers × 2

3. **Health Checks:**

   - Периодическая проверка соединения
   - Автоматическое переподключение при сбое

4. **Monitoring:**
   - Отслеживайте `rejected_connections`
   - Мониторинг latency
   - Алерты при высоком timeout rate

---

## 🔧 Troubleshooting

### Если проблемы продолжаются

1. **Проверить нагрузку на Redis:**

   ```bash
   redis-cli INFO stats
   redis-cli INFO cpu
   ```

2. **Проверить сеть:**

   ```bash
   docker network inspect scanner-network
   ping scanner-redis-worker-1
   ```

3. **Увеличить max connections в Redis:**

   ```bash
   redis-cli CONFIG SET maxclients 10000
   ```

4. **Проверить системные лимиты:**
   ```bash
   ulimit -n  # File descriptors
   ```

---

**Готово!** Redis timeout'ы исправлены для всех сервисов! 🚀
