# ✅ Redis Connection Error Fix - Multi-Symbol OrderFlow

**Дата**: 2025-11-04  
**Статус**: ИСПРАВЛЕНО

---

## 🔴 Проблема

```
❌ Ошибка в цикле обработки: Error 22 connecting to scanner-redis:6379. Invalid argument.
```

**Причина**: Error 22 (EINVAL) возникал из-за неправильных `socket_keepalive_options` в Redis connection pool.

---

## 🔧 Что было исправлено

### 1. **Удалены конфликтующие параметры Redis** (`docker-compose.yml`)

**Было**:

```yaml
environment:
  - REDIS_HOST=scanner-redis-worker-1 # ← конфликт
  - REDIS_PORT=6379
  - REDIS_DB=0
  - REDIS_URL=redis://scanner-redis:6379/0 # ← конфликт
```

**Стало**:

```yaml
environment:
  # REDIS_URL используется как основной параметр (без конфликтующих REDIS_HOST/PORT)
  - REDIS_URL=redis://scanner-redis:6379/0
```

### 2. **Исправлены socket_keepalive_options** (`python-worker/core/performance_optimizer.py`)

**Было**:

```python
self._pools[redis_url] = redis.ConnectionPool.from_url(
    redis_url,
    max_connections=max_connections,
    decode_responses=True,
    socket_keepalive=True,
    socket_keepalive_options={
        1: 1,  # TCP_KEEPIDLE
        2: 1,  # TCP_KEEPINTVL
        3: 3   # TCP_KEEPCNT
    },  # ← Вызывали Error 22 в Docker
    health_check_interval=30
)
```

**Стало**:

```python
# NOTE: socket_keepalive_options удалены - вызывали Error 22 (EINVAL)
# в некоторых Docker окружениях. Базовый socket_keepalive=True достаточно.
self._pools[redis_url] = redis.ConnectionPool.from_url(
    redis_url,
    max_connections=max_connections,
    decode_responses=True,
    socket_keepalive=True,
    health_check_interval=30
)
```

---

## ✅ Результат

Сервис `multi-symbol-orderflow` работает стабильно:

```
✅ XAUUSD       | RUNNING  | Restarts: 0
✅ BTCUSD       | RUNNING  | Restarts: 0
✅ ETHUSD       | RUNNING  | Restarts: 0
```

**Uptime**: 34+ минут без ошибок Redis подключения.

---

## 📝 Технические детали

### Почему возникала ошибка?

**Error 22 (EINVAL)** означает "Invalid argument" на уровне системных вызовов.

Параметры `socket_keepalive_options` передавались напрямую в `setsockopt()`:

- В некоторых Docker окружениях эти параметры интерпретируются по-другому
- Linux kernel может иметь разные значения констант TCP_KEEPIDLE/KEEPINTVL/KEEPCNT
- Конфликт между хост-системой и Docker контейнером

### Решение

Удалили специфичные опции и оставили только базовый `socket_keepalive=True`, который работает универсально.

---

## 🚀 Как применить

Если у вас похожая ошибка:

1. **Пересоздайте контейнер**:

```bash
docker-compose stop multi-symbol-orderflow
docker-compose rm -f multi-symbol-orderflow
docker-compose build multi-symbol-orderflow
docker-compose up -d multi-symbol-orderflow
```

2. **Проверьте логи**:

```bash
docker-compose logs -f multi-symbol-orderflow
```

3. **Проверьте статус**:

```bash
docker ps | grep multi-symbol-orderflow
```

---

## 🔍 Связанные файлы

- `docker-compose.yml` - конфигурация Redis для multi-symbol-orderflow
- `python-worker/core/performance_optimizer.py` - Redis connection pool
- `python-worker/core/redis_client.py` - базовый Redis client
- `python-worker/core/dual_redis_client.py` - dual Redis client

---

## 📚 Дополнительно

### Примеры сигналов из Redis

Все три компонента (OrderFlow, AggregatedHub-V2, TechnicalAnalysis) записывают сигналы в единообразном формате через `XAUUSDSignalFormatter`.

**Streams**:

- `signals:orderflow:XAUUSD` - OrderFlow сигналы
- `signals:ta:XAUUSD` - TechnicalAnalysis сигналы
- `notify:telegram` - Уведомления в Telegram (все источники)
- `signals:audit:XAUUSD` - Полный контекст для ML

**Формат сигнала**:

```json
{
	"sid": "1730735722000:LONG:265045",
	"symbol": "XAUUSD",
	"source": "OrderFlow | AggregatedHub-V2 | TechnicalAnalysis",
	"side": "LONG | SHORT",
	"entry": 2650.45,
	"sl": 2649.05,
	"tp_levels": [2651.85, 2653.25, 2654.65],
	"lot": 0.2,
	"confidence": 82.5,
	"atr": 1.4,
	"reason": "Описание причины сигнала",
	"ts": 1730735722000,
	"indicators": {
		"z_delta": -7.2,
		"obi": 0.38,
		"rsi": 28.5
	}
}
```

---

**Senior Go/Python Developer**  
**Опыт**: 40 лет совместного опыта в трейдинге и системной архитектуре
