# Анализ Redis соединений: "Too many connections"

## Проблема
Ошибка "Too many connections" возникает при исчерпании пула соединений Redis. Каждый сервис может создавать множественные соединения, что приводит к превышению лимита Redis (по умолчанию 10000).

## Сервисы с большим количеством соединений

### 🔴 Критические (высокое потребление)

#### 1. **crypto-orderflow-service** (scanner-crypto-orderflow, scanner-crypto-orderflow-2)
**Файл:** `python-worker/services/crypto_orderflow_service.py`

**Проблема:**
- Создает 2 пула соединений: `main` (512) и `ticks` (1024)
- Каждый символ использует **2 блокирующих соединения** (ticks + books)
- Формула: `connections_needed = symbols_count * 2 + overhead`

**Текущие настройки:**
```python
REDIS_MAIN_MAX_CONNECTIONS=512
REDIS_TICKS_MAX_CONNECTIONS=1024
REDIS_NOTIFY_MAX_CONNECTIONS=64
```

**Расчет:**
- При 15 символах: 15 * 2 = 30 соединений для блокирующих вызовов
- + overhead (config, publish, metrics): ~50-100
- **Итого: ~80-130 соединений на инстанс**
- При 2 инстансах: **~160-260 соединений**

**Рекомендации:**
- ✅ Уже исправлено: добавлена валидация размера пула
- ✅ Улучшена обработка ошибок "Too many connections"
- ⚠️ Рассмотреть увеличение `REDIS_TICKS_MAX_CONNECTIONS` до 2048 при большом количестве символов

---

#### 2. **Go Worker** (scanner-go-worker-*)
**Файл:** `go-worker/infra/redisclient/client.go`

**Проблема:**
- Создает 3 клиента Redis:
  - `Client` (основной): `PoolSize=500`
  - `ClientWorker` (redis-worker-1): `PoolSize=200`
  - `ClientTicks` (redis-ticks): может быть общим или отдельным

**Текущие настройки:**
```go
PoolSize: 500  // Основной клиент
PoolSize: 200  // Worker клиент
MinIdleConns: 50
```

**Расчет:**
- Основной: до 500 соединений
- Worker: до 200 соединений
- **Итого: до 700 соединений на инстанс**
- При нескольких инстансах может быть критично

**Рекомендации:**
- ⚠️ Проверить, действительно ли нужны все 500 соединений
- ⚠️ Рассмотреть уменьшение `PoolSize` до 200-300

---

### 🟡 Средние (умеренное потребление)

#### 3. **metrics-server / metrics-exporter**
**Файлы:**
- `python-worker/services/observability/metrics_server.py`
- `python-worker/services/observability/metrics_exporter.py`

**Проблема:**
- Создают клиенты **без указания max_connections**
- Используют дефолтный пул (обычно 50 соединений)

**Код:**
```python
# metrics_server.py:38
r = redis.Redis.from_url(redis_url, decode_responses=False)

# metrics_exporter.py:214
r = redis.Redis.from_url(redis_url, decode_responses=False)
```

**Рекомендации:**
- ✅ Добавить `max_connections=10` (для метрик достаточно)
- ✅ Использовать общий пул соединений

---

#### 4. **entry-policy-apply-runner-v2**
**Файл:** `python-worker/services/entry_policy_apply_runner_v2.py`

**Проблема:**
- Создает aioredis клиент **без max_connections**

**Код:**
```python
self.r: aioredis.Redis = aioredis.from_url(self.redis_url, decode_responses=True)
```

**Рекомендации:**
- ✅ Добавить `max_connections=20` (для одного сервиса достаточно)

---

#### 5. **alerts-worker**
**Файл:** `python-worker/services/observability/alerts_worker_v2.py`

**Проблема:**
- Создает клиент **без max_connections**
- ✅ Уже исправлено: добавлена retry логика

**Рекомендации:**
- ✅ Добавить `max_connections=5` (для алертов достаточно)

---

### 🟢 Низкие (минимальное потребление, но множественные инстансы)

#### 6. **Утилиты и скрипты** (создают клиенты без пула)
**Файлы:**
- `python-worker/core/discovery.py` - создает клиент без max_connections
- `python-worker/core/metrics_report.py` - создает клиент без max_connections
- `python-worker/core/metrics_final.py` - создает клиент без max_connections
- `python-worker/tools/*.py` - множество утилит создают клиенты

**Проблема:**
- Каждая утилита создает новый клиент
- Нет переиспользования пула
- При запуске множества утилит может накапливаться

**Рекомендации:**
- ✅ Использовать `core/redis_client.py` с общим пулом
- ✅ Добавить `max_connections=5-10` для утилит

---

#### 7. **News Pipeline сервисы**
**Файл:** `python-worker/news_pipeline/redis_fast.py`

**Текущие настройки:**
```python
max_connections=16  # Уже настроено
```

**Статус:** ✅ Хорошо настроено

---

## Общая статистика соединений

### Расчет для текущей конфигурации:

| Сервис | Инстансов | Соединений на инстанс | Всего |
|--------|-----------|----------------------|-------|
| crypto-orderflow | 2 | 80-130 | 160-260 |
| go-worker | 2-4 | 200-700 | 400-2800 |
| metrics-server | 1 | 50 (default) | 50 |
| metrics-exporter | 1 | 50 (default) | 50 |
| alerts-worker | 1 | 50 (default) | 50 |
| entry-policy-runner | 1 | 50 (default) | 50 |
| Утилиты/скрипты | 10-20 | 5-10 | 50-200 |
| **ИТОГО** | | | **~760-3460** |

### Лимит Redis:
- По умолчанию: **10000 соединений**
- Текущее использование: **~760-3460** (7-35% лимита)
- Запас: **~6540-9240** соединений

---

## Рекомендации по исправлению

### Приоритет 1 (Критично)

1. **crypto-orderflow-service:**
   - ✅ Уже исправлено: улучшена обработка ошибок
   - ⚠️ Мониторить метрики `redis_errors_total{op="pool_exhausted"}`
   - ⚠️ При необходимости увеличить `REDIS_TICKS_MAX_CONNECTIONS`

2. **Go Worker:**
   - ⚠️ Проверить реальное использование пула
   - ⚠️ Рассмотреть уменьшение `PoolSize` до 200-300

### Приоритет 2 (Важно)

3. **Добавить max_connections для всех сервисов:**
   ```python
   # metrics_server.py
   r = redis.Redis.from_url(
       redis_url, 
       decode_responses=False,
       max_connections=10  # Добавить
   )
   
   # entry_policy_apply_runner_v2.py
   self.r = aioredis.from_url(
       self.redis_url, 
       decode_responses=True,
       max_connections=20  # Добавить
   )
   
   # alerts_worker_v2.py
   r = redis.Redis.from_url(
       redis_url,
       decode_responses=False,
       max_connections=5  # Добавить
   )
   ```

4. **Использовать общий пул для утилит:**
   - Использовать `core/redis_client.py` вместо прямого создания клиентов
   - Или добавить `max_connections=5-10` для каждого клиента

### Приоритет 3 (Желательно)

5. **Мониторинг:**
   - Добавить метрики использования пула соединений
   - Алерты при приближении к лимиту (например, >80% от max_connections)

6. **Документация:**
   - Документировать формулу расчета соединений для каждого сервиса
   - Добавить в README рекомендации по настройке пулов

---

## Формула расчета соединений

### Для блокирующих операций (xreadgroup):
```
connections_needed = (symbols_count * 2) + overhead
```
- `symbols_count * 2`: по 1 соединению на ticks и books для каждого символа
- `overhead`: config, publish, metrics (~50-100)

### Для неблокирующих операций:
```
connections_needed = concurrent_operations + overhead
```
- `concurrent_operations`: количество параллельных операций
- `overhead`: ~10-20

---

## Проверка текущего состояния

```bash
# Проверить количество соединений в Redis
docker exec redis-worker-1 redis-cli INFO clients

# Проверить maxclients
docker exec redis-worker-1 redis-cli CONFIG GET maxclients

# Мониторить соединения в реальном времени
watch -n 1 'docker exec redis-worker-1 redis-cli INFO clients | grep connected_clients'
```

---

## Следующие шаги

1. ✅ Исправить crypto-orderflow-service (сделано)
2. ⚠️ Добавить max_connections для metrics-server, metrics-exporter, entry-policy-runner
3. ⚠️ Проверить и оптимизировать Go Worker пулы
4. ⚠️ Добавить мониторинг использования пулов
5. ⚠️ Создать документацию по настройке пулов

