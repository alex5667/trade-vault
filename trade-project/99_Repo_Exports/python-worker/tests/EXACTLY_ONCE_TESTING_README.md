# Exactly-Once Testing Suite

Этот набор тестов проверяет exactly-once семантику в системе сигналов - критически важную функциональность для предотвращения дублированных сигналов и потерь.

## 🎯 Что тестируется

### Ключевые инварианты exactly-once:

1. **Outbox publish**: либо `{sent}`, либо `{dedup}`, но не "молчаливый фейл"
2. **Dedup rollback**: если дедуп-ключ поставлен, а XADD не случился → потеря (тест ловит это)
3. **Dispatcher delivery**: сообщение считается "готовым к ACK" только после доставки во все targets или DLQ
4. **Idempotent reprocessing**: повторный запуск обработки того же outbox msg должен быть идемпотентным
5. **Marker atomicity**: delivery-маркеры не должны приводить к "dedup поставили, deliver не сделали"

## 📁 Структура тестов

### Unit тесты (без Redis)
```bash
test_outbox_key_generation_and_parsing.py
```
- Генерация дедуп ключей
- Валидация envelope
- Тесты чистых функций

### Integration тесты (с Redis)
```bash
test_outbox_deduplication_integration.py      # Dedup в outbox
test_dispatcher_exactly_once_integration.py   # Delivery маркеры
test_dlq_malformed_envelopes.py              # DLQ для bad envelopes
test_load_high_volume_scenarios.py           # Load testing
test_chaos_transient_failures.py             # Chaos testing
test_metrics_validation.py                   # Metrics correctness
```

## 🚀 Запуск тестов

### Базовая настройка

1. **Redis для тестов**: тесты используют отдельную БД Redis (db=15) чтобы не мешать продакшену
```bash
export TEST_REDIS_URL="redis://localhost:6379/15"
```

2. **Запуск всех exactly-once тестов**:
```bash
cd python-worker
pytest tests/test_outbox_* tests/test_dispatcher_* tests/test_dlq_* tests/test_load_* tests/test_chaos_* tests/test_metrics_* -v
```

3. **Запуск с coverage**:
```bash
pytest tests/test_outbox_* --cov=core.signal_outbox --cov-report=html
```

4. **Запуск только fast unit тестов** (без Redis):
```bash
pytest tests/test_outbox_key_generation_and_parsing.py -v
```

5. **Heavy load тесты** (отдельно, т.к. медленные):
```bash
pytest tests/test_load_high_volume_scenarios.py::TestLoadHighVolume::test_load_heavy_volume -v --runslow
```

## 🔍 Детали тестов

### Outbox Deduplication Tests

**test_dedup_same_bucket_blocks_second_publish**
- Проверяет что дедуп ключ блокирует повторную публикацию в одном бакете
- Валидирует что только первое сообщение попадает в outbox

**test_lua_rollback_on_xadd_error** 🔥 **КРИТИЧНЫЙ**
- Тестирует Lua rollback при ошибке XADD
- Если этот тест падает → у вас "dedup поставили, outbox не записали" = ПОТЕРЯ СИГНАЛОВ

### Dispatcher Delivery Tests

**test_dispatcher_idempotent_delivery_same_envelope**
- Проверяет идемпотентность повторной обработки
- Убеждается что маркеры доставки не дублируются

**test_partial_failure_does_not_poison_delivery_markers**
- Тестирует что failure одного target не портит маркеры других
- Проверяет корректность rollback при частичных failures

### DLQ Tests

**test_missing_sid_goes_to_dlq**
- Malformed envelopes должны идти в DLQ и быть ACK
- Проверяет quarantine bad data

**test_max_attempts_exceeded_goes_to_dlq**
- После max_attempts → DLQ
- Проверяет escalation от retry к quarantine

### Load Tests

**test_outbox_load_no_duplicates**
- N сигналов → N сообщений в outbox (без дубликатов)
- Проверяет дедуп под нагрузкой

**test_end_to_end_load_pipeline**
- Полный pipeline: outbox → dispatcher → targets
- Проверяет exactly-once end-to-end

### Chaos Tests

**test_outbox_recovers_from_redis_disconnect**
- Recovery после временных disconnect
- Проверяет resilience

**test_lua_script_fallback_on_evalsha_failure**
- Fallback к eval когда evalsha падает
- Проверяет graceful degradation

## 📊 Метрики и мониторинг

Тесты проверяют что система правильно инкрементирует метрики:

- `signal_publish_success` / `signal_publish_error`
- `signal_dedup_blocked`
- `signal_delivery_success` / `signal_delivery_retry`
- `signal_dlq_sent`
- Latency histograms для всех операций

## 🐛 Баги которые ловят эти тесты

### High-severity (production-breaking):

1. **Lost signals**: dedup поставили, но XADD не случился
2. **Duplicate delivery**: повторная обработка доставляет снова
3. **Poisoned markers**: failure портит delivery markers
4. **DLQ loops**: bad envelopes не quarantine, а зацикливаются

### Medium-severity:

1. **Memory leaks**: TTL на маркерах не работает
2. **Performance degradation**: дедуп не оптимизирован под load
3. **Metrics drift**: неправильный counting метрик

### Low-severity:

1. **Log noise**: excessive logging при retries
2. **Latency spikes**: non-optimal код paths

## 🔧 Настройка тестов

### Redis Configuration

```python
# tests/conftest.py
@pytest.fixture(scope="session")
def redis_url():
    return os.getenv("TEST_REDIS_URL", "redis://localhost:6379/15")
```

### Test Data

Тесты используют deterministic данные:
- `sid`: "test_signal_123"
- `symbol`: "BTCUSDT"
- `ts_ms`: 1700000000000 (2023-11-14 22:13:20 UTC)
- `bucket_ms`: 60000 (1 минута)

### Mock Components

Для изоляции используются:
- `MockMetricsCollector` для проверки метрик
- Monkeypatch для симуляции failures
- FakeRedis для некоторых unit тестов

## 📈 CI/CD Integration

Рекомендуется:

1. **Pre-merge**: запускать все unit + fast integration тесты
2. **Nightly**: heavy load тесты на staging
3. **Release**: полная exactly-once test suite

## 🔍 Debugging failures

### Если тест падает:

1. **Проверить Redis**: `redis-cli -n 15 MONITOR`
2. **Посмотреть ключи**: `redis-cli -n 15 KEYS "*"`
3. **Проверить Lua logs**: в Redis logs
4. **Debug mode**: добавить `import pdb; pdb.set_trace()`

### Common failure patterns:

- **Race conditions**: дедуп ключ существует дольше чем ожидалось
- **TTL issues**: маркеры не истекают
- **Encoding problems**: bytes vs str в Redis
- **Lua script caching**: evalsha SHA не найден

## 🎯 Coverage goals

- **Unit tests**: 90%+ coverage для key generation и validation
- **Integration tests**: 100% coverage для happy path + error paths
- **Load tests**: validate под 1000+ concurrent operations
- **Chaos tests**: 95%+ recovery rate от simulated failures

---

## 🚨 IMPORTANT

Эти тесты - ваш последний рубеж обороны против exactly-once багов. Если они проходят - система корректна. Если падают - **НЕ РЕЛИЗИТЬ** до фикса!

**Особое внимание к**: `test_lua_rollback_on_xadd_error` - этот тест спасает от катастрофических потерь сигналов.
