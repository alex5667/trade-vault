# Housekeeping для SignalPerformanceTracker - Быстрый старт

## Что было сделано

Интегрирован механизм автоматической финализации зависших позиций для исправления утечек памяти и искажений статистики.

## Изменения в коде

### 1. Конфигурация (`_RuntimeCfg`)

**Файл**: `python-worker/handlers/crypto_orderflow/config/runtime_config.py`

Добавлены 4 новых поля:
- `max_lifetime_bars_after_entry` - TTL в барах после входа
- `max_lifetime_ms_after_entry` - Fallback TTL в миллисекундах
- `housekeeping_every_ms` - Частота housekeeping
- `expiry_bars` - TTL до входа

### 2. Модель состояния (`SignalPerfState`)

**Файл**: `python-worker/signal_exec/performance_tracker.py`

Добавлены поля:
- `bar_signal`, `bar_entry`, `bar_exit` - индексы баров
- `finalize_reason` - причина финализации

### 3. Housekeeping логика

**Файл**: `python-worker/signal_exec/performance_tracker.py`

Добавлены методы:
- `_housekeep_expired()` - финализация зависших позиций
- `_is_recently_finalized()` - проверка финализированных signal_id
- Обновлены `on_bar()`, `on_execution_event()`, `_finalize_and_store()`

### 4. Тесты

**Файл**: `tests/test_expired_no_target.py`

Полный набор тестов (10 тестовых случаев).

## Быстрая интеграция

### Шаг 1: Добавить переменные окружения

В `docker-compose.yml` добавьте для сервисов, использующих `SignalPerformanceTracker`:

```yaml
environment:
  # NEW: Housekeeping для SignalPerformanceTracker
  - ORDERFLOW_EXPIRY_BARS=60
  - ORDERFLOW_MAX_LIFETIME_BARS_AFTER_ENTRY=180
  - ORDERFLOW_MAX_LIFETIME_MS_AFTER_ENTRY=0
  - ORDERFLOW_HOUSEKEEPING_EVERY_MS=1000
```

### Шаг 2: Обновить код (если используете SignalPerformanceTracker)

#### Было:
```python
tracker.register_signal(ctx, plan)
tracker.on_bar(symbol, bar)
tracker.on_execution_event(signal_id, event_type, ts, price)
```

#### Стало:
```python
tracker.register_signal(ctx, plan, bar_idx=current_bar_idx)
tracker.on_bar(symbol, bar, bar_idx=current_bar_idx)
tracker.on_execution_event(signal_id, event_type, ts, price, bar_idx=current_bar_idx)
```

**Важно**: `bar_idx` опционален. Если не передавать, будет использоваться fallback по времени.

### Шаг 3: Запустить тесты

```bash
cd /home/alex/front/trade/scanner_infra
python -m pytest tests/test_expired_no_target.py -v
```

Должно пройти 10 тестов успешно.

### Шаг 4: Перезапустить сервисы

```bash
docker-compose restart python-worker
# или конкретный сервис
docker-compose restart crypto-orderflow-handler
```

### Шаг 5: Проверить логи

```bash
# Проверить, что переменные загружены
docker logs python-worker 2>&1 | grep -i "expiry_bars"

# Проверить работу housekeeping (через несколько минут)
docker logs python-worker 2>&1 | grep -i "housekeeping"
```

## Что исправлено

✅ **Утечка памяти** - позиции автоматически финализируются по TTL  
✅ **Искажение статистики** - идемпотентная финализация  
✅ **Поздние события** - защита через LRU  
✅ **Outcome.EXPIRED_NO_TARGET** - теперь используется  

## Проверка работы

### 1. Размер памяти стабилизируется

```bash
# До интеграции: рост памяти со временем
# После интеграции: стабильный размер

docker stats python-worker
```

### 2. Логи показывают финализации

```bash
docker logs python-worker 2>&1 | grep "expired_no_target"
```

### 3. Тесты проходят

```bash
pytest tests/test_expired_no_target.py -v
# ========== 10 passed in X.XXs ==========
```

## Документация

- **Полное описание**: `SIGNAL_PERF_TRACKER_HOUSEKEEPING_INTEGRATION.md`
- **Переменные окружения**: `HOUSEKEEPING_ENV_VARS.md`
- **Тесты**: `tests/test_expired_no_target.py`

## Поддержка

При возникновении проблем:

1. Проверьте тесты: `pytest tests/test_expired_no_target.py -v`
2. Проверьте логи: `docker logs python-worker 2>&1 | grep -i "housekeeping"`
3. Проверьте переменные окружения в `docker-compose.yml`
4. См. раздел Troubleshooting в `HOUSEKEEPING_ENV_VARS.md`

## Обратная совместимость

✅ Все изменения обратно совместимы  
✅ Новые параметры имеют значения по умолчанию  
✅ Старый код продолжит работать без изменений  
✅ Рекомендуется обновить для полной функциональности  

## Рекомендации

1. **Начните с рекомендуемых значений** (см. выше)
2. **Мониторьте логи** первые несколько часов
3. **Настройте под свою стратегию** при необходимости
4. **Регулярно проверяйте размер `_states`** через логирование

## Итоги

Интеграция завершена успешно. Все изменения протестированы и документированы. Система теперь корректно обрабатывает зависшие позиции и гарантирует точность статистики.

