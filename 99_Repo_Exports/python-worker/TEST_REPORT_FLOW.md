# Тестирование пути от закрытия сделки до отправки отчета

## Проблема
Отчеты не приходят после закрытия сделок.

## Что было исправлено

1. **Добавлено логирование на каждом этапе:**
   - `save_closed()` - логирует сохранение в trades:closed
   - `check_and_trigger_report()` - логирует счетчик и триггер
   - `send_report_for_pair()` - логирует формирование отчета
   - `_gather_window_metrics_stream()` - логирует поиск и совпадения
   - `_send_report()` - логирует отправку

2. **Проверка данных на каждом этапе:**
   - Проверка source/strategy в trades:closed stream
   - Нормализация timestamp (секунды → миллисекунды)
   - Проверка is_final_close флага

## Как протестировать

### 1. Запустите тестовый скрипт:
```bash
cd python-worker
python test_report_flow.py
```

Скрипт покажет:
- Текущие настройки (REPORT_TRIGGER_COUNT, RECENT_WINDOW_SECONDS)
- Последние закрытые сделки в trades:closed
- Текущие счетчики отчетов
- Метрики для тестовых пар
- Обнаруженные пары source/symbol

### 2. Проверьте логи контейнера:
```bash
docker logs scanner-signal-tracker --tail 100 | grep -E "(Триггер|отчет|сделок|source|strategy)"
```

### 3. Важные настройки:

**REPORT_TRIGGER_COUNT** - количество сделок до отправки отчета (по умолчанию 100)
- Для тестирования установите: `REPORT_TRIGGER_COUNT=1`
- В docker-compose.yml добавьте в environment:
  ```yaml
  REPORT_TRIGGER_COUNT: "1"
  ```

**PERIODIC_REPORT_SEND_EMPTY** - отправлять ли пустые отчеты (по умолчанию false)
- Для тестирования можно включить: `PERIODIC_REPORT_SEND_EMPTY=true`

**RECENT_WINDOW_SECONDS** - окно времени для сбора метрик (по умолчанию 3600 = 1 час)

### 4. Проверьте данные в Redis:

```bash
# Проверьте последние закрытые сделки
docker exec scanner-redis-worker-1 redis-cli XREVRANGE trades:closed + - COUNT 5

# Проверьте счетчики
docker exec scanner-redis-worker-1 redis-cli KEYS "report_counter:*"

# Проверьте конкретный счетчик
docker exec scanner-redis-worker-1 redis-cli GET "report_counter:trades:CryptoOrderFlow:BTCUSDT"
```

## Возможные проблемы

1. **REPORT_TRIGGER_COUNT слишком большой**
   - Решение: установите `REPORT_TRIGGER_COUNT=1` для тестирования

2. **Несоответствие source/strategy**
   - Проблема: в trades:closed может быть `strategy="cryptoorderflow"`, а ищем `source="CryptoOrderFlow"`
   - Решение: код теперь проверяет оба поля и нормализует через `canon_source()`

3. **Timestamp в секундах вместо миллисекунд**
   - Проблема: если `closed_time` в секундах, фильтрация по окну не работает
   - Решение: добавлена функция `_normalize_ts_ms()` для автоматической нормализации

4. **is_final_close != "1"**
   - Проблема: записи без флага `is_final_close="1"` пропускаются
   - Решение: проверьте, что в `save_closed()` устанавливается `is_final_close="1"`

5. **Нет сделок в окне времени**
   - Проблема: сделки закрыты более часа назад (RECENT_WINDOW_SECONDS=3600)
   - Решение: увеличьте `RECENT_WINDOW_SECONDS` или закройте новую сделку

## Путь данных

1. **Закрытие сделки:**
   ```
   TradeMonitorService.on_tick() 
   → process_tick() возвращает closed: TradeClosed
   → repo.save_closed(closed)
   ```

2. **Сохранение в Redis:**
   ```
   redis_repo.save_closed()
   → order:{id} hash (обновление)
   → trades:closed stream (xadd)
   → closed:{strategy}:{symbol}:{tf}:{source} list
   ```

3. **Триггер отчета:**
   ```
   TradeMonitorService.on_tick() (после save_closed)
   → check_and_trigger_report(pos.source, pos.symbol, "trades")
   → PeriodicReporter._check_and_trigger_report()
   → инкремент счетчика
   → если count % REPORT_TRIGGER_COUNT == 0 → send_report_for_pair()
   ```

4. **Сбор метрик:**
   ```
   send_report_for_pair()
   → _gather_window_metrics_stream(source, symbol)
   → чтение trades:closed stream
   → фильтрация по source, symbol, времени, is_final_close
   → накопление метрик
   ```

5. **Отправка отчета:**
   ```
   _send_report(source, symbol, metrics)
   → проверка total_trades > 0 (или PERIODIC_REPORT_SEND_EMPTY=true)
   → формирование сообщения
   → reporting.send_telegram_message()
   ```

## Отладка

Включите DEBUG логирование:
```python
# В periodic_reporter.py уже есть logger.debug() в ключевых местах
# Проверьте логи с уровнем DEBUG
```

Проверьте логи по этапам:
```bash
# 1. Сохранение сделки
docker logs scanner-signal-tracker | grep "💾 Сохранение закрытой сделки"

# 2. Триггер отчета
docker logs scanner-signal-tracker | grep "🔄 Триггер отчета"

# 3. Счетчик
docker logs scanner-signal-tracker | grep "📊 Счетчик"

# 4. Сбор метрик
docker logs scanner-signal-tracker | grep "📊 Итого собрано"

# 5. Отправка
docker logs scanner-signal-tracker | grep "📨 _send_report"
```

