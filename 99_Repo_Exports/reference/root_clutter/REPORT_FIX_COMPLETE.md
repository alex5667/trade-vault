# Исправление системы отчетов - Завершено

## Дата: 2025-12-02

## Выполненные исправления

### 1. Очистка данных
- ✅ Очищены сигналы и сделки (5300 записей)
- ✅ Очищены логи в папке `logs/`

### 2. Исправлены ошибки методов

#### 2.1 PeriodicReporter
- ✅ Добавлен метод `send_periodic_report()` для отправки периодических отчетов
- ✅ Добавлен метод `_discover_pairs()` для обнаружения пар source/symbol
- ✅ Добавлен метод `_source_from_strategy()` для преобразования strategy → source
- ✅ Добавлена функция `_normalize_ts_ms()` для нормализации timestamp (секунды → миллисекунды)

#### 2.2 TradeMonitorService
- ✅ Добавлен метод `process_signal()` (алиас для `on_signal()`)
- ✅ Добавлен метод `apply_external_sl_hit()` для обработки внешних SL событий
- ✅ Добавлен метод `get_position_count()` для получения количества открытых позиций

### 3. Исправлена путаница strategy vs source

**Проблема:** Отчеты собирались по `source`, но пары обнаруживались из `stats:strategies` как strategy (ta, cryptoorderflow), что не совпадало с source (TechnicalAnalysis, CryptoOrderFlow).

**Решение:**
- В `_discover_pairs()` strategy преобразуется в source через маппинг:
  - `ta` → `TechnicalAnalysis`
  - `cryptoorderflow` → `CryptoOrderFlow`
  - `orderflow` → `OrderFlow`
  - `aggregated` → `AggregatedHub-V2`
- В `_gather_window_metrics_stream()` проверяются оба поля: `source` и `strategy`

### 4. Отключены пустые отчеты

**Проблема:** Отчеты отправлялись даже без сделок, засоряя Telegram.

**Решение:**
- В `send_periodic_report()` добавлена проверка `total_trades > 0`
- В `_send_report()` добавлен guard для пропуска пустых отчетов
- Добавлена переменная окружения `PERIODIC_REPORT_SEND_EMPTY` (по умолчанию `false`)
- В `main()` добавлена аналогичная проверка

### 5. Исправлен триггер отчетов

**Проблема:** Отчеты триггерились по сигналам, а не по закрытым сделкам.

**Решение:**
- Убран триггер по сигналам из `SignalPerformanceTracker._signals_listener_thread()`
- Добавлен триггер по закрытым сделкам в `TradeMonitorService.on_tick()` (в ветке `if closed:`)
- Отчеты теперь отправляются только при фактическом закрытии сделок

### 6. Исправлена нормализация timestamp

**Проблема:** Если `closed_time` в секундах, фильтрация по окну времени не работала.

**Решение:**
- Добавлена функция `_normalize_ts_ms()` для автоматической нормализации
- Если timestamp < 10_000_000_000, он умножается на 1000
- Применена во всех местах проверки `closed_ts`

### 7. Исправлена обработка SL_HIT событий

**Проблема:** Предупреждения "SL_HIT event missing sid/price", хотя данные присутствовали.

**Решение:**
- Добавлена проверка альтернативных полей: `price`, `exit_price`, `sl`
- Добавлена проверка альтернативных полей для sid: `signal_id`

### 8. Исправлен HTML в отчетах

**Проблема:** Telegram возвращал ошибку 400: "can't parse entities: Unsupported start tag".

**Решение:**
- Экранированы HTML-символы: `>` → `&gt;`, `<` → `&lt;`
- Добавлено экранирование для `top_str` через `html.escape()`
- Проверка длины сообщений (лимит Telegram: 4096 символов)

### 9. Улучшено логирование

**Добавлено логирование на каждом этапе:**
- `save_closed()` — логирует сохранение в trades:closed
- `check_and_trigger_report()` — логирует счетчик и триггер
- `send_report_for_pair()` — логирует формирование отчета
- `_gather_window_metrics_stream()` — логирует поиск и совпадения
- `_send_report()` — логирует отправку
- `improved_notifier.py` — выводит ошибки в stdout для docker logs

### 10. Настройки

**Изменено в docker-compose.yml:**
- `REPORT_TRIGGER_COUNT: 1` (было 100) — отчеты отправляются после каждой закрытой сделки

## Путь данных (полный цикл)

```
1. Закрытие сделки:
   TradeMonitorService.on_tick() 
   → process_tick() возвращает closed: TradeClosed
   → repo.save_closed(closed)

2. Сохранение в Redis:
   redis_repo.save_closed()
   → order:{id} hash (обновление)
   → trades:closed stream (xadd) ✅ с полями source, strategy, exit_ts_ms
   → closed:{strategy}:{symbol}:{tf}:{source} list

3. Триггер отчета:
   TradeMonitorService.on_tick() (после save_closed)
   → check_and_trigger_report(pos.source, pos.symbol, "trades")
   → PeriodicReporter._check_and_trigger_report()
   → инкремент счетчика report_counter:trades:{source}:{symbol}
   → если count % REPORT_TRIGGER_COUNT == 0 → send_report_for_pair()

4. Сбор метрик:
   send_report_for_pair()
   → _gather_window_metrics_stream(source, symbol)
   → чтение trades:closed stream
   → фильтрация по source, symbol, времени, is_final_close
   → нормализация timestamp через _normalize_ts_ms()
   → накопление метрик

5. Отправка отчета:
   _send_report(source, symbol, metrics)
   → проверка total_trades > 0
   → экранирование HTML
   → reporting.send_telegram_message()
   → публикация в notify:telegram stream с type="report"

6. Доставка в Telegram:
   notify-worker читает notify:telegram
   → обнаруживает type="report"
   → вызывает send_html_to_telegram()
   → ImprovedTelegramNotifier.send_notification()
   → HTTP POST к Telegram Bot API
   → HTTP 200 OK ✅
   → Сообщение доставлено в бот
```

## Результаты тестирования

### Тест 1: Проверка данных
```
✅ Найдено 12 пар source/symbol
✅ CryptoOrderFlow/BTCUSDT: 19 сделок в окне
✅ CryptoOrderFlow/ETHUSDT: 4 сделки в окне
✅ TechnicalAnalysis/XAUUSD: 1 сделка в окне
```

### Тест 2: Отправка отчета
```
✅ Отчет опубликован в notify:telegram: 1764688227573-0
✅ notifier: HTML отчет отправлен (841 символов)
✅ HTTP 200 OK от Telegram API
✅ Уведомление отправлено 1/1 получателям
```

### Тест 3: Проверка методов
```
✅ PeriodicReporter импортирован успешно
✅ TradeMonitorService импортирован успешно
✅ Метод get_position_count: True
✅ Метод apply_external_sl_hit: True
✅ Метод process_signal: True
```

## Текущие настройки

- `REPORT_TRIGGER_COUNT=1` — отчет после каждой закрытой сделки
- `PERIODIC_REPORT_WINDOW_SECONDS=3600` — окно 1 час
- `PERIODIC_REPORT_SEND_EMPTY=false` — пустые отчеты не отправляются
- `RECENT_LIMIT=2000` — лимит записей для анализа

## Статус системы

### Контейнеры
- ✅ scanner-signal-tracker: Up (health: starting)
- ✅ scanner-notify-worker: Up
- ✅ scanner-redis-worker-1: Up (healthy)

### Отчеты
- ✅ Публикуются в notify:telegram
- ✅ Обрабатываются notify-worker
- ✅ Отправляются в Telegram (HTTP 200 OK)
- ✅ Содержат реальные данные (сделки, WR, PnL, TP hits)

### Метрики последнего отчета (CryptoOrderFlow/BTCUSDT)
- Сделок: 19
- WR (net): 31.6%
- WR (strict): 31.6%
- P/L net: +206.22
- ProfitFactor: 1.44
- TP1/TP2/TP3 hits: 6/6/6
- Top close_reason: SL:13, TP3:6

## Что дальше

Система полностью работоспособна. Отчеты будут автоматически отправляться:
1. После каждой закрытой сделки (REPORT_TRIGGER_COUNT=1)
2. Только для пар с данными (пустые отчеты отключены)
3. С корректным маппингом strategy → source
4. С правильным HTML-форматированием

Для мониторинга используйте:
```bash
# Проверка отчетов в Redis
docker exec scanner-redis-worker-1 redis-cli XREVRANGE notify:telegram + - COUNT 10

# Проверка логов notify-worker
docker logs scanner-notify-worker --tail 50 | grep "HTML отчет отправлен"

# Проверка логов signal-tracker
docker logs scanner-signal-tracker --tail 50 | grep "Отчет отправлен"
```

## Файлы для справки

- `python-worker/TEST_REPORT_FLOW.md` — инструкции по тестированию
- `python-worker/test_report_flow.py` — тестовый скрипт

