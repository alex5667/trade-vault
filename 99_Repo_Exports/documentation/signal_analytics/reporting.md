# 📝 Формирование и отправка отчетов (2025-11-26)

> Детальное описание системы формирования отчетов, периодических сводок и отправки в Telegram.

---

## 📋 Содержание

1. [Обзор системы отчетов](#обзор-системы-отчетов)
2. [Компоненты системы](#компоненты-системы)
3. [Типы отчетов](#типы-отчетов)
4. [Формирование отчетов](#формирование-отчетов)
5. [Периодические отчеты](#периодические-отчеты)
6. [Отправка в Telegram](#отправка-в-telegram)
7. [Форматирование сообщений](#форматирование-сообщений)
8. [Метрики и мониторинг](#метрики-и-мониторинг)
9. [FAQ](#faq)

---

## 📊 Обзор системы отчетов

### Поток данных

```
1. Stats Aggregator собирает статистику
   ↓
2. Reporting Service формирует отчет
   ├─► Ежедневная сводка
   ├─► Периодические отчеты
   └─► Отчеты по стратегиям
   ↓
3. Публикация в notify:telegram (type=report)
   ↓
4. Telegram Worker читает сообщения
   ↓
5. Отправка в Telegram через Bot API
```

---

## 🧩 Компоненты системы

| Компонент                | Файл/директория                              | Назначение                                    |
| ------------------------ | -------------------------------------------- | --------------------------------------------- |
| **Reporting Service**    | `python-worker/services/reporting_service.py` | Формирование HTML-отчетов, API для статистики |
| **Periodic Reporter**    | `python-worker/services/periodic_reporter.py` | Периодические сводки по счетчику сделок       |
| **Embedded Periodic Reporter** | `python-worker/services/embedded_periodic_reporter.py` | Встроенный периодический репортер              |
| **Stats Aggregator**     | `python-worker/services/stats_aggregator.py` | Агрегация статистики по стратегиям             |
| **Telegram Worker**      | `telegram-worker/multithreaded_worker.py`    | Обработка сообщений из notify:telegram        |

---

## 📄 Типы отчетов

### 1. Ежедневная сводка

Отправляется один раз в день в заданный час (UTC):

- Общая статистика по всем стратегиям
- Разбивка по символам и таймфреймам
- Разбивка по источникам сигналов (опционально)
- Топ-5 лучших и худших сделок

### 2. Автоматические отчеты по счетчику сделок

Отправляются автоматически каждые N сделок (по умолчанию 100) через `PeriodicReporter`:

- Сводка за период
- Изменения метрик с предыдущего отчета
- Алерты при отклонениях

### 3. Отчеты по стратегии

Детальная статистика по конкретной стратегии:

- Общие показатели (сделок, winrate, P/L)
- TP метрики (TP1/TP2/TP3 hit rates)
- Упущенная прибыль (TP→SL статистика)
- Разбивка по источникам

### 4. Отчеты по сделкам

Список недавних сделок с пагинацией:

- Детали каждой сделки
- События по сделке
- Графики P&L

---

## 🔧 Формирование отчетов

### Reporting Service API

```python
from services.reporting_service import ReportingService

reporting = ReportingService()

# Получение отчета по стратегии
report = reporting.get_strategy_report(
    strategy="cryptoorderflow",
    symbol="XAUUSD",
    tf="M1",
    include_sources=True
)

# Получение сводного отчета
all_report = reporting.get_all_strategies_report()

# Отправка ежедневной сводки
reporting.send_daily_summary(include_sources=True)
```

### Структура отчета

```python
def generate_strategy_report(
    strategy: str,
    symbol: str,
    tf: str
) -> str:
    """Генерация HTML-отчета по стратегии."""
    # Получение статистики
    stats = StatsAggregator.get_stats(redis, strategy, symbol, tf)
    
    # Формирование HTML
    html = f"""
    <b>📊 Отчет: {strategy} - {symbol} ({tf})</b>
    
    <b>Общие показатели:</b>
    • Всего сделок: {stats['total_trades']}
    • Прибыльных: {stats['wins']}
    • Убыточных: {stats['losses']}
    • Winrate: {stats['winrate']:.2f}%
    
    <b>TP метрики:</b>
    • TP1 достигнуто: {stats['tp1_hits']}
    • TP2 достигнуто: {stats['tp2_hits']}
    • TP3 достигнуто: {stats['tp3_hits']}
    
    <b>Упущенная прибыль:</b>
    • TP1 → SL: {stats['tp1_then_sl']}
    • TP2 → SL: {stats['tp2_then_sl']}
    • TP3 → SL: {stats['tp3_then_sl']}
    
    <b>P&L:</b>
    • Общий P&L: ${stats['total_pnl']:.2f}
    • Средний P&L: ${stats['avg_pnl']:.2f}
    
    <b>Трейлинг:</b>
    • Трейлинг запущен: {stats['trailing_started']}
    • Закрыто по трейлинг стопу: {stats['trailing_stop_hits']}
    """
    
    return html
```

### Сводный отчет

```python
def generate_summary_report(all_stats: Dict) -> str:
    """Генерация сводного отчета по всем стратегиям."""
    # Агрегация общих метрик
    total_trades = sum(s.get("total_trades", 0) for s in all_stats.values())
    total_wins = sum(s.get("wins", 0) for s in all_stats.values())
    total_pnl = sum(s.get("total_pnl", 0) for s in all_stats.values())
    
    overall_winrate = (total_wins / total_trades * 100) if total_trades > 0 else 0
    
    html = f"""
    <b>📊 Сводный отчет</b>
    
    <b>Общие показатели:</b>
    • Всего сделок: {total_trades}
    • Прибыльных: {total_wins}
    • Общий P&L: ${total_pnl:.2f}
    • Общий Winrate: {overall_winrate:.2f}%
    
    <b>По стратегиям:</b>
    """
    
    # Добавление детализации по стратегиям
    for key, stats in sorted(all_stats.items()):
        strategy, symbol, tf = key.split(":")
        html += f"""
    <b>{strategy} - {symbol} ({tf}):</b>
    • Сделок: {stats.get('total_trades', 0)}
    • Winrate: {stats.get('winrate', 0):.2f}%
    • P&L: ${stats.get('total_pnl', 0):.2f}
    """
    
    return html
```

---

## ⏰ Периодические отчеты

### Periodic Reporter (основной механизм)

**Основной механизм отправки отчетов** — по счетчику сделок через `PeriodicReporter`:

```python
class PeriodicReporter:
    """Сервис для периодической отправки отчетов по счетчику сделок."""
    
    def __init__(self):
        self.report_trigger_count = int(os.getenv("REPORT_TRIGGER_COUNT", "100"))
        self.redis = get_redis()
        self.reporting = ReportingService()
    
    def _check_and_trigger_report(self, source: str, symbol: str, counter_type: str = "trades"):
        """Проверка счетчика и отправка отчета при достижении лимита."""
        canonical_symbol = self._canonical_symbol(symbol) or symbol.upper()
        counter_key = f"report_counter:{counter_type}:{source}:{canonical_symbol}"
        
        # Увеличение счетчика
        count = self.redis.incr(counter_key)
        self.redis.expire(counter_key, 86400)  # TTL 24 часа
        
        # Проверка лимита
        if count >= self.report_trigger_count:
            # Отправка отчета для пары source/symbol
            self.send_report_for_pair(source, canonical_symbol)
            # Сброс счетчика
            self.redis.delete(counter_key)
```

**Особенности:**

- Счетчик ведется отдельно для каждой пары `{source}/{symbol}` (например, `CryptoOrderFlow/BTCUSDT`, `OrderFlow/XAUUSD`)
- Счетчик увеличивается при каждой закрытой сделке в `StatsAggregator`
- Отчет отправляется при достижении лимита (100 сделок по умолчанию)
- После отправки счетчик сбрасывается

### Embedded Periodic Reporter (опционально)

Встроенный периодический репортер поддерживает дополнительные механизмы:

- **По счетчику сделок** — через `PeriodicReporter.check_and_trigger_report()` (основной механизм)
- **Ежедневно** — в заданный час UTC (настраивается через `DAILY_SUMMARY_HOUR`)

```python
class EmbeddedPeriodicReporter:
    """Встроенный периодический репортер."""
    
    def __init__(self):
        self.daily_summary_hour = int(os.getenv("DAILY_SUMMARY_HOUR", "0"))
        self.last_daily_summary = None
    
    def check_and_send_daily_summary(self):
        """Проверка и отправка ежедневной сводки."""
        now = datetime.utcnow()
        
        # Проверка часа
        if now.hour == self.daily_summary_hour:
            # Проверка, что еще не отправляли сегодня
            if not self.last_daily_summary or self.last_daily_summary.date() < now.date():
                self.send_daily_summary()
                self.last_daily_summary = now
```

**Примечание:** Периодические отчеты по времени (каждые N часов) также поддерживаются в `SignalPerformanceTracker`, но основная логика отправки отчетов работает по счетчику сделок через `PeriodicReporter`.

---

## 📱 Отправка в Telegram

### Публикация в Redis Stream

Отчеты публикуются в `notify:telegram` с типом `report`:

```python
def send_telegram_message(self, text: str, parse_mode: str = "HTML") -> bool:
    """Отправка сообщения в Telegram через Redis stream."""
    notify_stream = os.getenv("NOTIFY_STREAM", "notify:telegram")
    
    message_data = {
        "type": "report",
        "text": text,
        "source": "ReportingService",
        "timestamp": str(int(time.time() * 1000))
    }
    
    msg_id = self.redis.xadd(notify_stream, message_data, maxlen=1000)
    return True
```

### Обработка в Telegram Worker

`Telegram Worker` читает сообщения из `notify:telegram` и отправляет их в Telegram:

```python
def process_notify_messages():
    """Обработка сообщений из notify:telegram."""
    messages = redis.xreadgroup(
        group="notify-group",
        consumer="telegram-worker-1",
        streams={"notify:telegram": ">"},
        count=100,
        block=1000
    )
    
    for stream, msgs in messages:
        for msg_id, data in msgs:
            msg_type = data.get("type")
            
            if msg_type == "report":
                # Отправка HTML-отчета
                html_text = data.get("text")
                send_html_to_telegram(html_text)
            
            # Подтверждение обработки
            redis.xack("notify:telegram", "notify-group", msg_id)
```

### Отправка через Bot API

```python
def send_html_to_telegram(html_text: str) -> bool:
    """Отправка HTML-сообщения в Telegram."""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    payload = {
        "chat_id": chat_id,
        "text": html_text,
        "parse_mode": "HTML"
    }
    
    response = requests.post(url, json=payload, timeout=10)
    
    if response.status_code == 200:
        return True
    else:
        logger.error(f"Ошибка отправки в Telegram: {response.text}")
        return False
```

---

## 🎨 Форматирование сообщений

### HTML-форматирование

Telegram поддерживает HTML-форматирование:

```python
def format_signal_message(data: Dict) -> str:
    """Форматирование сообщения о сигнале."""
    direction_emoji = "🟢" if data["side"] == "LONG" else "🔴"
    
    message = f"""
    {direction_emoji} <b>Новый сигнал</b>
    
    <b>Символ:</b> {data['symbol']}
    <b>Направление:</b> {data['side']}
    <b>Вход:</b> {data['entry']}
    <b>SL:</b> {data['sl']}
    <b>TP:</b> {', '.join(json.loads(data['tp_levels']))}
    <b>Уверенность:</b> {data.get('confidence', 'N/A')}%
    <b>Источник:</b> {data.get('source', 'N/A')}
    """
    
    return message
```

### Эмодзи для отчетов

```python
# Эмодзи для результатов
RESULT_EMOJI = {
    "win": "✅",
    "loss": "❌",
    "breakeven": "➖"
}

# Эмодзи для направлений
DIRECTION_EMOJI = {
    "LONG": "📈",
    "SHORT": "📉"
}

# Эмодзи для типов отчетов
REPORT_EMOJI = {
    "daily": "📅",
    "periodic": "📊",
    "strategy": "📈",
    "trade": "💰"
}
```

### Форматирование чисел

```python
def format_currency(value: float) -> str:
    """Форматирование валюты."""
    return f"${value:+.2f}"

def format_percent(value: float) -> str:
    """Форматирование процентов."""
    return f"{value:+.2f}%"

def format_number(value: float, decimals: int = 2) -> str:
    """Форматирование чисел."""
    return f"{value:,.{decimals}f}"
```

---

## 📊 Метрики и мониторинг

### Prometheus метрики

| Метрика                    | Описание                          | Тип     |
| -------------------------- | --------------------------------- | -------- |
| `reports_published_total`  | Количество опубликованных отчетов | Counter  |
| `stats_report_latency_ms`  | Задержка формирования отчета     | Histogram|
| `telegram_send_errors_total` | Ошибки отправки в Telegram      | Counter  |
| `reports_queue_length`     | Длина очереди отчетов             | Gauge    |

### Redis ключи для мониторинга

```python
# Статистика отчетов
reports_key = "stats:reports"
redis.hincrby(reports_key, "total_reports", 1)
redis.hincrby(reports_key, "daily_reports", 1)
redis.hincrby(reports_key, "periodic_reports", 1)

# Счетчик для периодических отчетов
counter_key = "report:counter"
redis.incr(counter_key)
```

### Логирование

```python
logger.info(f"📊 Отчет опубликован: {report_type}")
logger.info(f"✅ Отчет отправлен в Telegram: {chat_id}")
logger.error(f"❌ Ошибка отправки отчета: {error}")
```

---

## ❓ FAQ

### Как настроить частоту отчетов?

Частота отчетов настраивается через переменные окружения:

- `REPORT_TRIGGER_COUNT` — количество сделок для триггера отчета (по умолчанию 100)
  - Отчеты отправляются автоматически каждые N сделок для каждой пары `{source}/{symbol}`
  - Счетчик увеличивается при каждой закрытой сделке в `StatsAggregator`
- `DAILY_SUMMARY_HOUR` — час отправки ежедневной сводки (UTC, по умолчанию 0:00)
  - Ежедневный сводный отчет по всем стратегиям и символам
- `REPORT_INTERVAL_HOURS` — интервал периодических отчетов по времени (опционально, используется в `SignalPerformanceTracker`)

### Можно ли отправить отчет вручную?

Да, через API или команду:

```bash
# Через Makefile
make send-real-report

# Через Python
from services.reporting_service import ReportingService
reporting = ReportingService()
reporting.send_daily_summary()
```

### Как получить отчет по конкретной стратегии?

```python
from services.reporting_service import ReportingService

reporting = ReportingService()
report = reporting.get_strategy_report(
    strategy="cryptoorderflow",
    symbol="XAUUSD",
    tf="M1"
)
```

### Можно ли настроить формат отчетов?

Да, форматирование настраивается в методах `ReportingService`:

- `format_strategy_report()` — формат отчета по стратегии
- `format_summary_report()` — формат сводного отчета
- `format_trade_message()` — формат сообщения о сделке

### Как отслеживать доставку отчетов?

Доставка отслеживается через:

- Метрики Prometheus (`telegram_send_errors_total`)
- Логи Telegram Worker
- Подтверждения обработки (`XACK` в Redis)

---

## 🔗 Связанные документы

- **[signal_lifecycle.md](signal_lifecycle.md)** — полный цикл сигнала
- **[pnl_analysis.md](pnl_analysis.md)** — анализ прибыли/убытков
- **[trailing_stop_tracking.md](trailing_stop_tracking.md)** — отслеживание трейлинг стопов

---

## ✅ Контроль версий

- **2025-11-26** — обновление документации по формированию отчетов
- **2025-11-21** — создание документации по формированию отчетов
- Ответственные: `@trading-analytics`, `@python-team`
