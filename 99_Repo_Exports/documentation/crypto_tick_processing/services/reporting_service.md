# ReportingService - Детальная документация

## Обзор

**ReportingService** - сервис генерации аналитических отчетов и уведомлений по торговым сигналам и позициям. Предоставляет комплексную аналитику производительности стратегий, отправляет уведомления в Telegram и формирует детальные отчеты для трейдеров и аналитиков.

**Расположение**: `python-worker/services/reporting_service.py`

**Назначение**: Трансформация сырых статистических данных в читаемые отчеты и уведомления для принятия решений.

## Архитектурные принципы

### 1. Fail-Open дизайн
- Продолжение работы при недоступности внешних сервисов
- Graceful degradation при ошибках генерации отчетов
- Оптимистическая обработка неполных данных

### 2. Многоуровневая аналитика
- **Микро-уровень**: Детали отдельных сделок
- **Мезо-уровень**: Статистика по символам/стратегиям
- **Макро-уровень**: Сводные отчеты по портфелю

### 3. Гибкая коммуникация
- **Telegram**: Оперативные уведомления и отчеты
- **API**: Программный доступ к данным
- **HTML**: Форматированные отчеты

## Детальная структура класса

### Основные атрибуты

#### Подключения и конфигурация
```python
self.redis: redis.Redis                    # Redis клиент
self.logger: logging.Logger               # Логгер
self.telegram_enabled: bool              # Флаг отправки в Telegram
self.telegram_config: Optional[Dict]     # Конфигурация Telegram
```

#### Вспомогательные методы
```python
self._to_int: Callable                    # Безопасное преобразование в int
self._to_float: Callable                 # Безопасное преобразование в float
self._safe_div: Callable                 # Безопасное деление
self._fmt_money: Callable                # Форматирование денежных сумм
self._fmt_pct: Callable                  # Форматирование процентов
self._fmt_rate: Callable                 # Форматирование коэффициентов
```

## Детальная логика методов

### Получение отчета по стратегии (get_strategy_report)

```python
def get_strategy_report(self, strategy: str, symbol: Optional[str] = None,
                       timeframes: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Получение детального отчета по стратегии.

    Args:
        strategy: Название стратегии
        symbol: Опционально - фильтр по символу
        timeframes: Опционально - список таймфреймов

    Returns:
        Словарь с агрегированной статистикой
    """
```

#### Логика агрегации:

1. **Определение таймфреймов**
   ```python
   tfs = timeframes or ["tick", "1m", "5m", "15m", "1h", "4h", "1d"]
   ```

2. **Инициализация структуры отчета**
   ```python
   combined = {
       "strategy": strategy,
       "symbol": symbol,
       "total_trades": 0,
       "wins": 0,
       "losses": 0,
       "breakevens": 0,
       "total_pnl": 0.0,
       "total_pnl_gross": 0.0,
       "total_fees": 0.0,
       "winrate": 0.0,
       "avg_pnl": 0.0,
       "profit_factor": 0.0,
       # ... дополнительные метрики
   }
   ```

3. **Агрегация данных по таймфреймам**
   ```python
   for tf_item in tfs:
       stats = StatsAggregator.get_stats(self.redis, strategy, symbol, tf_item)
       if not stats:
           continue

       # Агрегация счетчиков
       combined["total_trades"] += self._to_int(stats.get("total_trades"))
       combined["wins"] += self._to_int(stats.get("wins"))
       combined["losses"] += self._to_int(stats.get("losses"))
       combined["total_pnl"] += self._to_float(stats.get("total_pnl"))

       # ... остальные метрики
   ```

4. **Расчет производных метрик**
   ```python
   total = combined["total_trades"]
   if total > 0:
       combined["winrate"] = round(combined["wins"] / total * 100.0, 2)
       combined["avg_pnl"] = round(combined["total_pnl"] / total, 2)
       combined["profit_factor"] = round(
           self._safe_div(combined["gross_profit"], combined["gross_loss"], 0.0), 3
       )
   ```

### Получение отчета по всем стратегиям (get_all_strategies_report)

```python
def get_all_strategies_report(self) -> Dict[str, Any]:
    """
    Получение сводного отчета по всем стратегиям.
    """
    from services.stats_aggregator import StatsAggregator

    try:
        all_stats = StatsAggregator.get_all_stats_summary(self.redis)

        # Группировка по стратегиям
        by_strategy = {}
        for stat_key, stat_data in all_stats.items():
            strategy = stat_key.split(":")[0]
            if strategy not in by_strategy:
                by_strategy[strategy] = []
            by_strategy[strategy].append(stat_data)

        # Агрегация по стратегиям
        result = {}
        for strategy, stats_list in by_strategy.items():
            result[strategy] = self._aggregate_strategy_stats(stats_list)

        return result

    except Exception as e:
        self.logger.error(f"Error getting all strategies report: {e}")
        return {}
```

### Генерация HTML отчета (_build_html_report)

```python
def _build_html_report(self, data: Dict[str, Any], report_type: str = "strategy") -> str:
    """
    Генерация HTML отчета из данных.
    """

    # Шаблон HTML с CSS
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <title>Trading Report</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            .metric { display: inline-block; margin: 10px; padding: 10px; border: 1px solid #ccc; }
            .positive { color: green; }
            .negative { color: red; }
            .neutral { color: black; }
        </style>
    </head>
    <body>
        <h1>Trading Report - {strategy}</h1>
        <div class="metrics">
            <div class="metric">
                <strong>Total Trades:</strong> {total_trades}
            </div>
            <div class="metric">
                <strong>Win Rate:</strong> <span class="{winrate_class}">{winrate}%</span>
            </div>
            <div class="metric">
                <strong>Total P&L:</strong> <span class="{pnl_class}">{total_pnl}</span>
            </div>
            <!-- ... остальные метрики -->
        </div>
        <!-- ... таблицы и графики -->
    </body>
    </html>
    """

    # Форматирование данных
    formatted_data = self._format_report_data(data)

    return html_template.format(**formatted_data)
```

### Отправка в Telegram (send_telegram_message)

```python
def send_telegram_message(self, message: str, message_type: str = "report",
                         chat_id: Optional[str] = None) -> bool:
    """
    Отправка сообщения в Telegram.

    Args:
        message: Текст сообщения (может содержать HTML)
        message_type: Тип сообщения для маршрутизации
        chat_id: Опциональный chat ID

    Returns:
        True при успешной отправке
    """
```

#### Логика отправки:

1. **Формирование payload**
   ```python
   payload = {
       "type": message_type,
       "message": message,
       "timestamp": int(time.time() * 1000),
       "chat_id": chat_id or self.telegram_config.get("chat_id")
   }
   ```

2. **Отправка в Redis stream**
   ```python
   try:
       self.redis.xadd("notify:telegram", {
           "type": message_type,
           "data": json.dumps(payload)
       })
       return True
   except Exception as e:
       self.logger.error(f"Failed to send Telegram message: {e}")
       return False
   ```

### Уведомление о закрытии сделки (notify_trade_closed)

```python
def notify_trade_closed(self, trade_summary: Dict[str, Any]):
    """
    Отправка уведомления о закрытии сделки.
    """

    # Форматирование сообщения
    message = self._format_trade_closed_message(trade_summary)

    # Определение типа уведомления
    pnl = trade_summary.get("total_pnl", 0)
    msg_type = "trade_win" if pnl > 0 else "trade_loss" if pnl < 0 else "trade_be"

    # Отправка
    self.send_telegram_message(message, msg_type)
```

#### Форматирование сообщения:

```python
def _format_trade_closed_message(self, trade: Dict[str, Any]) -> str:
    """Форматирование сообщения о закрытой сделке."""

    pnl = trade.get("total_pnl", 0)
    pnl_class = "positive" if pnl > 0 else "negative" if pnl < 0 else "neutral"
    pnl_formatted = self._fmt_money(pnl)

    return f"""
🎯 Trade Closed

📊 {trade.get('symbol')} {trade.get('strategy')}
{'📈' if pnl > 0 else '📉'} {trade.get('direction')} {trade.get('lot')} lots

💰 P&L: <b>{pnl_formatted}</b>
⏱️ Duration: {self._ms_to_hhmm(trade.get('duration_ms', 0))}

Entry: {trade.get('entry_price')}
Exit: {trade.get('close_price')}
Reason: {trade.get('close_reason')}
    """.strip()
```

### Ежедневная сводка (send_daily_summary)

```python
def send_daily_summary(self, include_sources: bool = True):
    """
    Отправка ежедневной сводки по всем стратегиям.
    """

    try:
        # Получение данных за последние 24 часа
        yesterday = datetime.now() - timedelta(days=1)
        date_str = yesterday.strftime("%Y-%m-%d")

        # Агрегация по всем стратегиям
        all_strategies = self.get_all_strategies_report()

        # Форматирование сводки
        summary_html = self._build_daily_summary_html(all_strategies, date_str)

        # Отправка
        self.send_telegram_message(summary_html, "daily_summary")

    except Exception as e:
        self.logger.error(f"Error sending daily summary: {e}")
```

## Метрики и аналитика

### Основные метрики отчета

#### Счетчики сделок
- `total_trades`: Общее количество сделок
- `wins`: Прибыльные сделки
- `losses`: Убыточные сделки
- `breakevens`: Сделки в ноль

#### Финансовые метрики
- `total_pnl`: Общий P&L
- `total_pnl_gross`: P&L без учета комиссий
- `total_fees`: Сумма комиссий
- `gross_profit`: Сумма профитов
- `gross_loss`: Сумма убытков

#### Производные метрики
- `winrate`: Процент прибыльных сделок
- `avg_pnl`: Средний P&L на сделку
- `profit_factor`: Коэффициент профитности
- `avg_r`: Средний R-multiple
- `avg_duration_ms`: Средняя длительность сделки

#### TP/SL метрики
- `tp1_hits`, `tp2_hits`, `tp3_hits`: Попадания в TP уровни
- `tp1_then_sl`, `tp2_then_sl`, `tp3_then_sl`: TP с последующим SL

#### Trailing метрики
- `trailing_started`: Количество позиций с trailing
- `trailing_stop_hits`: Попадания в trailing SL
- `trailing_effectiveness`: Эффективность trailing (%)

### Анализ эффективности

```python
def _analyze_performance(self, stats: Dict[str, Any]) -> Dict[str, Any]:
    """Анализ эффективности стратегии."""

    analysis = {}

    # Risk-adjusted metrics
    analysis["sharpe_ratio"] = self._calculate_sharpe_ratio(stats)
    analysis["sortino_ratio"] = self._calculate_sortino_ratio(stats)
    analysis["calmar_ratio"] = self._calculate_calmar_ratio(stats)

    # Consistency metrics
    analysis["win_streak_max"] = stats.get("win_streak_max", 0)
    analysis["loss_streak_max"] = stats.get("loss_streak_max", 0)

    # TP efficiency
    tp_levels = [stats.get(f"tp{i}_hits", 0) for i in range(1, 4)]
    analysis["tp_distribution"] = self._analyze_tp_distribution(tp_levels)

    return analysis
```

## Форматирование и представление

### Форматирование чисел

```python
def _fmt_money(self, v: float, digits: int = 2) -> str:
    """Форматирование денежных сумм."""
    if abs(v) >= 1000:
        return ",.0f"
    elif abs(v) >= 1:
        return ",.2f"
    else:
        return ",.4f"

def _fmt_pct(self, v: float, digits: int = 2) -> str:
    """Форматирование процентов."""
    return ",.2f"

def _fmt_rate(self, hits: int, total: int, digits: int = 1) -> str:
    """Форматирование коэффициентов."""
    if total == 0:
        return "0.0"
    return ",.1f"
```

### HTML генерация

```python
def _build_html_report(self, data: Dict[str, Any], report_type: str = "strategy") -> str:
    """Генерация HTML отчета с встроенными стилями."""

    # Inline CSS для мобильной адаптивности
    css = """
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
        .container { max-width: 800px; margin: 0 auto; background: white; border-radius: 10px; padding: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .metric-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 20px 0; }
        .metric { padding: 15px; border-radius: 8px; text-align: center; }
        .metric.positive { background: #e8f5e8; border-left: 4px solid #4caf50; }
        .metric.negative { background: #ffebee; border-left: 4px solid #f44336; }
        .metric.neutral { background: #f5f5f5; border-left: 4px solid #9e9e9e; }
        .value { font-size: 24px; font-weight: bold; margin: 5px 0; }
        .label { font-size: 14px; color: #666; text-transform: uppercase; letter-spacing: 0.5px; }
    </style>
    """

    # Структура HTML
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>{css}</head>
    <body>
        <div class="container">
            <h1>Trading Report - {data.get('strategy', 'Unknown')}</h1>
            <div class="metric-grid">
                {self._build_metric_cards(data)}
            </div>
            {self._build_charts_section(data)}
        </div>
    </body>
    </html>
    """

    return html
```

## Конфигурационные параметры

### Переменные окружения

**Telegram:**
- `TELEGRAM_BOT_TOKEN`: Токен бота
- `TELEGRAM_CHAT_ID`: ID чата для уведомлений
- `TELEGRAM_API_URL`: Базовый URL API (опционально)

**Форматирование:**
- `REPORT_CURRENCY`: Валюта для отображения (default: USD)
- `REPORT_TIMEZONE`: Часовой пояс (default: UTC)
- `REPORT_LOCALE`: Локаль для форматирования (default: en_US)

### Структура конфигурации

```python
telegram_config = {
    "bot_token": "123456789:ABCdefGHIjklMNOpqrsTUVwxyz",
    "chat_id": "-1001234567890",
    "api_url": "https://api.telegram.org/bot",
    "timeout": 30,
    "retry_count": 3
}
```

## Производительность и оптимизации

### Кеширование

1. **Stats caching**: Кеш агрегированных статистик
2. **Template caching**: Кеш HTML шаблонов
3. **Connection pooling**: Пул соединений к Redis

### Batch операции

```python
def _batch_get_stats(self, keys: List[str]) -> Dict[str, Dict[str, Any]]:
    """Пакетное получение статистик из Redis."""
    pipeline = self.redis.pipeline()
    for key in keys:
        pipeline.hgetall(key)

    results = pipeline.execute()
    return dict(zip(keys, results))
```

### Асинхронная отправка

```python
async def send_telegram_async(self, message: str, message_type: str = "report") -> bool:
    """Асинхронная отправка в Telegram для высокой производительности."""

    # Использование aiohttp для асинхронных HTTP запросов
    async with aiohttp.ClientSession() as session:
        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": "HTML"
        }

        for attempt in range(self.retry_count):
            try:
                async with session.post(self.api_url, json=payload, timeout=self.timeout) as response:
                    if response.status == 200:
                        return True
            except Exception as e:
                self.logger.warning(f"Telegram send attempt {attempt + 1} failed: {e}")
                await asyncio.sleep(2 ** attempt)  # Exponential backoff

    return False
```

## Обработка ошибок

### Fail-Open стратегия

1. **Redis недоступен**: Использование кешированных данных или пустых значений
2. **Telegram недоступен**: Логирование, продолжение работы
3. **Некорректные данные**: Валидация, использование дефолтных значений
4. **Timeout'ы**: Retry с exponential backoff

### Валидация данных

```python
def _validate_stats_data(self, stats: Dict[str, Any]) -> Dict[str, Any]:
    """Валидация и нормализация статистических данных."""

    validated = {}

    # Проверка и нормализация основных полей
    validated["total_trades"] = max(0, self._to_int(stats.get("total_trades", 0)))
    validated["wins"] = max(0, self._to_int(stats.get("wins", 0)))
    validated["losses"] = max(0, self._to_int(stats.get("losses", 0)))

    # Валидация winrate
    total = validated["total_trades"]
    if total > 0:
        wins = validated["wins"]
        validated["winrate"] = round((wins / total) * 100, 2)
    else:
        validated["winrate"] = 0.0

    return validated
```

## Типичные проблемы и решения

### Проблема: Отчеты не приходят в Telegram
**Симптомы**: Сообщения не доходят до пользователей
**Решения**:
- Проверить TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID
- Проверить доступность Telegram API
- Проверить лимиты API (не более 30 сообщений в секунду)
- Добавить retry логику

### Проблема: Некорректные метрики в отчетах
**Симптомы**: Отрицательные значения, невозможные проценты
**Решения**:
- Добавить валидацию входных данных
- Использовать _safe_div для деления
- Добавить проверки на отрицательные значения
- Логировать подозрительные данные

### Проблема: Медленная генерация отчетов
**Симптомы**: Таймауты при генерации больших отчетов
**Решения**:
- Оптимизировать запросы к Redis (batch operations)
- Кешировать часто используемые данные
- Генерировать отчеты асинхронно
- Разбить большие отчеты на части

### Проблема: HTML отчеты не отображаются корректно
**Симптомы**: Нарушенная верстка, некорректные стили
**Решения**:
- Использовать inline CSS вместо внешних стилей
- Тестировать на разных устройствах/браузерах
- Валидировать HTML
- Использовать простые, надежные шаблоны

## Заключение

ReportingService предоставляет комплексное решение для аналитики и коммуникации результатов торговых стратегий. Его гибкая архитектура позволяет адаптироваться к различным требованиям по формату и частоте отчетности, обеспечивая надежную доставку критичной информации трейдерам и аналитикам.
