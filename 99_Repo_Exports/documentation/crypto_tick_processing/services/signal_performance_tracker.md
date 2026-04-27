# SignalPerformanceTracker - Детальная документация

## Обзор

**SignalPerformanceTracker** - многопоточный оркестратор системы отслеживания производительности сигналов. Основной компонент downstream пайплайна, который читает сигналы из Redis Streams, создает виртуальные позиции, отслеживает их P&L в реальном времени и генерирует отчеты.

**Расположение**: `python-worker/services/signal_performance_tracker.py`

**Назначение**: Координация всех аспектов жизненного цикла сигналов от генерации до закрытия и отчетности.

## Архитектурные принципы

### 1. Многопоточная архитектура
- **Поток сигналов**: Чтение и обработка входящих сигналов
- **Поток тиков**: Отслеживание P&L в реальном времени
- **Поток периодических задач**: Генерация отчетов и очистка
- **Поток событий**: Обработка trailing updates

### 2. Actor-based дизайн
- **ShardedSerialExecutor**: Гарантирует последовательную обработку по символам
- **TradeMonitorActorRuntime**: Изолированное состояние на шард
- **Отсутствие глобальных блокировок**: Каждый шард независим

### 3. Fail-Open архитектура
- Продолжение работы при сбоях отдельных компонентов
- Graceful degradation при недоступности PostgreSQL
- Fallback механизмы для всех критических функций

## Детальная структура класса

### Основные атрибуты

#### Конфигурация и подключения
```python
self.config: Dict[str, Any]                    # Конфигурация трекера
self.redis_url: str                           # URL основного Redis
self.redis_ticks_url: Optional[str]           # URL Redis для тиков (опционально)
self.redis: redis.Redis                       # Клиент основного Redis
self.redis_ticks: redis.Redis                 # Клиент Redis для тиков
```

#### Компоненты системы
```python
self.trade_monitor: TradeMonitorService       # Сервис мониторинга позиций
self.reporting_service: ReportingService      # Сервис отчетов
self.periodic_reporter: EmbeddedPeriodicReporter  # Периодический репортёр
self.trailing_orchestrator: TP1TrailingOrchestrator  # Оркестратор трейлинга
self.regime_guard: Optional[RegimeGuardService]     # Контроль качества сигналов
self.quality_monitor: SignalQualityMonitor    # Монитор качества
self.signal_logger: Optional[SignalLogger]    # Логгер сигналов в TimescaleDB
```

#### Исполнители и рантаймы
```python
self._exec: Optional[ShardedSerialExecutor]   # Sharded executor для сериализации
self.tm_runtime: Optional[TradeMonitorActorRuntime]  # Actor runtime для TM
self._use_symbol_exec: bool                   # Флаг использования executor
self._use_actor_runtime: bool                 # Флаг использования actor runtime
```

#### Потоки и состояние
```python
self.running: bool                            # Флаг работы
self.threads: List[threading.Thread]          # Список активных потоков
```

#### Мониторинг здоровья
```python
self.health_key: str                          # Ключ здоровья в Redis
self.health_ttl: int                          # TTL здоровья (сек)
```

#### Конфигурация стримов
```python
self.symbols: List[str]                       # Список символов для отслеживания
self.strategies: List[str]                    # Список стратегий для отслеживания
self.streams: Dict[str, str]                  # Мэппинг символ-стрим
```

## Детальная логика методов

### Инициализация (__init__)

#### Этапы инициализации:

1. **Загрузка конфигурации**
   ```python
   self.config = config or self._load_config_from_env()
   ```

2. **Настройка Redis подключений**
   ```python
   # Основное подключение
   self.redis = redis.from_url(self.redis_url, decode_responses=True)

   # Отдельное подключение для тиков (опционально)
   if self.redis_ticks_url:
       self.redis_ticks = redis.from_url(self.redis_ticks_url, decode_responses=True)
   else:
       self.redis_ticks = self.redis
   ```

3. **Инициализация компонентов**
   ```python
   # Trade Monitor с regime guard
   self.trade_monitor = TradeMonitorService(
       redis_url=self.redis_url,
       config=self.config,
       regime_guard=self.regime_guard
   )

   # Reporting Service
   self.reporting_service = ReportingService(
       redis_url=self.redis_url,
       telegram_config=self.config.get("telegram")
   )

   # Trailing orchestrator
   self.trailing_orchestrator = TP1TrailingOrchestrator(redis_client=self.redis)
   ```

4. **Настройка опциональных компонентов**
   ```python
   # PostgreSQL компоненты (с обработкой ошибок)
   try:
       self.regime_guard = RegimeGuardService(...)
       self.signal_logger = SignalLogger(...)
   except Exception as e:
       self.logger.warning(f"PostgreSQL components unavailable: {e}")
   ```

5. **Инициализация executor и runtime**
   ```python
   if self._use_symbol_exec:
       self._exec = ShardedSerialExecutor(
           shards=self._exec_shards,
           queue_max=self._exec_queue_max,
           ...
       )

   if self._use_actor_runtime:
       self.tm_runtime = TradeMonitorActorRuntime(...)
   ```

### Запуск (start)

```python
def start(self) -> None:
    """Запуск всех потоков."""
    if self.running:
        return

    self.running = True

    # Создание consumer groups
    self._create_consumer_groups()

    # Инициализация executor'ов
    self._init_executors()

    # Запуск потоков
    self._start_threads()

    self.logger.info(f"✅ All threads started ({len(self.threads)} threads)")
```

#### Создание Consumer Groups (_create_consumer_groups)

```python
def _create_consumer_groups(self) -> None:
    """Создание consumer groups для всех необходимых стримов."""
    for symbol in self.symbols:
        for strategy in self.strategies:
            stream_key = f"signals:{strategy}:{symbol}"
            group_name = f"{strategy}-{symbol}-group"

            try:
                self.redis.xgroup_create(
                    stream_key, group_name, mkstream=True
                )
                self.logger.info(f"✅ Consumer group created: {stream_key} -> {group_name}")
            except redis.ResponseError as e:
                if "BUSYGROUP" in str(e):
                    self.logger.debug(f"Consumer group already exists: {group_name}")
                else:
                    self.logger.error(f"Failed to create consumer group {group_name}: {e}")
```

### Поток сигналов (_signals_listener_thread)

**Основной цикл обработки сигналов:**

```python
def _signals_listener_thread(self) -> None:
    """Основной поток чтения сигналов."""
    while self.running:
        try:
            # Чтение из всех стримов
            streams = {stream: ">" for stream in self.streams.values()}

            # XREADGROUP с блокировкой
            response = self.redis.xreadgroup(
                groupname=self.consumer_group,
                consumername=self.consumer_name,
                streams=streams,
                block=1000,  # 1 секунда
                count=10     # batch size
            )

            if response:
                for stream_key, messages in response:
                    for msg_id, msg_data in messages:
                        try:
                            self._process_signal(stream_key, msg_id, msg_data)
                        except Exception as e:
                            self.logger.error(f"Error processing signal {msg_id}: {e}")
                        finally:
                            # Важно: ACK всегда в finally
                            self.redis.xack(stream_key, self.consumer_group, msg_id)

        except Exception as e:
            self.logger.error(f"Error in signals listener: {e}")
            time.sleep(1)
```

### Обработка сигнала (_process_signal)

```python
def _process_signal(self, stream_key: str, msg_id: str, msg_data: Dict[str, Any]) -> None:
    """Обработка одного сигнала."""

    # Извлечение символа из ключа стрима
    symbol = self._extract_symbol_from_stream(stream_key)

    # Создание позиции через executor (гарантирует последовательность)
    if self._use_symbol_exec:
        self._exec.submit(
            symbol,
            self._handle_signal_for_symbol,
            symbol, msg_data, msg_id
        )
    else:
        # Fallback: прямая обработка
        self._handle_signal_for_symbol(symbol, msg_data, msg_id)
```

### Обработка сигнала для символа (_handle_signal_for_symbol)

```python
def _handle_signal_for_symbol(self, symbol: str, signal_data: Dict[str, Any], msg_id: str) -> None:
    """Обработка сигнала для конкретного символа."""

    try:
        # Валидация сигнала
        if not self._validate_signal(signal_data):
            return

        # Проверка качества через regime guard
        if self.regime_guard and not self.regime_guard.should_trade(signal_data):
            self.logger.info(f"Signal vetoed by regime guard: {signal_data.get('signal_id')}")
            return

        # Создание позиции
        position = self.trade_monitor.create_position(signal_data)

        # Логирование сигнала
        if self.signal_logger:
            self.signal_logger.log_signal(signal_data)

        # Обновление метрик качества
        self.quality_monitor.update_metrics(signal_data)

        self.logger.info(f"✅ Signal processed: {signal_data.get('signal_id')} -> position {position.id}")

    except Exception as e:
        self.logger.error(f"Error handling signal {msg_id}: {e}")
        # Не поднимаем исключение выше - продолжаем обработку других сигналов
```

### Поток тиков (_ticks_listener_thread)

```python
def _ticks_listener_thread(self) -> None:
    """Поток чтения тиков для обновления P&L."""
    while self.running:
        try:
            # Чтение тиков по всем символам
            streams = {f"stream:tick_{symbol}": ">" for symbol in self.symbols}

            response = self.redis_ticks.xreadgroup(
                groupname="ticks-consumer-group",
                consumername="signal-tracker",
                streams=streams,
                block=1000,
                count=50
            )

            if response:
                for stream_key, messages in response:
                    symbol = stream_key.split("_", 1)[1]  # tick_BTCUSDT -> BTCUSDT

                    for msg_id, tick_data in messages:
                        try:
                            # Обновление P&L через executor
                            if self._use_symbol_exec:
                                self._exec.submit(
                                    symbol,
                                    self._update_pnl_for_symbol,
                                    symbol, tick_data
                                )
                            else:
                                self._update_pnl_for_symbol(symbol, tick_data)

                        finally:
                            self.redis_ticks.xack(stream_key, "ticks-consumer-group", msg_id)

        except Exception as e:
            self.logger.error(f"Error in ticks listener: {e}")
            time.sleep(1)
```

### Обновление P&L (_update_pnl_for_symbol)

```python
def _update_pnl_for_symbol(self, symbol: str, tick_data: Dict[str, Any]) -> None:
    """Обновление P&L для всех позиций символа."""

    try:
        # Получение цены из тика
        price = float(tick_data.get("price") or tick_data.get("last"))

        # Обновление всех активных позиций
        updated_positions = self.trade_monitor.update_positions(symbol, price)

        # Проверка условий закрытия
        for position in updated_positions:
            if self._should_close_position(position):
                self._close_position(position)

        # Обновление метрик качества
        self.quality_monitor.update_tick_metrics(symbol, tick_data)

    except Exception as e:
        self.logger.error(f"Error updating P&L for {symbol}: {e}")
```

### Поток периодических задач (_periodic_tasks_thread)

```python
def _periodic_tasks_thread(self) -> None:
    """Поток периодических задач (отчеты, очистка)."""
    while self.running:
        try:
            # Генерация отчетов каждые 3 часа
            current_hour = time.time() // 3600
            if current_hour % 3 == 0 and current_hour != self.last_report_hour:
                self._generate_periodic_report()
                self.last_report_hour = current_hour

            # Очистка старых данных каждый час
            current_hour = time.time() // 3600
            if current_hour != self.last_cleanup_hour:
                self._cleanup_old_data()
                self.last_cleanup_hour = current_hour

            # Обновление здоровья
            self._update_health_status()

            time.sleep(60)  # Проверка каждую минуту

        except Exception as e:
            self.logger.error(f"Error in periodic tasks: {e}")
            time.sleep(60)
```

### Генерация отчетов (_generate_periodic_report)

```python
def _generate_periodic_report(self) -> None:
    """Генерация и отправка периодического отчета."""

    try:
        # Сбор данных из всех компонентов
        report_data = {
            "positions": self.trade_monitor.get_summary(),
            "quality": self.quality_monitor.get_quality_report(),
            "regime": self.regime_guard.get_regime_status() if self.regime_guard else None,
            "timestamp": int(time.time() * 1000)
        }

        # Генерация отчета
        report_html = self.reporting_service.generate_report(report_data)

        # Отправка в Telegram
        success = self.reporting_service.send_telegram_notification(
            report_html,
            message_type="periodic_report"
        )

        if success:
            self.logger.info("✅ Periodic report sent to Telegram")
        else:
            self.logger.error("❌ Failed to send periodic report")

    except Exception as e:
        self.logger.error(f"Error generating periodic report: {e}")
```

## Конфигурационные параметры

### Переменные окружения

**Redis подключения:**
- `REDIS_URL`: URL основного Redis (default: redis://redis-worker-1-worker-1:6379/0)
- `REDIS_TICKS_URL`: URL Redis для тиков (опционально)

**Исполнители:**
- `USE_SYMBOL_EXECUTOR`: Использовать ShardedSerialExecutor (default: 1)
- `SYMBOL_EXECUTOR_SHARDS`: Количество шардов (default: 8)
- `SYMBOL_EXECUTOR_QUEUE_MAX`: Максимальный размер очереди (default: 20000)
- `USE_TM_ACTOR_RUNTIME`: Использовать actor runtime (default: 1)

**Мониторинг:**
- `TRACKER_HEALTH_KEY`: Ключ здоровья в Redis (default: health:signal_performance_tracker)
- `TRACKER_HEALTH_TTL`: TTL здоровья в сек (default: 300)

**PostgreSQL:**
- `DATABASE_URL`: DSN для TimescaleDB
- `TRADES_DB_DSN`: DSN для analytics базы
- `BASELINE_HORIZON_DAYS`: Горизонт для baseline (default: 180)

### Конфигурация символов и стратегий

```python
streams_cfg = self.config.get("streams", {})
symbols_cfg = streams_cfg.get("symbols", ["XAUUSD", "BTCUSDT", "ETHUSDT"])
strategies_cfg = streams_cfg.get("strategies", ["orderflow", "ta", "aggregated"])
```

## Мониторинг и метрики

### Health checks

```python
def _update_health_status(self) -> None:
    """Обновление статуса здоровья в Redis."""
    health_data = {
        "status": "healthy",
        "timestamp": int(time.time()),
        "threads": len(self.threads),
        "active_positions": len(self.trade_monitor.get_active_positions()),
        "uptime": time.time() - self.start_time
    }

    try:
        self.redis.setex(self.health_key, self.health_ttl, json.dumps(health_data))
    except Exception as e:
        self.logger.error(f"Failed to update health status: {e}")
```

### Метрики производительности

**Через quality_monitor:**
- Win rate по символам/стратегиям
- Average holding time
- Profit factor
- Maximum drawdown

**Через signal_logger (TimescaleDB):**
- L3 метрики по сигналам
- Временные ряды производительности
- Корреляция с рыночными условиями

## Обработка ошибок

### Fail-Open стратегия

1. **PostgreSQL недоступен**: Продолжение работы без regime guard и signal logger
2. **Redis недоступен**: Логирование ошибок, продолжение работы
3. **Ошибка в обработке сигнала**: Логирование, продолжение обработки других сигналов
4. **Ошибка в позициях**: Fallback на простое отслеживание без сложной логики

### Graceful shutdown

```python
def stop(self) -> None:
    """Остановка всех потоков."""
    self.logger.info("⚠️ Stopping Signal Performance Tracker...")
    self.running = False

    # Остановка executor
    if self._exec:
        self._exec.shutdown()

    # Остановка actor runtime
    if self.tm_runtime:
        self.tm_runtime.shutdown()

    # Ожидание завершения потоков
    for thread in self.threads:
        thread.join(timeout=10)

    self.logger.info("✅ All components stopped")
```

## Производительность и оптимизации

### Масштабирование

**Горизонтальное:**
- Запуск нескольких инстансов SignalPerformanceTracker
- Consumer groups автоматически балансируют нагрузку
- Каждый инстанс может обрабатывать подмножество символов

**Вертикальное:**
- Увеличение количества шардов в ShardedSerialExecutor
- Увеличение размеров очередей
- Оптимизация Redis подключений

### Оптимизации

1. **ShardedSerialExecutor**: Предотвращает race conditions при обработке одного символа
2. **Actor Runtime**: Локальное состояние на шард, отсутствие глобальных блокировок
3. **Batch processing**: Чтение множественных сообщений за раз
4. **Lazy evaluation**: Отложенные вычисления для неактивных символов

## Интеграция с другими компонентами

### TradeMonitorService
- Создание и управление виртуальными позициями
- Расчет P&L в реальном времени
- Управление lifecycle позиций

### ReportingService
- Генерация HTML отчетов
- Отправка в Telegram
- Агрегация статистик

### RegimeGuardService
- Оценка рыночных условий
- Вето сигналов в неблагоприятные периоды
- Адаптивные пороги

### SignalLogger
- Сохранение сигналов в TimescaleDB
- L3 метрики и анализ
- Историческая аналитика

## Типичные проблемы и решения

### Проблема: Consumer group lag растет
**Симптомы**: Задержки в обработке сигналов
**Решения**:
- Увеличить batch size в XREADGROUP
- Добавить больше инстансов трекера
- Оптимизировать обработку сигналов

### Проблема: Память растет
**Симптомы**: OOM ошибки, замедление работы
**Решения**:
- Уменьшить размеры очередей executor
- Реализовать cleanup для старых позиций
- Проверить утечки в actor runtime

### Проблема: PostgreSQL timeouts
**Симптомы**: Ошибки подключения к БД
**Решения**:
- Настроить connection pooling
- Добавить retry логику
- Перейти в fail-open режим без PostgreSQL

### Проблема: Race conditions
**Симптомы**: Несогласованные состояния позиций
**Решения**:
- Убедиться что ShardedSerialExecutor включен
- Проверить правильность shard key (символ)
- Добавить дополнительные блокировки при необходимости

## Мониторинг и алертинг

### Ключевые метрики

1. **Обработка сигналов:**
   - Количество обработанных сигналов в минуту
   - Среднее время обработки сигнала
   - Процент ошибок обработки

2. **Позиции:**
   - Количество активных позиций
   - Средний holding time
   - Win rate в реальном времени

3. **Производительность:**
   - Размер очередей executor
   - Загрузка потоков
   - Redis connection pool usage

### Алерты

- Consumer group lag > 1000 сообщений
- Очереди executor переполнены
- PostgreSQL недоступен > 5 минут
- Win rate ниже порога
- Количество активных позиций > лимит

## Заключение

SignalPerformanceTracker представляет собой комплексную систему для отслеживания жизненного цикла сигналов. Его многопоточная actor-based архитектура обеспечивает высокую производительность и надежность при обработке больших объемов данных в реальном времени.
