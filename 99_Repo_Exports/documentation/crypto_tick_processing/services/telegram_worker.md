# Telegram Worker - Детальная документация

## Обзор

**Telegram Worker** - высокопроизводительный многопоточный сервис для чтения сообщений из Telegram каналов и публикации их в Redis Streams для дальнейшей обработки. Использует Telethon библиотеку для асинхронного взаимодействия с Telegram API.

**Расположение**: `telegram-worker/`

**Назначение**: Мост между Telegram каналами и внутренней системой обработки сигналов, обеспечивающий надежный и эффективный сбор данных из мессенджера.

## Архитектурные принципы

### 1. Многопоточная архитектура
- **Главный поток**: Управление жизненным циклом
- **Поток авторизации**: Инициализация Telethon клиента
- **Потоки обработки**: Чтение и парсинг сообщений
- **Поток мониторинга**: Отслеживание здоровья и метрик

### 2. Fault Tolerance
- **Автоматические переподключения**: При сетевых ошибках
- **Graceful degradation**: Продолжение работы при частичных сбоях
- **Rate limiting**: Соблюдение лимитов Telegram API

### 3. High Performance
- **Async I/O**: Асинхронные операции с Telegram API
- **Bounded queues**: Предотвращение OOM при пиковых нагрузках
- **Connection pooling**: Оптимизация сетевых подключений

## Детальная структура класса

### Основные атрибуты

#### Telegram клиенты и подключения
```python
self.main_client: Optional[TelegramClient]       # Основной авторизованный клиент
self.channel_poller: Optional[ChannelPoller]     # Poller для чтения каналов
self.channel_entities: List[Any]                 # Entities подписанных каналов
```

#### Очереди и потоки
```python
self.message_queue: asyncio.Queue                 # Очередь сообщений (maxsize=1000)
self.thread_pool: Optional[ThreadPoolExecutor]   # Пул для синхронных операций
self.channel_groups: List[ChannelGroup]          # Группы каналов для обработки
```

#### Мониторинг и здоровье
```python
self.stats: Dict[str, Any]                       # Статистика работы
self.last_message_time: float                    # Timestamp последнего сообщения
self.last_health_check: float                    # Timestamp последней проверки здоровья
self.connection_errors: int                      # Счетчик ошибок подключения
self.max_connection_errors: int                  # Максимум ошибок подключения
```

#### Компоненты
```python
self.status_checker: ChannelStatusChecker        # Проверка статуса каналов
self.channel_monitor: ChannelMonitor             # Мониторинг каналов
self.alert_system: AlertSystem                   # Система алертов
```

## Детальная логика методов

### Инициализация (__init__)

#### Этапы инициализации:

1. **Загрузка настроек**
   ```python
   self.settings = load_settings()
   self.redis = redis.Redis.from_url(self.settings.redis_url, decode_responses=True)
   ```

2. **Инициализация очередей и структур**
   ```python
   self.message_queue = asyncio.Queue(maxsize=1000)
   self.channel_groups = []
   self.stats = {
       'total_messages': 0,
       'parsed_messages': 0,
       'errors': 0,
       'start_time': time.time(),
       'connection_status': 'disconnected'
   }
   ```

3. **Настройка компонентов**
   ```python
   self.status_checker = ChannelStatusChecker(self.redis, self.logger)
   self.channel_monitor = ChannelMonitor(self.redis, self.logger)
   self.alert_system = AlertSystem(...)
   ```

4. **Настройка сигналов**
   ```python
   signal.signal(signal.SIGINT, self._signal_handler)
   signal.signal(signal.SIGTERM, self._signal_handler)
   ```

### Запуск (start)

```python
async def start(self):
    """Основной метод запуска worker'а."""

    self.logger.info("🚀 Запуск MultithreadedTelegramWorker")

    try:
        # 1. Инициализация основного клиента
        await self.initialize_main_client()

        # 2. Получение списка каналов
        channels = await self._get_channels_list()

        # 3. Создание групп каналов
        self.channel_groups = self._create_channel_groups(channels)

        # 4. Запуск потоков обработки
        await self._start_processing_threads()

        # 5. Запуск polling
        await self._start_channel_polling()

        # 6. Запуск мониторинга
        await self._start_monitoring_loop()

    except Exception as e:
        self.logger.error(f"❌ Ошибка запуска: {e}")
        raise
```

#### Инициализация основного клиента (initialize_main_client)

```python
async def initialize_main_client(self):
    """Инициализация и авторизация Telegram клиента."""

    # Получение credentials
    api_id = os.getenv('TG_API_ID')
    api_hash = os.getenv('TG_API_HASH')
    phone = os.getenv('TG_PHONE')

    if not all([api_id, api_hash, phone]):
        raise ValueError("Missing TG_API_ID, TG_API_HASH, or TG_PHONE")

    # Создание клиента
    self.main_client = TelegramClient(
        'telegram_worker',
        int(api_id),
        api_hash,
        device_model="TelegramWorker",
        system_version="1.0"
    )

    # Авторизация
    await self.main_client.start(phone=phone)

    # Проверка авторизации
    if not await self.main_client.is_user_authorized():
        raise RuntimeError("Failed to authorize Telegram client")

    self.logger.info("✅ Telegram клиент авторизован")
```

### Обработка сообщений

#### Channel Polling

```python
async def _start_channel_polling(self):
    """Запуск polling сообщений из каналов."""

    # Создание poller'а
    self.channel_poller = ChannelPoller(
        client=self.main_client,
        channels=self.channel_entities,
        message_handler=self._handle_incoming_message,
        logger=self.logger
    )

    # Запуск polling в фоне
    asyncio.create_task(self.channel_poller.start_polling())
```

#### Обработка входящего сообщения (_handle_incoming_message)

```python
async def _handle_incoming_message(self, message: Any):
    """Обработка одного сообщения из Telegram."""

    try:
        # 1. Базовая валидация
        if not message or not message.text:
            return

        # 2. Извлечение метаданных
        channel_info = self._extract_channel_info(message)
        message_data = self._extract_message_data(message)

        # 3. Публикация сырых данных
        await self._publish_raw_message(message, channel_info)

        # 4. Парсинг и публикация обработанных данных
        parsed_data = await self._parse_and_publish_message(message_data, channel_info)

        # 5. Обновление статистики
        self._update_message_stats()

        # 6. Логирование (с throttling)
        if self.message_log_counter % self.MESSAGE_LOG_INTERVAL == 0:
            self.logger.info(f"📨 Processed message from {channel_info.get('channel_name')}")

    except Exception as e:
        self.logger.error(f"Error processing message: {e}")
        self.stats['errors'] += 1
```

### Публикация в Redis Streams

#### Публикация сырых данных (_publish_raw_message)

```python
async def _publish_raw_message(self, message: Any, channel_info: Dict[str, Any]):
    """Публикация сырых данных сообщения."""

    raw_data = {
        'channel_id': channel_info.get('channel_id'),
        'channel_name': channel_info.get('channel_name'),
        'message_id': message.id,
        'timestamp': int(message.date.timestamp() * 1000),
        'text': message.text,
        'raw_message': str(message),  # Полные данные для отладки
    }

    # Публикация в stream
    self.redis.xadd('signal:telegram:raw', {
        'data': json.dumps(raw_data),
        'timestamp': str(raw_data['timestamp'])
    })
```

#### Парсинг и публикация (_parse_and_publish_message)

```python
async def _parse_and_publish_message(self, message_data: Dict[str, Any],
                                   channel_info: Dict[str, Any]) -> Dict[str, Any]:
    """Парсинг сообщения и публикация обработанных данных."""

    try:
        # Использование parse_utils для извлечения сигналов
        parsed_signals = parse_signal(message_data, channel_info)

        for signal in parsed_signals:
            # Валидация сигнала
            if self._validate_signal(signal):
                # Публикация в основной stream
                self.redis.xadd('signal:telegram:parsed', {
                    'data': json.dumps(signal),
                    'channel': channel_info.get('channel_name'),
                    'timestamp': str(signal.get('timestamp'))
                })

                self.stats['parsed_messages'] += 1

        return parsed_signals

    except Exception as e:
        self.logger.error(f"Error parsing message: {e}")
        return []
```

### Управление группами каналов

#### Создание групп каналов (_create_channel_groups)

```python
def _create_channel_groups(self, channels: List[str]) -> List[ChannelGroup]:
    """Создание групп каналов для многопоточной обработки."""

    groups = []
    channels_per_group = max(1, len(channels) // self.settings.threads_per_group)

    for i in range(0, len(channels), channels_per_group):
        group_channels = channels[i:i + channels_per_group]
        group = ChannelGroup(
            group_id=len(groups),
            channels=group_channels
        )
        groups.append(group)

    return groups
```

#### Запуск потоков обработки (_start_processing_threads)

```python
async def _start_processing_threads(self):
    """Запуск потоков для обработки групп каналов."""

    for group in self.channel_groups:
        # Создание потока для группы
        thread = threading.Thread(
            target=self._process_channel_group,
            args=(group,),
            name=f"ChannelGroup-{group.id}"
        )
        thread.daemon = True
        thread.start()

        group.thread_id = thread.ident
        group.status = "running"
```

### Мониторинг и здоровье

#### Health check (_perform_health_check)

```python
async def _perform_health_check(self):
    """Выполнение проверки здоровья компонентов."""

    health_status = {
        'timestamp': time.time(),
        'client_connected': self.main_client and self.main_client.is_connected(),
        'redis_connected': self._check_redis_connection(),
        'channels_active': len(self.channel_entities),
        'queue_size': self.message_queue.qsize(),
        'stats': self.stats.copy()
    }

    # Публикация статуса здоровья
    self.redis.setex(
        'health:telegram_worker',
        300,  # 5 минут TTL
        json.dumps(health_status)
    )

    # Проверка на "засыпание"
    if self._is_worker_stuck():
        await self._perform_recovery_actions()
```

#### Проверка на "засыпание" (_is_worker_stuck)

```python
def _is_worker_stuck(self) -> bool:
    """Проверка, не застрял ли worker."""

    current_time = time.time()

    # Проверка времени последнего сообщения
    if current_time - self.last_message_time > self.max_idle_time:
        return True

    # Проверка ошибок подключения
    if self.connection_errors > self.max_connection_errors:
        return True

    # Проверка размера очереди
    if self.message_queue.qsize() > self.message_queue.maxsize * 0.9:
        return True

    return False
```

#### Восстановительные действия (_perform_recovery_actions)

```python
async def _perform_recovery_actions(self):
    """Выполнение действий по восстановлению."""

    self.logger.warning("🔄 Выполняем восстановительные действия")

    # 1. Перезапуск клиента
    if self.main_client:
        await self.main_client.disconnect()
        await self.initialize_main_client()

    # 2. Очистка очередей
    while not self.message_queue.empty():
        try:
            self.message_queue.get_nowait()
        except:
            break

    # 3. Сброс счетчиков ошибок
    self.connection_errors = 0
    self.stats['reconnections'] += 1

    # 4. Перезапуск polling
    if self.channel_poller:
        await self.channel_poller.restart()
```

## Конфигурационные параметры

### Переменные окружения

**Telegram API:**
- `TG_API_ID`: API ID от Telegram
- `TG_API_HASH`: API Hash от Telegram
- `TG_PHONE`: Номер телефона для авторизации

**Redis:**
- `REDIS_URL`: URL Redis сервера

**Каналы:**
- `TELEGRAM_CHANNELS`: Список каналов для мониторинга (через запятую)
- `CHANNEL_POLL_INTERVAL`: Интервал polling в секундах

**Производительность:**
- `THREADS_PER_GROUP`: Количество потоков на группу каналов
- `MESSAGE_QUEUE_MAX_SIZE`: Максимальный размер очереди сообщений

**Мониторинг:**
- `HEALTH_CHECK_INTERVAL`: Интервал проверки здоровья
- `MAX_IDLE_TIME`: Максимальное время без сообщений
- `MAX_CONNECTION_ERRORS`: Максимум ошибок подключения

### Структура настроек

```python
settings = {
    'redis_url': 'redis://localhost:6379/0',
    'channels': ['@channel1', '@channel2', '@channel3'],
    'threads_per_group': 2,
    'poll_interval': 30,
    'health_check_interval': 60,
    'max_idle_time': 300,
    'message_queue_max_size': 1000
}
```

## Парсинг сообщений

### Parse Utils (parse_utils.py)

```python
def parse_signal(message_data: Dict[str, Any], channel_info: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Парсинг торговых сигналов из сообщения."""

    text = message_data.get('text', '').strip()
    if not text:
        return []

    signals = []

    # 1. Определение типа канала
    channel_type = detect_channel_type(channel_info)

    # 2. Выбор парсера
    parser = get_parser_for_channel(channel_type)

    # 3. Парсинг
    try:
        parsed_signals = parser.parse(text, channel_info)
        signals.extend(parsed_signals)
    except Exception as e:
        logger.error(f"Parse error: {e}")

    # 4. Валидация и нормализация
    validated_signals = []
    for signal in signals:
        if validate_signal_structure(signal):
            normalized = normalize_signal(signal)
            validated_signals.append(normalized)

    return validated_signals
```

### Типы сигналов

**Поддерживаемые форматы:**
- **Crypto signals**: BUY/SELL с ценами и таргетами
- **Forex signals**: Пары с уровнями входа/выхода
- **Options signals**: С опционами и страйками
- **News alerts**: Новости и фундаментальные данные

**Структура сигнала:**
```python
{
    'type': 'crypto_signal',
    'symbol': 'BTCUSDT',
    'direction': 'BUY',
    'entry_price': 45000.0,
    'stop_loss': 44000.0,
    'take_profit': [46000.0, 47000.0, 48000.0],
    'timestamp': 1704888000000,
    'channel': '@trading_signals',
    'confidence': 0.8,
    'raw_text': 'BUY BTCUSDT @ 45000 SL 44000 TP 46000/47000/48000'
}
```

## Производительность и оптимизации

### Оптимизации

1. **Bounded Queue**: Предотвращает OOM при flood'е сообщений
2. **Batch Processing**: Групповая обработка для снижения overhead
3. **Connection Reuse**: Переиспользование Telegram соединений
4. **Throttling**: Ограничение частоты логирования

### Метрики производительности

```python
def _update_performance_metrics(self):
    """Обновление метрик производительности."""

    runtime = time.time() - self.stats['start_time']

    metrics = {
        'messages_per_second': self.stats['total_messages'] / runtime,
        'parse_success_rate': self.stats['parsed_messages'] / max(1, self.stats['total_messages']),
        'error_rate': self.stats['errors'] / max(1, self.stats['total_messages']),
        'queue_utilization': self.message_queue.qsize() / self.message_queue.maxsize,
        'uptime_seconds': runtime
    }

    # Публикация метрик
    self.redis.hmset('metrics:telegram_worker', metrics)
```

### Масштабирование

**Горизонтальное:**
- Запуск нескольких инстансов worker'ов
- Распределение каналов между инстансами
- Load balancing через Redis

**Вертикальное:**
- Увеличение количества потоков на группу
- Увеличение размера очередей
- Оптимизация polling интервалов

## Обработка ошибок

### Fault Tolerance

1. **Network errors**: Автоматические переподключения с exponential backoff
2. **Rate limits**: Соблюдение лимитов Telegram API (30 msg/sec)
3. **Parse errors**: Продолжение обработки при ошибках парсинга
4. **Queue overflow**: Backpressure через bounded queue

### Recovery Strategies

```python
async def _handle_connection_error(self, error: Exception):
    """Обработка ошибок подключения."""

    self.connection_errors += 1

    if self.connection_errors <= self.max_connection_errors:
        # Попытка быстрого восстановления
        await self._quick_recovery()
    else:
        # Полное перезапуска
        await self._full_restart()
```

### Monitoring и Alerting

```python
def _check_alert_conditions(self):
    """Проверка условий для алертов."""

    alerts = []

    # Проверка простоя
    if time.time() - self.last_message_time > self.max_idle_time:
        alerts.append("No messages received for extended period")

    # Проверка ошибок
    if self.stats['errors'] > self.stats['total_messages'] * 0.1:
        alerts.append("High error rate detected")

    # Проверка очереди
    if self.message_queue.qsize() > self.message_queue.maxsize * 0.8:
        alerts.append("Message queue near capacity")

    # Отправка алертов
    for alert in alerts:
        self.alert_system.send_alert(alert, level="warning")
```

## Типичные проблемы и решения

### Проблема: Flood control от Telegram
**Симптомы**: Блокировка аккаунта, ошибки "Too many requests"
**Решения**:
- Уменьшить частоту polling
- Добавить delays между запросами
- Использовать прокси или несколько аккаунтов
- Перейти на вебхуки вместо polling

### Проблема: Out of memory
**Симптомы**: Рост потребления памяти, crashes
**Решения**:
- Уменьшить maxsize message_queue
- Очистить channel_entities от неактивных каналов
- Мониторить и ограничивать размер thread_pool
- Реализовать cleanup для старых сообщений

### Проблема: Парсинг ошибок
**Симптомы**: Высокий процент failed парсингов
**Решения**:
- Улучшить регулярные выражения для парсинга
- Добавить fallback парсеры
- Логировать примеры failed сообщений для анализа
- Внедрить machine learning для парсинга

### Проблема: Задержки в обработке
**Симптомы**: Рост latency от получения до публикации
**Решения**:
- Увеличить количество threads_per_group
- Оптимизировать парсинг (lazy evaluation)
- Использовать async парсинг
- Предварительная компиляция regex patterns

## Безопасность

### Защита Credentials

1. **Environment variables**: Хранение API ключей в переменных окружения
2. **No hardcoding**: Отсутствие credentials в коде
3. **Access control**: Ограничение доступа к переменным окружения

### Rate Limiting

```python
class RateLimiter:
    """Rate limiter для Telegram API."""

    def __init__(self, max_requests: int = 30, window_seconds: int = 1):
        self.max_requests = max_requests
        self.window = window_seconds
        self.requests = []

    def allow(self) -> bool:
        """Проверка разрешения на запрос."""
        now = time.time()

        # Очистка старых запросов
        self.requests = [t for t in self.requests if now - t < self.window]

        if len(self.requests) < self.max_requests:
            self.requests.append(now)
            return True

        return False
```

## Заключение

Telegram Worker предоставляет надежный и эффективный мост между Telegram каналами и системой обработки сигналов. Его многопоточная архитектура, fault tolerance и оптимизации производительности обеспечивают стабильную работу в условиях высоких нагрузок и сетевых проблем.
