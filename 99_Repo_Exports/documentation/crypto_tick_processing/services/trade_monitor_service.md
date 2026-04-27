# TradeMonitorService - Детальная документация

## Обзор

**TradeMonitorService** - высокопроизводительный сервис управления виртуальными позициями для оценки производительности торговых сигналов. Сервис создает виртуальные позиции на основе сигналов, отслеживает их P&L в реальном времени и управляет жизненным циклом от открытия до закрытия.

**Расположение**: `python-worker/services/trade_monitor.py`

**Назначение**: Имитация реальной торговли для оценки качества сигналов без финансового риска.

## Архитектурные принципы

### 1. Thread-Safe архитектура
- **Symbol-level locking**: Независимая обработка разных символов
- **RLock для deadlock prevention**: Рекурсивные блокировки
- **Actor-based isolation**: Каждый символ - независимый актор

### 2. Fail-Open дизайн
- Продолжение работы при сбоях БД
- Graceful degradation при недоступности компонентов
- Оптимистическая обработка ошибок

### 3. High-Performance оптимизации
- **Async I/O**: Отдельный thread pool для БД операций
- **Batch processing**: Групповая обработка обновлений
- **Lazy evaluation**: Отложенные вычисления

## Детальная структура класса

### Основные атрибуты

#### Блокировка и многопоточность
```python
self._lock: threading.RLock                         # Глобальная блокировка
self._use_symbol_locks: bool                        # Флаг использования symbol-level locks
self._symbol_locks_guard: threading.Lock            # Защита доступа к словарю блокировок
self._symbol_locks: Dict[str, threading.RLock]      # Per-symbol блокировки
self._db_executor: ThreadPoolExecutor               # Пул для асинхронных БД операций
```

#### Структуры данных позиций
```python
self.open_positions: Dict[str, PositionState]       # Активные позиции (pos_id -> PositionState)
self.pos_by_sid: Dict[str, str]                     # Мэппинг signal_id -> position_id
self.open_by_symbol: Dict[str, Set[str]]            # position_ids по символам
self._last_price_by_symbol: Dict[str, Tuple[int, float]]  # Последние цены (ts, price)
```

#### Репозиторий и метрики
```python
self.repo: RedisTradeRepository                     # Репозиторий для персистентности
self._metrics: Optional[Any]                        # Метрики (fail-open)
self.health_metrics: Optional[HealthMetrics]        # Health snapshots
self.regime_guard: Optional[Any]                    # Контроль качества
```

#### Конфигурация
```python
self.config: Dict[str, Any]                         # Конфигурация сервиса
self.default_lot: float                             # Размер позиции по умолчанию
self.external_event_dedup_ttl: int                  # TTL для дедупликации событий
```

#### Orphan management
```python
self._orphan_housekeep_interval_ms: int             # Интервал очистки orphan позиций
self._last_housekeep_ms: int                        # Timestamp последней очистки
self._orphan_ttl_ms: int                            # TTL orphan позиций
self.orphan_timeout_enabled: bool                   # Флаг включения orphan timeout
```

### Модель данных

#### PositionState
```python
@dataclass
class PositionState:
    pos_id: str                    # Уникальный ID позиции
    sid: str                       # Signal ID
    symbol: str                    # Символ (BTCUSDT)
    side: str                      # Направление (LONG/SHORT)
    entry_price: float            # Цена входа
    lot: float                     # Размер позиции
    sl_price: Optional[float]      # Stop Loss цена
    tp_levels: List[float]         # Take Profit уровни
    open_ts: int                   # Timestamp открытия (ms)
    last_update_ts: int            # Timestamp последнего обновления
    current_price: float           # Текущая цена
    unrealized_pnl: float          # Нереализованная прибыль
    realized_pnl: float            # Реализованная прибыль
    status: str                    # Статус позиции
    # ... дополнительные поля
```

#### TradeClosed
```python
@dataclass
class TradeClosed:
    pos_id: str                    # ID позиции
    close_reason: str              # Причина закрытия
    close_price: float             # Цена закрытия
    close_ts: int                  # Timestamp закрытия
    total_pnl: float               # Итоговый P&L
    duration_ms: int               # Длительность позиции
    # ... метаданные
```

## Детальная логика методов

### Инициализация (__init__)

#### Этапы инициализации:

1. **Redis подключение**
   ```python
   if redis_client is not None:
       self.redis = redis_client
   else:
       self.redis = redis_lib.from_url(redis_url, decode_responses=True)
   ```

2. **Настройка многопоточности**
   ```python
   self._lock = threading.RLock()
   self._use_symbol_locks = os.getenv("TM_USE_SYMBOL_LOCKS", "1") == "1"
   self._symbol_locks_guard = threading.Lock()
   self._symbol_locks = {}
   ```

3. **Инициализация структур данных**
   ```python
   self.open_positions = {}
   self.pos_by_sid = {}
   self.open_by_symbol = {}
   self._last_price_by_symbol = {}
   ```

4. **Настройка асинхронных операций**
   ```python
   self._db_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="TM_DB")
   ```

5. **Загрузка конфигурации**
   ```python
   self.default_lot = float(mon.get("default_lot", 1.0))
   # TP ratios, orphan settings, etc.
   ```

### Создание позиции (create_position)

**Вызывается из:** SignalPerformanceTracker при получении нового сигнала

```python
def create_position(self, signal: Dict[str, Any]) -> Optional[str]:
    """
    Создание новой виртуальной позиции на основе сигнала.

    Returns: position_id или None при ошибке
    """

    # 1. Валидация сигнала
    if not self._validate_signal(signal):
        return None

    # 2. Проверка дедупликации
    sid = signal.get("signal_id")
    if self._is_sid_closed(sid):
        self.logger.warning(f"Signal {sid} already processed")
        return None

    # 3. Получение спецификации символа
    symbol = signal.get("symbol")
    spec = get_symbol_info(symbol)

    # 4. Нормализация сигнала
    signal_norm = self._normalize_signal(signal, spec)

    # 5. Создание позиции через domain handler
    position = create_position(signal_norm, spec)

    # 6. Сохранение в структуры данных
    with self._get_symbol_lock(symbol):
        pos_id = position.pos_id
        self.open_positions[pos_id] = position
        self.pos_by_sid[sid] = pos_id
        self.open_by_symbol.setdefault(symbol, set()).add(pos_id)

    # 7. Персистентность
    self._persist_position_creation(position, signal)

    # 8. Логирование
    self.logger.info(f"Created position {pos_id} for signal {sid}")

    return pos_id
```

#### Валидация сигнала (_validate_signal)

```python
def _validate_signal(self, signal: Dict[str, Any]) -> bool:
    """Проверка корректности сигнала."""
    required_fields = ["signal_id", "symbol", "direction", "entry"]

    for field in required_fields:
        if field not in signal:
            self.logger.error(f"Missing required field: {field}")
            return False

    # Проверка корректности значений
    direction = signal.get("direction", "").upper()
    if direction not in ["LONG", "SHORT"]:
        self.logger.error(f"Invalid direction: {direction}")
        return False

    entry_price = signal.get("entry")
    if not isinstance(entry_price, (int, float)) or entry_price <= 0:
        self.logger.error(f"Invalid entry price: {entry_price}")
        return False

    return True
```

#### Нормализация сигнала (_normalize_signal)

```python
def _normalize_signal(self, signal: Dict[str, Any], spec: SymbolSpec) -> SignalNorm:
    """Нормализация сигнала в стандартный формат."""

    # Расчет уровней SL/TP
    sl_price = self._calculate_sl(signal, spec)
    tp_levels = self._calculate_tp_levels(signal, spec)

    # Определение размера позиции
    lot = signal.get("lot", self.default_lot)

    return SignalNorm(
        sid=signal["signal_id"],
        symbol=signal["symbol"],
        side=signal["direction"],
        entry_price=signal["entry"],
        sl_price=sl_price,
        tp_levels=tp_levels,
        lot=lot,
        # ... остальные поля
    )
```

### Обновление позиций (process_tick)

**Вызывается из:** SignalPerformanceTracker при получении тика

```python
def process_tick(self, symbol: str, tick_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Обработка тика для всех позиций символа.

    Returns: Список событий (TP hits, closures, etc.)
    """

    # 1. Извлечение цены из тика
    price = self._extract_price(tick_data)
    if price is None:
        return []

    # 2. Обновление последней цены
    self._update_last_price(tick_data)

    # 3. Получение активных позиций символа
    with self._get_symbol_lock(symbol):
        position_ids = self.open_by_symbol.get(symbol, set()).copy()

    if not position_ids:
        return []

    # 4. Обработка каждой позиции
    events = []
    io_batch = _TickIOBatch()

    for pos_id in position_ids:
        with self._get_symbol_lock(symbol):
            position = self.open_positions.get(pos_id)
            if not position:
                continue

        # 5. Обработка тика для позиции
        tick_events, tick_io = self._process_tick_for_position(position, price, tick_data)

        events.extend(tick_events)
        self._merge_io_batch(io_batch, tick_io)

    # 6. Выполнение отложенных I/O операций
    self._execute_io_batch(io_batch, symbol)

    return events
```

#### Обработка тика для позиции (_process_tick_for_position)

```python
def _process_tick_for_position(self, position: PositionState, price: float,
                              tick_data: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], _TickIOBatch]:
    """Обработка тика для конкретной позиции."""

    events = []
    io_batch = _TickIOBatch()

    # 1. Обновление P&L
    old_pnl = position.unrealized_pnl
    position.unrealized_pnl = self._calculate_pnl(position, price)
    position.last_update_ts = int(time.time() * 1000)

    # 2. Проверка условий закрытия
    close_reason = self._check_close_conditions(position, price)

    if close_reason:
        # 3. Закрытие позиции
        closed_trade = self._close_position_internal(position, price, close_reason)
        events.append({
            "type": "position_closed",
            "pos_id": position.pos_id,
            "close_reason": close_reason,
            "pnl": closed_trade.total_pnl
        })

        # 4. Подготовка I/O для закрытия
        io_batch.closed = closed_trade
        io_batch.close_pos_id = position.pos_id
        io_batch.close_sid = position.sid
        io_batch.close_source = position.source
        io_batch.close_symbol = position.symbol

    else:
        # 5. Проверка TP hit (partial closes)
        tp_hits = self._check_tp_hits(position, price)
        for tp_hit in tp_hits:
            events.append(tp_hit)
            io_batch.tp_hits.append(tp_hit)

    return events, io_batch
```

### Проверка условий закрытия (_check_close_conditions)

```python
def _check_close_conditions(self, position: PositionState, price: float) -> Optional[str]:
    """Проверка условий закрытия позиции."""

    # 1. Stop Loss hit
    if position.sl_price:
        if position.side == "LONG" and price <= position.sl_price:
            return "sl_hit"
        elif position.side == "SHORT" and price >= position.sl_price:
            return "sl_hit"

    # 2. Take Profit hit (full close если последний TP)
    if position.tp_levels and price in position.tp_levels:
        tp_index = position.tp_levels.index(price)
        if tp_index == len(position.tp_levels) - 1:  # Последний TP
            return "tp_hit"

    # 3. Orphan timeout (опционально)
    if self.orphan_timeout_enabled:
        now_ms = int(time.time() * 1000)
        if self._is_orphan_expired(position, now_ms):
            return "orphan_timeout"

    # 4. Manual close signals
    # ...

    return None
```

### Закрытие позиции (_close_position_internal)

```python
def _close_position_internal(self, position: PositionState, close_price: float,
                           close_reason: str) -> TradeClosed:
    """Внутренняя логика закрытия позиции."""

    # 1. Расчет финального P&L
    total_pnl = position.unrealized_pnl + position.realized_pnl

    # 2. Создание объекта закрытой сделки
    closed = TradeClosed(
        pos_id=position.pos_id,
        close_reason=close_reason,
        close_price=close_price,
        close_ts=int(time.time() * 1000),
        total_pnl=total_pnl,
        duration_ms=position.last_update_ts - position.open_ts,
        # ... метаданные
    )

    # 3. Обновление структур данных
    with self._get_symbol_lock(position.symbol):
        self.open_positions.pop(position.pos_id, None)
        self.open_by_symbol[position.symbol].discard(position.pos_id)

    # 4. Маркировка signal_id как обработанного
    self._mark_sid_closed(position.sid)

    # 5. Логирование
    self.logger.info(f"Closed position {position.pos_id}: {close_reason}, P&L: {total_pnl}")

    return closed
```

### Trailing Stop Loss (apply_trailing_update)

```python
def apply_trailing_sl_sync(self, pos_id: str, new_sl: float) -> bool:
    """Применение trailing stop loss update."""

    with self._get_symbol_lock_by_pos_id(pos_id):
        position = self.open_positions.get(pos_id)
        if not position:
            return False

        # Валидация нового SL
        if not self._validate_trailing_sl(position, new_sl):
            return False

        # Применение update
        old_sl = position.sl_price
        position.sl_price = new_sl
        position.last_update_ts = int(time.time() * 1000)

        # Персистентность
        self._persist_trailing_update(position, old_sl, new_sl)

        self.logger.info(f"Applied trailing SL for {pos_id}: {old_sl} -> {new_sl}")

        return True
```

### Orphan management

```python
def _housekeep_orphans(self, now_ms: int) -> None:
    """Очистка истекших orphan позиций."""

    if now_ms - self._last_housekeep_ms < self._orphan_housekeep_interval_ms:
        return

    self._last_housekeep_ms = now_ms

    # Поиск orphan позиций
    orphans = []
    for pos_id, position in self.open_positions.items():
        if self._is_orphan_expired(position, now_ms):
            orphans.append(pos_id)

    # Закрытие orphans
    for pos_id in orphans:
        with self._get_symbol_lock_by_pos_id(pos_id):
            position = self.open_positions.get(pos_id)
            if position:
                self._close_position_internal(position, position.current_price, "orphan_timeout")
```

## Персистентность данных

### Redis структуры

#### Активные позиции
```
Key: position:{pos_id}
Type: Hash
Fields: pos_id, sid, symbol, side, entry_price, sl_price, tp_levels, etc.
```

#### Закрытые позиции
```
Key: closed:{pos_id}
Type: Hash
Fields: pos_id, close_reason, close_price, total_pnl, duration_ms, etc.
```

#### Статистика
```
Key: stats:{strategy}:{symbol}:{timeframe}
Type: Hash
Fields: total_trades, wins, losses, total_pnl, winrate, etc.
```

#### Dедупликация сигналов
```
Key: closed_sid:{sid}
Type: String (TTL)
Value: "1"
```

### Batch I/O операции

```python
def _execute_io_batch(self, io_batch: _TickIOBatch, symbol: str) -> None:
    """Выполнение отложенных I/O операций."""

    # 1. Сохранение событий
    for event in io_batch.events:
        self.repo.save_event(event)

    # 2. Обновление TP hits
    for tp_hit in io_batch.tp_hits:
        self._persist_tp_hit(tp_hit)

    # 3. Trailing updates
    for move in io_batch.trailing_moves:
        self._persist_trailing_move(move)

    # 4. Закрытие позиции
    if io_batch.closed:
        self._persist_closed_trade(io_batch.closed, io_batch.pos_snapshot, io_batch.closed_snapshot)

    # 5. Обновление статистики
    if io_batch.pos_snapshot and io_batch.closed_snapshot:
        self._update_stats_from_dicts(io_batch.pos_snapshot, io_batch.closed_snapshot)
```

## Конфигурационные параметры

### Переменные окружения

**Многопоточность:**
- `TM_USE_SYMBOL_LOCKS`: Использовать per-symbol блокировки (default: 1)
- `TM_ORPHAN_HOUSEKEEP_INTERVAL_MS`: Интервал очистки orphans (default: 30000)
- `TM_ORPHAN_TTL_MS`: TTL orphan позиций (default: 120000)
- `TM_ORPHAN_TIMEOUT_ENABLED`: Включить orphan timeout (default: 0)

**Торговля:**
- `TP_RATIO1`, `TP_RATIO2`, `TP_RATIO3`: Соотношения TP уровней
- `ATTACH_HEALTH_SNAPSHOT_ON_CLOSE`: Прикреплять health snapshot (default: 1)

### Конфигурация (config dict)

```python
{
    "monitor": {
        "default_lot": 1.0,
        "tp_ratio": [0.5, 0.3, 0.2],
        "sl_atr_multiplier": 1.0
    },
    "health": {
        "enabled": true,
        "snapshot_interval_ms": 60000
    }
}
```

## Производительность и оптимизации

### Thread Safety

1. **Symbol-level locking**: Предотвращает race conditions между позициями одного символа
2. **RLock**: Позволяет рекурсивные вызовы в рамках одного thread
3. **Guard lock**: Защищает доступ к словарю symbol locks

### Асинхронные операции

1. **ThreadPoolExecutor**: Отдельный пул для БД операций
2. **Batch I/O**: Группировка операций для снижения количества вызовов
3. **Fail-open**: Продолжение работы при ошибках I/O

### Оптимизации памяти

1. **Lazy cleanup**: Очистка orphan позиций по расписанию
2. **TTL для дедупликации**: Автоматическая очистка Redis ключей
3. **Snapshot copies**: Использование копий для thread safety

## Мониторинг и метрики

### Встроенные метрики

```python
self._m_inc("positions_created", tags={"symbol": symbol})
self._m_inc("positions_closed", tags={"symbol": symbol, "reason": close_reason})
self._m_obs("pnl_realized", pnl, tags={"symbol": symbol, "strategy": strategy})
```

### Health monitoring

```python
def _get_health_snapshot(self, symbol: str) -> Dict[str, Any]:
    """Создание health snapshot для позиции."""
    return {
        "open_positions_count": len(self.open_positions),
        "symbol_positions": len(self.open_by_symbol.get(symbol, [])),
        "last_price_ts": self._last_price_by_symbol.get(symbol, (0, 0.0))[0],
        "memory_usage_mb": self._get_memory_usage(),
        "uptime_sec": time.time() - self._start_time
    }
```

## Обработка ошибок

### Fail-Open стратегия

1. **БД недоступна**: Логирование, продолжение работы в памяти
2. **Некорректные данные**: Валидация, пропуск проблемных сигналов
3. **Thread errors**: Изоляция ошибок, продолжение обработки других позиций
4. **Memory pressure**: Очистка orphans, graceful degradation

### Валидация данных

```python
def _validate_position_data(self, position: PositionState) -> bool:
    """Комплексная валидация позиции."""
    checks = [
        position.pos_id,
        position.symbol,
        position.side in ["LONG", "SHORT"],
        position.entry_price > 0,
        position.lot > 0,
        position.open_ts > 0
    ]
    return all(checks)
```

## Типичные проблемы и решения

### Проблема: Memory leaks
**Симптомы**: Рост потребления памяти со временем
**Решения**:
- Включить orphan housekeeping
- Проверить корректность закрытия позиций
- Мониторить размер словарей

### Проблема: Race conditions
**Симптомы**: Несогласованные состояния позиций
**Решения**:
- Убедиться что TM_USE_SYMBOL_LOCKS=1
- Проверить корректность использования _get_symbol_lock
- Добавить дополнительные проверки consistency

### Проблема: High latency
**Симптомы**: Задержки в обработке тиков
**Решения**:
- Оптимизировать batch sizes
- Увеличить thread pool size для DB operations
- Профилировать bottleneck'ы

### Проблема: Duplicate positions
**Симптомы**: Множественные позиции на один сигнал
**Решения**:
- Проверить дедупликацию по sid
- Убедиться в корректности _is_sid_closed
- Добавить дополнительные проверки

## Заключение

TradeMonitorService предоставляет высокопроизводительную и надежную систему виртуального трейдинга для оценки качества сигналов. Его архитектура обеспечивает thread safety, fault tolerance и высокую производительность при обработке большого количества позиций в реальном времени.
