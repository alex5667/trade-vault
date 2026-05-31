# 🎯 Отслеживание трейлинг стопов (2025-11-26)

> Детальное описание системы отслеживания трейлинг стопов: от запуска после TP1 до финального закрытия позиции.

---

## 📋 Содержание

1. [Обзор системы](#обзор-системы)
2. [Профили трейлинг стопов](#профили-трейлинг-стопов)
3. [Запуск трейлинг стопа](#запуск-трейлинг-стопа)
4. [Отслеживание перемещений](#отслеживание-перемещений)
5. [Метрики и мониторинг](#метрики-и-мониторинг)
6. [Интеграция с MT5](#интеграция-с-mt5)
7. [FAQ](#faq)

---

## 🎯 Обзор системы

### Компоненты

| Компонент                      | Файл/директория                                       | Назначение                                           |
| ------------------------------ | ----------------------------------------------------- | ---------------------------------------------------- |
| **TP1 Trailing Orchestrator**  | `python-worker/services/tp1_trailing_orchestrator.py` | Оркестрация запуска трейлинг стопа после TP1         |
| **Trailing Profiles Registry** | `python-worker/services/trailing_profiles.py`         | Реестр профилей трейлинг стопов                      |
| **Order Trailing Dispatcher**  | `python-worker/services/order_trailing_dispatcher.py` | Отправка команд в Go Gateway                         |
| **MT5 Trailing Move Logger**   | `python-worker/services/mt5_trailing_move_logger.py`  | Логирование перемещений трейлинг стопа от MT5        |
| **Trade Monitor**              | `python-worker/services/trade_monitor.py`             | Обновление виртуальных позиций при перемещении стопа |
| **Trade Events Logger**        | `python-worker/services/trade_events_logger.py`       | Логирование событий трейлинг стопа                   |

### Поток данных

```
1. Достижение TP1
   ├─► Для виртуальных позиций (криптовалюты):
   │   └─► Trade Monitor обнаруживает TP1 при обработке тика
   │       └─► Trade Monitor публикует событие TP1_HIT в events:trades
   └─► Для реальных позиций в MT5:
       └─► MT5 EA обнаруживает TP1
           └─► MT5 EA публикует событие TP1_HIT через MT5 Event Executor в events:trades
   ↓
2. TP1 Trailing Orchestrator (слушает events:trades)
   ├─► Проверка trail_after_tp1 флага
   ├─► Выбор профиля трейлинга
   ├─► Конвертация ATR → points
   └─► Формирование команды
   ↓
3. Order Trailing Dispatcher
   └─► POST /orders/push (action=trail)
   ↓
4. Go Gateway
   └─► Команда в очередь orders:queue
   ↓
5. MT5 EA
   ├─► Получение команды через /orders/poll
   ├─► Активация трейлинг стопа
   └─► Отправка TRAILING_MOVE событий
   ↓
6. MT5 Event Executor
   └─► Публикация в events:trades
   ↓
7. Trade Monitor
   ├─► Обновление SL позиции (два механизма):
   │   ├─► Автоматически на каждом тике через `_update_trailing_from_tick()`
   │   │   - Рассчитывает новый SL на основе `trailing_distance` и текущей цены
   │   │   - Обновляет `max_favorable_price` (peak tracking)
   │   │   - Публикует событие `TRAILING_MOVE` в `events:trades`
   │   └─► По команде через `update_trailing_sl()` (от TP1 Trailing Orchestrator)
   │       - Принимает команду обновления SL из `events:trades`
   │       - Сохраняет параметры трейлинга (`trailing_distance`, `trailing_point`)
   │       - Публикует событие `TRAILING_SL_SYNC`
   └─► Публикация в trade:timeline
```

---

## 🎚️ Профили трейлинг стопов

### Режимы трейлинга

| Режим      | Описание                                    | Параметры                      |
| ---------- | ------------------------------------------- | ------------------------------ |
| **ATR**    | Трейлинг на основе ATR (Average True Range) | `atr_mult` (множитель ATR)     |
| **POINTS** | Фиксированное расстояние в пунктах          | `points` (количество пунктов)  |
| **STEP**   | Ступенчатый трейлинг                        | `step_points`, `hard_min_lock` |

### Стандартные профили

| Профиль          | Режим    | ATR × | Points | Hard Lock | Описание                                        |
| ---------------- | -------- | ----- | ------ | --------- | ----------------------------------------------- |
| `rocket_v1`      | `ATR`    | 0.6   | –      | 0.0       | Агрессивное сопровождение тренда (TP1=0.78 ATR) |
| `lock_and_trail` | `ATR`    | 0.8   | –      | 0.0       | Баланс защита/прибыль                           |
| `wide_swing`     | `ATR`    | 1.2   | –      | 0.0       | Для волатильных рынков                          |
| `crypto_tight`   | `ATR`    | 0.5   | –      | 0.0       | Для высокоскоростных инструментов               |
| `points_200`     | `POINTS` | –     | 200    | –         | Фиксированное значение (fallback)               |

### Структура профиля

```python
@dataclass
class TrailingProfile:
    name: str                    # Уникальное имя профиля
    mode: str                    # "ATR" | "POINTS" | "STEP"
    atr_mult: float = 1.0        # Множитель ATR для режима ATR
    points: float = 200.0        # Фиксированные пункты для режима POINTS
    hard_min_lock: Optional[float] = None  # Минимальная прибыль для фиксации
    step_points: Optional[float] = None   # Размер шага для ступенчатого трейлинга
    comment: str = ""            # Описание профиля
```

### Хранение профилей

Профили хранятся в Redis:

```python
# Redis key: trailing:profiles
# Format: JSON dict {profile_name: profile_data}

{
  "rocket_v1": {
    "name": "rocket_v1",
    "mode": "ATR",
    "atr_mult": 0.6,
    "hard_min_lock": 0.0,
    "comment": "ATR 0.6 trailing, TP1=0.78 ATR (default for crypto and XAUUSD)"
  },
  "lock_and_trail": {
    "name": "lock_and_trail",
    "mode": "ATR",
    "atr_mult": 0.8,
    "hard_min_lock": 0.0,
    "comment": "lock profit and trail with ATR 0.8"
  }
}
```

### Загрузка профилей

```python
from services.trailing_profiles import TrailingProfilesRegistry

registry = TrailingProfilesRegistry()
profile = registry.get("rocket_v1")

print(f"Mode: {profile.mode}")
print(f"ATR Multiplier: {profile.atr_mult}")
print(f"Comment: {profile.comment}")
```

---

## 🚀 Запуск трейлинг стопа

### Источники события TP1_HIT

Событие `TP1_HIT` может формироваться двумя источниками:

1. **Trade Monitor** (для виртуальных позиций):

   - Trade Monitor отслеживает виртуальные позиции по тикам из `stream:tick_<symbol>`
   - При достижении TP1 в методе `_handle_take_profit()` (строка 442-533) публикует событие `TP1_HIT` в `events:trades`
   - Используется для криптовалютных позиций (BTCUSDT, ETHUSDT и др.), где MT5 EA не работает
   - Формат события: `{"event_type": "TP1_HIT", "sid": "...", "symbol": "...", "price": ..., "source": "..."}`

2. **MT5 EA** (для реальных позиций в MT5):
   - MT5 EA отслеживает реальные позиции в терминале MT5
   - При достижении TP1 публикует событие через MT5 Event Executor в `events:trades`
   - Используется для позиций, открытых в MT5 (например, XAUUSD)

**TP1 Trailing Orchestrator** слушает события `TP1_HIT` из обоих источников в stream `events:trades` и обрабатывает их одинаково.

### Условия запуска

Трейлинг стоп запускается автоматически при выполнении условий:

1. **Событие TP1_HIT** получено из `events:trades`:
   - Для **виртуальных позиций** (криптовалюты): событие формируется **Trade Monitor** при достижении TP1
   - Для **реальных позиций в MT5**: событие может приходить от **MT5 EA** через MT5 Event Executor
   - **TP1 Trailing Orchestrator** слушает события TP1_HIT из обоих источников
2. **Флаг `trail_after_tp1=True`** в исходном сигнале
3. **Профиль трейлинга** указан в сигнале (`trail_profile`)
4. **Символ** в списке разрешенных (если фильтр включен)
5. **Источник** в списке разрешенных (если фильтр включен)

### Процесс запуска

**Важно:** Событие `TP1_HIT` может быть сформировано как Trade Monitor (для виртуальных позиций), так и MT5 EA (для реальных позиций). TP1 Trailing Orchestrator обрабатывает события из обоих источников одинаково.

```python
# В Trade Monitor при достижении TP1:
def _handle_take_profit(self, pos: Position, tp_index: int, exit_price: float, timestamp: int):
    """Обработка достижения TP: частичное закрытие позиции."""
    # ... закрытие части позиции ...

    # Для TP1 публикуем событие TP1_HIT в формате, ожидаемом TP1 Trailing Orchestrator
    # Это необходимо для криптовалют, где MT5 EA не работает
    if tp_index == 0 and pos.signal_id:
        tp1_hit_event = {
            "event_type": "TP1_HIT",
            "sid": pos.signal_id,
            "symbol": pos.symbol,
            "price": round(exit_price, 2),
            "position_id": pos.id,
            "volume": round(portion_lot, 2),
            "source": pos.source,
            "ts": timestamp
        }
        self.redis.xadd("events:trades", tp1_hit_event, maxlen=10000)

# В TP1 Trailing Orchestrator:
def handle_event(self, event: Dict[str, Any]) -> bool:
    """Обработать событие из Redis stream events:trades."""
    # 1. Получение исходного сигнала
    signal = get_signal(signal_id)
    if not signal:
        return TrailingResult(success=False, error="signal_not_found")

    # 2. Проверка флага trail_after_tp1
    if not signal.get("trail_after_tp1"):
        return TrailingResult(success=True, skipped=True, reason="trail_disabled")

    # 3. Выбор профиля
    profile_name = signal.get("trail_profile", "rocket_v1")
    profile = profiles_registry.get(profile_name)
    if not profile:
        return TrailingResult(success=False, error=f"profile_not_found: {profile_name}")

    # 4. Получение ATR (если режим ATR)
    if profile.mode == "ATR":
        atr = get_atr(signal["symbol"], signal.get("atr_tf", "M1"))
        if not atr:
            return TrailingResult(success=False, error="atr_not_available")

        # Конвертация ATR → points
        trail_distance = atr * profile.atr_mult
        trail_points = convert_to_points(signal["symbol"], trail_distance)
    else:
        trail_points = profile.points

    # 5. Получение текущей цены и SL
    current_price = tp_event["price"]
    current_sl = get_current_sl(signal_id)

    # 6. Расчет нового SL
    if signal["direction"] == "LONG":
        new_sl = current_price - trail_points
        # Проверка hard_min_lock
        if profile.hard_min_lock:
            min_sl = signal["entry"] + profile.hard_min_lock
            new_sl = max(new_sl, min_sl)
    else:  # SHORT
        new_sl = current_price + trail_points
        if profile.hard_min_lock:
            max_sl = signal["entry"] - profile.hard_min_lock
            new_sl = min(new_sl, max_sl)

    # 7. Формирование команды
    command = {
        "action": "trail",
        "signal_id": signal_id,
        "symbol": signal["symbol"],
        "position_id": tp_event["position_id"],
        "new_sl": new_sl,
        "profile": profile_name,
        "trail_points": trail_points,
        "metadata": {
            "atr": atr if profile.mode == "ATR" else None,
            "atr_mult": profile.atr_mult if profile.mode == "ATR" else None,
            "hard_min_lock": profile.hard_min_lock
        }
    }

    # 8. Отправка команды в Gateway
    result = dispatcher.send_trailing_command(command)

    if result.success:
        # 9. Логирование события
        events_logger.log_trailing_started(
            signal_id=signal_id,
            position_id=tp_event["position_id"],
            symbol=signal["symbol"],
            profile=profile_name,
            new_sl=new_sl
        )

        return TrailingResult(
            success=True,
            new_sl=new_sl,
            profile_name=profile_name,
            metadata=command["metadata"]
        )
    else:
        return TrailingResult(success=False, error=result.error)
```

### Конвертация ATR → Points

```python
def convert_to_points(symbol: str, atr_value: float) -> float:
    """Конвертация ATR в пункты для символа."""
    # Получение спецификации символа
    symbol_info = get_symbol_info(symbol)

    # Расчет пунктов
    # Для XAUUSD: 1 пункт = 0.01, ATR в долларах
    # Для криптовалют: зависит от символа

    if symbol == "XAUUSD":
        # ATR в долларах, конвертируем в пункты (1 пункт = 0.01)
        points = atr_value / 0.01
    elif symbol.startswith("BTC") or symbol.startswith("ETH"):
        # Для криптовалют используем tick_size
        tick_size = symbol_info.get("tick_size", 0.01)
        points = atr_value / tick_size
    else:
        # Fallback: используем point
        point = symbol_info.get("point", 0.00001)
        points = atr_value / point

    return round(points, 2)
```

---

## 📊 Отслеживание перемещений

Trade Monitor отслеживает обновления SL позиции при трейлинг стопе **двумя способами**:

### 1. Автоматическое обновление на каждом тике

Trade Monitor автоматически обновляет SL на каждом тике для виртуальных позиций с активным трейлингом через метод `_update_trailing_from_tick()`. Этот метод:

- Вызывается в `process_tick()` для каждой открытой позиции с активным трейлингом
- Рассчитывает новый SL на основе `trailing_distance` и текущей цены
- Автоматически отслеживает `max_favorable_price` (peak/MFE)
- Публикует событие `TRAILING_MOVE` в `events:trades` при каждом обновлении SL
- Работает только для виртуальных позиций (не требует MT5)

### 2. Обновление по команде от TP1 Trailing Orchestrator

Trade Monitor также может обновлять SL по команде из `events:trades` через метод `update_trailing_sl()`. Этот метод:

- Вызывается из `SignalPerformanceTracker` при получении события `TRAILING_STARTED` из `events:trades`
- Устанавливает начальные параметры трейлинга (`trailing_distance`, `trailing_point`)
- После установки параметров автоматическое обновление на тиках продолжает работать

**Важно:** После установки параметров трейлинга через `update_trailing_sl()`, дальнейшие обновления SL происходят автоматически на каждом тике через `_update_trailing_from_tick()`.

### События TRAILING_MOVE

Система отправляет события `TRAILING_MOVE` при каждом перемещении стопа (от Trade Monitor или MT5):

```json
{
 "event_type": "TRAILING_MOVE",
 "sid": "signal-XAUUSD-1731012450",
 "symbol": "XAUUSD",
 "position_id": "12345678",
 "old_sl": 2765.0,
 "new_sl": 2766.5,
 "price": 2770.0,
 "timestamp": 1731012500000,
 "metadata": {
  "profile": "rocket_v1",
  "trail_points": 3.5,
  "distance_from_entry": 4.5
 }
}
```

### Обработка в Trade Monitor

```python
def update_trailing_sl(
    self,
    signal_id: str,
    new_sl: float,
    source: Optional[str] = None,
    profile: Optional[str] = None,
    event_id: Optional[str] = None
) -> bool:
    """Обновление трейлинг стопа в виртуальной позиции."""
    # Получение позиции по signal_id
    pos_id = self.positions_by_signal.get(signal_id)
    if not pos_id:
        return False

    position = self.open_positions.get(pos_id)
    if not position or position.closed:
        return False

    # Сохранение старого SL
    old_sl = position.sl

    # Обновление SL
    position.sl = new_sl
    position.trailing_active = True

    # Установка флага trailing_started при первом обновлении
    if not position.trailing_started:
        position.trailing_started = True

    # Сохранение в Redis
    self.redis.hset(f"order:{position.id}", mapping={
        "sl": new_sl,
        "trailing_active": "1",
        "trailing_started": "1"
    })

    # Публикация события
    self.trade_events_logger.log_trailing_move(
        signal_id=signal_id,
        position_id=position.id,
        old_sl=old_sl,
        new_sl=new_sl,
        timestamp=int(time.time() * 1000)
    )

    return True
```

### Расчет дистанции от входа

```python
def calculate_distance_from_entry(
    position: Position,
    current_sl: float
) -> float:
    """Расчет дистанции трейлинг стопа от цены входа."""
    if position.direction == "LONG":
        distance = current_sl - position.entry_price
    else:  # SHORT
        distance = position.entry_price - current_sl

    return distance
```

---

## 📈 Метрики и мониторинг

### Prometheus метрики

| Метрика                    | Описание                                   | Тип       |
| -------------------------- | ------------------------------------------ | --------- |
| `trailing_started_total`   | Количество запущенных трейлинг стопов      | Counter   |
| `trailing_latency_ms`      | Задержка от TP1_HIT до команды trail       | Histogram |
| `trailing_moves_total`     | Количество перемещений трейлинг стопа      | Counter   |
| `trailing_stop_hits_total` | Количество закрытий по трейлинг стопу      | Counter   |
| `trailing_distance_points` | Дистанция трейлинг стопа от входа (пункты) | Gauge     |

### Redis ключи для мониторинга

```python
# Статистика трейлинга по стратегии
stats_key = f"stats:{strategy}:{symbol}:{tf}"
trailing_started = redis.hget(stats_key, "trailing_started")
trailing_stop_hits = redis.hget(stats_key, "trailing_stop_hits")

# Таймлайн событий трейлинга
timeline_key = f"trade:timeline:{signal_id}"
events = redis.zrange(timeline_key, 0, -1, withscores=True)

# История перемещений
trailing_moves = [
    event for event in events
    if event["event_type"] == "TRAILING_MOVE"
]
```

### Grafana Dashboard

**Trailing Stop Metrics** включает:

- График количества запущенных трейлинг стопов
- График задержки запуска (P50, P95, P99)
- График количества перемещений
- График дистанции от входа
- Статистика по профилям

---

## 🔌 Интеграция с MT5

### Команда трейлинга

```json
{
 "action": "trail",
 "signal_id": "signal-XAUUSD-1731012450",
 "symbol": "XAUUSD",
 "position_id": "12345678",
 "new_sl": 2766.5,
 "profile": "rocket_v1",
 "trail_points": 3.5,
 "metadata": {
  "atr": 2.6,
  "atr_mult": 0.6,
  "hard_min_lock": 0.0
 }
}
```

### Обработка в MT5 EA

```mql5
void HandleTrailingCommand(JsonObject& cmd) {
    string action = cmd["action"].ToString();
    if(action != "trail") return;

    ulong ticket = (ulong)cmd["position_id"].ToInt();
    double newSL = cmd["new_sl"].ToDouble();

    // Модификация позиции
    if(ModifyPosition(ticket, newSL, currentTP)) {
        Print("✅ Trailing: SL moved to ", newSL);

        // Отправка события
        SendTrailingMoveEvent(ticket, oldSL, newSL);
    }
}
```

### События от MT5

MT5 отправляет события через `/events/mt5`:

```json
{
 "event_type": "TRAILING_MOVE",
 "order_id": "12345678",
 "old_sl": 2765.0,
 "new_sl": 2766.5,
 "price": 2770.0,
 "timestamp": 1731012500000
}
```

---

## ❓ FAQ

### Как выбрать профиль трейлинга?

Выбор профиля зависит от:

- **Волатильности рынка** — `wide_swing` для волатильных, `rocket_v1` для трендовых
- **Инструмента** — `crypto_tight` для криптовалют, `rocket_v1` для XAUUSD
- **Стратегии** — агрессивные стратегии используют `rocket_v1`, консервативные — `lock_and_trail`

### Что такое hard_min_lock?

`hard_min_lock` — минимальная прибыль в пунктах, которую необходимо зафиксировать. Трейлинг стоп не может опуститься ниже этого уровня.

### Как Trade Monitor отслеживает обновление SL позиции при трейлинг стопе?

Trade Monitor отслеживает обновления SL позиции при трейлинг стопе **двумя способами**:

1. **Автоматически на каждом тике** (`_update_trailing_from_tick()`):

   - Вызывается в `process_tick()` для каждой открытой позиции с активным трейлингом
   - Рассчитывает новый SL на основе `trailing_distance` и текущей цены
   - Автоматически отслеживает `max_favorable_price` (peak/MFE)
   - Публикует событие `TRAILING_MOVE` в `events:trades` при каждом обновлении SL
   - Работает только для виртуальных позиций (не требует MT5)

2. **По команде от TP1 Trailing Orchestrator** (`update_trailing_sl()`):
   - Вызывается из `SignalPerformanceTracker` при получении события `TRAILING_STARTED` из `events:trades`
   - Устанавливает начальные параметры трейлинга (`trailing_distance`, `trailing_point`)
   - После установки параметров автоматическое обновление на тиках продолжает работать
   - Может получать команды от MT5 (через MT5 Event Executor) или от TP1 Trailing Orchestrator

**Важно:** После установки параметров трейлинга через `update_trailing_sl()`, дальнейшие обновления SL происходят автоматически на каждом тике через `_update_trailing_from_tick()`.

### Как отслеживается эффективность трейлинг стопов?

Эффективность отслеживается через метрики:

- `trailing_stop_hits` — количество закрытий по трейлинг стопу
- `tp1_then_sl` — количество случаев, когда после TP1 позиция закрылась по SL
- Сравнение P&L сделок с трейлингом и без

### Можно ли изменить профиль трейлинга на лету?

Да, профили можно обновлять в Redis:

```python
# Обновление профиля в Redis
profile_data = {
    "name": "rocket_v1",
    "mode": "ATR",
    "atr_mult": 0.7,  # Изменено с 0.6 на 0.7
    "hard_min_lock": 0.0,
    "comment": "updated rocket profile"
}

redis.set("trailing:profiles", json.dumps({"rocket_v1": profile_data}))
```

---

## 🔗 Связанные документы

- **[signal_lifecycle.md](signal_lifecycle.md)** — полный цикл сигнала
- **[pnl_analysis.md](pnl_analysis.md)** — расчет прибыли/убытков
- **[trading_workflow/tp1_trailing.md](../trading_workflow/tp1_trailing.md)** — система трейлинг стопов

---

## ✅ Контроль версий

- **2025-11-26** — обновление документации по отслеживанию трейлинг стопов
- **2025-11-21** — создание документации по отслеживанию трейлинг стопов
- Ответственные: `@trading-analytics`, `@python-team`
