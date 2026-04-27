# 💰 Анализ прибыли и убытков (P&L) (2026-01-06)

> Детальное описание расчета прибыли/убытков, отслеживания виртуальных позиций и статистики по сделкам.

---

## 📋 Содержание

1. [Обзор системы P&L](#обзор-системы-pl)
2. [Виртуальные позиции](#виртуальные-позиции)
3. [Расчет P&L](#расчет-pl)
4. [Частичное закрытие](#частичное-закрытие)
5. [Статистика по сделкам](#статистика-по-сделкам)
6. [Анализ упущенной прибыли](#анализ-упущенной-прибыли)
7. [Метрики и отчеты](#метрики-и-отчеты)
8. [FAQ](#faq)

---

## 💰 Обзор системы P&L

### Компоненты

| Компонент               | Файл/директория                                 | Назначение                                           |
| ----------------------- | ----------------------------------------------- | ---------------------------------------------------- |
| **Trade Monitor**       | `python-worker/services/trade_monitor.py`       | Отслеживание виртуальных позиций и расчет P&L, thread-safe операции и атомарные обновления состояния |
| **P&L Math Module**     | `python-worker/services/pnl_math.py`            | Корректный расчет P&L с учетом спецификаций символов |
| **Stats Aggregator**    | `python-worker/services/stats_aggregator.py`    | Агрегация статистики по стратегиям                   |
| **Trade Events Logger** | `python-worker/services/trade_events_logger.py` | Логирование событий сделок                           |

### Поток данных

```
1. Сигнал → Создание виртуальной позиции
   ↓
2. Отслеживание тиков → Обновление текущего P&L
   ↓
3. Достижение TP1/TP2/TP3 → Частичное закрытие → Фиксация прибыли
   ↓
4. Достижение SL → Закрытие остатка → Фиксация убытка
   ↓
5. Финализация позиции → Расчет итогового P&L
   ↓
6. Обновление статистики → Агрегация метрик
```

---

## 📊 Виртуальные позиции

### Структура позиции

```python
@dataclass
class Position:
    id: str                          # Уникальный ID позиции
    signal_id: str                   # ID сигнала
    strategy: str                    # Стратегия
    symbol: str                      # Символ
    tf: str                          # Таймфрейм
    source: str                      # Источник сигнала
    direction: str                   # LONG/SHORT
    entry_price: float              # Цена входа
    entry_time: float                # Время входа (timestamp)
    lot: float                       # Размер позиции
    sl: float                        # Стоп-лосс
    tp_levels: List[float]          # [TP1, TP2, TP3]
    remaining_lot: float            # Остаток позиции
    tp_hits: int                    # Количество достигнутых TP
    tp1_hit: bool                   # Флаг достижения TP1
    tp2_hit: bool                   # Флаг достижения TP2
    tp3_hit: bool                   # Флаг достижения TP3
    closed: bool                    # Флаг закрытия позиции
    close_time: Optional[float]     # Время закрытия
    realized_pnl: float             # Реализованная прибыль/убыток
    tp_before_sl: int               # Сколько TP было достигнуто до SL
    trailing_started: bool           # Флаг запуска трейлинг стопа
    trailing_active: bool           # Флаг активности трейлинг стопа
    signal_payload: Dict            # Полный payload сигнала
```

### Создание позиции

```python
def create_position(
    self,
    signal_id: str,
    strategy: str,
    symbol: str,
    tf: str,
    source: str,
    direction: str,
    entry_price: float,
    lot: float,
    sl: float,
    tp_levels: List[float],
    signal_payload: Dict
) -> Position:
    """Создание виртуальной позиции."""
    position_id = str(uuid.uuid4())

    position = Position(
        id=position_id,
        signal_id=signal_id,
        strategy=strategy,
        symbol=symbol,
        tf=tf,
        source=source,
        direction=direction,
        entry_price=entry_price,
        entry_time=time.time(),
        lot=lot,
        sl=sl,
        tp_levels=tp_levels,
        remaining_lot=lot,
        tp_hits=0,
        tp1_hit=False,
        tp2_hit=False,
        tp3_hit=False,
        closed=False,
        realized_pnl=0.0,
        tp_before_sl=0,
        trailing_started=False,
        trailing_active=False,
        signal_payload=signal_payload
    )

    # Сохранение в памяти
    self.open_positions[position_id] = position
    self.positions_by_signal[signal_id] = position_id

    # Сохранение в Redis
    self._save_position_to_redis(position)

    return position
```

---

## 🧮 Расчет P&L

Система использует модуль `pnl_math.py` для корректного расчета P&L с учетом спецификаций символов. Модуль поддерживает два подхода:

- **Тиковая модель**: `pnl = ticks × tick_value × lot`, где `ticks = (exit - entry) / tick_size`
- **Линейная модель**: `pnl = (exit - entry) × contract_size × lot`

### Модуль pnl_math.py

**Основные компоненты:**

1. **`SymbolSpec`** — dataclass со спецификацией символа:

   - `contract_size` — размер контракта (для линейной модели)
   - `tick_size` / `tick_value` — размер и стоимость тика (для тиковой модели)
   - `point_size` — размер пункта для метрик
   - `legacy_multiplier` — fallback множитель (нежелателен)

2. **`pnl_money()`** — метод расчета P&L в денежных единицах
3. **`risk_money()`** — метод расчета 1R (риска) в денежных единицах
4. **`spec_from_symbol_info()`** — адаптер для создания `SymbolSpec` из словаря
5. **`get_symbol_info()`** — получение спецификации символа из Redis

**Преимущества подхода:**

- Устраняет хардкод (например, `* 100` для XAUUSD)
- Поддержка различных моделей расчета (тики vs контракты)
- Автоматическое определение модели на основе доступных данных
- Fallback значения для символов без конфигурации

### Текущий P&L (unrealized)

Текущий P&L рассчитывается на основе текущей цены и обновляется при каждом тике:

```python
from services.pnl_math import SymbolSpec, spec_from_symbol_info, get_symbol_info

def calculate_current_pnl(position: Position, current_price: float) -> float:
    """Расчет текущего P&L для открытой позиции."""
    # Получение спецификации символа
    symbol_info = get_symbol_info(position.symbol)
    spec = spec_from_symbol_info(symbol_info)

    # Расчет P&L через pnl_math
    pnl = spec.pnl_money(
        entry=position.entry_price,
        exit=current_price,
        lot=position.remaining_lot,
        side=position.direction
    )

    return pnl
```

### Реализованный P&L (realized)

Реализованный P&L накапливается при частичном закрытии позиции:

```python
from services.pnl_math import SymbolSpec, spec_from_symbol_info, get_symbol_info

def calculate_realized_pnl(
    position: Position,
    exit_price: float,
    exit_lot: float
) -> float:
    """Расчет реализованного P&L при закрытии части позиции."""
    # Получение спецификации символа
    symbol_info = get_symbol_info(position.symbol)
    spec = spec_from_symbol_info(symbol_info)

    # Расчет P&L через pnl_math
    pnl = spec.pnl_money(
        entry=position.entry_price,
        exit=exit_price,
        lot=exit_lot,
        side=position.direction
    )

    return pnl
```

### Расчет риска (1R)

Для расчета размера позиции на основе риска используется метод `risk_money()`:

```python
from services.pnl_math import SymbolSpec, spec_from_symbol_info, get_symbol_info

def calculate_risk(position: Position) -> float:
    """Расчет риска позиции (1R) в денежных единицах."""
    symbol_info = get_symbol_info(position.symbol)
    spec = spec_from_symbol_info(symbol_info)

    risk = spec.risk_money(
        entry=position.entry_price,
        sl=position.sl,
        lot=position.lot,
        side=position.direction
    )

    return risk
```

### Итоговый P&L

Итоговый P&L = сумма всех реализованных P&L при частичных закрытиях:

```python
from services.pnl_math import SymbolSpec, spec_from_symbol_info, get_symbol_info

def finalize_position_pnl(position: Position, exit_price: float) -> Dict[str, float]:
    """Финализация P&L позиции."""
    # Получение спецификации символа
    symbol_info = get_symbol_info(position.symbol)
    spec = spec_from_symbol_info(symbol_info)

    # Расчет P&L для остатка позиции
    remaining_pnl = spec.pnl_money(
        entry=position.entry_price,
        exit=exit_price,
        lot=position.remaining_lot,
        side=position.direction
    )

    # Итоговый P&L
    total_pnl = position.realized_pnl + remaining_pnl

    # Расчет процента P&L
    if position.direction == "LONG":
        pnl_pct = (exit_price - position.entry_price) / position.entry_price * 100.0
    else:  # SHORT
        pnl_pct = (position.entry_price - exit_price) / position.entry_price * 100.0

    return {
        "total_pnl": total_pnl,
        "pnl_pct": pnl_pct,
        "realized_pnl": position.realized_pnl,
        "remaining_pnl": remaining_pnl
    }
```

### Модели расчета P&L

Модуль `pnl_math.py` поддерживает две модели расчета:

#### Тиковая модель (предпочтительная)

Используется, если доступны `tick_size` и `tick_value`:

```python
pnl = ticks × tick_value × lot
где ticks = (exit_price - entry_price) / tick_size  # для LONG
```

**Пример для XAUUSD:**

- `tick_size = 0.01`
- `tick_value = 1.0` ($1 за тик на 1 лот)
- Разница цен: `2770.0 - 2765.0 = 5.0`
- Тиков: `5.0 / 0.01 = 500`
- P&L для 0.1 лота: `500 × 1.0 × 0.1 = $50`

#### Линейная модель (fallback)

Используется, если тиковая модель недоступна:

```python
pnl = (exit_price - entry_price) × contract_size × lot
```

**Пример:**

- `contract_size = 100.0`
- Разница цен: `2770.0 - 2765.0 = 5.0`
- P&L для 0.1 лота: `5.0 × 100.0 × 0.1 = $50`

### Получение спецификации символа

Спецификация символа хранится в Redis по ключу `symbol_specs:{symbol}`:

```python
from services.pnl_math import get_symbol_info, spec_from_symbol_info

# Получение из Redis или defaults
symbol_info = get_symbol_info("XAUUSD")

# Создание SymbolSpec
spec = spec_from_symbol_info(symbol_info)

# Использование
pnl = spec.pnl_money(entry=2765.0, exit=2770.0, lot=0.1, side="LONG")
```

**Поддерживаемые поля в `symbol_info`:**

- `tick_size`, `tickSize`, `ticks_size`, `point`
- `tick_value`, `tickValue`, `ticks_value`, `tick_value_per_lot`, `pip_value`
- `contract_size`, `contractSize`, `multiplier`
- `point_size`, `pointSize`, `pip_size`
- `legacy_multiplier`, `pnl_multiplier`

**Fallback значения по умолчанию:**

- XAUUSD: `tick_size=0.01`, `tick_value=1.0`, `contract_size=100.0`
- Криптовалюты (BTCUSDT, ETHUSDT): `tick_size=0.01`, `tick_value=0.01`, `contract_size=1.0`

### Учет комиссий и свопов

Для реальных сделок учитываются комиссии и свопы:

```python
def calculate_net_pnl(
    gross_pnl: float,
    commission: float = 0.0,
    swap: float = 0.0
) -> float:
    """Расчет чистого P&L с учетом комиссий и свопов."""
    net_pnl = gross_pnl - commission - swap
    return net_pnl
```

---

## 🎯 Частичное закрытие

### Логика частичного закрытия

При достижении TP уровней позиция закрывается частично. Доли закрытия настраиваются через переменную окружения `TP_RATIO` (по умолчанию: `0.5,0.3,0.2`):

| TP уровень | Процент закрытия (по умолчанию) | Остаток позиции      | Настройка                    |
| ---------- | ------------------------------- | -------------------- | ---------------------------- |
| TP1        | 50% (0.5)                       | 50%                  | `TP_RATIO` (первое значение) |
| TP2        | 30% (0.3)                       | 20%                  | `TP_RATIO` (второе значение) |
| TP3        | 20% (0.2)                       | 0% (полное закрытие) | `TP_RATIO` (третье значение) |

**Настройка через переменную окружения:**

```bash
# Формат: "0.5,0.3,0.2" (доли от 0 до 1) или "50,30,20" (проценты)
export TP_RATIO="0.5,0.3,0.2"

# Или в docker-compose.yml:
environment:
  - TP_RATIO=${TP_RATIO:-0.5,0.3,0.2}
```

**Примеры:**

- `TP_RATIO="0.6,0.3,0.1"` → TP1 закрывает 60%, TP2 закрывает 30%, TP3 закрывает 10%
- `TP_RATIO="70,25,5"` → Автоматически конвертируется в `0.7,0.25,0.05` (если значение > 1, считается процентами)

### Обработка TP1_HIT

```python
def handle_tp1_hit(position: Position, price: float, timestamp: int):
    """Обработка достижения TP1."""
    # Расчет размера закрытия (доля берется из tp_ratio[0], по умолчанию 0.5 = 50%)
    # tp_ratio настраивается через переменную окружения TP_RATIO
    tp_ratio_tp1 = 0.5  # Значение из TP_RATIO (первое значение)
    close_lot = position.lot * tp_ratio_tp1  # Закрываем долю от исходного размера позиции
    position.remaining_lot -= close_lot

    # Расчет реализованной прибыли
    realized_pnl = calculate_realized_pnl(position, price, close_lot)
    position.realized_pnl += realized_pnl

    # Обновление флагов
    position.tp1_hit = True
    position.tp_hits = 1
    position.tp_before_sl = 1

    # Публикация события
    trade_events_logger.log_tp_hit(
        signal_id=position.signal_id,
        position_id=position.id,
        tp_level=1,
        price=price,
        lot=close_lot,
        pnl=realized_pnl,
        timestamp=timestamp
    )

    # Обновление в Redis
    redis.hset(f"order:{position.id}", mapping={
        "remaining_lot": position.remaining_lot,
        "realized_pnl": position.realized_pnl,
        "tp1_hit": "1",
        "tp_hits": "1"
    })
```

### Обработка TP2_HIT

```python
def handle_tp2_hit(position: Position, price: float, timestamp: int):
    """Обработка достижения TP2."""
    # Расчет размера закрытия (30% от исходной позиции)
    original_lot = position.lot
    close_lot = original_lot * 0.3

    # Проверка, что остаток достаточен
    if close_lot > position.remaining_lot:
        close_lot = position.remaining_lot

    position.remaining_lot -= close_lot

    # Расчет реализованной прибыли
    realized_pnl = calculate_realized_pnl(position, price, close_lot)
    position.realized_pnl += realized_pnl

    # Обновление флагов
    position.tp2_hit = True
    position.tp_hits = 2
    position.tp_before_sl = 2

    # Публикация события
    trade_events_logger.log_tp_hit(
        signal_id=position.signal_id,
        position_id=position.id,
        tp_level=2,
        price=price,
        lot=close_lot,
        pnl=realized_pnl,
        timestamp=timestamp
    )
```

### Обработка TP3_HIT

```python
def handle_tp3_hit(position: Position, price: float, timestamp: int):
    """Обработка достижения TP3."""
    # Закрытие остатка (доля берется из tp_ratio[2], по умолчанию 0.2 = 20%)
    # tp_ratio настраивается через переменную окружения TP_RATIO
    # Если остаток меньше расчетного, закрывается весь остаток
    close_lot = position.remaining_lot
    position.remaining_lot = 0
    position.closed = True
    position.close_time = timestamp

    # Расчет реализованной прибыли
    realized_pnl = calculate_realized_pnl(position, price, close_lot)
    position.realized_pnl += realized_pnl

    # Обновление флагов
    position.tp3_hit = True
    position.tp_hits = 3
    position.tp_before_sl = 3

    # Финализация позиции
    finalize_position(position, price, "TP3")
```

---

## 📈 Статистика по сделкам

### Агрегация статистики

Статистика агрегируется в Redis Hash `stats:{strategy}:{symbol}:{tf}`:

```python
def update_stats(position: Position):
    """Обновление статистики после закрытия позиции."""
    key = f"stats:{position.strategy}:{position.symbol}:{position.tf}"

    # Базовые счетчики
    redis.hincrby(key, "total_trades", 1)

    if position.realized_pnl > 0:
        redis.hincrby(key, "wins", 1)
    elif position.realized_pnl < 0:
        redis.hincrby(key, "losses", 1)

    # P&L метрики
    redis.hincrbyfloat(key, "total_pnl", position.realized_pnl)

    # TP метрики
    if position.tp1_hit:
        redis.hincrby(key, "tp1_hits", 1)
    if position.tp2_hit:
        redis.hincrby(key, "tp2_hits", 1)
    if position.tp3_hit:
        redis.hincrby(key, "tp3_hits", 1)

    # Упущенная прибыль
    if position.tp_before_sl > 0:
        if position.tp_before_sl >= 1:
            redis.hincrby(key, "tp1_then_sl", 1)
        if position.tp_before_sl >= 2:
            redis.hincrby(key, "tp2_then_sl", 1)
        if position.tp_before_sl >= 3:
            redis.hincrby(key, "tp3_then_sl", 1)

    # Трейлинг метрики
    if position.trailing_started:
        redis.hincrby(key, "trailing_started", 1)
    if position.close_reason == "TRAILING_STOP":
        redis.hincrby(key, "trailing_stop_hits", 1)

    # Пересчет производных метрик
    total = int(redis.hget(key, "total_trades") or 0)
    wins = int(redis.hget(key, "wins") or 0)
    winrate = (wins / total * 100) if total > 0 else 0
    redis.hset(key, "winrate", winrate)

    total_pnl = float(redis.hget(key, "total_pnl") or 0)
    avg_pnl = total_pnl / total if total > 0 else 0
    redis.hset(key, "avg_pnl", avg_pnl)
```

### Структура статистики

```json
{
 "total_trades": 150,
 "wins": 95,
 "losses": 55,
 "winrate": 63.33,
 "tp1_hits": 120,
 "tp2_hits": 80,
 "tp3_hits": 45,
 "tp1_then_sl": 25,
 "tp2_then_sl": 15,
 "tp3_then_sl": 5,
 "total_pnl": 1250.5,
 "avg_pnl": 8.34,
 "max_pnl": 45.2,
 "min_pnl": -12.3,
 "trailing_started": 100,
 "trailing_stop_hits": 30
}
```

---

## 📉 Анализ упущенной прибыли

> **Примечание:**
>
> - Функции `calculate_missed_profit` и `analyze_trailing_effectiveness` реализованы в `python-worker/services/stats_aggregator.py` как статические методы класса `StatsAggregator`.
> - Расчет P&L теперь использует модуль `pnl_math.py` с классом `SymbolSpec` для корректного учета спецификаций символов (contract_size, tick_size/tick_value).
> - Доли закрытия позиций (TP_RATIO) настраиваются через переменную окружения `TP_RATIO` (по умолчанию: `0.5,0.3,0.2`).

### Метрики упущенной прибыли

Упущенная прибыль — это случаи, когда позиция достигла TP, но затем закрылась по SL:

| Метрика       | Описание                                   |
| ------------- | ------------------------------------------ |
| `tp1_then_sl` | Количество сделок, достигших TP1, затем SL |
| `tp2_then_sl` | Количество сделок, достигших TP2, затем SL |
| `tp3_then_sl` | Количество сделок, достигших TP3, затем SL |

### Расчет потенциальной прибыли

Функция реализована в `python-worker/services/stats_aggregator.py`:

```python
from services.stats_aggregator import StatsAggregator

# Расчет упущенной прибыли для позиции
position_data = {
    "tp_before_sl": 1,
    "entry_price": 2000.0,
    "tp1": 2010.0,
    "tp2": 2020.0,
    "tp3": 2030.0,
    "direction": "LONG",
    "lot": 1.0,
    "realized_pnl": -5.0
}

missed_profit = StatsAggregator.calculate_missed_profit(position_data)
```

**Реализация:**

```474:537:python-worker/services/stats_aggregator.py
@staticmethod
def calculate_missed_profit(position: Dict[str, Any], tp_ratio: List[float] = None) -> float:
    """
    Расчет упущенной прибыли для позиции, которая достигла TP, но затем закрылась по SL.

    Упущенная прибыль = потенциальная прибыль при закрытии на последнем достигнутом TP - фактическая прибыль.

    Args:
        position: Словарь с данными позиции (должен содержать tp_before_sl, entry_price,
                 tp_levels, direction, lot, realized_pnl)
        tp_ratio: Доли закрытия на TP1, TP2, TP3 (по умолчанию [0.5, 0.3, 0.2])

    Returns:
        Упущенная прибыль в денежных единицах
    """
    if tp_ratio is None:
        tp_ratio = [0.5, 0.3, 0.2]

    tp_before_sl = int(position.get("tp_before_sl", 0))
    if tp_before_sl == 0:
        return 0.0

    # Получаем данные позиции
    entry_price = float(position.get("entry_price", 0))
    direction = position.get("direction", "LONG").upper()
    lot = float(position.get("lot", 0))
    realized_pnl = float(position.get("realized_pnl", 0))

    # Получаем TP уровни
    tp_levels = []
    for i in range(1, 4):
        tp_key = f"tp{i}"
        if tp_key in position:
            tp_levels.append(float(position[tp_key]))
        elif f"tp_levels" in position:
            # Если tp_levels это список
            tp_list = position.get("tp_levels", [])
            if isinstance(tp_list, list) and len(tp_list) > i - 1:
                tp_levels.append(float(tp_list[i - 1]))

    if not tp_levels or len(tp_levels) < tp_before_sl:
        return 0.0

     # Расчет потенциальной прибыли при закрытии на последнем достигнутом TP
     # Исправленная логика: сравниваем сценарий "закрыть ОСТАТОК в момент последнего TP"
     # против фактического результата
     tp_idx = min(3, tp_before_sl)
     tp_price = tp_levels[tp_idx - 1] if tp_idx > 0 and len(tp_levels) >= tp_idx else 0.0

     if tp_price <= 0:
         return 0.0

     # Сколько лота уже было закрыто ДО этого TP
     closed_before = 0.0
     if tp_idx >= 1 and len(tp_ratio) > 0:
         closed_before += lot * tp_ratio[0]
     if tp_idx >= 2 and len(tp_ratio) > 1:
         closed_before += lot * tp_ratio[1]
     if tp_idx >= 3 and len(tp_ratio) > 2:
         closed_before += lot * tp_ratio[2]

     remaining_at_tp = max(0.0, lot - closed_before)

     # Гипотеза: закрыть весь remaining_at_tp по tp_price сразу на TP_before_sl
     if direction == "LONG":
         hypothetical_rest = (tp_price - entry_price) * remaining_at_tp
     else:  # SHORT
         hypothetical_rest = (entry_price - tp_price) * remaining_at_tp

     # Фактический gross pnl (уже включает то, что случилось потом)
     actual = realized_pnl

     # P&L частей, закрытых на TP
     pnl_tp_parts = 0.0
     if tp_idx >= 1 and len(tp_ratio) > 0 and len(tp_levels) > 0:
         if direction == "LONG":
             pnl_tp_parts += (tp_levels[0] - entry_price) * lot * tp_ratio[0]
         else:
             pnl_tp_parts += (entry_price - tp_levels[0]) * lot * tp_ratio[0]
     if tp_idx >= 2 and len(tp_ratio) > 1 and len(tp_levels) > 1:
         if direction == "LONG":
             pnl_tp_parts += (tp_levels[1] - entry_price) * lot * tp_ratio[1]
         else:
             pnl_tp_parts += (entry_price - tp_levels[1]) * lot * tp_ratio[1]
     if tp_idx >= 3 and len(tp_ratio) > 2 and len(tp_levels) > 2:
         if direction == "LONG":
             pnl_tp_parts += (tp_levels[2] - entry_price) * lot * tp_ratio[2]
         else:
             pnl_tp_parts += (entry_price - tp_levels[2]) * lot * tp_ratio[2]

     # P&L остатка после TP (фактический)
     pnl_rest_after_tp = actual - pnl_tp_parts

     # Гипотетический общий P&L = P&L на TP частях + гипотетический P&L остатка
     hypothetical_total = pnl_tp_parts + hypothetical_rest

     # Упущенная прибыль = гипотетический - фактический
     missed_profit = hypothetical_total - actual

     return missed_profit
```

### Анализ эффективности трейлинг стопа

Функция реализована в `python-worker/services/stats_aggregator.py`:

```python
from services.stats_aggregator import StatsAggregator
from core.redis_client import get_redis

redis_client = get_redis()

# Анализ эффективности трейлинг стопа
effectiveness = StatsAggregator.analyze_trailing_effectiveness(
    redis_client,
    strategy="orderflow",
    symbol="XAUUSD",
    tf="tick"
)

print(f"Эффективность трейлинга: {effectiveness['trailing_effectiveness']}%")
print(f"Процент упущенной прибыли: {effectiveness['missed_profit_rate']}%")
```

**Реализация:**

```539:604:python-worker/services/stats_aggregator.py
@staticmethod
def analyze_trailing_effectiveness(redis_client, strategy: str, symbol: str, tf: str) -> Dict[str, Any]:
    """
    Анализ эффективности трейлинг стопа для стратегии/символа/TF.

    Args:
        redis_client: Redis клиент
        strategy: Название стратегии
        symbol: Символ
        tf: Таймфрейм

    Returns:
        Словарь с метриками эффективности трейлинга
    """
    try:
        stats_key = f"stats:{strategy}:{symbol}:{tf}"
        stats = redis_client.hgetall(stats_key)

        if not stats:
            return {
                "total_trades": 0,
                "tp1_then_sl": 0,
                "missed_profit_rate": 0.0,
                "trailing_started": 0,
                "trailing_stop_hits": 0,
                "trailing_effectiveness": 0.0
            }

        total_trades = int(stats.get("total_trades", 0))
        tp1_then_sl = int(stats.get("tp1_then_sl", 0))
        trailing_started = int(stats.get("trailing_started", 0))
        trailing_stop_hits = int(stats.get("trailing_stop_hits", 0))

        # Процент упущенной прибыли (TP1 достигнут, но закрылось по SL)
        missed_profit_rate = (tp1_then_sl / total_trades * 100.0) if total_trades > 0 else 0.0

        # Эффективность трейлинг стопа (сколько раз трейлинг стоп сработал из всех запусков)
        trailing_effectiveness = (
            (trailing_stop_hits / trailing_started * 100.0)
            if trailing_started > 0 else 0.0
        )

        return {
            "total_trades": total_trades,
            "tp1_then_sl": tp1_then_sl,
            "tp2_then_sl": int(stats.get("tp2_then_sl", 0)),
            "tp3_then_sl": int(stats.get("tp3_then_sl", 0)),
            "missed_profit_rate": round(missed_profit_rate, 2),
            "trailing_started": trailing_started,
            "trailing_stop_hits": trailing_stop_hits,
            "trailing_effectiveness": round(trailing_effectiveness, 2)
        }

    except Exception as e:
        StatsAggregator.logger.error(f"❌ Ошибка анализа эффективности трейлинга: {e}")
        return {
            "total_trades": 0,
            "tp1_then_sl": 0,
            "missed_profit_rate": 0.0,
            "trailing_started": 0,
            "trailing_stop_hits": 0,
            "trailing_effectiveness": 0.0,
            "error": str(e)
        }
```

---

## 📊 Метрики и отчеты

### Prometheus метрики

| Метрика                  | Описание                    | Тип     |
| ------------------------ | --------------------------- | ------- |
| `positions_opened_total` | Количество открытых позиций | Counter |
| `positions_closed_total` | Количество закрытых позиций | Counter |
| `total_pnl`              | Общий P&L                   | Gauge   |
| `avg_pnl`                | Средний P&L                 | Gauge   |
| `winrate`                | Процент прибыльных сделок   | Gauge   |
| `tp1_hits_total`         | Количество достижений TP1   | Counter |
| `tp1_then_sl_total`      | Количество TP1→SL           | Counter |

### Отчеты по P&L

```python
def generate_pnl_report(strategy: str, symbol: str, tf: str) -> str:
    """Генерация отчета по P&L."""
    stats = StatsAggregator.get_stats(redis, strategy, symbol, tf)

    report = f"""
    <b>💰 P&L Отчет: {strategy} - {symbol} ({tf})</b>

    <b>Общие показатели:</b>
    • Всего сделок: {stats['total_trades']}
    • Прибыльных: {stats['wins']}
    • Убыточных: {stats['losses']}
    • Winrate: {stats['winrate']:.2f}%

    <b>P&L:</b>
    • Общий P&L: ${stats['total_pnl']:.2f}
    • Средний P&L: ${stats['avg_pnl']:.2f}
    • Максимальный P&L: ${stats.get('max_pnl', 0):.2f}
    • Минимальный P&L: ${stats.get('min_pnl', 0):.2f}

    <b>Упущенная прибыль:</b>
    • TP1 → SL: {stats['tp1_then_sl']}
    • TP2 → SL: {stats['tp2_then_sl']}
    • TP3 → SL: {stats['tp3_then_sl']}
    """

    return report
```

---

## ❓ FAQ

### Как рассчитывается P&L для разных символов?

P&L рассчитывается с учетом спецификации символа:

- **XAUUSD**: 1 пункт = 0.01, tick_value = 1.0
- **Криптовалюты**: зависит от tick_size и tick_value символа
- **Форекс**: стандартная формула (tick_value / tick_size) × point

### Что такое реализованный и нереализованный P&L?

- **Реализованный P&L** — прибыль/убыток от закрытых частей позиции
- **Нереализованный P&L** — текущая прибыль/убыток от открытой части позиции

### Как учитывается частичное закрытие?

При достижении TP1 закрывается 50% позиции, при TP2 — 30%, при TP3 — 20%. P&L рассчитывается для каждой закрытой части отдельно.

### Можно ли получить историю всех сделок?

Да, закрытые сделки сохраняются в:

- `trades:closed` — Redis stream
- `closed:{strategy}:{symbol}:{tf}` — список ID сделок
- `order:{position_id}` — детальная информация по позиции

---

## 🔗 Связанные документы

- **[signal_lifecycle.md](signal_lifecycle.md)** — полный цикл сигнала
- **[trailing_stop_tracking.md](trailing_stop_tracking.md)** — отслеживание трейлинг стопов
- **[reporting.md](reporting.md)** — формирование отчетов

---

## ✅ Контроль версий

- **2025-11-26** — обновление документации по анализу P&L
- **2025-11-21** — создание документации по анализу P&L
- Ответственные: `@trading-analytics`, `@python-team`
