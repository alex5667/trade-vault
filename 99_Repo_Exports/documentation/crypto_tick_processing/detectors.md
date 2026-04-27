# Детекторы сигналов - Детальная документация

## Обзор

**Детекторы сигналов** - набор специализированных алгоритмов для выявления паттернов в рыночном микро-структуре. Детекторы анализируют тики и книгу заявок для обнаружения значимых рыночных событий, которые могут указывать на будущие движения цены.

**Расположение**: `python-worker/core/crypto_orderflow_detectors.py`

**Назначение**: Обеспечение многоуровневого анализа order flow для генерации качественных торговых сигналов.

## Архитектурные принципы

### 1. Многоуровневый анализ
- **DeltaSpikeDetector**: Первичный детектор агрессивного объема
- **OBIDetector**: Анализ дисбаланса книги заявок
- **AbsorptionDetector**: Выявление поглощения ликвидности
- **IcebergDetector**: Обнаружение скрытых крупных заявок

### 2. Stateless дизайн
- Каждый детектор независим и не зависит от состояния других
- Локальное состояние только для исторических данных
- Thread-safe для параллельной обработки

### 3. Configurable чувствительность
- Настраиваемые пороги для разных рыночных условий
- Адаптация к волатильности конкретных символов
- Баланс между чувствительностью и false positives

## Оглавление

- [DeltaSpikeDetector - Детектор всплесков дельты](#deltaspikedetector---детектор-всплесков-дельты)
- [OBIDetector - Детектор дисбаланса книги](#obidetector---детектор-дисбаланса-книги)
- [AbsorptionDetector - Детектор поглощения](#absorptiondetector---детектор-поглощения)
- [IcebergDetector - Детектор айсбергов](#icebergdetector---детектор-айсбергов)

---

# DeltaSpikeDetector - Детектор всплесков дельты

## Обзор

**DeltaSpikeDetector** - это статистический детектор всплесков агрессивного объема (дельты) в потоке крипто-тиков. Детектор использует z-score анализ на скользящем окне для выявления аномальных объемов покупок/продаж.

**Назначение**: Первичный детектор сигналов order flow, выявляющий моменты с экстремальным агрессивным объемом.

## Математическая основа

### Z-Score формула

Для каждого тика рассчитывается z-score текущей дельты относительно окна исторических значений:

```
z = (delta_current - mean_window) / std_dev_window
```

Где:
- `delta_current` - классифицированный объем текущего тика
- `mean_window` - среднее значение дельты в окне
- `std_dev_window` - стандартное отклонение дельты в окне

### Условия срабатывания

Детектор генерирует событие при одновременном выполнении двух условий:

1. **Z-threshold**: `|z| ≥ z_threshold`
2. **Absolute volume**: `|delta| ≥ min_abs_volume`

## Детальная структура класса

### Атрибуты

#### Конфигурационные параметры
```python
self.window: int           # Размер скользящего окна (default: 60)
self.z_threshold: float    # Порог z-score (default: 3.0)
self.min_abs_volume: float # Минимальный абсолютный объем (default: 0.0)
```

#### Состояние
```python
self.values: Deque[float]  # Кольцевой буфер значений дельты
```

### Метод classify_tick()

**Назначение**: Определяет направление и величину агрессивного объема в тике.

#### Алгоритм классификации

1. **Извлечение объема**
   ```python
   volume = float(tick.get("qty") or tick.get("volume") or 0)
   if volume <= 0:
       return 0.0
   ```

2. **Определение стороны сделки**

   **Способ 1: Binance format (is_buyer_maker)**
   ```python
   is_buyer_maker = tick.get("is_buyer_maker")
   if is_buyer_maker is not None:
       sign = -1.0 if is_buyer_maker else 1.0  # -1 = SELL, +1 = BUY
   ```

   **Способ 2: Generic format (side field)**
   ```python
   else:
       side = str(tick.get("side", "buy")).lower()
       sign = 1.0 if side == "buy" else -1.0
   ```

3. **Расчет подписанного объема**
   ```python
   return sign * volume
   ```

#### Логика интерпретации

| is_buyer_maker | side | sign | direction | meaning |
|----------------|------|------|-----------|---------|
| False | - | +1.0 | BUY | Агрессивная покупка (taker buy) |
| True | - | -1.0 | SELL | Агрессивная продажа (taker sell) |
| - | "BUY" | +1.0 | BUY | Покупка |
| - | "SELL" | -1.0 | SELL | Продажа |

### Метод push()

**Назначение**: Добавляет тик в анализ и проверяет условия генерации события.

#### Этапы обработки

1. **Классификация тика**
   ```python
   delta = self.classify_tick(tick)
   self.values.append(delta)
   ```

2. **Проверка заполненности буфера**
   ```python
   if len(self.values) < 10:
       # Недостаточно данных для статистики
       return None
   ```

3. **Расчет статистических показателей**
   ```python
   mean = sum(self.values) / len(self.values)
   variance = sum((val - mean) ** 2 for val in self.values) / len(self.values)
   std_dev = variance ** 0.5 if variance > 0 else 0.0

   if std_dev == 0:
       return None  # Деление на ноль
   ```

4. **Расчет z-score**
   ```python
   z_value = (delta - mean) / std_dev
   ```

5. **Проверка условий срабатывания**
   ```python
   if abs(z_value) >= self.z_threshold and abs(delta) >= self.min_abs_volume:
       return {
           "type": "delta_spike",
           "delta": delta,
           "z": z_value,
       }
   ```

## Конфигурационные параметры

### window (размер окна)
**Тип**: `int`, **Диапазон**: 10-1000, **Default**: 60
- Маленькое окно (10-30): более чувствительный, но шумный детектор
- Большое окно (100-300): менее чувствительный, но более стабильный

### z_threshold (порог z-score)
**Тип**: `float`, **Диапазон**: 1.5-5.0, **Default**: 3.0
- 2.0-2.5: Высокая чувствительность (больше сигналов)
- 3.0-3.5: Стандартная чувствительность
- 4.0+: Низкая чувствительность (меньше сигналов, выше качество)

### min_abs_volume (минимальный объем)
**Тип**: `float`, **Диапазон**: 0.0-100.0, **Default**: 0.0
- 0.0: Детектирование любых всплесков
- 0.1-1.0: Фильтрация мелких тиков

## Производительность и оптимизации

### Память
- **Буфер**: `deque` с ограниченным размером `maxlen=window`
- **Потребление**: `O(window)` float значений
- **Пример**: 60 значений × 8 байт = 480 байт на детектор

### CPU
- **Классификация**: O(1) - простые арифметические операции
- **Статистика**: O(window) - расчет mean и std_dev

---

# OBIDetector - Детектор дисбаланса книги

## Обзор

**OBIDetector** (Order Book Imbalance Detector) - детектор дисбаланса книги заявок, выявляющий моменты, когда объем на одной стороне книги значительно превышает объем на противоположной стороне. Детектор использует фильтр по времени удержания для исключения кратковременных шумов.

**Назначение**: Вторичный детектор сигналов order flow, подтверждающий направление движения через анализ ликвидности в книге заявок.

## Математическая основа

### Order Book Imbalance (OBI) формула

OBI рассчитывается как нормализованная разница между объемами bid и ask на заданной глубине:

```
OBI = (bid_volume - ask_volume) / (bid_volume + ask_volume)
```

Где:
- `bid_volume` - суммарный объем на покупку в первых N уровнях
- `ask_volume` - суммарный объем на продажу в первых N уровнях
- Диапазон значений: [-1.0, +1.0]

### Фильтр по времени удержания

Для исключения кратковременных шумов детектор требует, чтобы дисбаланс сохранялся в течение заданного времени:

```
if abs(OBI) ≥ threshold AND direction_stable_for ≥ hold_secs:
    trigger_event()
```

## Детальная структура класса

### Атрибуты

#### Конфигурационные параметры
```python
self.depth: int           # Глубина анализа книги (default: 5)
self.threshold: float     # Порог дисбаланса (default: 0.5)
self.hold_secs: float     # Минимальное время удержания (default: 2.0)
```

#### Состояние
```python
self.last_ok_ts: Optional[float]    # Timestamp последнего валидного дисбаланса
self.last_direction: Optional[str]  # Последнее направление дисбаланса
```

### Метод push()

**Назначение**: Анализирует книгу заявок и генерирует события при устойчивом дисбалансе.

#### Этапы обработки

1. **Извлечение данных книги**
   ```python
   bids = book.get("bids") or []
   asks = book.get("asks") or []
   ```

2. **Расчет объемов по глубине**
   ```python
   bid_vol = sum(float(level[1]) for level in bids[: self.depth])
   ask_vol = sum(float(level[1]) for level in asks[: self.depth])
   ```

3. **Расчет OBI**
   ```python
   if bid_vol + ask_vol == 0:
       return None
   obi = (bid_vol - ask_vol) / (bid_vol + ask_vol)
   current_ts = time.time()
   ```

4. **Проверка порога дисбаланса**
   ```python
   if abs(obi) >= self.threshold:
       direction = "long" if obi > 0 else "short"
       if self.last_direction == direction and self.last_ok_ts:
           if current_ts - self.last_ok_ts >= self.hold_secs:
               return {"type": "obi", "direction": direction, "obi": obi}
       else:
           self.last_direction = direction
           self.last_ok_ts = current_ts
   ```

## Конфигурационные параметры

### depth (глубина анализа)
**Тип**: `int`, **Диапазон**: 1-20, **Default**: 5
- 1-3: Только best bid/ask (высокая чувствительность)
- 5-10: Баланс между локальными и общими изменениями
- 10+: Анализ всей видимой ликвидности

### threshold (порог дисбаланса)
**Тип**: `float`, **Диапазон**: 0.1-0.8, **Default**: 0.5
- 0.3-0.4: Высокая чувствительность
- 0.5-0.6: Стандартная чувствительность
- 0.7+: Консервативный режим

### hold_secs (время удержания)
**Тип**: `float`, **Диапазон**: 0.5-10.0, **Default**: 2.0
- 0.5-1.0: Быстрые сигналы
- 2.0-3.0: Баланс скорости и надежности
- 5.0+: Только устойчивые дисбалансы

---

# AbsorptionDetector - Детектор поглощения

## Обзор

**AbsorptionDetector** - детектор абсорбции (поглощения) агрессивного потока лимитными заявками. Выявляет ситуации, когда крупный объем агрессивных сделок происходит на одном ценовом уровне, что указывает на наличие скрытой ликвидности или institutional activity.

**Назначение**: Третичный детектор сигналов order flow, подтверждающий наличие крупных лимитных заявок, поглощающих агрессивный поток.

## Математическая основа

### Критерии детектирования

```
Если:
- total_volume ≥ min_volume
- max_price - min_price ≤ price_tolerance
- time_window ≤ window_sec

Тогда: АБСОРБЦИЯ ОБНАРУЖЕНА
```

## Детальная структура класса

### Атрибуты

#### Конфигурационные параметры
```python
self.price_tolerance: float  # Максимальный разброс цен (default: 0.0)
self.min_volume: float       # Минимальный общий объем (default: 0.0)
self.window_sec: float       # Временное окно анализа (default: 10.0)
```

#### Состояние
```python
self._ticks: Deque[Tuple[float, float, float]]  # (timestamp, price, volume)
```

### Метод push()

**Назначение**: Добавляет тик в анализ и проверяет условия абсорбции.

#### Этапы обработки

1. **Добавление тика в буфер**
   ```python
   ts = float(ts_raw) / 1000.0  # Конвертация в секунды
   volume = float(tick.get("qty") or tick.get("volume") or 0)
   self._ticks.append((ts, price, volume))
   ```

2. **Очистка старых тиков**
   ```python
   cutoff = ts - self.window_sec
   while self._ticks and self._ticks[0][0] < cutoff:
       self._ticks.popleft()
   ```

3. **Расчет общего объема**
   ```python
   total_volume = sum(item[2] for item in self._ticks)
   if total_volume < self.min_volume:
       return None
   ```

4. **Проверка ценового диапазона**
   ```python
   prices = [item[1] for item in self._ticks]
   if max(prices) - min(prices) > self.price_tolerance:
       return None  # Слишком широкий диапазон
   ```

5. **Определение стороны**
   ```python
   side = "unknown"
   if book:
       asks = book.get("asks") or []
       bids = book.get("bids") or []
       if asks and price >= float(asks[0][0]):
           side = "short"  # Продажи выше best ask
       elif bids and price <= float(bids[0][0]):
           side = "long"   # Покупки ниже best bid
   ```

6. **Генерация события**
   ```python
   return {
       "type": "absorption",
       "volume": total_volume,
       "side": side,
   }
   ```

## Конфигурационные параметры

### price_tolerance (ценовой допуск)
**Тип**: `float`, **Диапазон**: 0.0-10.0, **Default**: 0.0
- 0.0: Только тики по одной цене
- 0.01-0.1: Малый разброс
- 1.0+: Широкий диапазон

### min_volume (минимальный объем)
**Тип**: `float`, **Диапазон**: 0.1-100.0, **Default**: 0.0
- Для BTC: 0.1-1.0
- Для ETH: 1.0-10.0

### window_sec (временное окно)
**Тип**: `float`, **Диапазон**: 5.0-300.0, **Default**: 10.0
- 5-15 сек: Краткосрочная абсорбция
- 30-60 сек: Среднесрочная абсорбция

---

# IcebergDetector - Детектор айсбергов

## Обзор

**IcebergDetector** - детектор айсберг-заявок (скрытых крупных лимитных заявок). Выявляет ситуации, когда крупная заявка постепенно пополняется, что указывает на institutional activity или скрытое накопление/распределение позиции.

**Назначение**: Детектор скрытой ликвидности, выявляющий постепенное раскрытие крупных заявок в книге ордеров.

## Математическая основа

### Критерии детектирования

```
Если:
- refresh_count ≥ min_refresh
- duration ≥ min_duration
- объем level постепенно увеличивается

Тогда: АЙСБЕРГ ОБНАРУЖЕН
```

## Детальная структура класса

### Атрибуты

#### Конфигурационные параметры
```python
self.min_refresh: int     # Минимальное количество обновлений (default: 2)
self.min_duration: float  # Минимальная длительность (default: 1.5)
```

#### Состояние
```python
self._level_state: Dict[Tuple[str, float], Dict[str, Any]]
# Ключ: (side, price)
# Значение: {
#   "start": timestamp начала,
#   "last_qty": последний объем,
#   "refresh": счетчик обновлений
# }
```

### Метод push()

**Назначение**: Анализирует обновления книги заявок и выявляет паттерны айсберг-заявок.

#### Этапы обработки

1. **Инициализация переменных**
   ```python
   now = time.time()
   bids = book.get("bids") or []
   asks = book.get("asks") or []
   ```

2. **Обработка каждого уровня**
   ```python
   for side, levels in (("bid", bids), ("ask", asks)):
       if not levels:
           continue
       price = float(levels[0][0])
       qty = float(levels[0][1])
       state = self._level_state.get((side, price))
   ```

3. **Логика отслеживания состояния**
   ```python
   if not state:
       # Новый уровень
       self._level_state[(side, price)] = {
           "start": now, "last_qty": qty, "refresh": 0
       }
       continue

   if qty >= state["last_qty"]:
       # Объем увеличился
       state["refresh"] += 1
   state["last_qty"] = qty
   ```

4. **Проверка условий айсберга**
   ```python
   if (state["refresh"] >= self.min_refresh and
       (now - state["start"]) >= self.min_duration):
       events.append({
           "type": "iceberg",
           "side": side,
           "price": price,
           "duration": now - state["start"],
           "refresh": state["refresh"],
       })
       state["refresh"] = 0  # Сброс
   ```

## Конфигурационные параметры

### min_refresh (минимальное количество обновлений)
**Тип**: `int`, **Диапазон**: 1-10, **Default**: 2
- 1: Любое увеличение объема
- 2-3: Стандартная настройка
- 5+: Только множественные обновления

### min_duration (минимальная длительность)
**Тип**: `float`, **Диапазон**: 0.5-30.0, **Default**: 1.5
- 0.5-1.0: Быстрое детектирование
- 1.5-3.0: Стандартная настройка
- 5.0+: Только долгосрочные паттерны

## Заключение

Детекторы сигналов предоставляют комплексный анализ order flow через четыре взаимодополняющих подхода. Каждый детектор специализируется на выявлении определенных паттернов рыночной микро-структуры, обеспечивая многоуровневую фильтрацию и подтверждение сигналов.
