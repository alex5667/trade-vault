# 📊 L2 Metrics Integration - Расширенные метрики Order Book

## 🎯 Описание

Интегрированы новые модули для расширенного анализа Order Book (Level 2 data):

1. **`signals/orderbook_l2_metrics.py`** - Чистая математика расчёта L2-метрик
2. **`signals/orderbook_l2_tracker.py`** - Отслеживание изменений глубины (refill/depletion)

## ✅ Что было добавлено

### 1. Модуль `orderbook_l2_metrics.py`

**Функционал**:
- ✅ Расчёт глубины на разных уровнях (`depth_5`, `depth_20`)
- ✅ Order Book Imbalance (OBI) на разных глубинах
- ✅ **Slope** (эластичность) - `cum_depth / distance_bps`
- ✅ **Microprice** (взвешенная цена с учётом размера и расстояния)
- ✅ **Wall detection** (крупные уровни близко к mid)
- ✅ Spread в базисных пунктах (bps)
- ✅ Поддержка float и string форматов (Binance compatibility)

**Основные метрики**:

```python
@dataclass
class L2Metrics:
    ts: int                         # Timestamp (ms)
    best_bid: float                 # Лучшая цена покупки
    best_ask: float                 # Лучшая цена продажи
    mid: float                      # Mid price
    spread_bps: float               # Spread в bps
    
    # Глубина на разных уровнях
    depth_bid_5: float              # Bid depth на 5 уровнях
    depth_ask_5: float              # Ask depth на 5 уровнях
    depth_bid_20: float             # Bid depth на 20 уровнях
    depth_ask_20: float             # Ask depth на 20 уровнях
    
    # OBI на разных глубинах
    obi_5: float                    # OBI на 5 уровнях
    obi_20: float                   # OBI на 20 уровнях
    
    # Эластичность (slope)
    slope_bid_20: float             # Bid slope на 20 уровнях
    slope_ask_20: float             # Ask slope на 20 уровнях
    
    # Microprice (взвешенная справедливая цена)
    microprice_20: float            # Microprice на 20 уровнях
    microprice_shift_bps_20: float  # Отклонение от mid (bps)
    
    # Wall detection
    wall_bid: bool                  # True если bid wall обнаружен
    wall_ask: bool                  # True если ask wall обнаружен
    wall_bid_dist_bps: float        # Расстояние до bid wall (bps)
    wall_ask_dist_bps: float        # Расстояние до ask wall (bps)
    
    # Top depth (для tracking)
    bid_top3: float                 # Bid depth на 3 уровнях
    ask_top3: float                 # Ask depth на 3 уровнях
    bid_top5: float                 # Bid depth на 5 уровнях
    ask_top5: float                 # Ask depth на 5 уровнях
```

### 2. Модуль `orderbook_l2_tracker.py`

**Функционал**:
- ✅ Отслеживание изменений топ-глубины (refill/depletion detection)
- ✅ Расчёт относительных изменений объёма (ratio)
- ✅ Хранение последнего снимка L2-метрик
- ✅ Impact proxy через изменения depth

**Основные классы**:

```python
@dataclass
class L2Change:
    """Относительные изменения топ-глубины"""
    bid_top3_ratio: float = 0.0  # + => refill, - => depletion
    ask_top3_ratio: float = 0.0
    bid_top5_ratio: float = 0.0
    ask_top5_ratio: float = 0.0

@dataclass
class L2Snapshot:
    """Снимок L2-метрик с изменениями"""
    m: L2Metrics    # Полные метрики
    ch: L2Change    # Изменения

class L2BookTracker:
    """Трекер с отслеживанием изменений"""
    def feed(self, book: dict) -> Optional[L2Snapshot]
    def get_last(self) -> Optional[L2Snapshot]
    def reset(self) -> None
```

### 3. Тесты

- ✅ `tests/test_orderbook_l2_metrics.py` - Тесты для расчёта метрик
- ✅ `tests/test_orderbook_l2_tracker.py` - Тесты для трекера

---

## 📖 Использование

### Базовый пример:

```python
from signals.orderbook_l2_metrics import compute_l2_metrics

book_data = {
    "ts": 1732881234567,
    "bids": [[96500.50, 1.234], [96500.00, 2.456], [96499.50, 0.789]],
    "asks": [[96501.00, 0.987], [96501.50, 1.543], [96502.00, 2.109]]
}

# Расчёт метрик
metrics = compute_l2_metrics(book_data, k_small=5, k_large=20)

if metrics:
    print(f"Best Bid: {metrics.best_bid:.2f}")
    print(f"Best Ask: {metrics.best_ask:.2f}")
    print(f"Spread: {metrics.spread_bps:.2f} bps")
    print(f"OBI_5: {metrics.obi_5:.3f}")
    print(f"OBI_20: {metrics.obi_20:.3f}")
    print(f"Microprice: {metrics.microprice_20:.2f}")
    print(f"Microprice shift: {metrics.microprice_shift_bps_20:.2f} bps")
    print(f"Wall on bid: {metrics.wall_bid}")
    print(f"Wall on ask: {metrics.wall_ask}")
```

### Отслеживание изменений:

```python
from signals.orderbook_l2_tracker import L2BookTracker

# Инициализация трекера
tracker = L2BookTracker(
    k_small=5,              # Малая глубина (5 уровней)
    k_large=20,             # Большая глубина (20 уровней)
    wall_mult=3.0,          # Wall = 3x медианы
    wall_max_dist_bps=15.0  # Wall в пределах 15 bps от mid
)

# При каждом book update
snap = tracker.feed(book_data)

if snap:
    # Доступ к метрикам
    print(f"OBI_5: {snap.m.obi_5:.3f}")
    print(f"Depth bid 5: {snap.m.depth_bid_5:.2f}")
    
    # Проверка изменений
    if snap.ch.bid_top3_ratio < -0.2:
        print("⚠️ Bid depletion: -20% volume on top 3 levels")
    
    if snap.ch.ask_top5_ratio > 0.3:
        print("📈 Ask refill: +30% volume on top 5 levels")
    
    # Wall detection
    if snap.m.wall_bid:
        print(f"🧱 Bid wall at {snap.m.wall_bid_dist_bps:.1f} bps from mid")
```

---

## 🔧 Интеграция в OrderFlow Handlers

### Вариант 1: Добавить в `base_orderflow_handler.py`

```python
from signals.orderbook_l2_tracker import L2BookTracker

class BaseOrderFlowHandler:
    def __init__(self, ...):
        # ... existing code ...
        
        # L2 Tracker для расширенных метрик
        self.l2_tracker = L2BookTracker(
            k_small=5,
            k_large=20,
            wall_mult=float(os.getenv("L2_WALL_MULT", "3.0")),
            wall_max_dist_bps=float(os.getenv("L2_WALL_MAX_DIST_BPS", "15.0"))
        )
        self._last_l2_snap = None
    
    def _process_book(self, book_data: Dict[str, Any]) -> None:
        """Обработка Order Book с L2-метриками"""
        self.processed_books += 1
        
        # Существующая логика (OBI для сигналов)
        ts = int(book_data.get("ts", 0)) or int(time.time() * 1000)
        real_obi = obi_from_book(book_data, depth=5)
        if real_obi is None:
            return
        
        self._last_obi = float(real_obi)
        self._last_obi_ts = ts
        self._track_obi(ts, self._last_obi)
        
        # ✅ НОВОЕ: Расширенные L2-метрики
        l2_snap = self.l2_tracker.feed(book_data)
        if l2_snap:
            self._last_l2_snap = l2_snap
            self._process_l2_signals(l2_snap)
    
    def _process_l2_signals(self, snap: L2Snapshot) -> None:
        """
        Обработка L2-метрик для дополнительных сигналов.
        
        Можно переопределить в subclass для специфичной логики.
        """
        # Пример: Wall detection
        if snap.m.wall_bid and snap.m.wall_bid_dist_bps < 10.0:
            self.logger.debug(
                f"Bid wall detected at {snap.m.wall_bid_dist_bps:.1f} bps, "
                f"depth_bid_5={snap.m.depth_bid_5:.2f}"
            )
        
        # Пример: Depletion detection
        if snap.ch.bid_top3_ratio < -0.3:
            self.logger.debug(
                f"Bid depletion: {snap.ch.bid_top3_ratio:.1%} "
                f"(depth: {snap.m.bid_top3:.2f})"
            )
        
        # Пример: Microprice divergence
        if abs(snap.m.microprice_shift_bps_20) > 5.0:
            self.logger.debug(
                f"Microprice divergence: {snap.m.microprice_shift_bps_20:.2f} bps"
            )
```

### Вариант 2: Использовать в `_generate_signals`

```python
def _generate_signals(self, ctx: SignalContext) -> bool:
    """Генерация сигналов с учётом L2-метрик"""
    # ... existing code ...
    
    # ✅ Дополнительная проверка через L2-метрики
    l2_snap = self._last_l2_snap
    if l2_snap:
        # Absorption: проверяем depletion на стороне импульса
        if ctx.weak_progress and l2_snap.ch.bid_top3_ratio < -0.2:
            # Bid depletion + weak progress = сильный absorption сигнал
            self.logger.debug(
                f"Absorption confirmed by bid depletion: {l2_snap.ch.bid_top3_ratio:.1%}"
            )
        
        # Breakout: проверяем wall на противоположной стороне
        if dir_up and l2_snap.m.wall_ask and l2_snap.m.wall_ask_dist_bps < 15.0:
            # Breakout вверх + ask wall близко = потенциальное сопротивление
            self.logger.debug(
                f"Breakout may face resistance: ask wall at {l2_snap.m.wall_ask_dist_bps:.1f} bps"
            )
        
        # Microprice divergence как дополнительный фильтр
        if abs(l2_snap.m.microprice_shift_bps_20) > 10.0:
            # Сильная дивергенция microprice может указывать на дисбаланс
            pass
    
    # ... existing signal generation logic ...
```

---

## 📊 Примеры сигналов с L2-метриками

### 1. Absorption с Depletion

```python
# Условие:
# - Delta spike (z_abs > threshold)
# - Weak progress (цена не двигается)
# - Bid depletion (bid_top3_ratio < -20%)
# - OBI не подтверждает импульс

if (
    z_abs >= self.absorption_z_threshold
    and ctx.weak_progress
    and l2_snap.ch.bid_top3_ratio < -0.2  # ✅ L2: Depletion
    and not obi_confirms
):
    # Сильный absorption сигнал
    self._publish_signal(side, ctx, "Absorption + Depletion", "🛡️🔻")
```

### 2. Breakout с Wall Detection

```python
# Условие:
# - Пересечение уровня
# - Delta spike
# - OBI подтверждает
# - НЕТ wall на противоположной стороне (чистый путь)

if (
    lvl
    and z_abs >= self.breakout_z_threshold
    and obi_confirms
    and not (l2_snap.m.wall_ask if dir_up else l2_snap.m.wall_bid)  # ✅ L2: No wall
):
    # Сильный breakout сигнал (нет препятствий)
    self._publish_signal(impulse_side, ctx, "Breakout (clear path)", "🚀✨")
```

### 3. Refill как подтверждение уровня

```python
# Условие:
# - Цена близко к pivot
# - Bid refill (bid_top5_ratio > +30%)
# - OBI sustained в сторону bid

if (
    is_near_level_atr(ctx.price, ctx.pivots, ctx.atr, 0.3)
    and l2_snap.ch.bid_top5_ratio > 0.3  # ✅ L2: Refill
    and ctx.obi_sustained
    and ctx.obi_avg > 0.5
):
    # Уровень поддержки усилился (refill)
    self._publish_signal("BUY", ctx, "Support reinforced (refill)", "🛡️📈")
```

### 4. Microprice Divergence

```python
# Условие:
# - Microprice сильно отклоняется от mid (> 10 bps)
# - Направление divergence совпадает с delta

if (
    abs(l2_snap.m.microprice_shift_bps_20) > 10.0
    and (l2_snap.m.microprice_shift_bps_20 > 0) == (z_delta > 0)  # ✅ L2: Divergence confirms
):
    # Microprice подтверждает направление импульса
    pass
```

---

## 🔧 Конфигурация

### Environment Variables:

```bash
# L2 Tracker settings
L2_WALL_MULT=3.0                    # Wall = 3x медианы объёма
L2_WALL_MAX_DIST_BPS=15.0           # Wall в пределах 15 bps от mid
L2_K_SMALL=5                        # Малая глубина (5 уровней)
L2_K_LARGE=20                       # Большая глубина (20 уровней)

# L2 Signal thresholds
L2_DEPLETION_THRESHOLD=-0.2         # -20% для depletion сигнала
L2_REFILL_THRESHOLD=0.3             # +30% для refill сигнала
L2_MICROPRICE_DIVERGENCE_BPS=10.0   # 10 bps для divergence сигнала
```

### Docker Compose:

```yaml
multi-symbol-orderflow:
  environment:
    # ... existing vars ...
    
    # L2 Metrics
    - L2_WALL_MULT=3.0
    - L2_WALL_MAX_DIST_BPS=15.0
    - L2_K_SMALL=5
    - L2_K_LARGE=20
    - L2_DEPLETION_THRESHOLD=-0.2
    - L2_REFILL_THRESHOLD=0.3
    - L2_MICROPRICE_DIVERGENCE_BPS=10.0
```

---

## 📈 Преимущества L2-метрик

### 1. **Более точная оценка давления рынка**
- OBI на разных глубинах (5 vs 20 уровней)
- Microprice учитывает не только best bid/ask
- Slope показывает плотность ликвидности

### 2. **Детекция скрытых уровней (Walls)**
- Крупные ордера близко к mid
- Потенциальные зоны поддержки/сопротивления
- Раннее предупреждение о препятствиях

### 3. **Отслеживание изменений (Refill/Depletion)**
- Depletion → слабая защита уровня (absorption)
- Refill → усиление уровня (support/resistance)
- Impact proxy для оценки агрессии

### 4. **Дополнительные фильтры для сигналов**
- Absorption + Depletion = более сильный сигнал
- Breakout + No Wall = чистый путь
- Microprice divergence = подтверждение направления

---

## 🧪 Тестирование

### Запуск тестов:

```bash
# Все тесты L2-метрик
cd python-worker
pytest tests/test_orderbook_l2_metrics.py -v
pytest tests/test_orderbook_l2_tracker.py -v

# Конкретный тест
pytest tests/test_orderbook_l2_metrics.py::test_compute_l2_metrics_basic -v

# С coverage
pytest tests/test_orderbook_l2*.py --cov=signals.orderbook_l2_metrics --cov=signals.orderbook_l2_tracker
```

### Ожидаемые результаты:

```
tests/test_orderbook_l2_metrics.py::test_norm_levels_float PASSED
tests/test_orderbook_l2_metrics.py::test_norm_levels_string PASSED
tests/test_orderbook_l2_metrics.py::test_compute_l2_metrics_basic PASSED
tests/test_orderbook_l2_metrics.py::test_compute_l2_metrics_wall_detection PASSED
tests/test_orderbook_l2_tracker.py::test_tracker_refill_detection PASSED
tests/test_orderbook_l2_tracker.py::test_tracker_depletion_detection PASSED
```

---

## 📝 Следующие шаги (опционально)

### 1. Интеграция в Handlers (рекомендуется)
- [ ] Добавить `L2BookTracker` в `BaseOrderFlowHandler.__init__`
- [ ] Реализовать `_process_l2_signals` метод
- [ ] Использовать L2-метрики в `_generate_signals`

### 2. Дополнительные метрики (future)
- [ ] Volume-weighted average price (VWAP) на N уровнях
- [ ] Liquidity concentration (Gini coefficient)
- [ ] Order book pressure (bid_depth / ask_depth ratio)
- [ ] Time-weighted OBI (exponential moving average)

### 3. Визуализация (future)
- [ ] Графики L2-метрик в Grafana
- [ ] Heatmap глубины книги
- [ ] Timeline depletion/refill events

---

## ✅ Статус

- ✅ `signals/orderbook_l2_metrics.py` создан
- ✅ `signals/orderbook_l2_tracker.py` создан
- ✅ Тесты написаны и проходят
- ✅ Документация создана
- ✅ Linter errors: 0
- ✅ **Ready for integration** 🚀

---

## 📚 Связанные файлы

### Новые модули:
- `python-worker/signals/orderbook_l2_metrics.py` - Расчёт L2-метрик
- `python-worker/signals/orderbook_l2_tracker.py` - Трекер изменений
- `python-worker/tests/test_orderbook_l2_metrics.py` - Тесты метрик
- `python-worker/tests/test_orderbook_l2_tracker.py` - Тесты трекера

### Существующие модули (для интеграции):
- `python-worker/handlers/base_orderflow_handler.py` - Базовый handler
- `python-worker/handlers/crypto_orderflow_handler.py` - Crypto handler
- `python-worker/signals/detectors.py` - Существующая функция `obi_from_book`
- `python-worker/signals/orderbook_metrics.py` - Существующие метрики

### Документация:
- `BOOK_DATA_FORMAT.md` - Формат book_data
- `L2_METRICS_INTEGRATION.md` - Этот документ

---

**Дата**: 2025-11-29  
**Версия**: 1.0  
**Статус**: ✅ Ready for Integration  
**Рекомендация**: Интегрировать в `BaseOrderFlowHandler` для расширенного анализа Order Book

