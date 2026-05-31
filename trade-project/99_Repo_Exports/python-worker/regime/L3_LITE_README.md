# L3-Lite Metrics System

Система расчета метрик из L3-Lite потока и книги ордеров для улучшения качества сигналов крипто-ордерфлоу.

## Обзор

L3-Lite система предоставляет дополнительные метрики из микроструктуры рынка:

- **Cancel/Trade ratios** - соотношение отмен/сделок по сторонам
- **Microprice drift** - сдвиг микропрайса во времени
- **Order Book Imbalance** - дисбаланс объема в разных горизонтах
- **Spread analysis** - анализ спреда

Эти метрики интегрируются в confidence scorer для более точной оценки сигналов.

## Архитектура

### L3LiteMetricsAggregator

Основной класс для агрегации метрик из потока L3-Lite событий и обновлений книги.

```python
from regime import L3LiteMetricsAggregator, L3LiteEvent, BookSnapshot

# Инициализация
aggregator = L3LiteMetricsAggregator(
    microprice_horizon_sec=20,  # горизонт для microprice drift
    obi_persistence_sec=30,     # горизонт для OBI persistence
)

# Обработка событий
aggregator.on_l3_event(L3LiteEvent(...))
aggregator.on_book_update(BookSnapshot(...))

# Получение метрик
features = aggregator.build_features(now_ms)
```

### Модели данных

#### L3LiteEvent
Событие L3-Lite потока:
```python
@dataclass
class L3LiteEvent:
    ts_ms: int      # время события
    kind: str       # 'trade', 'cancel', 'new', 'replace'
    side: str       # 'bid' / 'ask'
    price: float
    qty: float
```

#### BookSnapshot
Снимок книги ордеров:
```python
@dataclass
class BookSnapshot:
    ts_ms: int
    bids: List[Tuple[float, float]]  # [(price, qty), ...]
    asks: List[Tuple[float, float]]  # [(price, qty), ...]
```

#### L3LiteFeatures
Рассчитанные метрики:
```python
@dataclass
class L3LiteFeatures:
    cancel_to_trade_bid_5s: float
    cancel_to_trade_ask_5s: float
    cancel_to_trade_bid_20s: float
    cancel_to_trade_ask_20s: float
    microprice: float
    microprice_shift_bps_20: float
    spread_bps: float
    obi_5: float
    obi_20: float
    obi_50: float
    obi_persistence_score: float
```

## Метрики

### Cancel/Trade Ratios

**Формула**: `cancel_to_trade = sum(cancels) / max(sum(trades), eps)`

Рассчитывается отдельно для bid/ask сторон по окнам 5s и 20s.

**Интерпретация**:
- Высокий ratio → много отмен, слабость стороны
- Низкий ratio → активные торги, сила стороны

### Microprice Drift

**Формула**: `drift_bps = (mp_now - mp_past) / mid_now * 10000`

**Интерпретация**:
- Положительный drift → микропрайс растет
- Отрицательный drift → микропрайс падает
- Используется для подтверждения направления сигнала

### Order Book Imbalance (OBI)

**Формула**: `obi = (v_bid - v_ask) / (v_bid + v_ask)`

Рассчитывается для разных глубин: 5, 20, 50 уровней.

**Интерпретация**:
- > 0 → перевес покупателей
- < 0 → перевес продавцов
- = 0 → баланс

### OBI Persistence Score

**Формула**: доля времени в окне, когда OBI стабильно направлен в одну сторону.

**Интерпретация**:
- Высокий score → устойчивый дисбаланс
- Низкий score → хаотичное поведение

### Spread Analysis

**Формула**: `spread_bps = (ask - bid) / mid * 10000`

**Интерпретация**:
- Узкий спред → хорошая ликвидность
- Широкий спред → проблемы с ликвидностью

## Интеграция в SignalContext

SignalContext расширен L3-метриками:

```python
@dataclass
class SignalContext:
    # ... существующие поля ...

    # L3-Lite метрики
    cancel_to_trade_bid_5s: float = 0.0
    cancel_to_trade_ask_5s: float = 0.0
    cancel_to_trade_bid_20s: float = 0.0
    cancel_to_trade_ask_20s: float = 0.0

    microprice_shift_bps_20: float = 0.0

    spread_bps: float = 0.0
    obi_5: float = 0.0
    obi_20: float = 0.0
    obi_50: float = 0.0
    obi_persistence_score: float = 0.0
```

## CryptoConfScorer с L3-terms

Confidence scorer включает L3-метрики:

```python
from regime import CryptoConfScorer, CryptoConfScorerConfig

# Конфигурация
cfg = CryptoConfScorerConfig(
    l3_spread_max_ok_bps=5.0,        # порог хорошего спреда
    l3_spread_hard_limit_bps=20.0,   # предел спреда
    l3_cancel_to_trade_soft=2.0,     # мягкий порог cancel/trade
    l3_cancel_to_trade_hard=5.0,     # жесткий порог cancel/trade
    l3_mp_drift_max_bps=5.0,         # предел microprice drift
)

# Инициализация
scorer = CryptoConfScorer(cfg)

# Расчет confidence
confidence = scorer(signal_context)
```

### L3-Terms

#### Spread OK Term
```python
def _spread_ok_term(self, ctx) -> float:
    # +0.5 для узкого спреда, -2.0 для широкого
```

#### OBI Persistence Term
```python
def _obi_persistence_term(self, ctx) -> float:
    # +1.0 для устойчивого дисбаланса
```

#### Cancel/Trade Term
```python
def _cancel_to_trade_term(self, ctx) -> float:
    # штраф за экстремальные значения cancel/trade
```

#### Microprice Drift Term
```python
def _microprice_drift_term(self, ctx) -> float:
    # оценка направления drift
```

## Интеграция в CryptoOrderFlowHandler

```python
class CryptoOrderFlowHandler(BaseOrderFlowHandler):
    def __init__(self, ...):
        super().__init__(...)
        self.l3_agg = L3LiteMetricsAggregator()
        self.conf_scorer = CryptoConfScorer()

    def on_l3_event(self, ev):
        self.l3_agg.on_l3_event(ev)

    def on_book_update(self, snap):
        self.l3_agg.on_book_update(snap)

    # SignalContext автоматически получает L3-метрики
    # в _process_tick базового класса
```

## Конфигурация

### Переменные окружения

```bash
# L3-Lite конфигурация
L3_SPREAD_MAX_OK_BPS=5.0
L3_SPREAD_HARD_LIMIT_BPS=20.0
L3_CANCEL_TO_TRADE_SOFT=2.0
L3_CANCEL_TO_TRADE_HARD=5.0
L3_MP_DRIFT_MAX_BPS=5.0
```

### Параметры L3LiteMetricsAggregator

```python
L3LiteMetricsAggregator(
    microprice_horizon_sec=20,  # секунды для microprice history
    obi_persistence_sec=30,     # секунды для OBI persistence
)
```

## Мониторинг и отладка

### Логирование

L3-метрики логируются при расчете:

```
L3 Features: spread=2.1, obi_5=0.15, cancel_bid_5s=1.2
Confidence: +0.8 (spread: +0.3, obi: +0.4, cancel: -0.1)
```

### Тестирование

```bash
# Запуск примера
python -m regime.l3_lite_example
```

### Проверка данных

```sql
-- Проверка доступности L3-метрик в сигналах
SELECT
    symbol, family,
    spread_bps,
    obi_5, obi_20,
    cancel_to_trade_bid_5s,
    microprice_shift_bps_20
FROM signals
WHERE created_at > now() - interval '1 hour'
ORDER BY created_at DESC
LIMIT 10;
```

## Производительность

- **Память**: O(1) - ограниченные deque для хранения истории
- **CPU**: минимальная нагрузка, расчет метрик за O(1)
- **Задержка**: метрики доступны синхронно с обновлениями книги

## Расширение

Система легко расширяема новыми метриками:

1. Добавить поле в `L3LiteFeatures`
2. Реализовать расчет в `L3LiteMetricsAggregator.build_features()`
3. Добавить поле в `SignalContext`
4. Создать term в `CryptoConfScorer`

## Диагностика проблем

### Метрики не рассчитываются
- Проверить наличие обновлений книги
- Проверить корректность формата L3-событий

### Низкий confidence score
- Проверить экстремальные значения L3-метрик
- Настроить пороги в `CryptoConfScorerConfig`

### Высокая нагрузка
- Уменьшить горизонты хранения истории
- Оптимизировать частоту вызовов `build_features()`
