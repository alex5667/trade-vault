# Regime Engine

Regime Engine - система классификации рыночных режимов (trend/range/mixed) для улучшения качества сигналов orderflow.

## Обзор

Regime Engine рассчитывает `regime_score` ∈ [-1, +1]:
- **+1**: Сильный тренд (высокая волатильность, направленный дельта-поток)
- **-1**: Сильный рендж (низкая волатильность, частые пересечения VWAP)
- **0**: Смешанный режим (неопределенность)

На основе score присваивается лейбл: "trend", "range", "mixed".

## Интеграция

### В DataProcessor
```python
from handlers.data_processor import OrderFlowDataProcessor

processor = OrderFlowDataProcessor(symbol, specs, config)
# RegimeEngine автоматически инициализируется в __init__
```

### В SignalGenerator
```python
from handlers.signal_generator import SignalGenerator

generator = SignalGenerator(symbol, config, outbox)
# Hard gate по режимам применяется автоматически в _exec_quality_ok
```

## Hard Gate правила

- **Breakout сигналы** (z > 2.0): разрешены только при `regime_score >= 0` (trend/mixed)
- **Mean reversion сигналы** (z < -1.5): разрешены только при `regime_score <= 0` (range/mixed)
- **Mixed режим**: разрешает оба типа сигналов

## Конфигурация

```python
regime_atr_n = 14                    # Период ATR
regime_atr_hist = 120                # История для ATR квантилей (~2h)
regime_atr_hi_q = 0.70               # Верхний порог ATR квантиля для тренда
regime_atr_lo_q = 0.35               # Нижний порог ATR квантиля для ренджа

regime_delta_ema_alpha = 0.05        # EMA alpha для дельта-потока
regime_cross_hist = 30               # Окно для подсчета VWAP пересечений
regime_hold_ema_alpha = 0.10         # EMA alpha для persistence

regime_w_atr = 0.35                  # Вес ATR в итоговом score
regime_w_delta = 0.30                # Вес дельта-потока
regime_w_hold = 0.25                 # Вес persistence
regime_w_ping = 0.20                 # Вес VWAP пересечений

regime_label_hi = 0.35               # Порог для лейбла "trend"
regime_label_lo = -0.35              # Порог для лейбла "range"
```

## Features

1. **ATR квантили**: Волатильность относительно недавней истории
2. **VWAP deviation**: Отклонение цены от VWAP
3. **Delta flow**: Направленность объема (EMA сглаженная)
4. **Crossings frequency**: Частота пересечений VWAP (range indicator)
5. **Hold persistence**: Время удержания цены выше/ниже VWAP

## Тестирование

```bash
# Запуск всех тестов
pytest tests/test_regime_*.py -v

# Отдельные компоненты
pytest tests/test_regime_contract.py      # Правила применимости
pytest tests/test_regime_engine.py        # BarBuilder1m + RegimeEngine
pytest tests/test_regime_integration.py   # Интеграция в DataProcessor
pytest tests/test_regime_hard_gate.py     # Hard gate в SignalGenerator
pytest tests/test_regime_end_to_end.py    # Полный пайплайн
```

## Мониторинг

Regime state доступен в:
- `BucketState.regime_score` / `BucketState.regime_label`
- `OrderflowSignalContext.regime_score` / `OrderflowSignalContext.regime_label`

Для PostgreSQL/TimescaleDB можно сохранять regime snapshots для аналитики.

## Производительность

- **O(1)** на тик: VWAP, delta EMA, crossings
- **O(N)** на бар (N~240): ATR quantile update
- **O(1)**: compute() - взвешенная сумма features

Оптимально для HFT orderflow processing.
