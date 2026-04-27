# Signal Execution Module

Комплексная система планирования и анализа исполнения сигналов с расчетом TTD (Time-To-Decay), MFE/MAE и риск-менеджментом.

## Основные Компоненты

### 1. ExtendedSignalContext
Расширенный контекст сигнала с микроструктурными данными:
- Swing points для стопов
- HTF уровни для тейков
- L2 snapshot
- ATR данные
- Account state для risk sizing

### 2. ExecutionPlanner
Планировщик исполнения сигналов:
- Автоматический выбор уровней входа/выхода
- Риск-менеджмент на основе score
- TTD-based expiry times
- Микроструктурный анализ

### 3. SignalPerformanceTracker
Анализ результатов исполнения:
- TTD (Time-To-Decay) расчеты
- MFE/MAE (Max Favorable/Adverse Excursion)
- Realized R tracking
- Outcome classification

## Быстрый Старт

```python
from signal_execution import ExecutionPlanner, SignalPerformanceTracker, ExtendedSignalContext, SymbolSetupConfig

# Создаем контекст сигнала
ctx = ExtendedSignalContext(
    signal_id="sig-123",
    symbol="XAUUSD",
    side="long",
    setup_type="breakout_R1",
    ts_signal=datetime.now(),
    price_at_signal=1950.0,
    atr_1m=2.5,
    final_score=85.0,
    # ... остальные поля
)

# Настраиваем конфиг
config = SymbolSetupConfig(
    symbol="XAUUSD",
    setup_type="breakout_R1",
    expiry_bars=5,
)

planner = ExecutionPlanner({("XAUUSD", "breakout_R1"): config})
plan = planner.build_plan(ctx)

if plan:
    print(f"Entry zone: {plan.entry_zone_low} - {plan.entry_zone_high}")
    print(f"Stop: {plan.stop_price}")
    print(f"Risk: {plan.risk_usd} USD")
    print(f"Position size: {plan.position_size}")
```

## База Данных

### Миграция
```bash
# Применить миграцию
psql -f migrations/004_create_signal_execution_tables.sql
```

### Обновление TTD Config
```bash
# Запустить расчет TTD квантилей
python scripts/update_ttd_config.py
```

## Интеграция в Pipeline

### 1. Генерация Сигнала
```python
# В вашем handler'е
signal_ctx = self._create_signal_context(...)
execution_plan = self._create_execution_plan(signal_ctx)

# Сохранить сигнал и план
self._save_signal(signal_ctx)
self._save_execution_plan(execution_plan)

# Отправить в execution engine
```

### 2. Исполнение
```python
# В execution engine
# Мониторить вход в позицию по plan.entry_zone_*
# Управлять стопом plan.stop_price
# Частично закрывать по plan.tp_levels с plan.partials
```

### 3. Анализ Результатов
```python
# После закрытия позиции
tracker = SignalPerformanceTracker()
performance = tracker.build_performance(ctx, bars, entry_ts, exit_ts, entry_price, exit_price, stop_price)

# Сохранить результаты
self._save_performance(performance)
```

## Ключевые Метрики

### TTD (Time-To-Decay)
Время достижения цели в барах после входа:
```python
# Сигнал достигает 1R за 3 бара (3 минуты)
ttd_bars = 3
ttd_seconds = 180
```

### MFE/MAE в R
Максимальные отклонения от entry:
```python
# Достиг +2R максимум
mfe_R = 2.0
# Отклонился -0.5R минимум
mae_R = -0.5
# Закрылся с +1.2R
realized_R = 1.2
```

## Настройка Risk Management

### Score-based Sizing
```python
# Score 40-70: 50% от базового риска
# Score 70-85: 100% от базового риска
# Score 85+: 150% от базового риска

score_buckets = (0.4, 0.7, 0.85)
risk_multipliers = (0.5, 1.0, 1.5, 2.0)
```

### Account Limits
```python
max_risk_per_trade_pct = 0.5  # 0.5% от equity
max_portfolio_risk_pct = 5.0   # 5% от equity
```

## Payload Структура

```json
{
  "confidence": 85,
  "executionPlan": {
    "signalId": "sig-123",
    "entryZoneLow": 1945.0,
    "entryZoneHigh": 1955.0,
    "stopPrice": 1940.0,
    "tpLevels": [1960.0, 1970.0, 1980.0],
    "positionSize": 0.05,
    "riskUsd": 50.0
  }
}
```

## Мониторинг

### Key Metrics to Track
- **TTD Distribution**: Среднее время достижения цели
- **Win Rate by TTD**: Эффективность сигналов с разным TTD
- **MFE/MAE Ratios**: Качество entry timing
- **Risk-adjusted Returns**: Realized R vs Risk taken

### Alerts
- TTD config устарел (>24h)
- Высокий процент сигналов без входа
- Аномально высокий MAE

## Расширение

### Добавление Новых Setup Types
```python
# Добавить в SymbolSetupConfig
new_config = SymbolSetupConfig(
    symbol="BTCUSDT",
    setup_type="mean_reversion",
    expiry_bars=2,  # Более короткий expiry
    fade_weak_max=0.25,  # Для mean reversion
)
```

### Кастомные TP Strategies
```python
# Переопределить _build_tp_levels
def _build_tp_levels(self, ctx, cfg, stop_price, entry_low, entry_high):
    # Ваша логика для тейков
    pass
```

## Troubleshooting

### Common Issues
1. **No execution plan generated**: Проверить AccountState и risk limits
2. **TTD always None**: Проверить наличие bars_after_entry
3. **High MAE**: Проверить качество swing point selection

### Debug Mode
```python
# Включить подробное логирование
import logging
logging.getLogger('signal_execution').setLevel(logging.DEBUG)
```
