# Signal Execution Examples

Примеры использования системы планирования исполнения сигналов и анализа производительности.

## 🚀 Быстрый Старт

### Запуск демо
```bash
cd /home/alex/front/trade/scanner_infra/python-worker
python examples/demo_execution_and_performance.py
```

### Результат
Демо покажет:
- **EXECUTION PLAN**: Детальный план исполнения с уровнями и рисками
- **SIGNAL PERFORMANCE**: Метрики производительности (TTD, MFE/MAE, Realized R)
- **ANALYSIS**: Интерпретация результатов

## 📊 Что демонстрирует демо

### 1. Execution Planning
```
📊 Signal Details:
   Symbol         : XAUUSD
   Side           : LONG

🎯 Entry Zone:
   Entry Low      : 2600.95
   Entry High     : 2601.55

🛑 Risk Management:
   Stop Price     : 2600.50
   TP Levels      : [2602.0, 2602.75, 2603.5]

💰 Position Sizing:
   Risk R         : 1.50
   Risk USD       : $50.00
   Position Size  : 0.333 lots
```

### 2. Performance Analysis
```
📈 Performance Metrics:
   Realized R     : 2.00R
   MFE (Max Up)   : 4.19R
   MAE (Max Down) : 0.00R

🎯 TTD Analysis:
   TTD Bars       : 1 bars
   TTD Seconds    : 60 sec

📊 Trade Flow:
   Outcome        : REALIZED
```

## 🏗️ Архитектура демо

### Signal Context
- Моковый сигнал XAUUSD volatility_spike
- Микроструктурные данные (swing points, HTF levels)
- Состояние счета и ATR метрики

### Execution Planning
- Risk-based entry zones
- Микроструктурные стопы
- Dynamic position sizing по confidence score
- TTD-aware expiry times

### Performance Simulation
- Синтетические 1m бары после сигнала
- Симуляция входа/выхода из позиции
- Полный анализ TTD, MFE/MAE, realized R

## 📚 Использование в Production

### Интеграция в scanner_infra

```python
# В BaseOrderFlowHandler
def _create_execution_plan(self, ctx: SignalContext) -> Optional[ExecutionPlan]:
    from signal_exec import ExecutionPlanner, ExtendedSignalContext

    # Конвертация SignalContext -> ExtendedSignalContext
    extended_ctx = ExtendedSignalContext(
        signal_id=ctx.signal_id,
        symbol=ctx.symbol,
        side=ctx.side,
        setup_type=ctx.pattern_name,
        ts_signal=ctx.ts_utc,
        price_at_signal=ctx.last_price,
        final_score=ctx.confidence,
        # ... остальные поля
    )

    # Создание плана
    planner = ExecutionPlanner(setup_configs)
    plan = planner.build_plan(extended_ctx)

    return plan
```

### Сохранение в TimescaleDB

```python
# Требуется: pip install psycopg[binary]
from signal_exec import SignalExecutionRepository

repo = SignalExecutionRepository(dsn="postgresql://user:pass@host:port/db")
repo.insert_signal(ctx, extra_json={"source": "scanner_infra"})
repo.insert_execution_plan(plan)
```

### Анализ производительности

```python
from signal_exec import SignalPerformanceTracker

tracker = SignalPerformanceTracker(r_target=1.0)
bars = repo.load_1m_bars(symbol, start_ts, end_ts)
performance = tracker.build_performance(ctx, bars, entry_ts, exit_ts, ...)
repo.insert_signal_performance(performance)
```

## ⚙️ Конфигурация

### Symbol/Setup Configs
```python
setup_configs = {
    ("XAUUSD", "breakout_R1"): SymbolSetupConfig(
        symbol="XAUUSD",
        setup_type="breakout_R1",
        expiry_bars=5,
        score_buckets=(0.4, 0.7, 0.85),
        risk_multipliers=(0.5, 1.0, 1.5, 2.0),
        max_risk_R_per_trade=1.0,
        max_portfolio_risk_pct=5.0,
    ),
}
```

### TTD Optimization
```sql
-- Автоматическое обновление expiry_bars
INSERT INTO signal_ttd_config (symbol, setup_type, expiry_bars, ...)
SELECT symbol, setup_type,
       percentile_disc(0.75) WITHIN GROUP (ORDER BY ttd_bars)
FROM signal_performance
WHERE mfe_R >= 1.0;
```

## 🔧 Зависимости

### Обязательные
- Python 3.8+
- dataclasses (built-in)
- typing (built-in)

### Опциональные (для TimescaleDB)
```bash
pip install psycopg[binary]
```

## 📈 Ключевые Метрики

### Planning
- **Entry Zone**: R-based зоны от стоп-уровня
- **Stop Loss**: Микроструктурные уровни (swing points)
- **Take Profit**: HTF targets + R-multiples
- **Position Size**: Score-based risk sizing
- **Expiry**: TTD quantile-based timing

### Performance
- **TTD**: Time-To-Decay (время достижения 1R)
- **MFE/MAE**: Max favorable/adverse excursion
- **Realized R**: Итоговый результат в R-multiples
- **Outcome**: realized/stopped/expired классификация

## 🎯 Production Ready

Система полностью готова для продакшена:
- ✅ Полная типизация и обработка ошибок
- ✅ TimescaleDB интеграция для временных рядов
- ✅ Опциональные зависимости
- ✅ Подробное логирование и метрики
- ✅ Модульная архитектура для расширения

**Результат**: Готовая система execution planning и performance tracking интегрирована в scanner_infra! 🚀
