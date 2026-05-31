# Signal Execution Module - Unified Production System

Комплексная система планирования исполнения сигналов и анализа производительности для scanner_infra с единым SignalContext.

## 🚀 Новое в Unified Version

### Единый SignalContext
```python
@dataclass
class SignalContext:
    signal_id: str
    symbol: str
    setup_type: str
    side: Side
    ts_signal: datetime
    price_at_signal: float
    # ... только нужные поля для планировщика/трекера
    features: Dict[str, float]  # microstructure здесь
```

### Redis Streams Архитектура
```python
stream:signals:detected     # Сырые сигналы
stream:signals:plans        # Сигнал + ExecutionPlan
stream:signals:exec_events  # События исполнения
stream:signals:performance  # Результаты
```

### Unified SignalService
```python
service = SignalService(repo, planner, tracker, bus)
await service.on_new_signal(ctx)  # Полный pipeline
```

## Обзор Архитектуры

Система разделена на сервисы по ответственности:

## 🚀 Быстрый Старт с Unified System

### 1. Создание Базы Данных
```bash
# Создать таблицы в TimescaleDB
psql -f python-worker/signal_exec/db_schema.sql

# Проверить создание
psql -c "\dt signal_*"
```

### 2. Использование Unified SignalService
```python
from signal_exec import SignalService, SignalContext, Side, AccountState

# Инициализация (делается в BaseOrderFlowHandler автоматически)
service = SignalService(repo, planner, tracker, bus)

# Создание сигнала
ctx = SignalContext(
    signal_id="XAUUSD-breakout-123",
    symbol="XAUUSD",
    setup_type="breakout_R1",
    side=Side.LONG,
    ts_signal=datetime.now(timezone.utc),
    price_at_signal=2600.0,
    atr_1m=1.0,
    final_score=0.82,
    account_state=AccountState(...),
    features={'deltaSpikeZ': 2.5, 'OBI': 1.2}
)

# Полный pipeline: plan + DB + Redis + tracking
await service.on_new_signal(ctx)
```

### 3. Обработка Execution Events
```python
# MT5-bridge отправляет события
await service.on_exec_event(
    signal_id="XAUUSD-breakout-123",
    symbol="XAUUSD",
    event_type="ENTRY_FILLED",
    ts=datetime.now(timezone.utc),
    price=2600.5
)
```

### 4. Старый способ (ExecutionPlanner напрямую)
```python
from signal_exec import ExecutionPlanner, SignalRepository

# Инициализация
repo = SignalRepository(dsn="postgresql://user:pass@host:port/trade")
setup_configs = repo.load_setup_configs()
planner = ExecutionPlanner(setup_configs)

# При детекте сигнала
plan = planner.build_plan(ctx)
if plan:
    repo.insert_signal(ctx)
    repo.insert_execution_plan(plan)
    # Публикация в Redis/Kafka
    publish_to_redis("signals", {"ctx": ctx.to_dict(), "plan": asdict(plan)})
```

### 3. Использование в Performance Tracker
```python
from signal_exec import SignalPerformanceTracker

tracker = SignalPerformanceTracker(repo, ttd_target_R=1.0)

# Регистрация сигнала
tracker.register_signal(ctx, plan)

# Обработка баров
tracker.on_bar(symbol, bar)

# Обработка событий исполнения
tracker.on_execution_event(signal_id, "ENTRY_FILLED", ts, price)
tracker.on_execution_event(signal_id, "TP_HIT", ts, price)
```

## 📊 Ключевые Метрики

### Execution Planning
- **Entry Zone**: R-based зоны относительно стопа
- **Stop Loss**: Микроструктурные уровни + ATR буфер
- **Take Profit**: HTF уровни + R-multiples (1R, 2R, 3R)
- **Position Size**: Score-based risk sizing
- **Expiry**: TTD quantile-based timing

### Performance Analysis
- **TTD**: Time-To-Decay (время достижения 1R)
- **MFE/MAE**: Max favorable/adverse excursion
- **Realized R**: Итоговый результат в R-multiples
- **Outcome**: target_hit/stop_hit/breakeven/expired

## 🗄️ Схема Базы Данных

### Hypertables (TimescaleDB)
```sql
-- Сырые сигналы
CREATE TABLE signals (signal_id, ts_signal, symbol, ...);

-- Планы исполнения
CREATE TABLE signal_execution_plan (signal_id, entry_zone_low, ...);

-- Производительность
CREATE TABLE signal_performance (signal_id, realized_R, ttd_bars, ...);
SELECT create_hypertable('signal_performance', 'ts_signal');
```

### Конфигурация TTD
```sql
-- Настройки по символу/сетапу
CREATE TABLE signal_ttd_config (
    symbol, setup_type,
    recommended_expiry_bars,  -- Динамически обновляется
    ...
);
```

## ⚙️ Конфигурация

### Symbol/Setup Configs
```python
# Пример конфига для XAUUSD breakout
SymbolSetupConfig(
    symbol="XAUUSD",
    setup_type="breakout_R1",
    expiry_bars=5,              # TTD-based
    min_stop_ticks=10,
    max_stop_R=3.0,
    atr_buffer_ratio=0.15,
    entry_zone_min_R=0.3,       # 0.3R от стопа
    entry_zone_max_R=0.7,       # 0.7R от стопа
    default_tp_R=(1.0, 2.0, 3.0),  # 1R, 2R, 3R цели
    score_buckets=(0.4, 0.7, 0.85),
    risk_multipliers=(0.5, 1.0, 1.5, 2.0),
    max_risk_R_per_trade=1.0,
    max_portfolio_risk_pct=5.0,
)
```

### TTD Optimization
```sql
-- Ежедневное обновление expiry_bars
INSERT INTO signal_ttd_config(symbol, setup_type, recommended_expiry_bars)
SELECT symbol, setup_type,
       ceil(percentile_cont(0.75) WITHIN GROUP (ORDER BY ttd_bars))::int
FROM signal_performance
WHERE mfe_R >= 1.0 AND ts_signal >= now() - INTERVAL '60 days'
GROUP BY symbol, setup_type;
```

## 🔄 Интеграция в scanner_infra

### Автоматическая Интеграция
**BaseOrderFlowHandler автоматически:**
- Создает unified SignalContext из существующего ctx
- Обрабатывает сигналы через SignalService
- Публикует в Redis Streams
- Сохраняет в TimescaleDB

### Ручная Интеграция (если нужно)
```python
# В вашем детекторе сигналов
from signal_exec import SignalService, SignalContext, Side

# Сигнал обработается автоматически через BaseOrderFlowHandler
# Или вручную:
await signal_service.on_new_signal(unified_ctx)
```

### MT5/NestJS Интеграция
```javascript
// NestJS читает из Redis Streams
const streams = await redis.xread(
  'COUNT', '1', 'BLOCK', '1000',
  'STREAMS',
  'stream:signals:plans',
  '>'
);

const [{signal_id, symbol, payload}] = streams[0].messages;
const {ctx, plan} = JSON.parse(payload);

// Исполнить по правилам плана
await executePlan(plan);
```

### Signal Detector
```python
def on_signal_detected(ctx: SignalContext) -> None:
    plan = planner.build_plan(ctx)
    if plan is None:
        logger.warning(f"Plan rejected for {ctx.signal_id}")
        return

    # Сохранение
    repo.insert_signal(ctx)
    repo.insert_execution_plan(plan)

    # Публикация для исполнения
    redis.publish("execution:plans", json.dumps(asdict(plan)))

    logger.info(f"Signal {ctx.signal_id} planned: {plan.position_size} lots")
```

## 📊 SignalContext - Единое Хранилище

### Структура
```python
@dataclass
class SignalContext:
    # Идентификация
    signal_id: str
    symbol: str
    setup_type: str
    side: Side

    # Время и цена
    ts_signal: datetime
    price_at_signal: float

    # Спецификация инструмента
    atr_1m: float
    tick_size: float
    contract_size: float

    # Скоринг модели
    final_score: float

    # Риск-профиль
    account_state: AccountState

    # Микроструктура для стопов/тейков
    local_swings: List[SwingPoint]
    htf_levels: List[HTFLevel]

    # Кастомные фичи модели
    features: Dict[str, float]  # deltaSpikeZ, OBI, weakProgress...

    # Дополнительные данные
    extra: Dict[str, Any]
```

### Сериализация
```python
# JSON-compatible для Redis/Timescale
ctx_dict = ctx.to_dict()
ctx_restored = SignalContext.from_dict(ctx_dict)
```

### Преимущества
- ✅ **Строгая типизация** - все поля определены
- ✅ **JSON сериализация** - готов для распределенных систем
- ✅ **Микроструктура в features** - гибкое хранение
- ✅ **Легкая конвертация** - из существующего ctx

### Execution Engine
```python
def execute_plan(plan: ExecutionPlan) -> None:
    # Проверка expiry
    if bars_since_signal > plan.expiry_bars:
        return

    # Вход только в зоне
    if not (plan.entry_zone_low <= current_price <= plan.entry_zone_high):
        return

    # Исполнение
    order = place_market_order(
        symbol=plan.symbol,
        side=plan.side,
        size=plan.position_size,
        stop=plan.stop_price,
        targets=plan.tp_levels
    )

    # Сообщить о входе
    tracker.on_execution_event(plan.signal_id, "ENTRY_FILLED", now, fill_price)
```

### Performance Tracker
```python
def on_new_bar(bar: Bar1m) -> None:
    tracker.on_bar(bar.symbol, bar)

def on_execution_event(event: dict) -> None:
    tracker.on_execution_event(
        event["signal_id"],
        event["type"],
        event["timestamp"],
        event["price"]
    )
```

## 📈 Мониторинг и Аналитика

### Key Metrics to Monitor
- **Planning Success Rate**: % сигналов с валидными планами
- **TTD Distribution**: Среднее время достижения целей
- **MFE/MAE Ratios**: Качество entry timing
- **Outcome Distribution**: % target_hit vs stop_hit

### Views для Аналитики
```sql
-- Объединенный view сигналов, планов и производительности
CREATE VIEW signal_execution_summary AS
SELECT s.*, ep.*, sp.*
FROM signals s
LEFT JOIN signal_execution_plan ep ON s.signal_id = ep.signal_id
LEFT JOIN signal_performance sp ON s.signal_id = sp.signal_id;

-- Производительность по символу/сетапу
CREATE VIEW signal_performance_summary AS
SELECT symbol, setup_type,
       AVG(realized_R) as avg_return,
       AVG(ttd_bars) as avg_ttd,
       COUNT(*) as total_signals
FROM signal_performance
WHERE ts_signal >= now() - INTERVAL '30 days'
GROUP BY symbol, setup_type;
```

## 🔧 Расширение

### Добавление Новых Setup Types
```python
# Добавить в setup_configs
setup_configs[("BTCUSDT", "momentum")] = SymbolSetupConfig(
    symbol="BTCUSDT",
    setup_type="momentum",
    expiry_bars=6,  # BTC медленнее
    risk_multipliers=(0.3, 0.8, 1.2, 1.5),  # Консервативнее
)
```

### Кастомные TP Strategies
```python
def _compute_tp_levels(self, side, stop_price, entry_price, tp_Rs, atr):
    # Custom logic для специфических сетапов
    if self.setup_type == "special_case":
        return [entry_price + custom_levels...]
    return super()._compute_tp_levels(...)
```

## 🎯 Production Checklist

### ✅ Готово к Продакшену:
- **Полная типизация** - все структуры с type hints
- **Обработка ошибок** - graceful degradation
- **TimescaleDB интеграция** - hypertables для временных рядов
- **Модульная архитектура** - легко расширять
- **Подробное логирование** - structured logging
- **Конфигурация** - environment-based settings

### 🚀 Следующие Шаги:
1. **Создать таблицы** в TimescaleDB
2. **Интегрировать** в signal detector
3. **Подключить** execution engine
4. **Настроить** performance tracker
5. **Запустить** TTD optimization job

---

**Результат**: Полная production-ready система execution planning и performance tracking интегрирована в scanner_infra! 🚀📊✨