# Unified Signal Pipeline

Этот модуль содержит унифицированный пайплайн генерации сигналов, который консолидирует всю логику из разрозненных методов BaseOrderFlowHandler.

## Архитектура

### Основной пайплайн генерации сигнала:
1. **Собираем OrderflowContext** (raw orderflow features, bar, l2, trades, etc.)
2. **Attach regime (+local calibration)** → regime, atr_quantiles, regime_flags
3. **Считаем confidence / scoring** → base_score, sub-scores
4. **Golden logic + final_score** → final_score, tags
5. **Should emit + regime guard + exec filters**
6. **Publish** (outbox + optional execution plan)

### Ключевые компоненты

- `UnifiedSignalPipeline` - основной оркестратор
- `OrderflowContext` - сырые метрики из тиков/баров
- `SignalContext` - обогащенный контекст для скоринга
- `GoldenPatternService` - определение золотых паттернов
- `CalibrationService` - локальная калибровка метрик
- `ExecFiltersGroup` - фильтры исполнения
- `SignalPublisher` - публикация сигналов

## Использование

### Базовое использование

```python
from signals.unified_pipeline import UnifiedSignalPipeline
from signals.types import OrderflowContext

# Создаем пайплайн с сервисами
pipeline = UnifiedSignalPipeline(
    scoring_engine=scoring_engine,
    regime_service=regime_service,
    golden_logic=GoldenPatternService(),
    exec_filters=ExecFiltersGroup(),
    publisher=SignalPublisher(redis_client, outbox)
)

# Создаем контекст из тика/бара
of_ctx = OrderflowContext(
    ts=tick.ts,
    price=mid,
    symbol="EURUSD",
    z_delta=z_delta,
    obi=obi,
    atr=atr,
    # ... другие метрики
)

# Генерируем сигнал
signal = pipeline.process(of_ctx)
if signal:
    print(f"Generated signal: {signal}")
```

### Интеграция в BaseOrderFlowHandler

```python
class BaseOrderFlowHandler:
    def __init__(self, ..., unified_pipeline=None):
        self._unified_pipeline = unified_pipeline
        self._use_legacy_path = unified_pipeline is None

    def _process_tick(self, tick):
        of_ctx = self._build_orderflow_context(tick)

        if self._unified_pipeline:
            # Новый унифицированный путь
            self._unified_pipeline.process(of_ctx)
        else:
            # Fallback на старый путь
            self._generate_signals(of_ctx)
```

## Миграция

### Что изменилось

**ДО:**
- Два параллельных пути: `_generate_signals()` и `_generate_signals_unified()`
- Логика скоринга размазана между OrderflowContext и SignalContext
- Duplication калибровок, golden-pattern, regime guard

**ПОСЛЕ:**
- Один унифицированный пайплайн `UnifiedSignalPipeline.process()`
- Четкое разделение ответственности между сервисами
- Все метрики в одном месте, без дублирования

### Совместимость

Старые методы `_generate_signals()` и `_should_emit()` остаются для обратной совместимости, но теперь делегируют в новый пайплайн.

## Тестирование

```bash
cd python-worker
python test_unified_pipeline.py
```

## Production интеграция ✅

**Завершена интеграция UnifiedSignalPipeline в BaseOrderFlowHandler:**

1. ✅ **UnifiedSignalPipeline создается автоматически** в конструкторе BaseOrderFlowHandler
2. ✅ **Все сервисы инициализируются** (GoldenPatternService, CalibrationService, ExecFiltersGroup, SignalPublisher)
3. ✅ **self._use_legacy_path = False** - полный переход на unified pipeline после тестирования
4. ✅ **Обратная совместимость** - fallback на legacy path при ошибках
5. ✅ **Тестирование пройдено** - pipeline работает корректно

### Использование в production

```python
# Теперь UnifiedSignalPipeline используется автоматически:
handler = BaseOrderFlowHandler(symbol="EURUSD")
# handler._unified_pipeline уже создан и настроен
# handler._use_legacy_path = False (полный переход на новый пайплайн)
```

### Мониторинг

- Логи: `"✅ UnifiedSignalPipeline initialized successfully"`
- При ошибках: `"UnifiedSignalPipeline failed, falling back to legacy"`
- Метрики производительности можно добавить в будущем

## Следующие шаги

1. Улучшение сервисов (более точные golden pattern thresholds, etc.)
2. Добавление логирования и метрик производительности
3. Интеграция с execution planning
4. Оптимизация производительности
