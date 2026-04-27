# Unified Pipeline Integration

## Обзор

Система unified pipeline реализована с поддержкой трех режимов работы для постепенного rollout новой системы сигналов без риска потери существующей функциональности.

## Режимы работы

### Переменная окружения: `USE_UNIFIED_PIPELINE`

- `"0"` / `"legacy"` → **UnifiedLegacy**: только legacy-путь
- `"1"` / `"unified"` → **UnifiedStrict**: только unified-путь (без fallback)
- `"safe"` / `""` (пусто) → **UnifiedSafe**: unified + fallback на legacy (по умолчанию)

## Архитектура

### Python реализация

#### `config/unified_pipeline.py`
```python
from config.unified_pipeline import get_unified_mode, UnifiedPipelineMode

mode = get_unified_mode()  # Получить текущий режим
```

#### `signals/router.py`
Центральный роутер для принятия решений:
```python
from signals.router import should_emit

if should_emit(ctx, logger, metrics):
    # Генерировать сигнал
    generate_signal(ctx)
```

### Go реализация

#### `internal/config/unified.go`
```go
mode := config.ParseUnifiedModeFromEnv()
```

#### `internal/orderflow/signals.go`
```go
if orderflow.ShouldEmit(ctx, mode, logger, metrics) {
    // Генерировать сигнал
    emitSignal(ctx)
}
```

## Логика работы

### UnifiedLegacy (`"legacy"`)
- ✅ Используется только legacy pipeline
- ✅ Полная обратная совместимость
- ✅ Для отката в случае проблем

### UnifiedStrict (`"unified"`)
- ✅ Только unified pipeline
- ✅ При ошибке unified → сигнал не генерируется
- ✅ Жесткий режим после успешного тестирования

### UnifiedSafe (`"safe"`) - **по умолчанию**
- ✅ Сначала пытается unified pipeline
- ✅ При ошибке unified → fallback на legacy
- ✅ Логирует fallback один раз на символ
- ✅ Собирает метрики ошибок и fallback'ов

## Метрики

### Python: `health_metrics.py`
```python
# Ошибки unified pipeline
metrics.inc_unified_error(symbol)

# Fallback на legacy
metrics.inc_unified_fallback(symbol)
```

### Go: `internal/metrics/health.go`
```go
// Ошибки unified pipeline
metrics.IncUnifiedError(symbol)

// Fallback логируется один раз на символ
metrics.LogUnifiedFallbackOnce(symbol, err)
```

## Пример использования

### Python
```python
# В обработчике сигналов
from signals.router import should_emit

def process_signals(self, ctx):
    if should_emit(ctx, self.logger, self.health_metrics):
        self._generate_signals(ctx)
```

### Go
```go
// В обработчике тиков
func (h *Handler) HandleTick(msg TickMessage) {
    ctx := h.BuildCtxFromTick(msg)

    if orderflow.ShouldEmit(ctx, h.cfg.UnifiedMode, h.logger, h.metrics) {
        h.EmitSignal(ctx)
    }
}
```

## Rollout стратегия

1. **Тестирование** (локально):
   ```bash
   export USE_UNIFIED_PIPELINE="safe"
   # Запустить тесты
   ```

2. **Постепенный rollout**:
   ```bash
   # 1. Начать с safe режима (по умолчанию)
   export USE_UNIFIED_PIPELINE="safe"

   # 2. После успешного тестирования unified
   export USE_UNIFIED_PIPELINE="unified"

   # 3. Откат при проблемах
   export USE_UNIFIED_PIPELINE="legacy"
   ```

3. **Мониторинг**:
   - Следить за метриками unified_errors_total
   - Мониторить unified_fallback_total
   - Проверять логи fallback'ов

## Логирование

### Fallback (один раз на символ)
```
ERROR: unified pipeline failed — switching to legacy (safe mode) symbol=BTCUSDT err=...
```

### Strict mode ошибки
```
ERROR: unified pipeline error — strict mode, skipping signal symbol=BTCUSDT err=...
```

## Тестирование

### Python
```python
from config.unified_pipeline import set_unified_mode_for_testing, UnifiedPipelineMode

# Тестирование разных режимов
set_unified_mode_for_testing(UnifiedPipelineMode.LEGACY)
# ...

set_unified_mode_for_testing(UnifiedPipelineMode.SAFE)
# ...
```

### Go
```go
// Unit тесты для разных режимов
func TestShouldEmit(t *testing.T) {
    // Test UnifiedLegacy
    // Test UnifiedSafe
    // Test UnifiedStrict
}
```

## Расширение

Для добавления новой функциональности:

1. **Обновить unified pipeline** в `should_emit_unified()`
2. **Добавить метрики** для новых типов ошибок
3. **Обновить тесты** для новых сценариев
4. **Дополнить документацию** новыми режимами если нужно

## Совместимость

- ✅ **Обратная совместимость**: legacy режим всегда доступен
- ✅ **Плавный переход**: safe режим позволяет тестировать unified
- ✅ **Безопасность**: strict режим только после полного тестирования
- ✅ **Мониторинг**: полные метрики для всех режимов
