# Weak Progress System - Обязательный Компонент Качества Сигналов

## Обзор

Weak Progress система интегрирует метрику `|range| / ATR` как обязательный компонент качества для continuation и fade паттернов. Это обеспечивает, что сигналы генерируются только в подходящих рыночных условиях.

## Базовые Концепции

### Weak Progress Метрика

```python
weak_progress = |high - low| / ATR
```

- **< 0.3**: Слабый прогресс (хороший для fade паттернов)
- **0.3-0.7**: Умеренный прогресс
- **> 0.7**: Сильный прогресс (хороший для continuation паттернов)

### Pattern Families

- **Continuation**: Требуют сильного прогресса (breakout, trend continuation)
- **Fade**: Требуют слабого прогресса (fade PDH/PDL, absorption)
- **Other**: Нейтральные паттерны

## Конфигурация

### WeakProgressConfig

```python
@dataclass
class WeakProgressConfig:
    family: PatternFamily

    # Continuation настройки
    cont_strong_min: float = 0.7    # Порог сильного прогресса
    cont_weak_max: float = 0.3      # Порог слабого прогресса

    # Fade настройки
    fade_weak_max: float = 0.35     # Максимальный weak_progress для fade
    fade_min_delta_z: float = 1.8   # Минимальный delta_z для fade
    fade_min_volume_z: float = 1.5  # Альтернативный volume порог
    fade_confirm_delta_z: float = 1.5  # Подтверждающий reverse delta_z

    # Скоринговые веса
    bonus_cont_strong: int = 12     # +бонус за сильный continuation
    penalty_cont_weak: int = 15     # -штраф за слабый continuation
    bonus_fade_weak: int = 10       # +бонус за слабый fade
    penalty_fade_strong: int = 10   # -штраф за сильный fade
```

### Pattern-Specific Конфигурации

```python
PATTERN_WP_CONFIG = {
    "breakout_R1": WeakProgressConfig(
        family="continuation",
        cont_strong_min=0.8,     # Более строгий порог
        bonus_cont_strong=15,    # Выше бонус
    ),
    "fade_PDH": WeakProgressConfig(
        family="fade",
        fade_weak_max=0.3,       # Строгий weak_progress
        bonus_fade_weak=12,
    ),
    # ... остальные паттерны
}
```

## Логика Скоринга

### Continuation Паттерны

```python
if weak_progress >= cont_strong_min:
    score += bonus_cont_strong  # +12 to +15
elif weak_progress <= cont_weak_max:
    score -= penalty_cont_weak  # -15 (слабый прогресс = fade candidate)
```

### Fade Паттерны

```python
# Жесткие фильтры (hard reject если не выполняются)
if weak_progress > fade_weak_max:
    return 0  # Слишком сильный прогресс

if delta_z < fade_min_delta_z and volume_z < fade_min_volume_z:
    return 0  # Недостаточный импульс

if reverse_delta_z < fade_confirm_delta_z:
    return 0  # Нет подтверждающего reverse импульса

# Скоринг
if weak_progress <= fade_weak_max:
    score += bonus_fade_weak  # +10 to +12
```

## Интеграция в SignalScoringEngine

### Обновленный compute_confidence

```python
def compute_confidence(self, ctx: SignalContext) -> int:
    # 1. Базовый confidence от метрик (delta_z, obi, atr_quantile)
    base_confidence = calculate_base_confidence(...)

    # 2. Получить weak progress конфиг для паттерна
    wp_cfg = get_weak_progress_config(ctx.pattern_name)

    # 3. Применить weak progress фильтры и скоринг
    final_confidence = apply_weak_progress_and_fade_filters(
        ctx=ctx,
        pattern_cfg=wp_cfg,
        base_conf=base_confidence
    )

    return final_confidence
```

### SignalContext Поля

```python
@dataclass
class SignalContext:
    # ... existing fields ...

    # Weak progress поля
    weak_progress: float | None = None
    progress_score_component: int | None = None
    pattern_family: str | None = None

    # Fade-специфичные поля
    reverse_delta_spike_z: float | None = None
    volume_z: float | None = None
```

## Архитектура Модулей

```
signal_scoring/weak_progress/
├── __init__.py          # Экспорты
├── config.py            # WeakProgressConfig, PATTERN_WP_CONFIG
├── utils.py             # compute_weak_progress, классификация
├── filters.py           # fade_preconditions_passed, continuation_preconditions_passed
└── scorer.py            # compute_progress_score, apply_weak_progress_and_fade_filters
```

## Payload Формат

```json
{
  "symbol": "XAUUSD",
  "signal_type": "breakout_R1",
  "confidence": 85,
  "weakProgress": 0.8,
  "progressScoreComponent": 15,
  "patternFamily": "continuation",
  "reverseDeltaSpikeZ": null,
  "volumeZ": null,
  // ... остальные поля
}
```

## Примеры Использования

### Continuation Breakout (Хорошие Условия)

```python
ctx = SignalContext(
    pattern_name="breakout_R1",
    weak_progress=0.85,      # Сильный прогресс
    delta_spike_z=2.3,
    # ...
)

# Результат: confidence = base_conf + 15 (bonus_cont_strong)
```

### Fade PDH (Хорошие Условия)

```python
ctx = SignalContext(
    pattern_name="fade_PDH",
    weak_progress=0.25,      # Слабый прогресс
    delta_spike_z=2.1,       # Сильный импульс
    reverse_delta_spike_z=1.8,  # Подтверждение
    # ...
)

# Результат: confidence = base_conf + 12 (bonus_fade_weak)
```

### Fade PDH (Плохие Условия - Reject)

```python
ctx = SignalContext(
    pattern_name="fade_PDH",
    weak_progress=0.6,       # Слишком сильный прогресс
    delta_spike_z=2.1,
    reverse_delta_spike_z=1.8,
    # ...
)

# Результат: confidence = 0 (hard reject)
```

## Тестирование

### Unit Tests

```bash
cd python-worker
python3 test_weak_progress.py
```

### Integration Tests

```python
# Тест полной интеграции
from signal_scoring import SignalScoringEngine
from signal_scoring.weak_progress import get_weak_progress_config

engine = SignalScoringEngine(...)
ctx = SignalContext(pattern_name="breakout_R1", weak_progress=0.8)
confidence = engine.compute_confidence(ctx)
assert confidence > 80  # Должен получить бонус
```

## Настройка через ENV

### Pattern-Specific Конфигурация

```bash
# Пример для breakout_R1
WP_BREAKOUT_R1_CONT_STRONG_MIN=0.85
WP_BREAKOUT_R1_BONUS_CONT_STRONG=18

# Пример для fade_PDH
WP_FADE_PDH_FADE_WEAK_MAX=0.28
WP_FADE_PDH_BONUS_FADE_WEAK=15
```

### Глобальные Настройки

```bash
# Базовые пороги
WP_DEFAULT_CONT_STRONG_MIN=0.7
WP_DEFAULT_FADE_WEAK_MAX=0.35

# Скоринговые веса
WP_DEFAULT_BONUS_CONT_STRONG=12
WP_DEFAULT_PENALTY_FADE_STRONG=10
```

## Мониторинг и Отладка

### Логи

```python
# В scorer.py логируются:
logger.info(f"Weak progress validation: {validation}")
logger.info(f"Progress score: {progress_score}")
```

### Метрики для Мониторинга

- **Progress Score Distribution**: Распределение progress_score по паттернам
- **Rejection Rate by Family**: Доля отклоненных сигналов по pattern_family
- **Confidence Boost**: Средний бонус confidence от weak progress

### Диагностика Проблем

```python
# Валидация сигнала
validation = validate_signal_for_weak_progress(ctx)
print(f"Valid: {validation['is_valid']}")
print(f"Reasons: {validation['reasons']}")
```

## Производительность

- **CPU Impact**: Минимальный (~0.1ms per signal)
- **Memory**: Небольшое увеличение SignalContext
- **Filtering Efficiency**: Hard rejects предотвращают обработку плохих сигналов

## Совместимость

### Существующие Системы

- ✅ **Signal Quality**: weak_progress уже в feature_bucket для quality scoring
- ✅ **Local Calibration**: weak_progress учитывается в метриках
- ✅ **Golden Patterns**: weak_progress влияет на финальный confidence

### Backward Compatibility

- **Default Behavior**: Если weak_progress=None, применяется penalty
- **Fallback Config**: Неизвестные паттерны используют безопасные defaults
- **Graceful Degradation**: Система продолжает работать без weak_progress данных

## Будущие Улучшения

1. **Dynamic Thresholds**: Адаптация порогов по символу/сессии
2. **ML Integration**: Обучение оптимальных порогов
3. **Multi-Timeframe**: Weak progress по разным таймфреймам
4. **Pattern Evolution**: Автоматическое обнаружение новых паттернов

---

**Результат**: Weak Progress система обеспечивает качество сигналов, фильтруя неподходящие рыночные условия и усиливая confidence для правильных setup'ов.
