# BaseOrderFlowHandler - Шпаргалка по конфигурации

## 🚀 Быстрый старт (копируй и вставляй)

### Минимальная конфигурация (рекомендуется начать с этого)

```bash
# Строгий breakout + улучшенный sustained
export BREAKOUT_REQUIRE_OBI=true
export OBI_SUSTAINED_USE_FRACTION=true
export OBI_SUSTAINED_MIN_SAMPLES=3
export OBI_SUSTAINED_MIN_FRACTION=0.6
```

### Полная стандартная конфигурация

```bash
# Breakout
export BREAKOUT_REQUIRE_OBI=true
export BREAKOUT_Z_THRESHOLD=3.0

# Absorption
export ABSORPTION_Z_THRESHOLD=3.0
export ABSORPTION_REQUIRE_WEAK_PROGRESS=true

# Extreme
export EXTREME_Z_MULT=1.6

# OBI Sustained
export OBI_SUSTAINED_USE_FRACTION=true
export OBI_SUSTAINED_MIN_SAMPLES=3
export OBI_SUSTAINED_MIN_FRACTION=0.6
```

### Crypto с микроструктурой

```bash
export BREAKOUT_REQUIRE_OBI=true
export BREAKOUT_Z_THRESHOLD=3.2
export ABSORPTION_Z_THRESHOLD=2.5
export ABSORPTION_REQUIRE_WEAK_PROGRESS=false
export ABSORPTION_USE_MICRO_PROXY=true
export ABSORPTION_MICRO_ADVERSE_MIN=0.65
export ABSORPTION_MICRO_REALIZED_EMA_MAX=-0.50
export EXTREME_Z_MULT=2.0
export OBI_SUSTAINED_USE_FRACTION=true
export OBI_SUSTAINED_MIN_SAMPLES=5
export OBI_SUSTAINED_MIN_FRACTION=0.7
export DELTA_BUCKET_MS=1000
```

## 📊 Параметры по инструментам

| Параметр | Crypto | Forex | Commodities |
|----------|--------|-------|-------------|
| `BREAKOUT_Z_THRESHOLD` | 3.2 | 3.0 | 3.0 |
| `ABSORPTION_Z_THRESHOLD` | 2.5 | 3.0 | 2.8 |
| `EXTREME_Z_MULT` | 2.0 | 1.6 | 1.6 |
| `OBI_SUSTAINED_MIN_FRACTION` | 0.7 | 0.6 | 0.5 |
| `OBI_SUSTAINED_MIN_SAMPLES` | 5 | 3 | 2 |
| `ABSORPTION_USE_MICRO_PROXY` | ✅ | ❌ | ❌ |
| `DELTA_BUCKET_MS` | 1000 | 1500 | 2000 |

## 🎯 Эффекты параметров

### Больше сигналов

```bash
# Мягче пороги
export BREAKOUT_Z_THRESHOLD=2.5
export ABSORPTION_Z_THRESHOLD=2.5
export OBI_SUSTAINED_MIN_FRACTION=0.5
export ABSORPTION_REQUIRE_WEAK_PROGRESS=false
```

### Меньше сигналов (выше качество)

```bash
# Строже пороги
export BREAKOUT_Z_THRESHOLD=3.5
export ABSORPTION_Z_THRESHOLD=3.5
export OBI_SUSTAINED_MIN_FRACTION=0.7
export ABSORPTION_REQUIRE_WEAK_PROGRESS=true
```

### Больше breakout

```bash
export BREAKOUT_Z_THRESHOLD=2.5  # Ниже
export BREAKOUT_REQUIRE_OBI=false  # Мягче
```

### Больше absorption

```bash
export ABSORPTION_Z_THRESHOLD=2.5  # Ниже
export ABSORPTION_REQUIRE_WEAK_PROGRESS=false  # Мягче
export ABSORPTION_USE_MICRO_PROXY=true  # Дополнительный триггер
```

## 🔧 Troubleshooting

### Проблема: Слишком мало сигналов

```bash
# Решение: Снизить пороги
export BREAKOUT_Z_THRESHOLD=2.7
export ABSORPTION_Z_THRESHOLD=2.7
export OBI_SUSTAINED_MIN_FRACTION=0.5
```

### Проблема: Слишком много ложных breakout

```bash
# Решение: Строже breakout
export BREAKOUT_REQUIRE_OBI=true
export BREAKOUT_Z_THRESHOLD=3.5
export OBI_SUSTAINED_MIN_FRACTION=0.7
```

### Проблема: Пропускаем absorption

```bash
# Решение: Мягче absorption
export ABSORPTION_Z_THRESHOLD=2.5
export ABSORPTION_REQUIRE_WEAK_PROGRESS=false
export ABSORPTION_USE_MICRO_PROXY=true  # Если crypto
```

### Проблема: OBI никогда не sustained

```bash
# Решение: Мягче sustained
export OBI_SUSTAINED_MIN_SAMPLES=2
export OBI_SUSTAINED_MIN_FRACTION=0.5
# Или отключить
export OBI_SUSTAINED_USE_FRACTION=false
```

## 📝 Значения по умолчанию

Если параметр не задан, используется:

```python
BREAKOUT_REQUIRE_OBI = true
BREAKOUT_Z_THRESHOLD = delta_z_threshold
ABSORPTION_Z_THRESHOLD = delta_z_threshold
ABSORPTION_REQUIRE_WEAK_PROGRESS = true
ABSORPTION_USE_MICRO_PROXY = false
EXTREME_Z_MULT = 1.6
OBI_SUSTAINED_USE_FRACTION = true
OBI_SUSTAINED_MIN_SAMPLES = 3
OBI_SUSTAINED_MIN_FRACTION = 0.6
DELTA_BUCKET_MS = 1000
```

## 🎓 Когда что использовать

### Используй `BREAKOUT_REQUIRE_OBI=true` если:
- ✅ Много ложных пробоев
- ✅ Хочешь выше качество breakout
- ✅ Готов к меньшему количеству сигналов

### Используй `ABSORPTION_USE_MICRO_PROXY=true` если:
- ✅ Crypto инструмент
- ✅ Есть `is_buyer_maker` в тиках
- ✅ `weak_progress` слишком строгий
- ✅ Хочешь ловить absorption раньше

### Используй `OBI_SUSTAINED_USE_FRACTION=true` если:
- ✅ Хочешь настоящую устойчивость OBI
- ✅ Много ложных sustained
- ✅ Готов к строже критерию

### Снизь `BREAKOUT_Z_THRESHOLD` если:
- ✅ Мало breakout сигналов
- ✅ Пропускаем хорошие пробои
- ✅ Готов к большему количеству

### Повысь `ABSORPTION_Z_THRESHOLD` если:
- ✅ Много ложных absorption
- ✅ Хочешь только явные поглощения
- ✅ Готов пропустить ранние

## 🧪 A/B тестирование

### Группа A (консервативная)

```bash
export BREAKOUT_Z_THRESHOLD=3.5
export ABSORPTION_Z_THRESHOLD=3.5
export OBI_SUSTAINED_MIN_FRACTION=0.7
export BREAKOUT_REQUIRE_OBI=true
```

### Группа B (агрессивная)

```bash
export BREAKOUT_Z_THRESHOLD=2.5
export ABSORPTION_Z_THRESHOLD=2.5
export OBI_SUSTAINED_MIN_FRACTION=0.5
export BREAKOUT_REQUIRE_OBI=false
```

### Сравнить метрики:
- Количество сигналов
- Win rate
- Sharpe ratio
- Max drawdown
- Profit factor

## 📚 Документация

- **Полная документация:** `RECOMMENDED_CONFIG.md`
- **OBI Sustained:** `OBI_SUSTAINED_IMPROVEMENTS.md`
- **Z-пороги:** `PER_SIGNAL_Z_THRESHOLDS.md`
- **Микроструктура:** `MICROSTRUCTURE_INTEGRATION.md`

---

**Совет:** Начни с минимальной конфигурации, протестируй, потом добавляй параметры постепенно! 🚀

