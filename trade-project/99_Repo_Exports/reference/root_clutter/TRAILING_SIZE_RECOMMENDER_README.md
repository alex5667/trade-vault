# Trailing Size Recommender

Модуль для автоматического расчёта рекомендуемого размера трейлинг-стопа на основе истории закрытых сделок.

## Концепция

### Проблема

Как определить оптимальный `TRAILING_TP1_OFFSET_ATR` для символа/стратегии?

### Решение

Анализ распределения MFE (Maximum Favorable Excursion) по выигрышным сделкам:

1. **MFE_R** = `mfe_pnl / one_r_money` - максимальная прибыль в R
2. **Lock_R** = квантиль MFE_R (обычно 25-й перцентиль)
3. **TRAILING_TP1_OFFSET_ATR** = `lock_r * stop_atr_mult`

### Логика расчёта

```python
# Для выигрышных сделок (pnl_net > 0)
r = pnl_net / one_r_money              # реализованный результат
mfe_r = mfe_pnl / one_r_money         # максимум в R
giveback_r = mfe_r - r               # сколько отдали обратно

# Lock_r = 25-й перцентиль MFE_R (75% сделок имеют MFE выше этого)
lock_r = quantile(mfe_r_win, 0.25)

# Кап по медиане реализованного R (не завышать)
lock_r = min(lock_r, 0.9 * median(r_win))

# Флор/клип
lock_r = max(0.05, min(lock_r, 1.0))

# Конвертация в ATR
trailing_tp1_offset_atr = lock_r * stop_atr_mult
```

## Использование

### 1. Прямое использование модуля

```python
from services.trailing_size_recommender import (
    ClosedTradeSnapshot,
    recommend_trailing_size
)

# Подготовка данных
trades = [
    ClosedTradeSnapshot.from_trade_closed_dict(trade_dict)
    for trade_dict in redis_trades
]

# Расчёт рекомендации
rec = recommend_trailing_size(
    trades=trades,
    stop_atr_mult=0.6,        # ATR множитель для SL
    min_trades=50,           # мин. количество сделок
    winners_only=True,       # только выигрышные
    mfe_quantile=0.25        # квантиль MFE
)

if rec:
    print(f"Lock R: {rec.lock_r:.2f}R")
    print(f"TRAILING_TP1_OFFSET_ATR: {rec.trailing_tp1_offset_atr:.2f}")
```

### 2. Скрипт командной строки

```bash
# Анализ ETHUSDT
python scripts/trailing_size_analysis.py \
  --redis-url "redis://localhost:6379/0" \
  --source CryptoOrderFlow \
  --symbol ETHUSDT \
  --count 1000 \
  --stop-atr-mult 0.6

# Анализ по entry_tag
python scripts/trailing_size_analysis.py \
  --redis-url "redis://localhost:6379/0" \
  --source CryptoOrderFlow \
  --symbol ETHUSDT \
  --count 1500 \
  --stop-atr-mult 0.6 \
  --per-entry-tag
```

### 3. Интеграция в analyze_trades_from_redis_advanced.py

```bash
# Автоматически добавляет рекомендации в отчёт
python -m scripts.analyze_trades_from_redis_advanced \
  --redis-url "redis://localhost:6379/0" \
  --stream "trades:closed" \
  --source CryptoOrderFlow \
  --symbol ETHUSDT \
  --count 1000 \
  --stop-atr-mult 0.6 \
  --markdown
```

## Структура данных

### ClosedTradeSnapshot

```python
@dataclass
class ClosedTradeSnapshot:
    source: str
    symbol: str
    strategy: str
    entry_tag: str
    exit_ts_ms: int

    pnl_net: float          # фактический P&L
    pnl_if_fixed_exit: float # P&L при фиксации на TP1
    one_r_money: float      # размер 1R в деньгах

    mfe_pnl: float          # максимум в R
    mae_pnl: float          # минимум в R
    giveback: float         # отданная прибыль
    missed_profit: float    # упущенная прибыль

    trailing_started: bool
    trailing_active: bool
    close_reason: str
    close_reason_raw: str
    close_reason_detail: str
```

### TrailingSizeRecommendation

```python
class TrailingSizeRecommendation(NamedTuple):
    lock_r: float                          # основной lock_r
    lock_r_low: float                      # нижняя граница
    lock_r_high: float                     # верхняя граница

    trailing_tp1_offset_atr: float         # основной ATR offset
    trailing_tp1_offset_atr_low: float     # нижняя граница
    trailing_tp1_offset_atr_high: float    # верхняя граница

    # Диагностика
    sample_size_win: int                   # размер выборки
    avg_r_win: float                       # средний R
    median_r_win: float                    # медианный R
    median_mfe_r_win: float                # медианный MFE
    avg_giveback_r_win: float              # средний giveback в R
    avg_giveback_ratio_win: float          # средний giveback ratio
```

## Параметры

### Основные параметры recommend_trailing_size()

- **stop_atr_mult**: множитель ATR для SL (из конфига)
- **min_trades**: минимальное количество выигрышных сделок (default: 50)
- **winners_only**: использовать только pnl_net > 0 (default: True)
- **mfe_quantile**: квантиль MFE_R для lock_r (default: 0.25)

### Примеры значений

```python
# Консервативный (больше сделок достигают)
rec = recommend_trailing_size(trades, stop_atr_mult=0.6, mfe_quantile=0.3)

# Агрессивный (меньше сделок достигают, но выше потенциал)
rec = recommend_trailing_size(trades, stop_atr_mult=0.6, mfe_quantile=0.1)
```

## Интеграция в систему

### 1. Обновление конфигурации символов

```python
# В trailing_decision_config.py
def update_trailing_config(symbol: str, recommendation: TrailingSizeRecommendation):
    """Обновить конфиг на основе рекомендации"""

    config = {
        "trailing_tp1_offset_atr": recommendation.trailing_tp1_offset_atr,
        "reason": f"Auto: lock_r={recommendation.lock_r:.2f}R from {recommendation.sample_size_win} trades"
    }

    # Сохранить в symbol_specs или ENV
    save_symbol_config(symbol, config)
```

### 2. Периодический анализ

```bash
# Cron job для ежедневного анализа
0 6 * * * /path/to/python scripts/trailing_size_analysis.py \
  --redis-url "redis://localhost:6379/0" \
  --source CryptoOrderFlow \
  --symbol ETHUSDT \
  --count 2000 \
  --from "$(date -d '30 days ago' +%Y-%m-%d)" \
  --stop-atr-mult 0.6 \
  --per-entry-tag > /var/log/trailing_analysis.log
```

### 3. Валидация рекомендаций

```python
# Проверка адекватности
def validate_recommendation(rec: TrailingSizeRecommendation) -> bool:
    if rec.lock_r < 0.05 or rec.lock_r > 1.0:
        return False
    if rec.sample_size_win < 50:
        return False
    if rec.median_mfe_r_win < rec.lock_r:
        return False
    return True
```

## Диагностика

### Полезные метрики для анализа

- **sample_size_win**: размер выборки (должно быть > 50)
- **median_mfe_r_win**: медианный MFE (базис для lock_r)
- **avg_giveback_ratio_win**: средний giveback ratio (< 0.3 - хорошо)
- **lock_r vs median_r_win**: lock_r не должен превышать median_r

### Визуализация распределения

```python
import matplotlib.pyplot as plt

# Гистограмма MFE_R
plt.hist([t.mfe_pnl / t.one_r_money for t in trades if t.pnl_net > 0])
plt.axvline(rec.lock_r, color='red', label=f'Lock R: {rec.lock_r:.2f}')
plt.legend()
plt.show()
```

## Примеры результатов

### ETHUSDT (агрессивный рынок)

```
Lock R: 0.75R (0.53 - 0.98)
TRAILING_TP1_OFFSET_ATR: 0.45 (0.32 - 0.59)
Выборка: 245 сделок
Средний R: 1.23, Медианный MFE: 2.1
```

### BTCUSDT (волатильный рынок)

```
Lock R: 1.20R (0.84 - 1.56)
TRAILING_TP1_OFFSET_ATR: 0.72 (0.50 - 0.94)
Выборка: 189 сделок
Средний R: 1.85, Медианный MFE: 3.2
```

## Файлы

- `services/trailing_size_recommender.py` - основной модуль
- `scripts/trailing_size_analysis.py` - CLI инструмент
- `TRAILING_SIZE_RECOMMENDER_README.md` - документация
