# Система Качества Сигналов

## Обзор

Система качества сигналов предоставляет комплексную оценку качества сигналов на основе исторических данных. Она включает два уровня оценки: **offline** (долгосрочная статистика) и **online** (текущая производительность), которые комбинируются для принятия решений о публикации сигналов.

## Архитектура

### Основные Компоненты

1. **Feature Bucketing** - кластеризация сигналов по характеристикам
2. **Offline Job** - расчет качества по историческим данным
3. **Online Job** - rolling качество по недавним сигналам
4. **Quality Estimator** - оценка качества для новых сигналов
5. **Signal Scoring Engine** - интеграция качества с confidence

### База Данных

#### Таблица signal_quality_offline
Хранит качество по кластерам фич для каждого (symbol, signal_type, side, session, regime):

```sql
CREATE TABLE signal_quality_offline (
    symbol TEXT, signal_type TEXT, side TEXT,
    session TEXT, regime TEXT, feature_bucket TEXT,
    n_signals INT, win_rate REAL, expectancy_r REAL,
    var_r REAL, cvar_r REAL, quality_score REAL
);
```

#### Таблица signal_quality_online
Хранит rolling качество по типам сигналов:

```sql
CREATE TABLE signal_quality_online (
    symbol TEXT, signal_type TEXT, side TEXT,
    n_recent INT, win_rate_recent REAL, expectancy_r_recent REAL,
    quality_score_online REAL, status TEXT
);
```

## Алгоритм Качества

### 1. Feature Bucketing

Сигналы кластеризуются по характеристикам для оценки качества в похожих условиях:

```python
def make_feature_bucket(delta_spike_z, obi, weak_progress, atr_quantile):
    # dz: [0.5, 1.0, 1.5, 2.0, 3.0]
    # obi: [0.5, 1.0, 1.5, 2.0]
    # wp: [0.15, 0.3, 0.5]
    # atr: [0.3, 0.7, 0.9]
    return f"dz:{dz_bin}|obi:{obi_bin}|wp:{wp_bin}|atr:{atr_bin}"
```

### 2. Статистические Метрики

Для каждого кластера рассчитываются:
- **Win Rate** - доля прибыльных сигналов
- **Expectancy R** - математическое ожидание R
- **VaR (5%)** - Value at Risk на 5% уровне
- **CVaR (5%)** - Conditional VaR (хвостовой риск)

### 3. Quality Score

Нормализованный скор качества (0-100):

```python
def compute_quality_score(exp_r, win_rate, var_r, cvar_r, n):
    if n < 30:  # Недостаточно данных
        return 0.0

    # Нормализация expectancy (0..2R оптимально)
    exp_norm = max(0.0, min(2.0, exp_r)) / 2.0

    # Win rate уже 0..1
    wr_norm = max(0.0, min(1.0, win_rate))

    # Штраф за хвостовой риск
    tail_penalty = 1.0
    if cvar_r < -1.0:
        tail_penalty = max(0.1, 1.0 + cvar_r / 5.0)

    # Взвешенная комбинация
    base = 0.6 * exp_norm + 0.4 * wr_norm
    base *= tail_penalty

    return min(100.0, max(0.0, base * 100.0))
```

### 4. Online Status

На основе недавней производительности:
- **ok** - нормальная работа
- **degraded** - сниженная производительность
- **disabled** - сигнал отключен

## Интеграция с Scoring

### SignalContext Расширение

```python
@dataclass
class SignalContext:
    # ... существующие поля ...

    # Качество сигнала
    quality_offline: float | None = None
    quality_online: float | None = None
    quality_combined: float | None = None
    quality_status: str | None = None

    # Финальный скор с качеством
    final_score: float | None = None
    is_disabled_by_quality: bool = False
```

### Quality Estimator

```python
class SignalQualityEstimator:
    def estimate(self, symbol, signal_type, side, session, regime, feature_bucket):
        # 1. Ищем offline качество по точному bucket
        # 2. Fallback: среднее по всем buckets для типа
        # 3. Получаем online rolling качество
        # 4. Комбинируем с весами (70% offline, 30% online)
        return QualityEstimate(...)
```

### SignalScoringEngine

```python
class SignalScoringEngine:
    def __init__(self, calib_store, config, quality_estimator=None):
        self.quality_estimator = quality_estimator

    def compute_confidence(self, ctx):
        # Вычисление confidence как раньше
        # + локальные квантили по метрикам

    def _enrich_with_quality(self, ctx):
        # Оценка качества через quality_estimator
        # Установка quality_* полей в ctx

    def should_emit(self, ctx):
        # 1. Проверка базового confidence
        # 2. Обогащение качеством
        # 3. Проверка final_score
        # 4. Проверка на отключение по качеству
```

## Использование

### 1. Запуск Джобов Качества

```bash
# Offline качество (раз в день/ночь)
make quality-offline

# Online качество (каждые 15-60 мин)
make quality-online

# Полный цикл
make quality-run
```

### 2. Интеграция в Handler

```python
# В BaseOrderFlowHandler.__init__
quality_estimator = SignalQualityEstimator(pg_dsn)
scoring_engine = SignalScoringEngine(calib_store, config, quality_estimator)

# В _compute_confidence
scoring_engine.compute_confidence(ctx)
# Автоматически обогащает ctx полями качества

# В _should_emit
should_emit = scoring_engine.should_emit(ctx)
```

### 3. Payload Сигнала

```json
{
  "symbol": "XAUUSD",
  "confidence": 85,
  "quality": {
    "offline": 78.5,
    "online": 82.3,
    "combined": 80.2,
    "status": "ok"
  },
  "finalScoreWithQuality": 83.0,
  "isDisabledByQuality": false
}
```

## Конфигурация

### ENV Переменные

```bash
# Качество
QUALITY_HORIZON=R_main                    # R горизонт
QUALITY_LOOKBACK_DAYS=180                # История для offline
QUALITY_ROLLING_WINDOW=200               # Rolling окно для online

# Scoring (существующие)
CRYPTO_SIGNAL_MIN_CONF=80
CRYPTO_SIGNAL_MIN_CONF__XAUUSD=20
GOLDEN_PATTERN_MIN_CONFIDENCE=90
SIGNAL_METRIC_WEIGHT__DELTA_SPIKE_Z=1.0
SIGNAL_METRIC_WEIGHT__OBI=0.7
SIGNAL_METRIC_WEIGHT__WEAK_PROGRESS=0.5
SIGNAL_METRIC_WEIGHT__ATR_QUANTILE=0.3
SIGNAL_PATTERN_WEIGHT__BREAKOUT_R1=1.2
SIGNAL_PATTERN_MIN_CONF__BREAKOUT_R1=85
```

## Мониторинг

### Проверка Качества

```bash
# Статистика offline качества
SELECT signal_type, COUNT(*), AVG(quality_score), AVG(expectancy_r)
FROM signal_quality_offline
GROUP BY signal_type;

# Online статус
SELECT signal_type, status, quality_score_online, expectancy_r_recent
FROM signal_quality_online;
```

### Логи

```bash
# Логи джобов качества
tail -f logs/quality_offline.log
tail -f logs/quality_online.log

# Логи скоринга с качеством
grep "quality_combined" logs/scoring.log
```

## Производительность

### Оптимизации

1. **Индексы** на ключевых полях поиска
2. **Кэширование** quality estimates (Redis)
3. **Batch updates** для online джоба
4. **Feature bucket** для снижения размерности

### Масштабирование

- **Offline job**: раз в сутки, полная перестройка
- **Online job**: каждые 30 мин, инкрементальные обновления
- **Estimator**: кэширование на 15-30 мин

## Диагностика

### Возможные Проблемы

1. **Низкое качество** - проверить исторические данные
2. **Disabled signals** - проверить online статус
3. **Performance** - оптимизировать индексы и кэширование
4. **Data gaps** - проверить полноту исторических данных

### Отладка

```python
# Ручная оценка качества
from signal_quality import SignalQualityEstimator, make_feature_bucket

estimator = SignalQualityEstimator(pg_dsn)
bucket = make_feature_bucket(delta_spike_z=2.1, obi=1.5, weak_progress=0.1, atr_quantile=0.8)
quality = estimator.estimate("XAUUSD", "breakout_r1", "buy", "asia", "trend", bucket)
print(f"Quality: {quality.combined_score}, Status: {quality.status}")
```

## Заключение

Система качества сигналов предоставляет:
- ✅ **Историческую оценку** качества по кластерам
- ✅ **Текущую производительность** через rolling окно
- ✅ **Автоматическую фильтрацию** низкокачественных сигналов
- ✅ **Гибкую настройку** через ENV переменные
- ✅ **Полную интеграцию** с существующей системой скоринга

Это позволяет значительно улучшить качество генерируемых сигналов и автоматически отключать проблемные паттерны.
