# Интеграция Новостей в Торговые Сигналы

## Обзор

Новостной пайплайн интегрируется в торговые сигналы через механизм обогащения контекста (`ctx.news`). Система предоставляет агрегированные метрики новостного риска, которые используются для фильтрации, весов и временных ограничений сигналов.

## Архитектура Интеграции

### NewsFeatures в Контексте Сигнала

```python
@dataclass(frozen=True, slots=True)
class NewsFeatures:
    """
    Компактное представление новостных данных для использования в сигналах
    """
    # Основные метрики
    news_risk: float = 0.0        # 0..1: Агрегированный новостной риск
    surprise_score: float = 0.0   # -1..1: Неожиданность событий
    news_grade_id: int = 0         # 0..4: Уровень важности

    # Категоризация
    tags_mask: int = 0             # Битовая маска тегов
    primary_tag_id: int = 0        # Основной тег новости

    # Календарные события
    event_tminus_sec: int = -1     # Секунд до следующего события
    event_grade_id: int = 0        # Важность события

    # Метаданные
    ref: str = ""                  # Ссылка на детальный анализ
    confidence: float = 0.0        # Уверенность анализа
    horizon_sec: int = 0           # Временной горизонт влияния
    asof_ts_ms: int = 0            # Время обновления данных
```

### Enricher Pattern

#### Синхронный Enricher
```python
class NewsEnricherSync:
    """
    Синхронное обогащение контекста новостными данными
    - 1 Redis RTT для высокой производительности
    - In-memory кеширование для burst режимов
    - Fail-open архитектура
    """

    def attach(self, ctx: OrderflowSignalContext, *, asset_class: str = "") -> None:
        """
        Добавляет ctx.news к сигналу

        Args:
            ctx: Контекст торгового сигнала
            asset_class: Класс актива (crypto, equity, forex)
        """
        try:
            symbol = getattr(ctx, "symbol", "GLOBAL").upper()

            # Чтение из Redis с кешированием
            news_data = self._get_cached_news_data(symbol)
            calendar_data = self._get_calendar_data(asset_class)

            # Формирование NewsFeatures
            ctx.news = NewsFeatures(
                news_risk=news_data.get("risk_ema", 0.0),
                surprise_score=news_data.get("surprise_ema", 0.0),
                news_grade_id=news_data.get("news_grade_id", 0),
                # ... другие поля
            )

        except Exception:
            # Fail-open: сигнал продолжает работать без новостей
            ctx.news = None
            _append_dq_flag(ctx, "news_enrich_fail_open")
```

## Использование в Торговых Сигналах

### Фильтрация по Новостному Риску

#### Базовая Фильтрация
```python
def should_skip_signal(ctx: OrderflowSignalContext) -> bool:
    """
    Фильтрация сигналов на основе новостного риска
    """
    if not ctx.news:
        return False  # Нет данных - пропускаем фильтр

    news_risk = ctx.news.news_risk
    news_grade = ctx.news.news_grade_id

    # Критический уровень: блокируем все сигналы
    if news_grade >= 4:
        return True

    # Высокий уровень: только высококачественные сигналы
    if news_grade >= 3 and ctx.confidence < 0.8:
        return True

    # Умеренный уровень: снижаем частоту
    if news_grade >= 2 and random.random() < 0.3:
        return True

    return False
```

#### Категорийная Фильтрация
```python
def filter_by_news_tags(ctx: OrderflowSignalContext) -> bool:
    """
    Фильтрация на основе конкретных типов новостей
    """
    if not ctx.news:
        return False

    tags_mask = ctx.news.tags_mask

    # Блокируем сигналы во время FOMC
    if tags_mask & TAG_FOMC:
        return True

    # Блокируем во время геополитических кризисов
    if tags_mask & TAG_GEOPOLITICS and ctx.news.news_risk > 0.7:
        return True

    # Разрешаем сигналы на росте во время risk-on
    if tags_mask & TAG_RISK_ON and ctx.direction == "long":
        return False

    return False
```

### Взвешивание Сигналов

#### Динамическое Взвешивание
```python
def adjust_signal_weight(ctx: OrderflowSignalContext) -> float:
    """
    Корректировка веса сигнала на основе новостного контекста
    """
    base_weight = ctx.weight

    if not ctx.news:
        return base_weight

    news_risk = ctx.news.news_risk
    news_grade = ctx.news.news_grade_id
    surprise = ctx.news.surprise_score

    # Снижаем вес во время высокой волатильности
    if news_grade >= 3:
        base_weight *= 0.5

    # Увеличиваем вес на неожиданных событиях
    if abs(surprise) > 0.5:
        base_weight *= 1.2

    # Корректируем на risk-on/risk-off
    if ctx.news.tags_mask & TAG_RISK_ON:
        if ctx.direction == "long":
            base_weight *= 1.1
        else:
            base_weight *= 0.9

    return min(base_weight, 1.0)  # Ограничение максимального веса
```

#### Confidence-Based Scaling
```python
def scale_by_confidence(ctx: OrderflowSignalContext) -> float:
    """
    Масштабирование на основе уверенности анализа
    """
    if not ctx.news:
        return 1.0

    confidence = ctx.news.confidence

    # Линейное масштабирование: 0.0 -> 0.5, 1.0 -> 1.0
    return 0.5 + 0.5 * confidence
```

### Временные Ограничения

#### Horizon-Based Filtering
```python
def apply_news_horizon(ctx: OrderflowSignalContext) -> Optional[int]:
    """
    Применение временных ограничений на основе типа новости
    """
    if not ctx.news:
        return None

    horizon_sec = ctx.news.horizon_sec
    news_grade = ctx.news.news_grade_id

    if news_grade == 0:
        return None  # Нет ограничений

    # Для высоких grade - строгие ограничения
    if news_grade >= 3:
        return max(horizon_sec, 3600)  # Минимум 1 час

    return horizon_sec
```

#### Календарные События
```python
def calendar_aware_timing(ctx: OrderflowSignalContext) -> bool:
    """
    Учет предстоящих календарных событий
    """
    if not ctx.news:
        return False

    tminus_sec = ctx.news.event_tminus_sec
    event_grade = ctx.news.event_grade_id

    # Избегаем входа перед важными событиями
    if event_grade >= 3 and tminus_sec < 3600:  # < 1 час
        return True

    # Осторожность перед средними событиями
    if event_grade >= 2 and tminus_sec < 1800:  # < 30 мин
        return True

    return False
```

## Grade System

### Вычисление Grade ID
```python
def compute_news_grade_id(
    *,
    news_risk: float,
    confidence: float,
    primary_tag_id: int,
    tags_mask: int = 0,
) -> int:
    """
    Вычисление уровня важности новости (0-4)

    Grade Levels:
    0 = ignore/none
    1 = low impact
    2 = medium impact
    3 = high impact
    4 = extreme/critical impact
    """

    # Базовый score из риска и уверенности
    score = news_risk * (0.6 + 0.4 * confidence)

    # Усиление по категориям
    tm = int(tags_mask)

    if (tm & MASK_MACRO_HIGH) != 0:
        score += 0.10  # FOMC, CPI, NFP, etc.
    if (tm & MASK_CRYPTO_SHOCK) != 0:
        score += 0.12  # Hack, exchange outage, reg
    if (tm & MASK_GEO) != 0:
        score += 0.08  # Geopolitical events
    if (tm & MASK_EQUITIES) != 0:
        score += 0.05  # Earnings, ETF flows
    if (tm & MASK_RISK_REGIME) != 0:
        score += 0.04  # Risk-on/off

    score = min(score, 1.0)

    # Пороги для grade levels
    if score >= 0.78: return 4  # Critical
    if score >= 0.58: return 3  # High
    if score >= 0.36: return 2  # Medium
    if score >= 0.20: return 1  # Low
    return 0  # None
```

### Tag Masks для Быстрой Фильтрации
```python
# Предопределенные маски для быстрой проверки
MASK_MACRO_HIGH = (
    _bit("cpi") | _bit("ppi") | _bit("fomc") | _bit("nfp") |
    _bit("rates") | _bit("inflation") | _bit("fed_speech") | _bit("macro")
)

MASK_CRYPTO_SHOCK = (
    _bit("hack") | _bit("exchange") | _bit("crypto_reg") | _bit("liquidation")
)

MASK_RISK_REGIME = (_bit("risk_off") | _bit("risk_on"))
MASK_EQUITIES = (_bit("earnings") | _bit("etf"))
MASK_GEO = _bit("geopolitics")

def has_macro_news(tags_mask: int) -> bool:
    """Быстрая проверка на макро-новости"""
    return (tags_mask & MASK_MACRO_HIGH) != 0

def has_crypto_shock(tags_mask: int) -> bool:
    """Быстрая проверка на крипто-шоки"""
    return (tags_mask & MASK_CRYPTO_SHOCK) != 0
```

## Примеры Интеграции

### Полная Стратегия с Новостями
```python
class NewsAwareSignalStrategy:
    """
    Полная стратегия с учетом новостного контекста
    """

    def evaluate_signal(self, ctx: OrderflowSignalContext) -> SignalDecision:
        """
        Оценка сигнала с учетом всех новостных факторов
        """
        if not self._has_news_data(ctx):
            return SignalDecision.SKIP

        # 1. Фильтрация по критическим событиям
        if self._is_critical_period(ctx):
            return SignalDecision.BLOCK

        # 2. Взвешивание по риску
        weight_multiplier = self._calculate_weight_multiplier(ctx)

        # 3. Временные ограничения
        horizon = self._calculate_horizon(ctx)

        # 4. Направление с учетом risk regime
        direction_adjusted = self._adjust_direction(ctx)

        return SignalDecision(
            action="trade",
            weight=ctx.weight * weight_multiplier,
            horizon_sec=horizon,
            direction=direction_adjusted
        )

    def _has_news_data(self, ctx) -> bool:
        return ctx.news is not None

    def _is_critical_period(self, ctx) -> bool:
        """Блокировка во время критических событий"""
        news = ctx.news
        return (
            news.news_grade_id >= 4 or  # Critical news
            (news.event_grade_id >= 3 and news.event_tminus_sec < 3600) or  # Major event soon
            (news.tags_mask & MASK_MACRO_HIGH and news.news_risk > 0.8)  # High macro risk
        )

    def _calculate_weight_multiplier(self, ctx) -> float:
        """Расчет множителя веса"""
        news = ctx.news
        multiplier = 1.0

        # Снижаем вес при высоком риске
        if news.news_grade_id >= 3:
            multiplier *= 0.6
        elif news.news_grade_id >= 2:
            multiplier *= 0.8

        # Увеличиваем при risk-on для long позиций
        if (news.tags_mask & _bit("risk_on")) and ctx.direction == "long":
            multiplier *= 1.2

        # Корректируем по уверенности анализа
        multiplier *= (0.7 + 0.3 * news.confidence)

        return multiplier

    def _calculate_horizon(self, ctx) -> int:
        """Расчет временного горизонта"""
        news = ctx.news

        # Базовый horizon из новости
        horizon = news.horizon_sec or 3600  # 1 час default

        # Увеличиваем для high grade
        if news.news_grade_id >= 3:
            horizon = int(horizon * 1.5)

        # Уменьшаем для низкой уверенности
        if news.confidence < 0.6:
            horizon = int(horizon * 0.7)

        return max(horizon, 300)  # Минимум 5 минут

    def _adjust_direction(self, ctx) -> str:
        """Корректировка направления с учетом новостей"""
        news = ctx.news
        original_direction = ctx.direction

        # Во время risk-off предпочитаем short
        if news.tags_mask & _bit("risk_off"):
            if original_direction == "long":
                return "short"

        # Во время risk-on предпочитаем long
        if news.tags_mask & _bit("risk_on"):
            if original_direction == "short":
                return "long"

        return original_direction
```

### Risk Management Integration
```python
class NewsBasedRiskManager:
    """
    Управление рисками на основе новостного контекста
    """

    def adjust_position_size(self, ctx: OrderflowSignalContext) -> float:
        """
        Корректировка размера позиции по новостям
        """
        if not ctx.news:
            return 1.0  # Базовый размер

        news_risk = ctx.news.news_risk
        news_grade = ctx.news.news_grade_id

        # Снижаем размер при высоком риске
        if news_grade >= 4:
            return 0.3  # Только 30% от обычного размера
        elif news_grade >= 3:
            return 0.5
        elif news_grade >= 2:
            return 0.7

        # Увеличиваем при низком риске
        if news_risk < 0.2:
            return 1.2

        return 1.0

    def adjust_stop_loss(self, ctx: OrderflowSignalContext) -> float:
        """
        Корректировка стоп-лосса по волатильности новостей
        """
        if not ctx.news:
            return 1.0

        news_risk = ctx.news.news_risk
        surprise = abs(ctx.news.surprise_score)

        # Увеличиваем стоп при высокой волатильности
        multiplier = 1.0 + (news_risk * 0.5) + (surprise * 0.3)

        return min(multiplier, 2.0)  # Максимум 2x стоп
```

## Мониторинг и Аналитика

### Метрики Использования
```python
def log_news_signal_interaction(ctx: OrderflowSignalContext, decision: str):
    """
    Логирование взаимодействия сигналов с новостями
    """
    if not ctx.news:
        return

    log.info("news_signal_interaction", {
        "symbol": ctx.symbol,
        "signal_id": ctx.signal_id,
        "news_risk": ctx.news.news_risk,
        "news_grade": ctx.news.news_grade_id,
        "surprise_score": ctx.news.surprise_score,
        "decision": decision,  # "trade", "skip", "block"
        "weight_multiplier": getattr(ctx, "weight_multiplier", 1.0),
        "horizon_sec": getattr(ctx, "horizon_sec", 0),
        "tags_mask": ctx.news.tags_mask,
        "primary_tag": ctx.news.primary_tag_id,
        "confidence": ctx.news.confidence
    })
```

### A/B Testing
```python
class NewsIntegrationExperiment:
    """
    A/B тестирование новостной интеграции
    """

    def assign_group(self, signal_id: str) -> str:
        """Присвоение группы для A/B теста"""
        # Простое хеширование для детерминированного распределения
        hash_val = hash(signal_id) % 100
        return "control" if hash_val < 50 else "news_aware"

    def evaluate_performance(self, results: List[TradeResult]) -> Dict[str, float]:
        """
        Оценка производительности разных групп
        """
        control_results = [r for r in results if r.group == "control"]
        news_results = [r for r in results if r.group == "news_aware"]

        return {
            "control_pnl": sum(r.pnl for r in control_results),
            "news_pnl": sum(r.pnl for r in news_results),
            "control_win_rate": sum(1 for r in control_results if r.pnl > 0) / len(control_results),
            "news_win_rate": sum(1 for r in news_results if r.pnl > 0) / len(news_results),
            "control_sharpe": self._calculate_sharpe(control_results),
            "news_sharpe": self._calculate_sharpe(news_results)
        }
```

## Производительность

### Оптимизации для Tick Loop

#### In-Memory Кеширование
```python
class NewsCache:
    """
    Кеш новостных данных для высокой производительности
    """

    def __init__(self, ttl_ms: int = 1500):
        self.cache: Dict[str, Tuple[int, NewsFeatures]] = {}
        self.ttl_ms = ttl_ms

    def get(self, symbol: str) -> Optional[NewsFeatures]:
        """Получение из кеша"""
        if symbol not in self.cache:
            return None

        timestamp, features = self.cache[symbol]
        if time.time() * 1000 - timestamp > self.ttl_ms:
            del self.cache[symbol]
            return None

        return features

    def put(self, symbol: str, features: NewsFeatures):
        """Сохранение в кеш"""
        self.cache[symbol] = (int(time.time() * 1000), features)

        # Очистка старых записей
        self._cleanup()

    def _cleanup(self):
        """Очистка устаревших данных"""
        now = time.time() * 1000
        expired = [k for k, (ts, _) in self.cache.items()
                  if now - ts > self.ttl_ms]

        for k in expired:
            del self.cache[k]
```

#### Batch Enrichment
```python
def enrich_batch_contexts(contexts: List[OrderflowSignalContext]) -> None:
    """
    Пакетное обогащение нескольких контекстов одним Redis запросом
    """
    if not contexts:
        return

    # Группировка по символам
    symbol_groups = defaultdict(list)
    for ctx in contexts:
        symbol = getattr(ctx, "symbol", "GLOBAL").upper()
        symbol_groups[symbol].append(ctx)

    # Batch запросы к Redis
    pipe = redis.pipeline(transaction=False)

    for symbol in symbol_groups.keys():
        pipe.hgetall(f"news:agg:{symbol}")

    results = pipe.execute()

    # Распределение результатов
    for (symbol, ctx_list), news_data in zip(symbol_groups.items(), results):
        for ctx in ctx_list:
            ctx.news = self._build_news_features(news_data)
```

## Безопасность и Fail-Open

### Graceful Degradation
```python
def safe_news_enrichment(ctx: OrderflowSignalContext) -> None:
    """
    Безопасное обогащение с graceful degradation
    """
    try:
        # Основная логика
        enricher.attach(ctx)

        # Валидация результата
        if ctx.news and not _validate_news_features(ctx.news):
            ctx.news = None
            _flag_data_quality_issue(ctx, "news_validation_failed")

    except RedisConnectionError:
        # Redis недоступен - работаем без новостей
        ctx.news = None
        _flag_data_quality_issue(ctx, "redis_unavailable")

    except Exception as e:
        # Неожиданная ошибка - логируем и продолжаем
        log.error(f"News enrichment failed: {e}")
        ctx.news = None
        _flag_data_quality_issue(ctx, "news_enrichment_error")

def _validate_news_features(news: NewsFeatures) -> bool:
    """Валидация корректности новостных данных"""
    if not (0.0 <= news.news_risk <= 1.0):
        return False
    if not (-1.0 <= news.surprise_score <= 1.0):
        return False
    if not (0 <= news.news_grade_id <= 4):
        return False

    return True
```

### Circuit Breaker Pattern
```python
class NewsEnrichmentCircuitBreaker:
    """
    Circuit breaker для защиты от cascade failures
    """

    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.last_failure_time = 0
        self.state = "closed"  # closed, open, half-open

    def call(self, func: Callable, *args, **kwargs):
        """Вызов функции с circuit breaker"""
        if self.state == "open":
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = "half-open"
            else:
                raise CircuitBreakerOpen("News enrichment circuit is open")

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result

        except Exception as e:
            self._on_failure()
            raise e

    def _on_success(self):
        """Обработка успешного вызова"""
        self.failure_count = 0
        self.state = "closed"

    def _on_failure(self):
        """Обработка неудачного вызова"""
        self.failure_count += 1
        self.last_failure_time = time.time()

        if self.failure_count >= self.failure_threshold:
            self.state = "open"
```

## Конфигурация и Тюнинг

### Environment Variables
```bash
# Производительность
NEWS_CACHE_TTL_MS=1500
NEWS_ENRICHER_TIMEOUT_MS=100

# Grade thresholds
NEWS_GRADE_T1=0.15
NEWS_GRADE_T2=0.30
NEWS_GRADE_T3=0.50
NEWS_GRADE_T4=0.70

# Weight multipliers
NEWS_WEIGHT_HIGH_RISK=0.6
NEWS_WEIGHT_MEDIUM_RISK=0.8
NEWS_WEIGHT_LOW_RISK=1.2

# Time horizons
NEWS_HORIZON_MACRO_HOURS=12
NEWS_HORIZON_GEO_HOURS=48
NEWS_HORIZON_CRYPTO_HOURS=24
```

### Динамическая Конфигурация
```python
class NewsIntegrationConfig:
    """
    Динамическая конфигурация интеграции новостей
    """

    def __init__(self):
        self.grade_thresholds = self._load_grade_thresholds()
        self.weight_multipliers = self._load_weight_multipliers()
        self.time_horizons = self._load_time_horizons()

    def get_grade_thresholds(self) -> Dict[str, float]:
        """Получение порогов для grade levels"""
        return self.grade_thresholds

    def get_weight_multiplier(self, grade: int) -> float:
        """Получение множителя веса для grade"""
        return self.weight_multipliers.get(grade, 1.0)

    def get_time_horizon(self, tags_mask: int) -> int:
        """Получение горизонта времени по тегам"""
        # Логика выбора горизонта по типам новостей
        if tags_mask & MASK_GEO:
            return self.time_horizons["geo"]
        elif tags_mask & MASK_MACRO_HIGH:
            return self.time_horizons["macro"]
        elif tags_mask & MASK_CRYPTO_SHOCK:
            return self.time_horizons["crypto"]

        return self.time_horizons["default"]

    def _load_grade_thresholds(self) -> Dict[str, float]:
        """Загрузка порогов из environment"""
        return {
            "t1": float(os.getenv("NEWS_GRADE_T1", "0.15")),
            "t2": float(os.getenv("NEWS_GRADE_T2", "0.30")),
            "t3": float(os.getenv("NEWS_GRADE_T3", "0.50")),
            "t4": float(os.getenv("NEWS_GRADE_T4", "0.70")),
        }
```

## Мониторинг и Отладка

### Debug Logging
```python
def debug_news_context(ctx: OrderflowSignalContext) -> Dict[str, Any]:
    """
    Отладочная информация по новостному контексту
    """
    if not ctx.news:
        return {"news_available": False}

    news = ctx.news
    return {
        "news_available": True,
        "news_risk": news.news_risk,
        "surprise_score": news.surprise_score,
        "news_grade": news.news_grade_id,
        "confidence": news.confidence,
        "horizon_sec": news.horizon_sec,
        "primary_tag": news.primary_tag_id,
        "tags_mask": news.tags_mask,
        "event_tminus_sec": news.event_tminus_sec,
        "event_grade": news.event_grade_id,
        "ref": news.ref,
        "asof_ts_ms": news.asof_ts_ms
    }
```

### Performance Metrics
```python
class NewsIntegrationMetrics:
    """
    Метрики производительности новостной интеграции
    """

    def __init__(self):
        self.enrichment_time = Histogram("news_enrichment_duration_seconds")
        self.enrichment_success = Counter("news_enrichment_success_total")
        self.enrichment_failure = Counter("news_enrichment_failure_total")
        self.grade_distribution = Histogram("news_signal_grade_distribution")
        self.cache_hit_ratio = Gauge("news_cache_hit_ratio")

    def record_enrichment(self, duration: float, success: bool):
        """Запись метрики обогащения"""
        self.enrichment_time.observe(duration)

        if success:
            self.enrichment_success.inc()
        else:
            self.enrichment_failure.inc()

    def record_grade(self, grade: int):
        """Запись распределения grade"""
        self.grade_distribution.observe(grade)

    def update_cache_stats(self, hits: int, misses: int):
        """Обновление статистики кеша"""
        total = hits + misses
        if total > 0:
            ratio = hits / total
            self.cache_hit_ratio.set(ratio)
```

## Примеры Использования

### Крипто-Трейдинг
```python
def crypto_signal_filter(ctx: OrderflowSignalContext) -> bool:
    """Фильтр сигналов для крипто-торговли"""
    if not ctx.news:
        return False

    # Блокировка во время exchange outages
    if ctx.news.tags_mask & _bit("exchange"):
        return True

    # Осторожность во время liquidation events
    if ctx.news.tags_mask & _bit("liquidation"):
        return ctx.confidence < 0.8

    # Разрешаем сильные сигналы во время risk-on
    if ctx.news.tags_mask & _bit("risk_on"):
        return False

    return False
```

### Форекс Трейдинг
```python
def forex_signal_enhancement(ctx: OrderflowSignalContext) -> float:
    """Усиление сигналов для форекс"""
    if not ctx.news:
        return 1.0

    multiplier = 1.0

    # Усиление во время FOMC
    if ctx.news.tags_mask & _bit("fomc"):
        multiplier *= 1.3

    # Снижение во время CPI
    if ctx.news.tags_mask & _bit("cpi"):
        multiplier *= 0.8

    # Учет календарных событий
    if ctx.news.event_grade_id >= 3:
        if ctx.news.event_tminus_sec < 3600:
            multiplier *= 0.7  # Снижаем перед важными событиями

    return multiplier
```

### Акции и ETF
```python
def equity_signal_timing(ctx: OrderflowSignalContext) -> Optional[int]:
    """Временные ограничения для акций"""
    if not ctx.news:
        return None

    # Earnings reports - короткий horizon
    if ctx.news.tags_mask & _bit("earnings"):
        return 7200  # 2 часа

    # ETF flows - средний horizon
    if ctx.news.tags_mask & _bit("etf"):
        return 14400  # 4 часа

    # Macro news - длинный horizon
    if ctx.news.tags_mask & MASK_MACRO_HIGH:
        return 43200  # 12 часов

    return None
```

## Заключение

Интеграция новостей в торговые сигналы предоставляет мощный инструмент для улучшения качества и timing торговых решений. Ключевые принципы успешной интеграции:

1. **Fail-Open Architecture**: Система продолжает работать при недоступности новостей
2. **Graceful Degradation**: Постепенное снижение функциональности при проблемах
3. **Performance Optimization**: Кеширование и batch операции для высокой производительности
4. **Configurable Behavior**: Настраиваемые пороги и веса для разных стратегий
5. **Comprehensive Monitoring**: Метрики и логи для отслеживания эффективности

Правильная интеграция новостей может значительно улучшить соотношение риск/прибыль торговых стратегий за счет избежания торговли в периоды высокой неопределенности и усиления сигналов в благоприятных условиях.
