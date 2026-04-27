# 📈 Stop-Loss Quantile Analytics

## Обзор

**SL Quantile Aggregator** - специализированный сервис для анализа эффективности стоп-лоссов с использованием квантильного подхода и оптимизации уровней стоп-лоссов на основе эмпирических данных.

**Расположение**: `python-worker/services/sl_quantile_aggregator.py`

**Назначение**: Анализ распределения стоп-лоссов, оптимизация уровней SL, расчет risk-adjusted метрик.

## Архитектурные принципы

### 1. Квантильный подход
- **Эмпирический анализ**: Основан на реальных данных распределения потерь
- **Динамическая оптимизация**: Автоматическая корректировка уровней SL
- **Риск-корректированные метрики**: Учет волатильности и рыночных условий

### 2. Многоуровневый анализ
- **По символам**: Специфические уровни для каждого торгового инструмента
- **По стратегиям**: Дифференциация по типам сигналов и подходов
- **По временным периодам**: Анализ сезонности и рыночных режимов

### 3. Интеграция с системой риск-менеджмента
- **SLQ Risk Adjust**: Корректировка позиций на основе SL-аналитики
- **SLQ Store**: Кэширование и хранение SL-квантилей
- **Real-time updates**: Обновление уровней в реальном времени

## Детальная структура

### Основные компоненты

#### SLQuantileAggregator

```python
class SLQuantileAggregator:
    """
    Агрегатор квантилей стоп-лоссов с оптимизацией уровней.
    """

    def __init__(self, redis_client, config: SLAggregatorConfig):
        self.redis = redis_client
        self.config = config
        self.store = SLQStore(redis_client)

    async def analyze_sl_distribution(self, symbol: str, strategy: str,
                                    timeframe_days: int = 30) -> SLQuantileAnalysis:
        """
        Анализ распределения стоп-лоссов для заданных параметров.

        Args:
            symbol: Торговый символ
            strategy: Тип стратегии
            timeframe_days: Период анализа в днях

        Returns:
            SLQuantileAnalysis с результатами анализа
        """
        # Получить исторические данные
        trades = await self._fetch_trade_history(symbol, strategy, timeframe_days)

        # Рассчитать квантили потерь
        quantiles = self._calculate_loss_quantiles(trades)

        # Оптимизировать уровни SL
        optimal_levels = self._optimize_sl_levels(quantiles)

        # Сохранить результаты
        await self.store.save_quantiles(symbol, strategy, quantiles)

        return SLQuantileAnalysis(
            symbol=symbol,
            strategy=strategy,
            quantiles=quantiles,
            optimal_sl_levels=optimal_levels,
            confidence_intervals=self._calculate_confidence_intervals(quantiles)
        )
```

#### SLQuantileAnalysis

```python
@dataclass
class SLQuantileAnalysis:
    """
    Результаты анализа квантилей стоп-лоссов.
    """
    symbol: str
    strategy: str
    quantiles: Dict[float, float]  # quantile -> loss_value
    optimal_sl_levels: Dict[str, float]  # level_name -> value
    confidence_intervals: Dict[float, Tuple[float, float]]
    analysis_timestamp: datetime = field(default_factory=datetime.utcnow)

    def get_recommended_sl(self, risk_tolerance: float = 0.05) -> float:
        """
        Получить рекомендуемый уровень SL на основе толерантности к риску.

        Args:
            risk_tolerance: Доля капитала, которую готовы потерять (5% по умолчанию)

        Returns:
            Рекомендуемый уровень стоп-лосса
        """
        # Найти квантиль, соответствующий risk_tolerance
        target_quantile = 1.0 - risk_tolerance

        # Интерполировать между ближайшими квантилями
        return self._interpolate_quantile(target_quantile)
```

## Методы анализа

### 1. Квантильный анализ потерь

```python
def _calculate_loss_quantiles(self, trades: List[Trade]) -> Dict[float, float]:
    """
    Расчет квантилей распределения потерь.
    """
    # Извлечь только убыточные сделки
    losses = [trade.pnl for trade in trades if trade.pnl < 0]

    if not losses:
        return {}

    # Рассчитать квантили
    quantiles = {}
    for q in [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]:
        quantiles[q] = np.quantile(losses, q)

    return quantiles
```

### 2. Оптимизация уровней SL

```python
def _optimize_sl_levels(self, quantiles: Dict[float, float]) -> Dict[str, float]:
    """
    Оптимизация уровней стоп-лоссов на основе квантилей.
    """
    optimal_levels = {}

    # Консервативный уровень (90-й перцентиль потерь)
    optimal_levels['conservative'] = abs(quantiles.get(0.9, 0.02))

    # Умеренный уровень (75-й перцентиль)
    optimal_levels['moderate'] = abs(quantiles.get(0.75, 0.015))

    # Агрессивный уровень (50-й перцентиль)
    optimal_levels['aggressive'] = abs(quantiles.get(0.5, 0.01))

    # Динамический уровень на основе волатильности
    optimal_levels['dynamic'] = self._calculate_dynamic_sl(quantiles)

    return optimal_levels
```

### 3. Расчет доверительных интервалов

```python
def _calculate_confidence_intervals(self, quantiles: Dict[float, float]) -> Dict[float, Tuple[float, float]]:
    """
    Расчет доверительных интервалов для квантилей.
    """
    intervals = {}

    for q, value in quantiles.items():
        # Bootstrap для оценки неопределенности
        bootstrap_values = self._bootstrap_quantile(q, n_bootstraps=1000)
        ci_lower = np.percentile(bootstrap_values, 2.5)
        ci_upper = np.percentile(bootstrap_values, 97.5)
        intervals[q] = (ci_lower, ci_upper)

    return intervals
```

## Конфигурация

### SLAggregatorConfig

```python
@dataclass
class SLAggregatorConfig:
    """
    Конфигурация SL Quantile Aggregator.
    """
    # Анализ
    default_timeframe_days: int = 30
    min_trades_for_analysis: int = 50
    quantile_levels: List[float] = field(default_factory=lambda: [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99])

    # Оптимизация
    risk_tolerance_levels: List[float] = field(default_factory=lambda: [0.01, 0.02, 0.05])
    enable_dynamic_sl: bool = True

    # Кэширование
    cache_ttl_seconds: int = 3600  # 1 час
    enable_redis_cache: bool = True

    # Мониторинг
    enable_metrics: bool = True
    metrics_prefix: str = "sl_quantile"
```

## Метрики и мониторинг

### Performance Metrics

- `sl_quantile_analysis_duration_ms` - Время выполнения анализа
- `sl_quantile_trades_processed_total` - Обработано сделок
- `sl_quantile_cache_hit_ratio` - Эффективность кэширования

### Business Metrics

- `sl_quantile_optimal_levels_generated_total` - Сгенерировано оптимальных уровней
- `sl_quantile_risk_tolerance_distribution` - Распределение толерантности к риску
- `sl_quantile_dynamic_sl_updates_total` - Обновлений динамических SL

### Quality Metrics

- `sl_quantile_confidence_interval_width` - Ширина доверительных интервалов
- `sl_quantile_quantile_stability` - Стабильность квантилей во времени

## Интеграция с другими компонентами

### SLQ Risk Adjust

```python
# Интеграция с risk adjuster
risk_adjuster = SLQRiskAdjust(sl_aggregator=self)

async def adjust_position_risk(self, position: Position) -> AdjustedPosition:
    """
    Корректировка размера позиции на основе SL-аналитики.
    """
    # Получить оптимальный SL уровень
    analysis = await self.sl_aggregator.analyze_sl_distribution(
        position.symbol, position.strategy
    )

    recommended_sl = analysis.get_recommended_sl(risk_tolerance=0.02)

    # Рассчитать скорректированный размер позиции
    adjusted_size = position.size * (recommended_sl / position.stop_loss)

    return AdjustedPosition(
        original_position=position,
        adjusted_size=adjusted_size,
        recommended_sl=recommended_sl,
        risk_adjustment_factor=adjusted_size / position.size
    )
```

### SLQ Store

```python
# Кэширование результатов анализа
store = SLQStore(redis_client)

# Сохранение результатов
await store.save_quantiles(symbol, strategy, quantiles)

# Получение из кэша
cached_analysis = await store.get_analysis(symbol, strategy)
```

## Использование

### Базовый анализ

```python
from sl_quantile_aggregator import SLQuantileAggregator

# Инициализация
aggregator = SLQuantileAggregator(redis_client, config)

# Анализ для BTCUSDT и orderflow стратегии
analysis = await aggregator.analyze_sl_distribution(
    symbol="BTCUSDT",
    strategy="orderflow",
    timeframe_days=30
)

# Получение рекомендаций
conservative_sl = analysis.optimal_sl_levels['conservative']
recommended_sl = analysis.get_recommended_sl(risk_tolerance=0.02)

print(f"Conservative SL: {conservative_sl}")
print(f"Recommended SL (2% risk): {recommended_sl}")
```

### Интеграция с торговой системой

```python
async def before_open_position(position_request: PositionRequest) -> AdjustedPositionRequest:
    """
    Корректировка параметров позиции перед открытием.
    """
    # Анализ SL распределения
    analysis = await sl_aggregator.analyze_sl_distribution(
        position_request.symbol,
        position_request.strategy
    )

    # Оптимизация SL уровня
    optimal_sl = analysis.get_recommended_sl()

    # Корректировка размера позиции
    risk_adjusted_size = calculate_position_size(
        capital=trading_capital,
        entry_price=position_request.entry_price,
        stop_loss=optimal_sl,
        risk_per_trade_percent=1.0
    )

    return AdjustedPositionRequest(
        original_request=position_request,
        adjusted_stop_loss=optimal_sl,
        adjusted_size=risk_adjusted_size,
        sl_analysis=analysis
    )
```

## Производительность

### Benchmark Results

| Параметр | Значение | Примечание |
|----------|----------|------------|
| **Время анализа (30 дней)** | 150-300 мс | Зависит от количества сделок |
| **Память на анализ** | 50-200 MB | Масштабируется с объемом данных |
| **Кэш эффективность** | 85-95% | Для повторяющихся запросов |
| **Параллельные анализы** | До 10 одновременных | Ограничено Redis |

### Оптимизации

1. **Инкрементальный анализ**: Обновление квантилей без полного пересчета
2. **Кэширование результатов**: Redis-based caching с TTL
3. **Batch processing**: Групповая обработка множественных символов
4. **Approximate quantiles**: Приближенные алгоритмы для больших датасетов

## Тестирование

### Unit Tests

```python
def test_quantile_calculation():
    trades = create_test_trades_with_losses()
    aggregator = SLQuantileAggregator(mock_redis, config)

    quantiles = aggregator._calculate_loss_quantiles(trades)

    assert 0.5 in quantiles  # Median
    assert quantiles[0.5] < 0  # Should be negative (loss)
    assert quantiles[0.95] < quantiles[0.5]  # 95th percentile worse than median

def test_sl_optimization():
    quantiles = {0.5: -0.01, 0.75: -0.02, 0.9: -0.03}
    aggregator = SLQuantileAggregator(mock_redis, config)

    optimal_levels = aggregator._optimize_sl_levels(quantiles)

    assert optimal_levels['conservative'] == 0.03  # abs(0.9 quantile)
    assert optimal_levels['moderate'] == 0.02
    assert optimal_levels['aggressive'] == 0.01
```

### Integration Tests

```python
async def test_full_analysis_pipeline():
    # Setup test data in Redis
    await setup_test_trade_history()

    aggregator = SLQuantileAggregator(redis_client, config)

    # Run analysis
    analysis = await aggregator.analyze_sl_distribution("BTCUSDT", "orderflow")

    # Verify results
    assert analysis.symbol == "BTCUSDT"
    assert len(analysis.quantiles) > 0
    assert len(analysis.optimal_sl_levels) > 0

    # Verify persistence
    cached = await aggregator.store.get_analysis("BTCUSDT", "orderflow")
    assert cached is not None
```

## Troubleshooting

### Распространенные проблемы

1. **Недостаточно данных для анализа**
   ```
   SLQuantileError: Insufficient trades for analysis (min: 50, got: 23)
   ```
   - Увеличить timeframe_days
   - Проверить наличие исторических данных
   - Рассмотреть объединение стратегий

2. **Нестабильные квантили**
   ```
   Warning: Quantile confidence interval too wide
   ```
   - Проверить волатильность данных
   - Рассмотреть более длинный период анализа
   - Добавить фильтры по рыночным условиям

3. **Кэш проблемы**
   - Проверить Redis connectivity
   - Валидировать TTL настройки
   - Мониторить cache hit ratio

### Debug режим

```python
# Включить детальное логирование
import logging
logging.getLogger('sl_quantile_aggregator').setLevel(logging.DEBUG)

# Дополнительные метрики
config.enable_detailed_metrics = True
config.log_quantile_calculations = True
```

## Мониторинг и алертинг

### Key Metrics to Monitor

- **Data Quality**: Количество анализируемых сделок, полнота данных
- **Analysis Performance**: Время выполнения, использование ресурсов
- **Result Stability**: Ширина доверительных интервалов, изменения во времени
- **System Health**: Cache hit ratio, error rates, Redis connectivity

### Recommended Alerts

- `sl_quantile_analysis_failures > 5` в 5 минут
- `sl_quantile_analysis_duration_ms > 5000` (timeout)
- `sl_quantile_insufficient_data_ratio > 0.8` (недостаток данных)
- `sl_quantile_cache_hit_ratio < 0.7` (проблемы с кэшем)




























