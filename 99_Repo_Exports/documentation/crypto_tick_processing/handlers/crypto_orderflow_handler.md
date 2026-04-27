# Crypto OrderFlow Handler - Детальная документация

## Обзор

**Crypto OrderFlow Handler** - высокопроизводительный обработчик криптовалютных тиков с pipeline V2, cost/edge gate интеграцией и расширенной телеметрией.

**Расположение**: `python-worker/services/crypto_orderflow_handler.py`

**Назначение**: Обработка тиков криптовалютных фьючерсов с генерацией сигналов order flow, интеграцией с экспериментальным слоем и A/B-тестированием.

## Архитектурные принципы

### 1. Pipeline V2 Architecture
- **Модульная обработка**: Разделение на независимые стадии обработки
- **Конфигурируемые фильтры**: Плагинная система фильтров сигналов
- **Метрики по стадиям**: Детальный мониторинг каждой фазы pipeline

### 2. Cost/Edge Gate Integration
- **Динамическая фильтрация**: Фильтры на основе стоимости исполнения и edge
- **Адаптивные пороги**: Автоматическая корректировка threshold'ов
- **Риск-менеджмент**: Интеграция с системами управления рисками

### 3. Extended Telemetry
- **Структурированное логирование**: Детальные логи по всем операциям
- **Метрики производительности**: Latency, throughput, error rates
- **Экспериментальные метрики**: Метрики A/B-тестирования и экспериментов

## Детальная структура

### Основные компоненты

#### CryptoOrderFlowHandlerV2

```python
class CryptoOrderFlowHandlerV2(BaseOrderFlowHandler):
    """
    Основной обработчик крипто тиков с pipeline V2.
    """

    def __init__(self, config: OrderFlowConfig):
        self.config = config
        self.pipeline = self._build_pipeline()
        self.telemetry = TelemetryCollector()
        self.experiment_layer = ExperimentManager()

    async def process_tick(self, tick: Dict[str, Any]) -> Optional[Signal]:
        """
        Основной метод обработки тика.
        """
        # Pipeline V2 processing
        result = await self.pipeline.process(tick)

        # Cost/edge gate filtering
        if not self._passes_cost_gate(result):
            return None

        # Experiment layer routing
        variant = await self.experiment_layer.assign_variant(tick['symbol'])

        # Signal generation with telemetry
        signal = await self._generate_signal(result, variant)
        await self.telemetry.record_signal(signal)

        return signal
```

#### Pipeline Stages

1. **Data Ingestion Stage**
   - Валидация входных данных
   - Нормализация форматов
   - Базовая фильтрация

2. **Feature Extraction Stage**
   - Расчет order flow метрик
   - Вычисление OBI (Order Book Imbalance)
   - Анализ microstructure

3. **Signal Detection Stage**
   - Детекторы absorption, iceberg, delta
   - ML-based confidence scoring
   - Multi-timeframe analysis

4. **Risk Filtering Stage**
   - Cost-based filtering
   - Edge threshold validation
   - Position size validation

5. **Experiment Layer Stage**
   - A/B testing assignment
   - Variant-specific processing
   - Experimental metrics collection

## Конфигурация

### Основные параметры

```python
@dataclass
class CryptoOrderFlowConfig:
    # Pipeline configuration
    enable_pipeline_v2: bool = True
    max_concurrent_symbols: int = 50

    # Cost/edge gates
    min_edge_threshold: float = 0.0001
    max_cost_threshold: float = 0.001

    # Experiment layer
    experiment_enabled: bool = True
    experiment_sample_rate: float = 0.1

    # Telemetry
    telemetry_enabled: bool = True
    metrics_interval_seconds: int = 60
```

### Environment Variables

| Переменная | Описание | Значение по умолчанию |
|------------|----------|----------------------|
| `CRYPTO_OF_ENABLE_PIPELINE_V2` | Включить pipeline V2 | `true` |
| `CRYPTO_OF_MAX_SYMBOLS` | Максимум символов | `50` |
| `CRYPTO_OF_MIN_EDGE` | Минимальный edge | `0.0001` |
| `CRYPTO_OF_EXPERIMENT_ENABLED` | Эксперименты включены | `true` |

## Метрики и мониторинг

### Pipeline Metrics

- `crypto_of_pipeline_latency_ms` - Latency обработки pipeline
- `crypto_of_stage_duration_ms` - Время выполнения каждой стадии
- `crypto_of_signals_generated_total` - Количество сгенерированных сигналов
- `crypto_of_filtered_signals_total` - Количество отфильтрованных сигналов

### Cost/Edge Metrics

- `crypto_of_edge_distribution` - Распределение edge значений
- `crypto_of_cost_threshold_breaches` - Нарушения cost threshold
- `crypto_of_risk_filters_applied` - Примененные риск-фильтры

### Experiment Metrics

- `crypto_of_experiment_assignments_total` - Назначения экспериментов
- `crypto_of_variant_performance` - Производительность вариантов
- `crypto_of_experiment_precision` - Точность экспериментов

## Интеграция с экспериментальным слоем

### A/B Testing Integration

```python
# Assignment logic
variant = await experiment_manager.assign_variant(
    symbol=tick['symbol'],
    features=extracted_features,
    deterministic=True  # For reproducible testing
)

# Variant-specific processing
if variant.name == 'control':
    signal = generate_baseline_signal(tick)
elif variant.name == 'treatment_a':
    signal = generate_enhanced_signal(tick, variant.params)
```

### Metrics Collection

```python
# Record experiment metrics
await experiment_metrics.record(
    experiment_id=variant.experiment_id,
    variant=variant.name,
    signal=signal,
    outcome=trading_outcome
)
```

## Производительность

### Benchmark Results

| Конфигурация | Throughput (ticks/sec) | Latency P95 (ms) | Memory (MB) |
|-------------|----------------------|------------------|-------------|
| Pipeline V1 | 5,000 | 150 | 200 |
| Pipeline V2 | 15,000 | 45 | 180 |
| V2 + Experiments | 12,000 | 55 | 220 |

### Оптимизации

1. **Async Processing**: Полностью асинхронная обработка всех стадий
2. **Batch Operations**: Групповая обработка для снижения I/O overhead
3. **Memory Pooling**: Переиспользование объектов для снижения GC pressure
4. **Connection Pooling**: Redis connection pools для высокой concurrency

## Тестирование

### Unit Tests

```python
def test_pipeline_v2_processing():
    handler = CryptoOrderFlowHandlerV2(config)
    tick = create_test_tick()

    signal = await handler.process_tick(tick)

    assert signal is not None
    assert signal.confidence > 0.5

def test_cost_gate_filtering():
    handler = CryptoOrderFlowHandlerV2(config)

    # High cost signal should be filtered
    high_cost_signal = create_high_cost_signal()
    result = handler._passes_cost_gate(high_cost_signal)

    assert result == False
```

### Integration Tests

```python
async def test_full_pipeline_integration():
    # Setup Redis with test data
    await setup_test_redis_streams()

    # Start handler
    handler = CryptoOrderFlowHandlerV2(config)
    await handler.start()

    # Send test ticks
    await send_test_ticks_to_redis()

    # Verify signals generated
    signals = await redis.xrange('signals:orderflow:BTCUSDT')
    assert len(signals) > 0
```

## Troubleshooting

### Распространенные проблемы

1. **High Latency**
   - Проверить Redis connectivity
   - Проверить experiment layer performance
   - Мониторить pipeline stage metrics

2. **Signal Quality Issues**
   - Проверить cost/edge gate thresholds
   - Валидировать feature extraction
   - Проверить experiment assignments

3. **Memory Issues**
   - Мониторить object pooling efficiency
   - Проверить connection pool sizes
   - Оптимизировать batch sizes

### Debug режим

```python
# Enable detailed logging
config.telemetry_enabled = True
config.debug_mode = True

# Log all pipeline stages
logger.setLevel(logging.DEBUG)
```

## Резервное копирование и восстановление

### State Management

- **Configuration Persistence**: Все параметры сохраняются в Redis
- **Experiment State**: Состояние экспериментов в PostgreSQL
- **Metrics Backup**: Метрики экспортируются в Prometheus/Grafana

### Recovery Procedures

1. **Service Restart**: Автоматическое восстановление состояния из Redis
2. **Data Replay**: Возможность replay исторических тиков для восстановления
3. **Experiment Continuity**: Сохранение assignment consistency при restart




























