# Experiment Layer Implementation

Полноценный "слой экспериментов" для A/B-тестирования фильтров и фич сигналов в торговой системе.

## Обзор

Этот слой позволяет тестировать новые фильтры и фичи как контролируемые эксперименты, прежде чем внедрять их в продакшн. Система автоматически:

- Назначает сигналы в control/treatment группы
- Применяет разные фильтры к разным группам
- Логирует результаты торговли
- Рассчитывает метрики сравнения
- Определяет успешность экспериментов

## Архитектура

### 1. Database Schema

#### `signal_experiment` - определения экспериментов
```sql
CREATE TABLE signal_experiment (
    experiment_id   text primary key,
    name            text not null,
    filter_name     text not null,
    signal_family   text not null,
    direction       int not null default 0,
    created_at      timestamptz not null default now(),
    start_at        timestamptz not null,
    end_at          timestamptz,
    status          text not null default 'draft',
    target_metric   text not null,
    config          jsonb
);
```

#### `signal_experiment_snapshot` - метрики по экспериментам
```sql
CREATE TABLE signal_experiment_snapshot (
    experiment_id   text not null,
    as_of           timestamptz not null,
    variant         text not null,
    signals_total   integer not null,
    traded_total    integer not null,
    winners_total   integer not null,
    losers_total    integer not null,
    expectancy_r    double precision,
    sharpe_r        double precision,
    max_dd_r        double precision,
    cl_ratio        double precision,
    winrate         double precision,
    precision       double precision,
    recall          double precision,
    f1              double precision,
    extra           jsonb,
    primary key (experiment_id, as_of, variant)
);
```

#### Обновленная таблица `signals`
```sql
ALTER TABLE signals
    ADD COLUMN experiment_id      text,
    ADD COLUMN experiment_variant text,
    ADD COLUMN filter_flags       jsonb;
```

### 2. Runtime Components

#### ExperimentManager (`handlers/experiment_manager.py`)
- Загружает активные эксперименты из БД
- Детерминированно назначает варианты (control/treatment)
- Кэширует эксперименты с автоматическим обновлением

#### Experiment Metrics (`handlers/experiment_metrics.py`)
- `expectancy_r()` - математическое ожидание доходности
- `sharpe_r()` - Sharpe ratio
- `max_drawdown_r()` - максимальная просадка
- `precision_recall()` - точность и полнота
- `calculate_experiment_metrics()` - полный набор метрик

#### Signal Integration
- `BaseOrderFlowHandler._generate_signals()` - интеграция экспериментов
- `SignalContext` - расширен полями экспериментов
- `SignalRepository` - сохранение experiment_id и variant

### 3. Offline Processing

#### `experiment_eval_job.py` - джоб расчета метрик
```bash
# Запуск каждые 5-15 минут
python scripts/experiment_eval_job.py
```

- Считывает сигналы + результаты торговли
- Группирует по experiment_id и variant
- Рассчитывает метрики для каждой группы
- Сохраняет snapshots в БД

## Использование

### 1. Создание эксперимента

```bash
# Создать sample эксперимент
python scripts/setup_sample_experiment.py create_sample

# Посмотреть все эксперименты
python scripts/setup_sample_experiment.py list

# Активировать эксперимент
python scripts/setup_sample_experiment.py activate confidence_threshold_boost_v1

# Посмотреть результаты
python scripts/setup_sample_experiment.py results confidence_threshold_boost_v1
```

### 2. Пример эксперимента

```python
# В коде эксперимент выглядит так:
exp_info = experiment_manager.assign_variant(
    now_ms=ctx.ts,
    symbol=ctx.symbol,
    signal_family="orderflow",
    direction=1,  # long
    signal_id=ctx.signal_id,
)

if exp_info:
    ctx.experiment_id = exp_info["experiment_id"]
    ctx.experiment_variant = exp_info["variant"]
    ctx.experiment_config = exp_info["config"]

    # Применяем фильтры на основе variant
    if ctx.experiment_variant == "treatment":
        # Treatment: применяем новый фильтр
        passed = apply_experimental_filter(ctx, exp_info["config"])
        ctx.filter_flags[f"{exp_info['filter_name']}_passed"] = passed
    else:
        # Control: только baseline фильтры
        passed = apply_baseline_filters(ctx)
        ctx.filter_flags["baseline_passed"] = passed
```

### 3. Мониторинг результатов

```sql
-- Сравнение control vs treatment
SELECT
    variant,
    expectancy_r,
    sharpe_r,
    max_dd_r,
    winrate,
    traded_total
FROM signal_experiment_snapshot
WHERE experiment_id = 'confidence_threshold_boost_v1'
ORDER BY as_of DESC, variant;

-- Детальный анализ сигналов
SELECT
    experiment_variant,
    COUNT(*) as signals,
    COUNT(CASE WHEN sp.realized_r >= 0.2 THEN 1 END) as winners,
    AVG(sp.realized_r) as avg_r
FROM signals s
LEFT JOIN signal_performance sp ON s.signal_id = sp.signal_id
WHERE s.experiment_id = 'confidence_threshold_boost_v1'
GROUP BY experiment_variant;
```

## Decision Rules

Эксперимент считается успешным если:

1. **Достаточная выборка**: `traded_total >= 200` для каждой группы
2. **Целевая метрика улучшилась**: treatment > control + min_improvement
3. **Drawdown не ухудшился**: treatment_dd <= control_dd + max_worsening

```python
def is_experiment_successful(control_metrics, treatment_metrics, target_metric):
    # Пример для expectancy_r
    improvement = treatment_metrics[target_metric] - control_metrics[target_metric]
    return improvement >= 0.05  # 5% improvement minimum
```

## Примеры экспериментов

### 1. Confidence Threshold Boost
```json
{
  "experiment_id": "confidence_threshold_boost_v1",
  "filter_name": "confidence_boost",
  "config": {
    "confidence_threshold": 70.0
  },
  "target_metric": "expectancy_r"
}
```

### 2. Weak Progress Filter
```json
{
  "experiment_id": "weak_progress_filter_v1",
  "filter_name": "weak_progress_filter",
  "config": {
    "require_weak_progress": true,
    "weak_progress_threshold": 0.25
  },
  "target_metric": "sharpe_r"
}
```

### 3. Z-Threshold Relaxation
```json
{
  "experiment_id": "z_threshold_relax_v1",
  "filter_name": "z_threshold_relax",
  "config": {
    "z_threshold_multiplier": 1.2
  },
  "target_metric": "precision"
}
```

## Best Practices

### Дизайн экспериментов
1. **Одна фича за раз** - тестируйте только одно изменение
2. **Достаточная длительность** - минимум 1-2 недели для накопления статистики
3. **Релевантные метрики** - выбирайте target_metric осмысленно
4. **Контроль рисков** - следите за drawdown в treatment группе

### Мониторинг
1. **Регулярные отчеты** - проверяйте результаты каждые несколько часов
2. **Раннее прекращение** - останавливайте если treatment сильно хуже
3. **Валидация** - проверяйте что assignment детерминированное

### Внедрение
1. **Gradual rollout** - после успеха вводите постепенно
2. **Monitoring** - следите за метриками после внедрения
3. **Rollback plan** - имейте план отката если что-то пойдет не так

## Troubleshooting

### Проблемы с assignment
- Проверьте что ExperimentManager правильно инициализируется
- Убедитесь что experiment.status = 'running'
- Проверьте логи ExperimentManager

### Нет результатов в snapshot
- Проверьте что experiment_eval_job.py запущен
- Убедитесь что сигналы доходят до signal_performance
- Проверьте логи evaluation job

### Неправильные метрики
- Проверьте success_threshold_r в experiment_eval_job.py
- Убедитесь что pnl_r правильно рассчитывается
- Проверьте логику grouping по variant

## Files Overview

```
python-worker/
├── handlers/
│   ├── experiment_manager.py      # Runtime experiment management
│   ├── experiment_metrics.py      # Metrics calculation functions
│   └── base_orderflow_handler.py  # Integration point
├── scripts/
│   ├── experiment_eval_job.py     # Offline metrics calculation
│   └── setup_sample_experiment.py # Experiment management utilities
├── signal_exec/
│   └── repository.py               # Updated signal persistence
└── migrations/
    └── 005_create_experiment_tables.sql  # Database schema
```

## Integration Status

✅ Database schema created
✅ ExperimentManager implemented
✅ Metrics functions implemented
✅ Signal processing integration
✅ Persistence layer updated
✅ Evaluation job created
✅ Sample experiments setup

Готово к использованию! 🚀























































