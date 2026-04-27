# Оффлайн-джоб расчета Baseline для L3-метрик

## Обзор

Оффлайн-джоб `baseline_job.py` рассчитывает baseline-квантиля по L3-метрикам для каждой группы сигналов `(symbol, signal_family, direction)` и генерирует YAML-конфигурацию для `CryptoConfScorer`.

## Что делает джоб

1. **Вытаскивает данные** из TimescaleDB:
   - `signal_facts` - факты сигналов с L3-метриками
   - `trade_performance` - результаты исполнения сигналов

2. **Группирует по** `(symbol, signal_family, direction)`

3. **Считает для каждой группы**:
   - Базовую статистику: hit_rate, expectancy_R, R-квантиля
   - L3-квантиля: spread, obi_persistence, cancel_to_trade, microprice_drift

4. **Выводит thresholds** из квантилей для CryptoConfScorer

5. **Сохраняет**:
   - В таблицу `signal_family_baseline` (TimescaleDB)
   - В YAML-файл для загрузки в CryptoConfScorer

## Запуск

### Через скрипт

```bash
# Из корня проекта
./python-worker/run_baseline_job.sh
```

### Через Python

```bash
cd python-worker
python -m regime.baseline_job
```

### Через Docker

```bash
docker exec -it scanner-python-worker bash -c "cd /app && python -m regime.baseline_job"
```

## Переменные окружения

| Переменная | Дефолт | Описание |
|------------|--------|----------|
| `DATABASE_URL` | `postgresql://postgres:password@localhost:5432/trade` | DSN для TimescaleDB |
| `BASELINE_LOOKBACK_DAYS` | `60` | Период анализа в днях |
| `BASELINE_MIN_SIGNALS` | `200` | Минимальное кол-во сигналов в группе |
| `BASELINE_MIN_TRADES` | `50` | Минимальное кол-во сделок в группе |
| `BASELINE_YAML_PATH` | `crypto_conf_scorer_baseline.yaml` | Путь к выходному YAML |
| `BASELINE_INSERT_DB` | `1` | Сохранять ли в базу данных (1/0) |

## Структура выходного YAML

```yaml
crypto_conf_scorer:
  default:
    l3:
      spread_max_ok_bps: 5.0
      spread_hard_limit_bps: 18.0
      cancel_soft: 2.0
      cancel_hard: 4.5
      obi_good_min: 0.55
      obi_bad_max: 0.20
      mp_drift_max_bps: 4.0

  by_symbol:
    BTCUSDT:
      crypto_orderflow:
        long:
          l3:
            spread_max_ok_bps: 3.0
            spread_hard_limit_bps: 15.0
            cancel_soft: 1.5
            cancel_hard: 4.0
            obi_good_min: 0.6
            obi_bad_max: 0.15
            mp_drift_max_bps: 3.0
```

## Логика расчета thresholds

### Spread
- `max_ok_bps` = p50(spread) - "нормальный" спред
- `hard_limit_bps` = p95(spread) - "критический" спред

### OBI Persistence
- `good_min` = p50(obi) - где уже заметен устойчивый перекос
- `bad_max` = p25(obi) - уровень "нет сигнала"

### Cancel-to-Trade
- `soft` = median(p50_bid, p50_ask) - порог слабости
- `hard` = median(p80_bid, p80_ask) - порог отключения

### Microprice Drift
- `max_bps` = p80(abs(drift)) - максимальный "нормальный" drift

## Интеграция в пайплайн

### 1. Cron/Airflow
```bash
# Еженедельно или ежедневно
0 2 * * 1 ./run_baseline_job.sh  # Каждое воскресенье в 2:00
```

### 2. Автоматическая загрузка в CryptoConfScorer

```python
# В CryptoOrderFlowHandler.__init__
baseline_yaml = os.getenv("BASELINE_YAML_PATH", "crypto_conf_scorer_baseline.yaml")
if os.path.exists(baseline_yaml):
    self.conf_scorer_cfg = CryptoConfScorerConfig.from_yaml(baseline_yaml)
else:
    self.conf_scorer_cfg = CryptoConfScorerConfig()  # defaults
```

### 3. Мониторинг изменений

```sql
-- Проверить последние baseline
SELECT symbol, signal_family, direction, hit_rate, expectancy_r,
       l3_spread_max_ok_bps, l3_obi_good_min
FROM signal_family_baseline
WHERE as_of_ts >= now() - interval '7 days'
ORDER BY as_of_ts DESC;
```

## Диагностика

### Логи
```
Starting baseline job: lookback=60d, min_signals=200
Fetched 15420 signal-performance records
Computed baseline for 12 signal groups
Inserted baseline data to database
Generated YAML config: crypto_conf_scorer_baseline.yaml
Baseline job completed successfully!
```

### Проверка качества

```python
# В signal_analysis.py можно проверить
analyzer = SignalAnalyzer(dsn)
df = analyzer.fetch_signals_with_results(days=30)

# Корреляции L3 с результатами
correlations = analyzer.analyze_l3_correlations(df)
print(correlations)
```

## Архитектура

```
TimescaleDB
├── signal_facts (сигналы + L3-метрики)
├── trade_performance (результаты сигналов)
└── signal_family_baseline (вычисленные baseline)

Оффлайн-джоб
├── baseline_job.py (основная логика)
├── run_baseline_job.sh (скрипт запуска)
└── crypto_conf_scorer_baseline.yaml (выходной конфиг)

CryptoConfScorer
├── from_yaml() (загрузка конфигурации)
└── get_symbol_config() (получение thresholds)
```

## Производительность

- **Время выполнения**: 2-5 минут для 60 дней данных
- **Память**: ~500MB для больших датасетов
- **I/O**: Основное время уходит на чтение из TimescaleDB

## Troubleshooting

### Нет данных
```
Fetched 0 signal-performance records
```
**Решение**: Проверить наличие данных в `signal_facts` и `trade_performance`

### Мало групп
```
Computed baseline for 2 signal groups
```
**Решение**: Уменьшить `MIN_SIGNALS`/`MIN_TRADES` или увеличить `LOOKBACK_DAYS`

### Ошибка подключения к БД
```
Error: connection failed
```
**Решение**: Проверить `DATABASE_URL` и доступность TimescaleDB
