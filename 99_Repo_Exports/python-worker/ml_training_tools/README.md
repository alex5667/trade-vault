# ML Training Tools Reference

Эта папка содержит референсные файлы для ML training pipeline: dataset building, labeling, training, threshold optimization, и feature engineering.

## Структура

### A) Dataset / Labeling / Targets

- **`build_dataset_from_inputs_tb_labels_v3_mh.py`** - Основной builder для создания dataset из OF inputs и TB labels. Объединяет features из indicators с utility targets по горизонтам (60s, 180s, 300s).

- **`tb_labeling.py`** - Triple-barrier labeling: вычисление TP/SL barriers, barrier_stats (MAE/MFE, adverse_proxy, y_edge), exec_cost_r.

- **`build_of_dataset.py`** - Join OF replay rows ↔ POSITION_CLOSED по sid. Используется для создания dataset из engine replay outputs и trade outcomes.

- **`dataset_sample.ndjson`** - Пример реального dataset (500 строк из closed_trades.ndjson). Формат: NDJSON с полями sid, symbol, ts_ms, direction, features, labels (r_mult, pnl, etc).

### B) Training / Threshold Tools

- **`train_ml_confirm_tb_util_mh_v1.py`** - Training utility model (multi-horizon). Использует Ridge + GBDT ensemble, PurgedEmbargoTimeSeriesSplit, Platt calibration.

- **`optimize_util_floor_mh_v1.py`** - Оптимизация utility floor thresholds (global + per-bucket: trend/range/other). Grid search по floor_min..floor_max с шагом floor_step.

- **`ml_threshold_proposer_v2_utility.py`** - Автоматическое предложение p_min thresholds по символам. Использует utility gates (meanR, tail_rate, ES05) + calibration gates (ECE, Brier) + drift guard.

- **`ml_promotion_ladder_v3.py`** - Promotion ladder для per-symbol shares. Multi-step ladder (0.05 → 0.10 → 0.20 → 0.35 → 0.50) с utility gates + range exec-risk veto.

- **`eval_meta_enforce.py`** - Evaluation meta model thresholds для ENFORCE mode. Ищет оптимальный meta_p_min, максимизируя meanR при контроле tail loss rate.

### C) Feature Schema / Engineering

- **`feature_engineering.py`** - Утилиты для feature engineering: transforms (log1p, clip, winsor), robust scaling, bucketization, session/regime labels.

- **`ml_feature_schema.py`** - Legacy feature schema (v1): explicit feature list с scenario one-hots, time features (sin/cos).

- **`ml_feature_schema_v2.py`** - Feature schema v2: стабильный порядок features, numeric + bool keys, direction/bucket one-hots.

- **`ml_feature_schema_v3.py`** - Feature schema v3: расширяет v2, добавляет session features (UTC hour + day-of-week one-hot), optional outcome/hawkes fields.

### D) Infra Wiring

- **`docker-compose-timers.yml`** - Docker Compose конфигурация для timer-based workers (nightly training, threshold proposals, promotion ladder).

- **`of_reports.env`** - Актуальный ENV конфиг для python-worker/of_reports. Содержит:
  - Redis streams (OF_INPUTS_STREAM, TRADE_EVENTS_STREAM, metrics streams)
  - Paths (PROJECT_PYWORKER, STATE_DIR, OUT_DIR, BASELINE_DIR)
  - Thresholds (PASS_RATE_MIN, EXEC_RISK_NORM_P90_WARN, etc)
  - ML training params (TB_HORIZONS_MS, UTIL_UNC_K, ML_SPLITS, etc)
  - Meta ENFORCE rollout params (META_ENFORCE_*, META_RAMP_*, etc)
  - Recommendations system (RECS_*, RECS_HMAC_SECRET, etc)

## Зависимости

Все Python файлы требуют:
- `pandas`, `numpy`, `scikit-learn`, `joblib`
- `redis` (для threshold proposer, promotion ladder)
- Внутренние модули: `core.bucket_utils`, `core.ml_model_types`, `services.ml_calibration`, `tools.redis_window`, `tools.ml_metrics_agg`, etc.

## Использование

### Dataset Building

```bash
# 1. Labeling: создать TB labels из trade paths
python -m tools.tb_labeling --inputs inputs.ndjson --out labels.ndjson

# 2. Build dataset: объединить inputs + labels
python -m tools.build_dataset_from_inputs_tb_labels_v3_mh \
  --inputs inputs.ndjson \
  --tb labels.ndjson \
  --out dataset.parquet \
  --horizons 60000,180000,300000
```

### Training

```bash
# Train utility model
python -m tools.train_ml_confirm_tb_util_mh_v1 \
  --dataset dataset.parquet \
  --out-dir models/util_mh_v1 \
  --horizons 60000,180000,300000 \
  --unc-k 0.5

# Optimize utility floors
python -m tools.optimize_util_floor_mh_v1 \
  --dataset dataset.parquet \
  --model models/util_mh_v1/model.joblib \
  --out floors.json \
  --floor-min -0.05 \
  --floor-max 0.10 \
  --floor-step 0.005
```

### Threshold Proposals (timer-based)

```bash
# Запускается через docker-compose-timers.yml
# Читает metrics:ml_confirm, metrics:ml_outcome
# Предлагает p_min updates через recs bundle system
```

## Интеграция

Эти файлы используются в:
- Nightly training pipeline (timer workers)
- Threshold optimization loops (ml_threshold_proposer, ml_promotion_ladder)
- Feature engineering в runtime (ml_feature_schema_v3)
- Dataset building для backtesting/calibration

## Примечания

- Все файлы актуальны на момент копирования
- `build_dataset_from_inputs_tb_labels_v3_mh.py` - актуальный builder (v3_mh = multi-horizon)
- `ml_feature_schema_v3.py` - актуальная schema (используется в production)
- `of_reports.env` - актуальный конфиг (включает все последние параметры для meta ENFORCE, unclamp v5, etc)










