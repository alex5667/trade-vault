# Ответы на вопросы о контрактах и архитектуре

## 1. Контракт входов и экспорт OFInputs (OFInputsV1)

### Зачем нужен OFInputsV1?

**Цель:** Детерминированный replay и обучение моделей на реальных inputs, захваченных в момент принятия решения.

**Ключевые принципы:**
- Минимальный набор входных данных, достаточный для воспроизведения решения
- Версионирование контракта (`v: int`) для обратной совместимости
- Снапшот конфигурации (`cfg`) для детерминизма при replay
- Все фичи представлены как детерминированные значения (не потоки данных)

### Структура OFInputsV1

```python
@dataclass
class OFInputsV1:
    v: int                    # версия контракта (1)
    symbol: str
    ts_ms: int                # timestamp в миллисекундах
    regime: str               # "na" | "trend" | "range" | "thin"
    direction: str            # LONG/SHORT
    scenario: str             # reversal/continuation
    
    # Reversal inputs
    delta_z: float
    weak_progress: int        # 1/0
    sweep_recent: int         # 1/0
    reclaim_recent: int       # 1/0
    obi_stable: int           # 1/0
    iceberg_strict: int       # 1/0
    abs_lvl_ok: int           # 1/0
    
    # Continuation inputs
    trend_dir: str            # LONG/SHORT/NONE
    hidden_ctx_recent: int    # 1/0
    cont_ctx_recent: int      # 1/0
    
    # Config subset (для детерминизма)
    cfg: Dict[str, Any]
    
    # Calibration inputs
    fp_eff_quote: float
    fp_quote_delta: float
    fp_move_bp: float = 0.0
```

### Экспорт OFInputs

**Файл:** `python-worker/tools/export_of_inputs_ndjson_v2.py`

**Источник:** Redis Stream `signals:of:inputs` (или `stream:of:inputs`)

**Формат:** NDJSON (один JSON объект на строку)

**Особенности:**
- Пагинация через XRANGE с детерминированным порядком по stream ID
- Resume через state file (хранит последний обработанный stream ID)
- Робастный парсинг: поддерживает bytes/str payload, валидирует JSON
- Fail-open на плохих строках (логирует ошибки, продолжает работу)

**Зависимости:**
- `redis` (redis-py)
- Стандартная библиотека Python (json, argparse, pathlib)

## 2. Контракт OFConfirm и упаковка gate_bits

### Зачем нужен OFConfirmV3?

**Цель:** Стабильный контракт для встраивания в raw signals и публикации в `signals:of:confirm`. Версионированный и интроспектируемый: каждое решение объяснимо.

### Структура OFConfirmV3

```python
@dataclass
class OFConfirmV3:
    v: int                    # версия контракта (3)
    symbol: str
    ts_ms: int
    direction: str            # LONG/SHORT
    scenario: str             # reversal/continuation/none
    ok: int                   # 1/0 (финальное решение)
    score: float              # 0..1
    have: int                 # количество пройденных гейтов
    need: int                 # требуемое количество гейтов
    gate_bits: int            # BIT_A|BIT_B|BIT_C|BIT_D
    reason: str               # стабильный reason code для veto/allow
    evidence: Dict[str, Any]  # компактное evidence (ages, key flags, fp stats)
    contrib: Dict[str, float] # вклад фич в score (опционально)
```

### Gate bits (стабильные, для UI/analytics)

```python
BIT_A = 1 << 0  # A: delta_z + weak_progress OR abs_lvl (per cfg)
BIT_B = 1 << 1  # B: sweep + reclaim
BIT_C = 1 << 2  # C: obi_stable or iceberg_strict OR abs_lvl (per cfg)
BIT_D = 1 << 3  # D: abs_lvl_ok (explicit) (optional)

def pack_bits(a: bool, b: bool, c: bool, d: bool = False) -> int:
    """Упаковывает булевы флаги в один int для компактности и стабильности схемы."""
    x = 0
    if a: x |= BIT_A
    if b: x |= BIT_B
    if c: x |= BIT_C
    if d: x |= BIT_D
    return x
```

**Зачем gate_bits:**
- Совместимость replay/output: битовая маска стабильнее строковых флагов
- Стабильность схемы: не нужно менять структуру при добавлении новых гейтов
- Эффективность: один int вместо множества булевых полей

## 3. Evidence-утилиты

### Зачем нужны evidence-утилиты?

**Цель:** Корректное расширение фич/legs/объяснимости. Без них нельзя правильно расширять feature space и обеспечивать explainability.

### Ключевые модули:

1. **`of_evidence.py`**
   - `compute_sweep_recent`: вычисляет, был ли недавний sweep
   - `compute_reclaim_recent`: вычисляет, был ли недавний reclaim
   - `compute_absorption_flags`: вычисляет флаги поглощения

2. **`scenario_v4.py`**
   - `classify_v4`: классифицирует сценарий (reversal/continuation)

3. **`fp_edge_evidence.py`**
   - `compute_fp_edge_absorb`: вычисляет evidence для FP edge absorption

4. **`absorption_level_score.py`**
   - `compute_absorption_level_score`: вычисляет score уровня поглощения

5. **`cfg_merge.py`**
   - `merged_cfg`: объединяет конфигурации (runtime + static)

6. **`strong_need_policy.py`**
   - `compute_strong_need_same_tick`: политика для strong need на том же тике

**Зависимости между модулями:**
- Все evidence-утилиты используются в `of_confirm_engine.py`
- Они обеспечивают детерминированное вычисление фич из runtime состояния
- Без них нельзя корректно воспроизвести решение в replay

## 4. Сервисные гейты/метрики

### CancellationSpikeGate

**Файл:** `python-worker/services/cancellation_spike_gate.py`

**Назначение:** Гейт, который блокирует сигналы при всплеске отмен ордеров (cancellation spike). Это важный компонент veto semantics.

**Почему важно:**
- Участвует в решении (veto/allow)
- Влияет на правильный feature set
- Критичен для veto semantics

### metrics_stage.py

**Файл:** `python-worker/common/metrics_stage.py`

**Ключевые метрики:**
- `veto_total`: общее количество veto
- `dist`: распределение метрик

**Почему важно:**
- Участвует в решении (метрики влияют на score)
- Важно для правильного feature set
- Критично для veto semantics

## 5. Нормализация/режимы/ATR

### Зачем нужны режимы и ATR?

**Цель:** "Мировой" слой режимов + drift по распределениям. Без этого нельзя корректно нормализовать фичи и учитывать рыночные режимы.

### Ключевые модули:

1. **`robust_stats.py`**
   - Rolling median/MAD и другие робастные статистики
   - Защита от выбросов

2. **`regime_service.py`**
   - Определение режима рынка (trend/range/thin/na)
   - ATR quantile для нормализации

3. **`atr_bps_calibrator.py`**
   - Калибровка ATR в базисных пунктах

4. **`atr_tf_calibrator.py`**
   - Калибровка ATR по таймфреймам

5. **`atr_floor_policy.py`**
   - Политика минимального ATR (floor)

**Почему важно:**
- Нормализация фич зависит от режима и ATR
- Drift по распределениям требует робастных статистик
- Без этого нельзя корректно сравнивать фичи между разными режимами

## 6. Модули с ключевыми индикаторами

### Где рождаются ключевые индикаторы до engine?

Для расширения feature space "по-взрослому" нужно понимать, где вычисляются:

- **delta_event.z**: OFI z-score (обычно в `of_evidence.py` или отдельном модуле OFI)
- **obi_stable**: OBI stability flag (в `of_evidence.py`)
- **iceberg события**: Iceberg detection (в `of_evidence.py` или отдельном модуле)
- **spread_bps**: Spread в базисных пунктах (обычно в tick processing)
- **expected_slippage_bps**: Ожидаемый slippage (в calibration или risk модулях)
- **book_age_ms**: Возраст стакана (в tick processing или book monitoring)

**Рекомендация:** Изучить зависимости `of_confirm_engine.py` и найти все модули, которые вычисляют эти индикаторы до вызова engine.

## Общие принципы

### Версионирование контрактов

- Все контракты имеют поле `v: int` для версионирования
- При изменении контракта увеличивается версия
- Старые версии поддерживаются для обратной совместимости

### Детерминизм

- Все inputs должны быть детерминированными
- Config snapshot в inputs для воспроизводимости
- Timestamps в миллисекундах (epoch ms)

### Fail-open семантика

- При ошибках парсинга/валидации система продолжает работу
- Ошибки логируются, но не останавливают pipeline
- Это критично для production reliability

### Explainability

- Каждое решение должно быть объяснимо через evidence
- Gate bits позволяют быстро понять, какие гейты прошли
- Reason codes стабильны и документированы

