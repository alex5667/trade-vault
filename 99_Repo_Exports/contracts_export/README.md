# Contracts Export - Контракты и утилиты для ML/Replay

Эта папка содержит экспортированные контракты, утилиты и документацию, необходимые для полноценного replay/train по реальным inputs.

## Структура

```
contracts_export/
├── README.md                          # Этот файл
├── ANSWERS.md                         # Ответы на вопросы о контрактах
├── NDJSON_EXAMPLES.md                 # Примеры NDJSON строк для проверки консистентности
└── python-worker/
    ├── core/
    │   ├── of_inputs_contract.py      # Контракт OFInputsV1
    │   ├── of_confirm_contract.py     # Контракт OFConfirmV3 + pack_bits
    │   ├── of_evidence.py             # Evidence-утилиты (sweep, reclaim, absorption)
    │   ├── scenario_v4.py            # classify_v4
    │   ├── fp_edge_evidence.py       # compute_fp_edge_absorb
    │   ├── absorption_level_score.py  # compute_absorption_level_score
    │   ├── cfg_merge.py              # merged_cfg
    │   ├── strong_need_policy.py     # compute_strong_need_same_tick
    │   ├── robust_stats.py           # Rolling median/MAD
    │   ├── atr_bps_calibrator.py     # ATR calibration (bps)
    │   ├── atr_tf_calibrator.py      # ATR calibration (timeframe)
    │   ├── atr_floor_policy.py       # ATR floor policy
    │   └── ndjson_utils.py           # NDJSON utilities
    ├── tools/
    │   ├── export_of_inputs_ndjson_v2.py  # Экспорт OFInputsV1 из Redis Stream
    │   ├── of_engine_replay_from_inputs.py # Replay engine из inputs
    │   └── export_trade_closed_ndjson.py  # Экспорт закрытых трейдов
    ├── services/
    │   └── cancellation_spike_gate.py     # CancellationSpikeGate
    ├── common/
    │   ├── metrics_stage.py               # veto_total, dist
    │   └── robust_stats.py                # Rolling median/MAD (альтернативная версия)
    └── regime_service.py                  # Режимы/ATR quantile
```

## Назначение компонентов

### 1. Контракты входов и экспорт (P1_TRAINING_MODEL.md)

**OFInputsV1** (`core/of_inputs_contract.py`):
- Минимальный, детерминированный набор входных данных для replay/train
- Версионированный контракт (`v: int`)
- Снапшот конфигурации для детерминизма

**Экспорт** (`tools/export_of_inputs_ndjson_v2.py`):
- Экспорт OFInputsV1 из Redis Stream `signals:of:inputs`
- Поддержка resume через state file
- Робастный парсинг (bytes/str, валидация JSON)

### 2. Контракт OFConfirm и gate_bits

**OFConfirmV3** (`core/of_confirm_contract.py`):
- Стабильный контракт для встраивания в raw signals
- Версионированный и интроспектируемый
- `pack_bits()` для упаковки булевых флагов в int (стабильность схемы)

### 3. Evidence-утилиты

Все модули в `core/`, которые вычисляют evidence:
- `of_evidence.py`: sweep_recent, reclaim_recent, absorption_flags
- `scenario_v4.py`: classify_v4
- `fp_edge_evidence.py`: compute_fp_edge_absorb
- `absorption_level_score.py`: compute_absorption_level_score
- `cfg_merge.py`: merged_cfg
- `strong_need_policy.py`: compute_strong_need_same_tick

**Зачем:** Без них нельзя корректно расширять фичи/legs/объяснимость.

### 4. Сервисные гейты/метрики

**CancellationSpikeGate** (`services/cancellation_spike_gate.py`):
- Гейт для блокировки сигналов при всплеске отмен ордеров
- Участвует в veto semantics

**metrics_stage.py** (`common/metrics_stage.py`):
- Метрики `veto_total`, `dist`
- Участвуют в решении (score calculation)

### 5. Нормализация/режимы/ATR

**robust_stats.py**: Rolling median/MAD, защита от выбросов

**regime_service.py**: Определение режима рынка (trend/range/thin/na)

**atr_*.py**: Калибровка ATR (bps, timeframe, floor policy)

**Зачем:** "Мировой" слой режимов + drift по распределениям. Без этого нельзя корректно нормализовать фичи.

### 6. Дополнительные утилиты

**ndjson_utils.py**: Утилиты для чтения/парсинга NDJSON

**export_trade_closed_ndjson.py**: Экспорт закрытых трейдов из `events:trades`

**of_engine_replay_from_inputs.py**: Детерминированный replay OFConfirmEngine.build() из inputs

## Использование

### Экспорт inputs из Redis

```bash
cd python-worker
python -m tools.export_of_inputs_ndjson_v2 \
    --redis-url redis://localhost:6379/0 \
    --stream signals:of:inputs \
    --field payload \
    --out /tmp/of_inputs.ndjson \
    --start-id 0-0 \
    --batch 2000
```

### Replay из inputs

```bash
cd python-worker
python -m tools.of_engine_replay_from_inputs \
    --inputs /tmp/of_inputs.ndjson \
    --out /tmp/replay.ndjson \
    --tf 1s
```

### Экспорт закрытых трейдов

```bash
cd python-worker
python tools/export_trade_closed_ndjson.py \
    --since-hours 168 \
    --out /tmp/closed_7d.ndjson
```

## Проверка консистентности

См. `NDJSON_EXAMPLES.md` для примеров NDJSON строк и скрипта проверки консистентности между:
- `stream:of:inputs` (OFInputsV1)
- `replay output` (OFConfirmV3)
- `events:trades` (trade events)

**Ключевые поля для проверки:**
- `sid` / `signal_id`: должен совпадать во всех трех источниках
- `ts_ms`: должен быть близок (в пределах 1 секунды)
- `symbol`, `direction`: должны совпадать
- Фичи и лейблы: должны соответствовать между inputs и replay output

## Зависимости

Все файлы используют стандартную библиотеку Python и следующие внешние зависимости:

- `redis` (redis-py): для работы с Redis Streams
- `dataclasses`: для контрактов (встроено в Python 3.7+)
- `typing`: для type hints (встроено в Python 3.5+)

## Версионирование

Все контракты версионированы:
- `OFInputsV1`: `v: int = 1`
- `OFConfirmV3`: `v: int = 3`

При изменении контракта увеличивается версия, старые версии поддерживаются для обратной совместимости.

## Детерминизм

Для обеспечения детерминизма:
1. Все inputs должны быть детерминированными значениями (не потоки данных)
2. Config snapshot в inputs для воспроизводимости
3. Timestamps в миллисекундах (epoch ms)
4. Стабильный порядок обработки (сортировка по `(ts_ms, symbol, direction)`)

## Fail-open семантика

При ошибках парсинга/валидации:
- Система продолжает работу (fail-open)
- Ошибки логируются, но не останавливают pipeline
- Критично для production reliability

## Explainability

Каждое решение объяснимо через:
- `evidence`: компактное evidence (ages, key flags, fp stats)
- `gate_bits`: битовая маска пройденных гейтов
- `reason`: стабильный reason code для veto/allow
- `contrib`: вклад фич в score (опционально)
- `legs_detail`: детали по каждому leg (A, B, C, D)

## Дополнительная документация

- `ANSWERS.md`: Подробные ответы на вопросы о контрактах и архитектуре
- `NDJSON_EXAMPLES.md`: Примеры NDJSON строк и скрипт проверки консистентности

## Примечания

- Некоторые файлы могут иметь дубликаты в разных местах исходного проекта (например, `robust_stats.py` в `core/` и `common/`)
- Выбраны версии из `python-worker/` как основные
- Все пути относительны к корню проекта `scanner_infra/`

