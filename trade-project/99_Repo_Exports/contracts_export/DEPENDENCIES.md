# Зависимости между модулями

## Иерархия зависимостей

### Уровень 1: Контракты (базовый уровень, без зависимостей от других модулей проекта)

- `core/of_inputs_contract.py` - OFInputsV1 (только dataclasses, typing)
- `core/of_confirm_contract.py` - OFConfirmV3, pack_bits (только dataclasses, typing)

### Уровень 2: Evidence-утилиты (зависят только от стандартной библиотеки)

- `core/of_evidence.py` - compute_sweep_recent, compute_reclaim_recent, compute_absorption_flags
- `core/scenario_v4.py` - classify_v4
- `core/fp_edge_evidence.py` - compute_fp_edge_absorb
- `core/absorption_level_score.py` - compute_absorption_level_score
- `core/strong_need_policy.py` - compute_strong_need_same_tick
- `core/cfg_merge.py` - merged_cfg

### Уровень 3: Статистика и нормализация

- `core/robust_stats.py` - Rolling median/MAD (может использоваться evidence-утилитами)
- `common/robust_stats.py` - Альтернативная версия (проверить, какая используется)

### Уровень 4: Режимы и ATR

- `regime_service.py` - Режимы/ATR quantile (может зависеть от robust_stats)
- `core/atr_bps_calibrator.py` - ATR calibration (bps)
- `core/atr_tf_calibrator.py` - ATR calibration (timeframe)
- `core/atr_floor_policy.py` - ATR floor policy

### Уровень 5: Сервисы и метрики

- `services/cancellation_spike_gate.py` - CancellationSpikeGate (может зависеть от evidence-утилит)
- `common/metrics_stage.py` - veto_total, dist (может зависеть от evidence-утилит)

### Уровень 6: Инструменты (зависят от контрактов и утилит)

- `tools/export_of_inputs_ndjson_v2.py` - Экспорт OFInputsV1 (зависит от of_inputs_contract.py, redis)
- `tools/of_engine_replay_from_inputs.py` - Replay engine (зависит от of_inputs_contract.py, of_confirm_contract.py, of_confirm_engine.py)
- `tools/export_trade_closed_ndjson.py` - Экспорт трейдов (зависит от redis)
- `core/ndjson_utils.py` - NDJSON utilities (только стандартная библиотека)

## Внешние зависимости

### Обязательные

- `redis` (redis-py): для работы с Redis Streams
  - Используется в: `export_of_inputs_ndjson_v2.py`, `export_trade_closed_ndjson.py`

### Стандартная библиотека Python

- `dataclasses`: для контрактов (Python 3.7+)
- `typing`: для type hints (Python 3.5+)
- `json`: для работы с JSON
- `argparse`: для CLI инструментов
- `pathlib`: для работы с путями
- `collections`: для deque и других структур данных
- `math`: для математических операций
- `statistics`: для статистических функций
- `time`: для работы со временем
- `os`: для работы с ОС

## Зависимости от других модулей проекта (не включены в экспорт)

### Модули, которые могут понадобиться для полного replay:

1. **`core/of_confirm_engine.py`** - Основной engine (используется в `of_engine_replay_from_inputs.py`)
   - Зависит от всех evidence-утилит
   - Зависит от контрактов
   - Зависит от сервисов (cancellation_spike_gate, metrics_stage)

2. **`core/strong_of_gate.py`** - Strong gate logic (может использоваться в replay)
   - Зависит от evidence-утилит
   - Зависит от контрактов

3. **`core/redis_client.py`** - Redis client utilities
   - Используется в export инструментах

4. **Runtime объекты** - Могут понадобиться для replay:
   - `core/runtime.py` или аналогичный модуль
   - Используется в `of_engine_replay_from_inputs.py` (создается RuntimeStub)

## Рекомендации по использованию

### Для минимального replay (только контракты):

```python
from core.of_inputs_contract import OFInputsV1
from core.of_confirm_contract import OFConfirmV3, pack_bits
```

### Для replay с evidence:

```python
from core.of_inputs_contract import OFInputsV1
from core.of_confirm_contract import OFConfirmV3
from core.of_evidence import compute_sweep_recent, compute_reclaim_recent
from core.scenario_v4 import classify_v4
# ... и другие evidence-утилиты
```

### Для полного replay (требуется of_confirm_engine.py):

```python
# Используйте of_engine_replay_from_inputs.py как пример
# Требуется of_confirm_engine.py (не включен в экспорт)
```

## Проверка зависимостей

Для проверки, что все зависимости доступны:

```python
import sys
import importlib

REQUIRED_MODULES = [
    'dataclasses',
    'typing',
    'json',
    'argparse',
    'pathlib',
    'collections',
    'math',
    'statistics',
    'time',
    'os',
]

EXTERNAL_MODULES = [
    'redis',
]

print("Проверка стандартной библиотеки:")
for mod in REQUIRED_MODULES:
    try:
        importlib.import_module(mod)
        print(f"  ✓ {mod}")
    except ImportError:
        print(f"  ✗ {mod} - НЕ НАЙДЕН")

print("\nПроверка внешних зависимостей:")
for mod in EXTERNAL_MODULES:
    try:
        importlib.import_module(mod)
        print(f"  ✓ {mod}")
    except ImportError:
        print(f"  ✗ {mod} - НЕ НАЙДЕН (установите: pip install {mod})")
```

## Установка зависимостей

```bash
pip install redis
```

Или через requirements.txt:

```
redis>=4.0.0
```

## Примечания

- Все модули в `core/` и `common/` должны быть доступны через `PYTHONPATH`
- При использовании в другом проекте, убедитесь, что структура папок сохранена или импорты адаптированы
- Некоторые модули могут иметь циклические зависимости в исходном проекте - в экспорте они разорваны через контракты

