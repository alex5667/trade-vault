# Файлы для вычисления полей `ok` и `ok_soft`

Эта папка содержит все файлы, связанные с формированием и вычислением полей `ok` и `ok_soft` в системе orderflow.

## Основные файлы

### 1. `core/of_confirm_engine.py` - Основной движок вычисления

**Роль:** Главный файл, который вычисляет поля `ok` и `ok_soft`.

**Ключевые методы:**
- `build()`: Основной метод, который вычисляет:
  - `ok`: 1/0 - прошел ли сигнал жесткие проверки (Enforce)
  - `ok_soft`: 1/0 - прошел ли сигнал по "мягкому" критерию (не хватило 1 подтверждения, но качество выше порога)
  - `have`: количество подтверждений ("ног")
  - `need`: необходимое количество подтверждений
  - `score`: качественная оценка (0..1)

**Логика вычисления `ok`:**
- Проверяет количество подтверждений: `have >= need`
- Проверяет минимальный score: `score >= score_min`
- Применяет жесткие вето (cancel spike, ML gate, meta model)
- Результат: `ok = 1` если все проверки пройдены, иначе `ok = 0`

**Логика вычисления `ok_soft` (строки 986-1007):**
- Условия для `ok_soft = 1`:
  - `ok == 0` (не прошел жесткие проверки)
  - `have == need - 1` (не хватило 1 подтверждения)
  - `score >= soft_score_min` (по умолчанию 0.60, можно задать через `OF_SOFT_SCORE_MIN` или `cfg.soft_score_min`)
  - `exec_risk_norm <= soft_exec_max` (по умолчанию 0.65, можно задать через `OF_SOFT_EXEC_RISK_NORM_MAX` или `cfg.soft_exec_risk_norm_max`)

**Дополнительная логика для `range_meanrev` сценария (строки 826-861):**
- Если `have == need - 1` и `score >= range_soft_score_min` (по умолчанию 0.72) и `exec_risk_norm <= range_soft_exec_risk_norm_max` (по умолчанию 0.60)
- Тогда устанавливается `ok_soft = 1` и `range_ok_soft = 1` в indicators

### 2. `services/orderflow/strategy.py` - Использование и запись в метрики

**Роль:** Использует `OFConfirmEngine` и записывает результаты (`ok` и `ok_soft`) в Redis Stream `metrics:of_gate`.

**Ключевые места:**
- Строка 108: Импорт `OFConfirmEngine`
- Строки 1774-1852: Использование результатов `ofc` и запись в метрики
- Строка 1811: Запись `ok` в метрики
- Строка 1812: Запись `ok_soft` в метрики
- Строка 1892: Запись `of_confirm_ok_soft` в indicators
- Строки 1944-1948: Использование `ok_soft` для виртуальных сигналов

## Зависимости

### Core модули:
- `book_evidence.py` - вычисление OBI/Iceberg/OFI флагов
- `meta_model_lr.py` - мета-модель логистической регрессии
- `of_evidence.py` - вычисление sweep/reclaim/absorption
- `strong_of_gate.py` - логика Strong Gate (reversal/continuation)
- `absorption_level_score.py` - оценка поглощения на уровне
- `of_confirm_contract.py` - контракты данных (OFConfirmV3)
- `cfg_merge.py` - слияние конфигураций
- `strong_need_policy.py` - политика Strong Need
- `fp_edge_evidence.py` - FP edge evidence
- `scenario_v4.py` - классификация сценариев v4
- `compat_utils.py` - утилиты совместимости
- `instrument_config.py` - конфигурация инструментов

### Services:
- `cancellation_spike_gate.py` - детектор отмены спайков (может установить `ok=0`)
- `ml_confirm_gate.py` - ML фильтр (может установить `ok=0` в режиме ENFORCE)

### Common:
- `metrics_stage.py` - метрики и распределения

## Структура каталога

```
ok_ok_soft_services/
├── README.md                          # Этот файл
├── core/                              # Ядро логики
│   ├── of_confirm_engine.py          # OFConfirmEngine (главный файл)
│   ├── book_evidence.py               # OBI/Iceberg/OFI evidence
│   ├── meta_model_lr.py              # Мета-модель
│   ├── of_evidence.py                # Sweep/Reclaim/Absorption
│   ├── strong_of_gate.py             # Strong Gate логика
│   ├── absorption_level_score.py     # Absorption на уровне
│   ├── of_confirm_contract.py        # Контракты данных
│   ├── cfg_merge.py                  # Слияние конфигураций
│   ├── strong_need_policy.py         # Политика Strong Need
│   ├── fp_edge_evidence.py           # FP edge evidence
│   ├── scenario_v4.py                # Классификация сценариев
│   ├── compat_utils.py               # Утилиты совместимости
│   └── instrument_config.py          # Конфигурация инструментов
├── services/                          # Сервисы
│   ├── cancellation_spike_gate.py   # Детектор отмены спайков
│   ├── ml_confirm_gate.py            # ML фильтр
│   └── orderflow/
│       └── strategy.py               # Стратегия (использует engine)
└── common/                            # Общие модули
    └── metrics_stage.py              # Метрики
```

## Формулы

### ok
```
ok = 1 если:
  - have >= need
  - score >= score_min
  - не заблокирован cancel_spike_gate
  - не заблокирован ml_confirm_gate (в режиме ENFORCE)
  - не заблокирован meta_model (в режиме ENFORCE)
  - не применены жесткие вето (vol_shock, saw_chop и т.д.)
иначе ok = 0
```

### ok_soft
```
ok_soft = 1 если:
  - ok == 0 (не прошел жесткие проверки)
  - have == need - 1 (не хватило 1 подтверждения)
  - score >= soft_score_min (по умолчанию 0.60)
  - exec_risk_norm <= soft_exec_max (по умолчанию 0.65)
иначе ok_soft = 0
```

## Переменные окружения

- `OF_SOFT_SCORE_MIN` - минимальный score для ok_soft (по умолчанию 0.60)
- `OF_SOFT_EXEC_RISK_NORM_MAX` - максимальный exec_risk_norm для ok_soft (по умолчанию 0.65)
- `META_MODEL_ENABLE` - включить мета-модель (0/1)
- `META_MODEL_MODE` - режим мета-модели (SHADOW/ENFORCE)
- `META_P_MIN` - минимальный p для мета-модели (по умолчанию 0.55)

## Конфигурационные параметры

- `soft_score_min` - минимальный score для ok_soft
- `soft_exec_risk_norm_max` - максимальный exec_risk_norm для ok_soft
- `range_soft_score_min` - минимальный score для ok_soft в range_meanrev (по умолчанию 0.72)
- `range_soft_exec_risk_norm_max` - максимальный exec_risk_norm для ok_soft в range_meanrev (по умолчанию 0.60)
- `of_score_min` - минимальный score для ok (по умолчанию 0.65)

## Где используются ok и ok_soft

1. **Redis Stream `metrics:of_gate`** - записываются стратегией для мониторинга SRE
2. **Indicators** - `of_confirm_ok`, `of_confirm_ok_soft` доступны в indicators для downstream
3. **Виртуальные сигналы** - `ok_soft=1` может использоваться для создания виртуальных сигналов (строка 1948 в strategy.py)

