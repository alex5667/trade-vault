# ML Diagnostics Tools

Набор инструментов для диагностики проблем ML rollout.

## Проблема: ML rollout FREEZE

Когда guard обнаруживает проблемы (100% ошибок, высокая латентность), он предлагает снизить `enforce_share` с 100% до 5%.

### Метрики из сообщения:
```json
{
  "cur_share": 1.0,
  "new_share": 0.05,
  "metrics": {
    "n": 2407.0,
    "p50": 0.0,
    "p10": 0.0,
    "lat_p99": 0.172,
    "err_rate": 1.0,
    "missing_rate": 0.0
  }
}
```

**Проблемы:**
- `err_rate: 1.0` = 100% ошибок (порог: 1%)
- `lat_p99: 0.172` = 172 мс (порог: 6 мс)
- `p50: 0.0` = медиана p_edge = 0 (плохо, должно быть ≥ 0.20)

## Инструменты

### 1. `ml_diagnose_all.py` - Комплексная диагностика

Запускает все проверки последовательно:

```bash
python python-worker/tools/ml_diagnose_all.py --window-min 60
```

Опции:
- `--redis-url` - URL Redis (по умолчанию из REDIS_URL)
- `--window-min` - окно анализа в минутах (по умолчанию 60)
- `--skip-config` - пропустить проверку конфигурации
- `--skip-errors` - пропустить диагностику ошибок
- `--skip-latency` - пропустить диагностику латентности
- `--skip-guard` - пропустить список guard предложений

### 2. `ml_check_config.py` - Проверка конфигурации

Проверяет:
- Существование `model_path` и `meta_path`
- Корректность `enforce_share`
- Режим работы (OFF/SHADOW/ENFORCE)
- Fail policy
- Canary настройки

```bash
python python-worker/tools/ml_check_config.py
```

**Типичные проблемы:**
- ❌ `model_path` не существует → модель не загружена → 100% ошибок
- ❌ `meta_path` не существует → метаданные не загружены
- ⚠️ `enforce_share` вне диапазона [0,1]

### 3. `ml_diagnose_errors.py` - Диагностика ошибок

Анализирует ошибки из `metrics:ml_confirm` stream:
- Топ ошибок с частотами
- Распределение по символам/сценариям
- Временные паттерны
- Детали последних ошибок

```bash
python python-worker/tools/ml_diagnose_errors.py --window-min 60 --top-n 20
```

**Типичные ошибки:**
- `model_not_loaded` → модель не загружена (проверить `model_path`)
- `model_no_predict_proba` → модель не поддерживает `predict_proba`
- `RuntimeError: ...` → ошибка при inference

### 4. `ml_diagnose_latency.py` - Диагностика латентности

Анализирует латентность из `metrics:ml_confirm` stream:
- Перцентили (p50/p95/p99)
- Распределение по символам/сценариям
- Сравнение с/без ошибок
- Временные паттерны

```bash
python python-worker/tools/ml_diagnose_latency.py --window-min 60 --threshold-ms 6.0
```

**Типичные проблемы:**
- p99 > 6 мс → превышен порог
- Высокая латентность при ошибках → модель не загружена, быстрый fail

### 5. `ml_guard_approve.py` - Управление предложениями guard

Управление предложениями ML rollout guard:
- `list` - список pending предложений
- `preview` - просмотр изменений
- `confirm` - подтвердить изменения
- `reject` - отклонить изменения

```bash
# Список pending предложений
python python-worker/tools/ml_guard_approve.py --action list

# Просмотр изменений
python python-worker/tools/ml_guard_approve.py --action preview --bundle-id <ID>

# Подтвердить FREEZE (снизить enforce_share до 0.05)
python python-worker/tools/ml_guard_approve.py --action confirm --bundle-id <ID>

# Отклонить предложение
python python-worker/tools/ml_guard_approve.py --action reject --bundle-id <ID>
```

## Типичные сценарии

### Сценарий 1: 100% ошибок, модель не загружена

**Симптомы:**
- `err_rate: 1.0`
- Топ ошибка: `model_not_loaded`

**Диагностика:**
```bash
# 1. Проверить конфигурацию
python python-worker/tools/ml_check_config.py

# 2. Проверить ошибки
python python-worker/tools/ml_diagnose_errors.py
```

**Решение:**
1. Проверить `model_path` в Redis `cfg:ml_confirm`
2. Убедиться, что файл существует и доступен
3. Проверить права доступа
4. Перезапустить worker после исправления

### Сценарий 2: Высокая латентность

**Симптомы:**
- `lat_p99: 0.172` (172 мс) > порог 6 мс

**Диагностика:**
```bash
python python-worker/tools/ml_diagnose_latency.py --window-min 60
```

**Решение:**
1. Проверить, не связано ли с ошибками (высокая латентность при ошибках - нормально)
2. Если латентность высокая без ошибок:
   - Проверить размер модели
   - Проверить нагрузку на CPU
   - Оптимизировать модель или использовать более легкую версию

### Сценарий 3: Подтверждение FREEZE предложения

**Симптомы:**
- Guard предложил снизить `enforce_share` с 1.0 до 0.05

**Действия:**
```bash
# 1. Посмотреть список предложений
python python-worker/tools/ml_guard_approve.py --action list

# 2. Просмотреть детали
python python-worker/tools/ml_guard_approve.py --action preview --bundle-id <ID>

# 3. После исправления проблем - подтвердить FREEZE
python python-worker/tools/ml_guard_approve.py --action confirm --bundle-id <ID>
```

**После исправления:**
- Guard автоматически предложит разморозку после 7 дней хороших метрик
- Можно вручную разморозить через изменение `enforce_share` в Redis

## Пороги (из кода)

```python
ML_SRE_PEDGE_P50_MIN = 0.20      # медиана p_edge должна быть ≥ 0.20
ML_SRE_MISSING_RATE_MAX = 0.02   # missing rate ≤ 2%
ML_SRE_ERR_RATE_MAX = 0.01       # error rate ≤ 1%
ML_SRE_LAT_P99_MAX_MS = 6.0      # p99 латентность ≤ 6 мс
ML_ROLLOUT_FREEZE_FLOOR = 0.05   # минимум enforce_share при freeze
ML_ROLLOUT_GOOD_DAYS_TO_UNFREEZE = 7  # дней хороших метрик для разморозки
```

## Автоматизация

### Запуск всех диагностик:
```bash
python python-worker/tools/ml_diagnose_all.py --window-min 60
```

### Автоматическое подтверждение FREEZE:
```bash
python python-worker/tools/ml_guard_approve.py --action auto-confirm
```

⚠️ **Внимание:** Автоматическое подтверждение применяет все pending FREEZE предложения без проверки!

## Мониторинг

Guard запускается по расписанию (обычно каждые 10-60 минут) и проверяет метрики за последние 60 минут.

После исправления проблем:
1. Guard автоматически обнаружит улучшение
2. После 7 дней хороших метрик предложит разморозку
3. Можно вручную разморозить через изменение `enforce_share` в Redis

## Полезные команды Redis

```bash
# Проверить конфигурацию
redis-cli HGETALL cfg:ml_confirm

# Проверить текущий enforce_share
redis-cli HGET cfg:ml_confirm enforce_share

# Вручную изменить enforce_share (после исправления проблем)
redis-cli HSET cfg:ml_confirm enforce_share 1.0

# Проверить размер stream
redis-cli XLEN metrics:ml_confirm

# Посмотреть последние метрики
redis-cli XREVRANGE metrics:ml_confirm + - COUNT 10
```

