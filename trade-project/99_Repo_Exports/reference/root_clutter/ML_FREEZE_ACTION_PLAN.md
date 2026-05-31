# План действий: ML Rollout FREEZE

## Проблема

Guard обнаружил критические проблемы:
- **err_rate: 1.0** (100% ошибок, порог: 1%)
- **lat_p99: 0.172** (172 мс, порог: 6 мс)
- **p50: 0.0** (медиана p_edge = 0, должно быть ≥ 0.20)

Guard предложил снизить `enforce_share` с **1.0** (100%) до **0.05** (5%).

## Шаги диагностики и исправления

### Шаг 1: Комплексная диагностика

Запустите все проверки сразу:

```bash
cd /home/alex/front/trade/scanner_infra
python3 python-worker/tools/ml_diagnose_all.py --window-min 60
```

Это покажет:
1. ✅/❌ Конфигурация ML (model_path, meta_path, enforce_share)
2. 📊 Топ ошибок и их причины
3. ⏱️ Статистика латентности
4. 📋 Список pending предложений guard

### Шаг 2: Детальная диагностика ошибок

Если есть ошибки, посмотрите детали:

```bash
python3 python-worker/tools/ml_diagnose_errors.py --window-min 60 --top-n 20
```

**Типичные причины 100% ошибок:**
- `model_not_loaded` → файл модели не существует или недоступен
- `model_no_predict_proba` → модель не поддерживает нужный метод
- Другие RuntimeError → ошибка при inference

### Шаг 3: Проверка конфигурации

```bash
python3 python-worker/tools/ml_check_config.py
```

**Проверьте:**
- ✅ `model_path` существует и доступен
- ✅ `meta_path` существует и корректен
- ✅ `enforce_share` в диапазоне [0, 1]

### Шаг 4: Анализ латентности

```bash
python3 python-worker/tools/ml_diagnose_latency.py --window-min 60 --threshold-ms 6.0
```

**Если латентность высокая:**
- Проверьте, не связано ли с ошибками (нормально, если ошибки)
- Если высокая без ошибок → оптимизировать модель/инфраструктуру

### Шаг 5: Управление предложением guard

#### 5.1. Посмотреть список предложений

```bash
python3 python-worker/tools/ml_guard_approve.py --action list
```

#### 5.2. Просмотреть детали предложения

```bash
python3 python-worker/tools/ml_guard_approve.py --action preview --bundle-id <ID>
```

#### 5.3. Подтвердить FREEZE (после диагностики)

**ВАЖНО:** Подтверждайте только после того, как:
1. ✅ Поняли причину проблем
2. ✅ Исправили проблемы (если возможно)
3. ✅ Убедились, что FREEZE необходим для безопасности

```bash
python3 python-worker/tools/ml_guard_approve.py --action confirm --bundle-id <ID>
```

#### 5.4. Отклонить предложение (если проблемы уже исправлены)

```bash
python3 python-worker/tools/ml_guard_approve.py --action reject --bundle-id <ID>
```

## Типичные сценарии исправления

### Сценарий A: Модель не загружена

**Симптомы:**
- Топ ошибка: `model_not_loaded`
- `model_path` не существует или недоступен

**Решение:**
1. Проверить `model_path` в Redis:
   ```bash
   redis-cli HGET cfg:ml_confirm model_path
   ```

2. Убедиться, что файл существует:
   ```bash
   ls -lh <model_path>
   ```

3. Если файл не существует:
   - Найти правильный путь к модели
   - Обновить в Redis:
     ```bash
     redis-cli HSET cfg:ml_confirm model_path /correct/path/to/model.joblib
     ```

4. Перезапустить worker для перезагрузки модели

### Сценарий B: Высокая латентность

**Симптомы:**
- `lat_p99: 0.172` (172 мс) > порог 6 мс
- Ошибок нет или мало

**Решение:**
1. Проверить, не связано ли с ошибками (высокая латентность при ошибках - нормально)
2. Если высокая без ошибок:
   - Проверить размер модели (слишком большая?)
   - Проверить нагрузку на CPU
   - Рассмотреть оптимизацию модели или использование более легкой версии

### Сценарий C: Проблемы исправлены, нужно разморозить

**После исправления проблем:**

1. Guard автоматически обнаружит улучшение
2. После **7 дней** хороших метрик guard предложит разморозку
3. Можно вручную разморозить (если уверены):
   ```bash
   redis-cli HSET cfg:ml_confirm enforce_share 1.0
   redis-cli HDEL cfg:ml_confirm freeze_reason
   ```

## Быстрые команды

### Полная диагностика за 1 команду:
```bash
python3 python-worker/tools/ml_diagnose_all.py --window-min 60
```

### Проверка конфигурации:
```bash
python3 python-worker/tools/ml_check_config.py
```

### Список pending предложений:
```bash
python3 python-worker/tools/ml_guard_approve.py --action list
```

### Подтвердить FREEZE (после диагностики):
```bash
python3 python-worker/tools/ml_guard_approve.py --action confirm --bundle-id <ID>
```

## Пороги guard

- **err_rate_max**: 0.01 (1%)
- **lat_p99_max_ms**: 6.0 мс
- **missing_rate_max**: 0.02 (2%)
- **p_edge_p50_min**: 0.20
- **freeze_floor**: 0.05 (минимум enforce_share при freeze)
- **good_days_to_unfreeze**: 7 дней

## После FREEZE

1. ✅ Guard снизит `enforce_share` до 0.05 (5% трафика)
2. ✅ Продолжит мониторить метрики
3. ✅ После 7 дней хороших метрик предложит разморозку
4. ✅ Можно вручную разморозить после исправления проблем

## Документация

Подробная документация: `python-worker/tools/ML_DIAGNOSTICS_README.md`

