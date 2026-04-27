# ML Confirm Gate Diagnostics Report

## Дата: 2026-02-03

## Проблема
ML SRE алерт показывает 100% ошибок `no_cfg` (1699/1699 запросов) за последние 10 минут.

## Результаты проверок

### 1. Конфигурация в Redis ✅
- **Ключ champion**: `cfg:ml_confirm:champion` - **СУЩЕСТВУЕТ**
- **Ключ challenger**: `cfg:ml_confirm:challenger` - отсутствует (нормально)
- **Конфигурация валидна**:
  ```json
  {
    "version": 1,
    "kind": "util_mh",
    "model_path": "/var/lib/trade/of_reports/models/model.joblib",
    "p_min": 0.55,
    "abstain_band": 0.02
  }
  ```
- **TTL**: -1 (без срока истечения)

### 2. Подключение к Redis ✅
- **REDIS_URL**: `redis://redis-worker-1:6379/0`
- **Статус**: Подключение работает (PONG)
- **Доступность конфигурации**: Worker может читать конфигурацию

### 3. Переменные окружения ✅
- `REDIS_URL=redis://redis-worker-1:6379/0`
- `ML_CFG_CHAMPION_KEY=cfg:ml_confirm:champion` (по умолчанию)
- `ML_MODEL_CACHE_TTL_MS=60000` (60 секунд)
- `ML_CONFIRM_MODE=SHADOW`

### 4. Файл модели ✅
- **Путь**: `/var/lib/trade/of_reports/models/model.joblib`
- **Размер**: 6148 байт (6.1 KB)
- **Последнее обновление**: 2026-02-01 11:23:41
- **Права доступа**: `-rw-r--r--` (root:root)

### 5. Формат модели ❌ **ПРОБЛЕМА**

**Ожидается**: `UtilMHModelV1` (dataclass с методами `predict_util` и `predict_unc`)

**Фактически**: `CalibratedClassifierCV` (sklearn) - **НЕСОВМЕСТИМЫЙ ФОРМАТ**

**Ошибка при использовании**:
```
AttributeError: 'CalibratedClassifierCV' object has no attribute 'predict_util'
```

## Причина ошибки `no_cfg`

При загрузке модели возникает исключение `AttributeError`, которое не обрабатывалось в коде. Это приводило к тому, что:
1. Конфигурация загружается успешно
2. При попытке использовать модель возникает ошибка
3. Gate возвращает `ERR_NO_CFG` из-за отсутствия валидной модели

## Внесенные исправления

### Улучшена обработка ошибок загрузки модели

**Файл**: `python-worker/services/ml_confirm_gate.py`

**Изменения**:
1. Добавлен `try/except` блок при загрузке модели
2. Добавлена валидация наличия методов `predict_util` и `predict_unc` для моделей типа `util_mh`
3. Добавлено логирование ошибок с полным traceback
4. При ошибке загрузки возвращается конфигурация без модели (gate обработает это корректно)

**Код**:
```python
try:
    model = joblib.load(model_path)
    # Validate model has required methods for util_mh
    if cfg.get("kind", "").lower().startswith("util_mh"):
        if not hasattr(model, "predict_util") or not hasattr(model, "predict_unc"):
            logger.error(
                f"Model at {model_path} is missing required methods (predict_util/predict_unc). "
                f"Model type: {type(model).__name__}, module: {type(model).__module__}. "
                f"Expected UtilMHModelV1 format."
            )
            return cfg, None
    return cfg, model
except Exception as e:
    logger.error(f"Failed to load model from {model_path}: {e}", exc_info=True)
    return cfg, None
```

## Рекомендации

### Немедленные действия

1. **Проверить логи worker'а** на наличие ошибок загрузки модели:
   ```bash
   docker logs <worker-container> | grep -i "ml_confirm_gate\|predict_util\|AttributeError"
   ```

2. **Обновить модель** до правильного формата:
   - Использовать скрипт `train_ml_confirm_tb_util_mh_v1.py` для обучения модели
   - Сохранить модель в формате `UtilMHModelV1`
   - Обновить файл `/var/lib/trade/of_reports/models/model.joblib`

3. **Временное решение** (если нужно быстро восстановить работу):
   - Установить `ML_CONFIRM_MODE=OFF` для отключения ML gate
   - Или установить `ML_CONFIRM_MODE=SHADOW` (уже установлено) - gate будет работать, но не блокировать сделки

### Долгосрочные действия

1. **Добавить валидацию модели при сохранении**:
   - Проверять формат модели перед сохранением в Redis конфигурацию
   - Добавить версионирование моделей

2. **Улучшить мониторинг**:
   - Добавить метрику `model_load_errors` в ML SRE мониторинг
   - Добавить алерт на несовместимость формата модели

3. **Документировать формат модели**:
   - Создать спецификацию ожидаемого формата модели
   - Добавить примеры валидных моделей

## Статус

- ✅ Конфигурация в Redis - OK
- ✅ Подключение к Redis - OK  
- ✅ Переменные окружения - OK
- ✅ Файл модели существует - OK
- ❌ **Формат модели - ОШИБКА** (несовместимый формат)
- ✅ Код обработки ошибок - ИСПРАВЛЕНО

## Следующие шаги

1. Обновить модель до формата `UtilMHModelV1`
2. Проверить логи после обновления кода
3. Мониторить метрики ML SRE после исправления

