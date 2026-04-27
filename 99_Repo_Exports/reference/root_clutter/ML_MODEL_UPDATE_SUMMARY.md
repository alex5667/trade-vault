# Итоговый отчет: Обновление ML модели до формата UtilMHModelV1

## Выполненные задачи ✅

### 1. ✅ Создана обертка для обучения модели
**Файл**: `update_ml_model.sh` (новый)

**Примечание**: 
- Скрипт обучения `ml_train_report_tb_v10_4.py` уже существовал в проекте (создан 2026-02-01)
- **Таймер для автоматического обучения уже настроен**: `ml-train-tb-v10-4-timer` в `docker-compose-timers.yml`
  - Запускается **ежедневно в 03:10-03:20**
  - Автоматически вызывает `python3 -m tools.ml_train_report_tb_v10_4`
  - Сохраняет модель в `challenger` конфигурацию в Redis

Обертка `update_ml_model.sh` добавляет ручной запуск с дополнительными шагами:
- Вызывает существующий скрипт `ml_train_report_tb_v10_4.py` для обучения
- Валидирует формат модели после обучения
- Копирует модель в production location (`/var/lib/trade/of_reports/models/model.joblib`)
- Создает backup старой модели перед заменой

**Использование:**
```bash
./update_ml_model.sh [since-hours] [models-root]
```

### 2. ✅ Создан скрипт для проверки статуса
**Файл**: `check_ml_confirm_status.sh`

Скрипт проверяет:
- Конфигурацию в Redis (champion/challenger)
- Формат модели
- Логи worker'а
- Метрики ML SRE

**Использование:**
```bash
./check_ml_confirm_status.sh [container-name]
```

### 3. ✅ Улучшена обработка ошибок в коде
**Файл**: `python-worker/services/ml_confirm_gate.py`

Добавлено:
- Обработка ошибок при загрузке модели
- Валидация наличия методов `predict_util` и `predict_unc`
- Логирование ошибок с полным traceback

### 4. ✅ Создана документация
**Файлы**:
- `ML_MODEL_UPDATE_INSTRUCTIONS.md` - подробная инструкция
- `ML_CONFIRM_DIAGNOSTICS_REPORT.md` - отчет о диагностике
- `ML_MODEL_UPDATE_SUMMARY.md` - этот файл

## Следующие шаги

### Шаг 1: Обучение модели

Запустите обучение модели (требуются данные в Redis):

```bash
cd python-worker
python3 -m tools.ml_train_report_tb_v10_4 \
    --since-hours 72 \
    --models-root /var/lib/trade/ml_models \
    --horizons 60000,180000,300000
```

**Или используйте автоматизированный скрипт:**
```bash
./update_ml_model.sh 72 /var/lib/trade/ml_models
```

### Шаг 2: Обновление конфигурации в Redis

После обучения модель будет сохранена в `challenger`. Обновите `champion`:

```bash
# В контейнере worker или с доступом к Redis
python3 <<EOF
import redis
import json

r = redis.Redis.from_url("redis://redis-worker-1:6379/0", decode_responses=True)

challenger_key = "cfg:ml_confirm:challenger"
champion_key = "cfg:ml_confirm:champion"

challenger_raw = r.get(challenger_key)
if challenger_raw:
    challenger = json.loads(challenger_raw)
    champion = challenger.copy()
    champion["model_path"] = "/var/lib/trade/of_reports/models/model.joblib"
    r.set(champion_key, json.dumps(champion, ensure_ascii=False, separators=(",", ":")))
    print("✅ Champion updated")
EOF
```

### Шаг 3: Копирование модели

Скопируйте обученную модель в production location:

```bash
# Найти последнюю модель
LATEST=$(find /var/lib/trade/ml_models -name "model.joblib" -type f -printf '%T@ %p\n' | sort -n | tail -1 | cut -d' ' -f2-)

# Backup старой модели
sudo cp /var/lib/trade/of_reports/models/model.joblib \
       /var/lib/trade/of_reports/models/model.joblib.backup.$(date +%Y%m%d_%H%M%S)

# Копировать новую модель
sudo cp "$LATEST" /var/lib/trade/of_reports/models/model.joblib
```

### Шаг 4: Перезапуск worker

```bash
# Найти и перезапустить worker
WORKER=$(docker ps --format "{{.Names}}" | grep -E "python.*worker|of.*confirm" | head -1)
docker restart "$WORKER"
```

### Шаг 5: Проверка логов

```bash
WORKER=$(docker ps --format "{{.Names}}" | grep -E "python.*worker|of.*confirm" | head -1)
docker logs "$WORKER" --tail 100 | grep -i "ml_confirm\|predict_util\|no_cfg"
```

**Ожидаемый результат:**
- ✅ Нет ошибок `AttributeError`
- ✅ Нет ошибок `no_cfg`
- ✅ Модель загружается успешно

### Шаг 6: Проверка метрик ML SRE

```bash
./check_ml_confirm_status.sh
```

**Или вручную через Redis:**

```bash
docker exec redis-worker-1 redis-cli XREVRANGE metrics:ml_confirm + - COUNT 10
```

**Ожидаемый результат:**
- ✅ `err_rate < 0.01` (меньше 1%)
- ✅ `p_edge_p50 > 0.20` (медиана вероятности > 0.2)
- ✅ Нет ошибок `no_cfg`

## Структура файлов

```
scanner_infra/
├── update_ml_model.sh                    # Скрипт обучения и обновления модели
├── check_ml_confirm_status.sh            # Скрипт проверки статуса
├── ML_MODEL_UPDATE_INSTRUCTIONS.md       # Подробная инструкция
├── ML_CONFIRM_DIAGNOSTICS_REPORT.md      # Отчет о диагностике
├── ML_MODEL_UPDATE_SUMMARY.md            # Этот файл
└── python-worker/
    └── services/
        └── ml_confirm_gate.py            # Обновленный код с обработкой ошибок
```

## Важные замечания

1. **Данные для обучения**: Скрипт обучения требует данные в Redis streams:
   - `signals:of:inputs` - входные данные
   - `labels:tb` - метки (timebucket labels)

2. **Формат модели**: Модель должна быть типа `UtilMHModelV1` с методами:
   - `predict_util(X)` - предсказание utility
   - `predict_unc(X)` - предсказание uncertainty

3. **TTL кэша**: Модель кэшируется на 60 секунд (`ML_MODEL_CACHE_TTL_MS`). 
   После обновления конфигурации подождите до 60 секунд или перезапустите worker.

4. **Режим работы**: Текущий режим `ML_CONFIRM_MODE=SHADOW` означает, что модель работает, но не блокирует сделки.

## Troubleshooting

### Проблема: "No training data"
**Решение**: Проверьте наличие данных в Redis:
```bash
docker exec redis-worker-1 redis-cli XLEN signals:of:inputs
docker exec redis-worker-1 redis-cli XLEN labels:tb
```

### Проблема: "Model format is incorrect"
**Решение**: Убедитесь, что используется правильный скрипт:
- ✅ `train_ml_confirm_tb_util_mh_v1.py` - правильный
- ❌ `train_ml_confirm_tb_stack_v2_strict_oof.py` - неправильный (создает CalibratedClassifierCV)

### Проблема: "no_cfg" после обновления
**Решение**: 
1. Проверьте путь к модели в Redis конфигурации
2. Убедитесь, что файл существует: `ls -lh /var/lib/trade/of_reports/models/model.joblib`
3. Проверьте логи worker на ошибки загрузки

## Контакты и поддержка

При возникновении проблем:
1. Проверьте логи worker'а
2. Проверьте метрики ML SRE
3. Используйте скрипт `check_ml_confirm_status.sh` для диагностики

---

**Дата создания**: 2026-02-03
**Статус**: Готово к использованию ✅

