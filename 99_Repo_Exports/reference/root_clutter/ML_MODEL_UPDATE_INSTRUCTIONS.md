# Инструкция по обновлению ML модели до формата UtilMHModelV1

## Обзор

Текущая модель имеет несовместимый формат (`CalibratedClassifierCV` вместо `UtilMHModelV1`). 
Необходимо обучить новую модель в правильном формате и обновить конфигурацию.

**Важно**: В проекте уже настроен автоматический таймер `ml-train-tb-v10-4-timer`, который:
- Запускается **ежедневно в 03:10-03:20**
- Автоматически обучает модель через `ml_train_report_tb_v10_4.py`
- Сохраняет результат в `challenger` конфигурацию в Redis

Ручной запуск через `update_ml_model.sh` нужен только для:
- Немедленного обучения (не дожидаясь таймера)
- Автоматического копирования модели в production location
- Создания backup перед заменой

## Шаги обновления

### Шаг 1: Обучение новой модели

Используйте скрипт `ml_train_report_tb_v10_4.py` для автоматического обучения:

```bash
cd python-worker
python3 -m tools.ml_train_report_tb_v10_4 \
    --since-hours 72 \
    --models-root /var/lib/trade/ml_models \
    --horizons 60000,180000,300000
```

**Вариант 2**: Используйте обертку `update_ml_model.sh` (добавляет валидацию и копирование):

```bash
./update_ml_model.sh 72 /var/lib/trade/ml_models
```

**Примечание**: Скрипт `ml_train_report_tb_v10_4.py` уже существовал в проекте. Обертка добавляет:
- Автоматическую валидацию формата модели
- Копирование в production location
- Создание backup старой модели

**Что делает скрипт:**
1. Экспортирует данные из Redis streams (`signals:of:inputs` и `labels:tb`)
2. Создает датасет с multi-horizon targets (`util_r_60000`, `util_r_180000`, `util_r_300000`)
3. Обучает модель `UtilMHModelV1` (Ridge + GBDT ensemble)
4. Оптимизирует пороги (`util_floors`)
5. Сохраняет модель в `challenger` конфигурацию в Redis

### Шаг 2: Проверка модели

После обучения проверьте формат модели:

```bash
python3 <<EOF
import sys
sys.path.insert(0, 'python-worker')
import joblib

model_path = "/var/lib/trade/ml_models/tb_v10_4_<run_id>/model.joblib"
model = joblib.load(model_path)

print(f"Model type: {type(model).__name__}")
print(f"Has predict_util: {hasattr(model, 'predict_util')}")
print(f"Has predict_unc: {hasattr(model, 'predict_unc')}")
print(f"Horizons: {model.horizons}")
EOF
```

### Шаг 3: Обновление production модели

Скопируйте обученную модель в production location:

```bash
# Найти последнюю обученную модель
LATEST_MODEL=$(find /var/lib/trade/ml_models -name "model.joblib" -type f -printf '%T@ %p\n' | sort -n | tail -1 | cut -d' ' -f2-)

# Создать backup старой модели
sudo cp /var/lib/trade/of_reports/models/model.joblib /var/lib/trade/of_reports/models/model.joblib.backup.$(date +%Y%m%d_%H%M%S)

# Скопировать новую модель
sudo cp "$LATEST_MODEL" /var/lib/trade/of_reports/models/model.joblib
```

### Шаг 4: Обновление конфигурации в Redis

Обновите `champion` конфигурацию в Redis:

```bash
python3 <<EOF
import redis
import json
import os

r = redis.Redis.from_url("redis://redis-worker-1:6379/0", decode_responses=True)

# Получить challenger конфигурацию (созданную при обучении)
challenger_key = "cfg:ml_confirm:challenger"
champion_key = "cfg:ml_confirm:champion"

challenger_raw = r.get(challenger_key)
if challenger_raw:
    challenger = json.loads(challenger_raw)
    
    # Обновить путь к модели на production путь
    champion = challenger.copy()
    champion["model_path"] = "/var/lib/trade/of_reports/models/model.joblib"
    
    # Сохранить в champion
    r.set(champion_key, json.dumps(champion, ensure_ascii=False, separators=(",", ":")))
    print("✅ Champion configuration updated")
    print(f"   Model path: {champion['model_path']}")
    print(f"   Kind: {champion.get('kind')}")
else:
    print("❌ Challenger configuration not found. Run training first.")
EOF
```

### Шаг 5: Перезапуск worker

Перезапустите worker для загрузки новой модели:

```bash
# Найти контейнер worker
WORKER=$(docker ps --format "{{.Names}}" | grep -E "python.*worker|of.*confirm" | head -1)

# Перезапустить
docker restart "$WORKER"

# Или если используете docker-compose
docker-compose restart python-worker
```

### Шаг 6: Проверка логов

Проверьте логи worker на ошибки:

```bash
WORKER=$(docker ps --format "{{.Names}}" | grep -E "python.*worker|of.*confirm" | head -1)
docker logs "$WORKER" --tail 100 | grep -i "ml_confirm\|predict_util\|AttributeError\|no_cfg"
```

Ожидаемый результат:
- ✅ Нет ошибок `AttributeError: 'CalibratedClassifierCV' object has no attribute 'predict_util'`
- ✅ Нет ошибок `no_cfg`
- ✅ Модель загружается успешно

### Шаг 7: Проверка метрик ML SRE

Проверьте метрики через скрипт:

```bash
./check_ml_confirm_status.sh
```

Или вручную:

```bash
python3 <<EOF
import redis
from datetime import datetime, timedelta

r = redis.Redis.from_url("redis://redis-worker-1:6379/0", decode_responses=True)

stream = "metrics:ml_confirm"
now_ms = int(datetime.now().timestamp() * 1000)
window_ms = 10 * 60 * 1000  # 10 minutes
start_ms = now_ms - window_ms

messages = r.xrevrange(stream, max="+", min="-", count=100)
recent = [m for m in messages if int(m[1].get("ts_ms", 0) or 0) >= start_ms]

if recent:
    errors = [m for m in recent if m[1].get("err", "").strip()]
    no_cfg = [m for m in recent if m[1].get("err", "") == "no_cfg"]
    
    print(f"Total metrics: {len(recent)}")
    print(f"Errors: {len(errors)}/{len(recent)}")
    print(f"no_cfg errors: {len(no_cfg)}/{len(recent)}")
    
    if len(no_cfg) == 0:
        print("✅ No 'no_cfg' errors - model is working!")
    else:
        print(f"⚠️  Still have {len(no_cfg)} 'no_cfg' errors")
        
    # Check p_edge
    p_edges = [float(m[1].get("p_edge", 0) or 0) for m in recent if m[1].get("p_edge")]
    if p_edges:
        avg_p_edge = sum(p_edges) / len(p_edges)
        print(f"Average p_edge: {avg_p_edge:.3f}")
EOF
```

## Автоматизация

Используйте готовые скрипты:

1. **Обучение и обновление модели:**
   ```bash
   ./update_ml_model.sh [--since-hours 72] [--models-root /var/lib/trade/ml_models]
   ```

2. **Проверка статуса:**
   ```bash
   ./check_ml_confirm_status.sh [container-name]
   ```

## Troubleshooting

### Ошибка: "No training data"
- Проверьте наличие данных в Redis streams:
  ```bash
  docker exec redis-worker-1 redis-cli XLEN signals:of:inputs
  docker exec redis-worker-1 redis-cli XLEN labels:tb
  ```

### Ошибка: "Model format is incorrect"
- Убедитесь, что используется правильный скрипт обучения: `train_ml_confirm_tb_util_mh_v1.py`
- Проверьте, что модель имеет методы `predict_util` и `predict_unc`

### Ошибка: "no_cfg" после обновления
- Проверьте путь к модели в Redis конфигурации
- Убедитесь, что файл модели существует и доступен
- Проверьте логи worker на ошибки загрузки

## Дополнительная информация

- **Формат модели**: `UtilMHModelV1` (dataclass с методами `predict_util` и `predict_unc`)
- **Путь к модели**: `/var/lib/trade/of_reports/models/model.joblib`
- **Redis ключ**: `cfg:ml_confirm:champion`
- **TTL кэша**: 60 секунд (`ML_MODEL_CACHE_TTL_MS`)

