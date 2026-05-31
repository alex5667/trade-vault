# Отчет диагностики загрузки ML модели

## Дата: $(date)

## Результаты диагностики

### 1. Конфигурация в Redis
- **Статус**: ❌ НЕ НАЙДЕНА
- **Ключ**: `cfg:ml_confirm:champion`
- **Fallback ключ**: `cfg:ml_confirm` (hash) - также не найден
- **Проблема**: Конфигурация отсутствует в Redis, поэтому модель не может быть загружена

### 2. Файл модели
- **Статус**: ⚠️ НЕ ПРОВЕРЕН (путь неизвестен из-за отсутствия конфигурации)
- **Стандартные пути проверены**: не найдены

### 3. joblib
- **Статус**: ⚠️ Не установлен в текущем окружении
- **Примечание**: Это нормально, если диагностика запущена вне контейнера worker'а

### 4. Логи ошибок
- **Stream**: `metrics:ml_confirm`
- **Статистика**: 100/100 записей имеют ошибки
- **Тип ошибки**: `no_cfg` (100 записей)
- **Примечание**: Это подтверждает, что проблема в отсутствии конфигурации

## Корневая причина

**Основная проблема**: Конфигурация ML модели отсутствует в Redis.

Когда конфигурация отсутствует:
- `ml_confirm_gate.py` не может получить путь к модели
- Возвращается ошибка `no_cfg` или `no_model_loaded`
- Все запросы получают `p_edge=0.0`, `err_rate=1.0`

## План исправления

### Шаг 1: Проверить наличие модели

```bash
# Проверить стандартные пути
ls -la /var/lib/trade/of_reports/models/model.joblib
ls -la /var/lib/trade/ml_models/model.joblib

# Если модель не найдена, обучить новую
./update_ml_model.sh
```

### Шаг 2: Установить конфигурацию в Redis

Если модель существует, нужно создать конфигурацию:

```bash
# Подключиться к Redis (в контейнере или локально)
redis-cli -h redis-worker-1 -p 6379

# Создать конфигурацию (пример)
SET cfg:ml_confirm:champion '{
  "kind": "util_mh_v1",
  "model_path": "/var/lib/trade/of_reports/models/model.joblib",
  "run_id": "run_20240101_120000",
  "mode": "SHADOW",
  "fail_policy": "OPEN",
  "enforce_share": 0.05,
  "util_floors": {
    "global": {"floor": 0.0},
    "by_bucket": {
      "trend": {"floor": 0.0},
      "range": {"floor": 0.0}
    },
    "unc_k": 0.5
  }
}'
```

### Шаг 3: Проверить права доступа к файлу модели

```bash
# Проверить права
ls -la /var/lib/trade/of_reports/models/model.joblib

# Если нужно, исправить права
sudo chmod 644 /var/lib/trade/of_reports/models/model.joblib
sudo chown $(whoami):$(whoami) /var/lib/trade/of_reports/models/model.joblib
```

### Шаг 4: Проверить, что worker имеет доступ

```bash
# Проверить логи worker'а
docker logs <worker-container-name> | grep -i "model\|ml_confirm"

# Или через systemd
journalctl -u trade-of-confirm -f | grep -i "model\|ml_confirm"
```

### Шаг 5: Перезапустить worker (если нужно)

```bash
# Docker
docker restart <worker-container-name>

# Systemd
sudo systemctl restart trade-of-confirm
```

## Автоматическая диагностика

Запустить диагностику:

```bash
# С указанием Redis URL
REDIS_URL=redis://redis-worker-1:6379/0 python3 python-worker/tools/diagnose_ml_model_loading.py

# Или локально
REDIS_URL=redis://localhost:6379/0 python3 python-worker/tools/diagnose_ml_model_loading.py
```

## Проверка после исправления

После установки конфигурации проверьте:

1. **Метрики SRE**:
   ```bash
   python3 python-worker/tools/ml_sre_monitor.py
   ```

2. **Логи worker'а**:
   ```bash
   docker logs <worker-container-name> | grep -i "ml_confirm\|model"
   ```

3. **Redis конфигурация**:
   ```bash
   redis-cli GET cfg:ml_confirm:champion
   ```

## Ожидаемые результаты после исправления

- `err_rate` должен снизиться с 1.000 до < 0.010
- `p_edge_p50` должен быть > 0.200 (если модель работает корректно)
- `missing_rate` должен остаться < 0.020
- В логах не должно быть ошибок `no_model_loaded` или `no_cfg`














