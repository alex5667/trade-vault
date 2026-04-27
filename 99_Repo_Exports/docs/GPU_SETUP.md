# 🚀 GPU Support для scanner_infra

## Обзор

Добавлена поддержка GPU для ускорения вычислений в `scanner_infra`, аналогично `trade_back`. Вычисления переносятся на видеокарту для снижения нагрузки на CPU.

## Что было сделано

### 1. Добавлены GPU библиотеки
- **CuPy** (CUDA 12.x) добавлен в `python-worker/requirements.txt`
- Автоматический fallback на CPU (NumPy) если GPU недоступен

### 2. Создан GPU Compute Service
- **Файл**: `python-worker/services/gpu_compute_service.py`
- **Функции**:
  - Массовое вычисление Delta для батчей свечей
  - Cumulative Volume Delta (CVD)
  - Z-scores с rolling window
  - ATR вычисления (батч)
  - Body ATR ratio
  - Delta ratio
  - Массовая обработка свечей

### 3. Обновлен Dockerfile
- **Файл**: `python-worker/Dockerfile.gpu`
- Использует `nvidia/cuda:12.1.0-cudnn8-runtime-ubuntu22.04`
- Включает CUDA runtime и библиотеки
- Устанавливает CuPy с GPU поддержкой

### 4. Обновлен docker-compose.yml
- `python-worker` использует `Dockerfile.gpu`
- Добавлен доступ к GPU устройствам через `nvidia` driver
- Увеличены ресурсы (memory: 2G, cpus: 2.0)
- Добавлены переменные окружения для GPU

### 5. Интеграция в обработчики
- `candle_of_worker.py` обновлен для использования GPU сервиса
- Автоматическое определение доступности GPU
- Логирование статуса GPU

## Использование

### Запуск с GPU

1. **Убедитесь, что установлен NVIDIA Docker runtime**:
```bash
# Проверка
nvidia-smi

# Установка nvidia-docker (если нужно)
distribution=$(. /etc/os-release;echo $ID$VERSION_ID)
curl -s -L https://nvidia.github.io/nvidia-docker/gpgkey | sudo apt-key add -
curl -s -L https://nvidia.github.io/nvidia-docker/$distribution/nvidia-docker.list | sudo tee /etc/apt/sources.list.d/nvidia-docker.list
sudo apt-get update && sudo apt-get install -y nvidia-docker2
sudo systemctl restart docker
```

2. **Запуск с GPU**:
```bash
cd /home/alex/front/trade/scanner_infra
# Для docker-compose v2 с GPU поддержкой
docker compose up -d python-worker --gpus all

# Или через переменные окружения (уже установлены в docker-compose.yml)
docker compose up -d python-worker
```

**Примечание**: Если используете старый `docker-compose` (v1), GPU может не работать. Используйте `docker compose` (v2) или добавьте в Makefile:
```makefile
up:
	docker compose --gpus all up -d
```

3. **Проверка GPU**:
```bash
docker exec scanner-python-worker python -c "from services.gpu_compute_service import get_gpu_service; s = get_gpu_service(); print('GPU available:', s.is_gpu_available())"
```

### Переменные окружения

- `GPU_ENABLED=true` - включить GPU (по умолчанию true)
- `NVIDIA_VISIBLE_DEVICES=all` - использовать все GPU
- `NVIDIA_DRIVER_CAPABILITIES=compute,utility` - возможности драйвера

### Отключение GPU

Если нужно использовать только CPU:
```bash
# В docker-compose.yml изменить:
# dockerfile: python-worker/Dockerfile.gpu -> dockerfile: python-worker/Dockerfile
# И установить GPU_ENABLED=false
```

## Производительность

### Ожидаемые улучшения

- **Массовая обработка свечей**: 10-100x ускорение для батчей >1000 свечей
- **Order Flow вычисления**: 5-20x ускорение для Delta/CVD/z-scores
- **ATR вычисления**: 10-50x ускорение для батчей
- **Снижение нагрузки на CPU**: с 100% до 20-30%

### Мониторинг

GPU использование можно проверить:
```bash
# Внутри контейнера
nvidia-smi

# Или через docker
docker exec scanner-python-worker nvidia-smi
```

## Архитектура

```
┌─────────────────┐
│  python-worker  │
│                 │
│  ┌───────────┐  │
│  │ GPU       │  │
│  │ Service   │  │
│  └─────┬─────┘  │
│        │        │
│  ┌─────▼─────┐  │
│  │ CuPy      │  │
│  │ (CUDA)    │  │
│  └─────┬─────┘  │
│        │        │
│  ┌─────▼─────┐  │
│  │ NVIDIA    │  │
│  │ GPU       │  │
│  └───────────┘  │
└─────────────────┘
```

## Fallback механизм

Если GPU недоступен, система автоматически переключается на CPU:
1. Проверка доступности CUDA
2. Проверка установки CuPy
3. Проверка переменной `GPU_ENABLED`
4. Автоматический fallback на NumPy

## Troubleshooting

### GPU не определяется

1. Проверьте драйверы NVIDIA:
```bash
nvidia-smi
```

2. Проверьте Docker GPU support:
```bash
docker run --rm --gpus all nvidia/cuda:12.1.0-base-ubuntu22.04 nvidia-smi
```

3. Проверьте логи контейнера:
```bash
docker logs scanner-python-worker | grep -i gpu
```

### CuPy не устанавливается

Если CuPy не устанавливается, проверьте версию CUDA:
```bash
# Для CUDA 11.x используйте:
# cupy-cuda11x>=13.0.0

# Для CUDA 12.x используйте:
# cupy-cuda12x>=13.0.0
```

### Память GPU

Если не хватает памяти GPU, можно ограничить в `gpu_compute_service.py`:
```python
mempool.set_limit(size=512**3)  # 512MB вместо 1GB
```

## Следующие шаги

1. ✅ Базовая GPU поддержка
2. ✅ Интеграция в candle_of_worker
3. 🔄 Интеграция в другие обработчики (microstructure, features)
4. 🔄 Batch processing для массовых операций
5. 🔄 Мониторинг GPU метрик

## Сравнение с trade_back

| Функция | trade_back | scanner_infra |
|---------|-----------|---------------|
| GPU библиотека | gpu.js (WebGL/WebGPU) | CuPy (CUDA) |
| Язык | TypeScript/Node.js | Python |
| Использование | Candle analysis, Order flow | Order flow, ATR, Delta |
| Fallback | CPU (JavaScript) | CPU (NumPy) |

## Документация

- [CuPy Documentation](https://docs.cupy.dev/)
- [NVIDIA Docker](https://github.com/NVIDIA/nvidia-docker)
- [CUDA Toolkit](https://developer.nvidia.com/cuda-toolkit)

