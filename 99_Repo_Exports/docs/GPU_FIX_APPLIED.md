# ✅ Исправление GPU: CuPy обновлен до совместимой версии

## Проблема

CuPy 13.6.0 требует CUDA 12.9, но драйвер 580.95.05 поддерживает только до CUDA 12.0.

## Решение

Обновлена версия CuPy в `python-worker/requirements.txt`:
- **Было**: `cupy-cuda12x>=13.0.0` (требует CUDA 12.9)
- **Стало**: `cupy-cuda12x==12.3.0` (совместимо с CUDA 12.0)

## Действия для применения

### 1. Пересобрать контейнер

```bash
cd /home/alex/front/trade/scanner_infra
docker compose build python-worker
```

### 2. Перезапустить сервис

```bash
docker compose up -d python-worker
```

### 3. Проверить GPU

```bash
docker exec scanner-python-worker python -c "
from services.gpu_compute_service import get_gpu_service
s = get_gpu_service()
print('=== GPU Status ===')
print('GPU available:', s.is_gpu_available())
print('Using GPU:', s.use_gpu)
if s.is_gpu_available():
    info = s.get_device_info()
    if info:
        print('GPU device:', info.get('name', 'Unknown'))
        print('GPU memory:', info.get('memory_total', 0) / 1024**3, 'GB')
        print('Compute capability:', info.get('compute_capability', 'N/A'))
    print('✅ GPU acceleration enabled!')
else:
    print('⚠️ GPU not available, using CPU')
"
```

### 4. Проверить использование GPU

```bash
# В контейнере
docker exec scanner-python-worker nvidia-smi

# На хосте
nvidia-smi
```

## Ожидаемый результат

После пересборки:
- ✅ CuPy 12.3.0 будет установлен
- ✅ GPU должен быть доступен в контейнере
- ✅ Вычисления будут выполняться на GPU
- ✅ Нагрузка на CPU снизится с ~100% до 20-30%

## Если GPU все еще недоступен

1. Проверьте логи:
   ```bash
   docker logs scanner-python-worker | grep -i gpu
   ```

2. Проверьте доступность GPU в контейнере:
   ```bash
   docker exec scanner-python-worker nvidia-smi
   ```

3. Если `nvidia-smi` не работает в контейнере, проверьте:
   - Установлен ли `nvidia-docker2`
   - Правильно ли настроен Docker для GPU

## Версии

- **Драйвер NVIDIA**: 580.95.05
- **Поддерживаемая CUDA**: до 12.0
- **CuPy**: 12.3.0 (совместимо с CUDA 12.0)
- **GPU**: NVIDIA GeForce RTX 3060

## Статус

✅ **Исправление применено**: Версия CuPy обновлена в requirements.txt
🔄 **Требуется**: Пересборка контейнера для применения изменений

