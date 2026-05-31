# ✅ Статус использования GPU

## Результаты проверки

### 🎯 GPU активно используется!

**Дата проверки**: 2025-11-27 07:15

### 1. Системный уровень (nvidia-smi)
- **GPU**: NVIDIA GeForce RTX 3060
- **Utilization**: 15% (GPU), 18% (Memory)
- **Memory used**: 809 MB / 12288 MB (6.6%)
- **Активные процессы**: Python процесс использует 104 MB GPU памяти

### 2. Контейнер multi-symbol-orderflow
- **Runtime**: `nvidia` ✅
- **Status**: Up 49 minutes (healthy) ✅
- **GPU enabled**: `True` ✅
- **GPU device**: NVIDIA GeForce RTX 3060 ✅
- **Compute capability**: 8.6 ✅

### 3. CuPy и CUDA
- **CuPy version**: 12.3.0 ✅
- **CUDA available**: `True` ✅
- **Device count**: 1 ✅
- **GPU acceleration**: Enabled ✅

### 4. Конфигурация обработки
- **Batch size**: 10 (оптимизировано) ✅
- **Batch interval**: 5.0 секунд (оптимизировано) ✅
- **GPU service**: Available ✅

## Выводы

✅ **GPU полностью функционален и используется**
- Контейнер имеет доступ к GPU через nvidia runtime
- CuPy успешно инициализирован
- GPU acceleration включен в candle_of_worker
- Система показывает активное использование GPU (15% utilization, 809 MB памяти)

### Оптимизации применены
- ✅ Размер батча уменьшен до 10 свечей
- ✅ Интервал обработки уменьшен до 5 секунд
- ✅ Одиночные свечи обрабатываются через GPU
- ✅ Батчи обрабатываются чаще

## Мониторинг

Для постоянного мониторинга использования GPU:

```bash
# Проверить использование GPU
watch -n 2 nvidia-smi

# Проверить логи контейнера
docker logs -f scanner_infra-multi-symbol-orderflow-1 | grep -E "(GPU|batch|OrderFlow)"

# Проверить статус GPU в контейнере
docker exec scanner_infra-multi-symbol-orderflow-1 python -c "
from services.gpu_compute_service import get_gpu_service
s = get_gpu_service()
print('GPU enabled:', s.is_gpu_available())
print('GPU info:', s.get_device_info())
"
```

---

**Статус**: ✅ GPU активно используется
**Рекомендация**: Система работает корректно, GPU загружен на 15%, что нормально для текущей нагрузки

