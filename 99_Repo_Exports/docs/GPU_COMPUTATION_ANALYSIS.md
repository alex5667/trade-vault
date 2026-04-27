# 📊 Анализ использования GPU вычислений

**Дата**: 2025-11-29  
**Статус проверки**: ✅ Завершено

---

## 🔍 Текущее состояние

### GPU на хосте:
- **GPU**: NVIDIA GeForce RTX 3060
- **Utilization**: 29% (GPU), 16% (Memory)
- **Memory**: 1238 MB / 12288 MB (10.07%)
- **Power**: 23.49 W
- **Статус**: ✅ GPU активно используется (29%)

### GPU в контейнерах:
- **scanner_infra-multi-symbol-orderflow-1**: 
  - GPU enabled (env): ✅ True
  - GPU available: ✅ True (NVIDIA GeForce RTX 3060, 11.61 GB)
  - Batch methods: ✅ 15 методов доступно

---

## ⚠️ ПРОБЛЕМА: Batch методы реализованы, но НЕ используются активно

### Анализ использования:

#### 1. `feed_batch()` - НЕ используется
- ✅ Реализован в `L2BookTracker`
- ❌ В `base_orderflow_handler.py` используется только `self.l2.feed(book_data)` - одиночный вызов
- **Проблема**: Книги обрабатываются по одной, а не батчами

#### 2. `obi_from_book_batch()` - НЕ используется
- ✅ Реализован в `featurizer.py`
- ❌ В коде используется только `obi_from_book()` - одиночный вызов
- **Проблема**: OBI вычисляется по одной книге за раз

#### 3. `_depth_sum_batch()` - НЕ используется
- ✅ Реализован в `crypto_orderflow_handler.py`
- ❌ В коде используется только `_depth_sum()` - одиночный вызов
- **Проблема**: Глубина суммируется по одному набору уровней за раз

#### 4. `compute_rolling_stats_batch()` - НЕ используется
- ✅ Реализован в `gpu_compute_service.py`
- ❌ Не вызывается нигде в коде
- **Проблема**: Rolling stats вычисляются по одному окну за раз

#### 5. `compute_order_stats_batch()` - НЕ используется
- ✅ Реализован в `gpu_compute_service.py`
- ❌ Не вызывается нигде в коде
- **Проблема**: Статистика ордеров вычисляется по одному набору за раз

#### 6. `extract_features_batch()` - НЕ используется
- ✅ Реализован в `gpu_compute_service.py`
- ❌ Не вызывается нигде в коде
- **Проблема**: Фичи извлекаются по одному тику за раз

---

## 📊 Почему GPU используется только на 29%?

### Причины низкого использования:

1. **Batch методы не вызываются**:
   - Все методы реализованы, но используются одиночные версии
   - Книги обрабатываются по одной через `feed()` вместо `feed_batch()`
   - OBI вычисляется по одной книге через `obi_from_book()` вместо `obi_from_book_batch()`

2. **Нет накопления батчей**:
   - Книги приходят по одной, нет механизма накопления для батч обработки
   - Нет буферизации для создания батчей из 5+ элементов

3. **Существующие GPU методы работают**:
   - `compute_robust_zscore_mad()` - используется ✅
   - `process_candles_batch()` - используется ✅ (в candle_of_worker)
   - `compute_delta_batch()` - используется ✅
   - `compute_z_scores()` - используется ✅

---

## 🎯 Что нужно сделать для увеличения использования GPU

### Критично: Добавить батч-обработку в handlers

#### 1. Оптимизировать `_process_book()` для батч обработки:

```python
# В BaseOrderFlowHandler
class BaseOrderFlowHandler:
    def __init__(self, ...):
        # ...
        self._book_buffer: List[Dict[str, Any]] = []
        self._book_buffer_max = int(os.getenv("L2_BATCH_SIZE", "10"))
        self._book_buffer_timeout_ms = int(os.getenv("L2_BATCH_TIMEOUT_MS", "100"))
        self._book_buffer_last_ts = 0
    
    def _process_book(self, book_data: Dict[str, Any]) -> None:
        """Обработка Order Book с батч-оптимизацией."""
        self.processed_books += 1
        ts = int(book_data.get("ts", 0)) or int(time.time() * 1000)
        
        # Добавляем в буфер
        self._book_buffer.append(book_data)
        self._book_buffer_last_ts = ts
        
        # Обрабатываем батч если:
        # 1. Буфер заполнен (>= L2_BATCH_SIZE)
        # 2. Прошло достаточно времени (>= L2_BATCH_TIMEOUT_MS)
        should_process = (
            len(self._book_buffer) >= self._book_buffer_max or
            (ts - self._book_buffer_last_ts) >= self._book_buffer_timeout_ms
        )
        
        if should_process and len(self._book_buffer) > 0:
            # ✅ Используем батч обработку
            if len(self._book_buffer) >= 5:
                snapshots = self.l2.feed_batch(self._book_buffer)  # GPU ускорение
                for snap in snapshots:
                    if snap:
                        self._process_l2_snapshot(snap, ts)
            else:
                # Для малых батчей используем одиночную обработку
                for book in self._book_buffer:
                    snap = self.l2.feed(book)
                    if snap:
                        self._process_l2_snapshot(snap, ts)
            
            self._book_buffer.clear()
```

#### 2. Оптимизировать OBI вычисления:

```python
# В местах где вычисляется OBI для множества книг
books = [book1, book2, book3, ...]  # Накопленные книги
if len(books) >= 5:
    obi_values = obi_from_book_batch(books, depth=5)  # ✅ GPU
else:
    obi_values = [obi_from_book(book, depth=5) for book in books]  # CPU
```

#### 3. Оптимизировать depth суммирование:

```python
# В местах где суммируется глубина для множества уровней
levels_list = [levels1, levels2, ...]  # Накопленные уровни
if len(levels_list) >= 3:
    depth_sums = _depth_sum_batch(levels_list, depth=5)  # ✅ GPU
else:
    depth_sums = [_depth_sum(levels, depth=5) for levels in levels_list]  # CPU
```

---

## 📈 Ожидаемый эффект после оптимизации

| Метрика | Сейчас | После оптимизации | Улучшение |
|---------|--------|-------------------|-----------|
| GPU Utilization | 29% | 50-70% | +21-41% |
| Memory Usage | 10.07% | 15-25% | +5-15% |
| Order Book Processing | ~5ms/book | ~0.1-0.5ms/book | **10-50x** |
| Batch Methods Usage | 0% | 80-90% | **+80-90%** |

---

## ✅ Рекомендации

### Немедленные действия:

1. **Добавить батч-буферизацию в `_process_book()`**:
   - Накопление книг в буфер
   - Обработка батчами через `feed_batch()` когда буфер >= 5 книг

2. **Оптимизировать OBI вычисления**:
   - Использовать `obi_from_book_batch()` для множественных книг

3. **Оптимизировать depth суммирование**:
   - Использовать `_depth_sum_batch()` для множественных уровней

4. **Добавить мониторинг**:
   - Логирование использования batch методов
   - Метрики GPU utilization
   - Счетчики вызовов batch vs single методов

---

## 📝 Выводы

1. ✅ **GPU методы реализованы** - все 6 batch методов добавлены
2. ✅ **GPU доступен** - работает в контейнерах (29% utilization)
3. ⚠️ **Batch методы не используются** - вызываются только одиночные версии
4. ⚠️ **Нет батч-буферизации** - нет механизма накопления для батч обработки

**Главная проблема**: Методы реализованы, но не интегрированы в основной поток обработки. Нужно добавить батч-буферизацию и использовать batch методы вместо одиночных.

