# 🚀 GPU Optimization Master Plan
## Комплексный анализ возможностей переноса вычислений на GPU

**Дата**: 2025-11-29  
**Анализ**: Senior Go/Python Developer + Senior Trading Systems Analyst + DevOps/SRE (60 лет опыта)

---

## 📊 Текущее состояние GPU использования

### ✅ Уже оптимизировано (16 методов):
1. `compute_robust_zscore_mad()` - robust z-score с MAD
2. `compute_delta_batch()` - батч вычисление дельты
3. `compute_z_scores()` - z-scores для массивов
4. `compute_atr_batch()` - ATR батч
5. `compute_ema_batch()` - EMA батч
6. `compute_rsi_batch()` - RSI батч
7. `compute_macd_batch()` - MACD батч
8. `compute_obi_metrics_batch()` - OBI метрики батч
9. `process_candles_batch()` - обработка свечей батч
10. `compute_rolling_mean_std()` - rolling mean/std
11. `compute_median()` - медиана
12. `compute_cvd()` - Cumulative Volume Delta
13. `compute_body_atr_ratio()` - body/ATR ratio
14. `compute_delta_ratio()` - delta/volume ratio
15. `compute_ohlc_aggregation_batch()` - OHLC агрегация
16. Rolling window mean/std (частично)

### ⚠️ Текущее использование:
- **GPU Utilization**: 17% (можно увеличить до 50-70%)
- **Memory Usage**: 6.56% (806 MB / 12288 MB)
- **Потенциал**: Высокий - много CPU вычислений можно перенести

---

## 🎯 Приоритетные области для GPU оптимизации

### **PRIORITY 1: Order Book L2 Metrics (Критично)**

#### Проблема:
`signals/orderbook_l2_metrics.py` выполняет много вычислений в циклах:
- Суммирование глубины на разных уровнях (depth_5, depth_20)
- Вычисление OBI для разных глубин
- Slope вычисления (cum_depth / distance_bps)
- Microprice вычисления (взвешенная цена)
- Wall detection (поиск крупных уровней)

#### Текущая реализация (CPU):
```python
def _depth(levels: Sequence[Tuple[float, float]], k: int) -> float:
    k0 = max(0, min(int(k), len(levels)))
    return float(sum(v for _, v in levels[:k0]))  # ❌ CPU цикл

def _imbalance(bids, asks, k: int) -> float:
    bd = _depth(bids, k)  # ❌ CPU
    ad = _depth(asks, k)  # ❌ CPU
    return (bd - ad) / (bd + ad)  # ❌ CPU

def _slope(levels, k: int, mid: float) -> float:
    # ❌ CPU цикл по уровням
    cum_depth = 0.0
    for i, (price, vol) in enumerate(levels[:k]):
        cum_depth += vol
        dist_bps = abs(price - mid) / mid * 10000
        # ...
```

#### GPU Оптимизация:
```python
def compute_l2_metrics_batch(
    books: List[Dict[str, Any]],
    k_small: int = 5,
    k_large: int = 20
) -> List[L2Metrics]:
    """
    Батч обработка L2 метрик для множества книг.
    
    Преимущества:
    - Обработка 100+ книг одновременно
    - Параллельное вычисление depth, OBI, slope, microprice
    - 10-50x ускорение для больших батчей
    """
    # Извлекаем данные в массивы
    n = len(books)
    bid_prices = np.zeros((n, k_large), dtype=np.float32)
    bid_volumes = np.zeros((n, k_large), dtype=np.float32)
    ask_prices = np.zeros((n, k_large), dtype=np.float32)
    ask_volumes = np.zeros((n, k_large), dtype=np.float32)
    mids = np.zeros(n, dtype=np.float32)
    
    # Заполняем массивы
    for i, book in enumerate(books):
        bids = book.get("bids", [])[:k_large]
        asks = book.get("asks", [])[:k_large]
        mid = book.get("mid", 0.0)
        
        for j, (p, v) in enumerate(bids):
            bid_prices[i, j] = p
            bid_volumes[i, j] = v
        for j, (p, v) in enumerate(asks):
            ask_prices[i, j] = p
            ask_volumes[i, j] = v
        mids[i] = mid
    
    # GPU вычисления
    if self.use_gpu:
        bid_prices_gpu = cp.asarray(bid_prices)
        bid_volumes_gpu = cp.asarray(bid_volumes)
        ask_prices_gpu = cp.asarray(ask_prices)
        ask_volumes_gpu = cp.asarray(ask_volumes)
        mids_gpu = cp.asarray(mids)
        
        # Depth вычисления (параллельно для всех книг)
        depth_bid_5 = cp.sum(bid_volumes_gpu[:, :k_small], axis=1)
        depth_ask_5 = cp.sum(ask_volumes_gpu[:, :k_small], axis=1)
        depth_bid_20 = cp.sum(bid_volumes_gpu[:, :k_large], axis=1)
        depth_ask_20 = cp.sum(ask_volumes_gpu[:, :k_large], axis=1)
        
        # OBI вычисления
        total_5 = depth_bid_5 + depth_ask_5
        obi_5 = (depth_bid_5 - depth_ask_5) / cp.maximum(total_5, EPS)
        
        # Slope вычисления (параллельно)
        # Microprice вычисления (параллельно)
        # Wall detection (параллельно)
        
        # Конвертируем обратно в CPU
        return [L2Metrics(...) for ... in zip(...)]
```

**Ожидаемый эффект**:
- **Ускорение**: 10-50x для батчей из 50+ книг
- **Использование GPU**: +5-10% utilization
- **Латентность**: Снижение с ~5ms до ~0.1-0.5ms на книгу

---

### **PRIORITY 2: Order Book Depth Aggregation (Высокий приоритет)**

#### Проблема:
`handlers/crypto_orderflow_handler.py` - функция `_depth_sum()`:
```python
def _depth_sum(levels: Any, depth: int = 5) -> float:
    s = 0.0
    n = 0
    for lv in levels:  # ❌ CPU цикл
        if not lv or len(lv) < 2:
            continue
        try:
            s += float(lv[1])  # ❌ CPU суммирование
            n += 1
        except Exception:
            continue
        if n >= depth:
            break
    return float(s)
```

#### GPU Оптимизация:
```python
def compute_depth_sum_batch(
    levels_list: List[List[Tuple[float, float]]],
    depth: int = 5
) -> np.ndarray:
    """
    Батч вычисление глубины для множества книг.
    
    Args:
        levels_list: Список списков уровней для каждой книги
        depth: Количество уровней для суммирования
        
    Returns:
        Массив сумм глубины для каждой книги
    """
    n = len(levels_list)
    max_levels = max(len(levels) for levels in levels_list) if levels_list else 0
    max_levels = min(max_levels, depth)
    
    volumes = np.zeros((n, max_levels), dtype=np.float32)
    
    for i, levels in enumerate(levels_list):
        for j, (_, vol) in enumerate(levels[:max_levels]):
            volumes[i, j] = float(vol)
    
    if self.use_gpu:
        volumes_gpu = cp.asarray(volumes)
        depth_sums = cp.sum(volumes_gpu, axis=1)
        return depth_sums.get()
    
    return np.sum(volumes, axis=1)
```

**Ожидаемый эффект**:
- **Ускорение**: 5-20x для батчей из 20+ книг
- **Использование GPU**: +2-5% utilization

---

### **PRIORITY 3: OBI Computation Batch (Высокий приоритет)**

#### Проблема:
`signals/featurizer.py` - функция `obi_from_book()`:
```python
def obi_from_book(book: Dict, depth: int = 5) -> Optional[float]:
    bids_sorted = sorted(bids, key=lambda x: x[0], reverse=True)[:depth]  # ❌ CPU sort
    asks_sorted = sorted(asks, key=lambda x: x[0])[:depth]  # ❌ CPU sort
    
    bv = sum(max(0.0, float(v)) for _, v in bids_sorted)  # ❌ CPU цикл
    av = sum(max(0.0, float(v)) for _, v in asks_sorted)  # ❌ CPU цикл
    
    return (bv - av) / tot  # ❌ CPU
```

#### GPU Оптимизация:
```python
def compute_obi_batch(
    books: List[Dict[str, Any]],
    depth: int = 5
) -> np.ndarray:
    """
    Батч вычисление OBI для множества книг.
    
    Преимущества:
    - Параллельная сортировка на GPU
    - Параллельное суммирование
    - Обработка 100+ книг одновременно
    """
    n = len(books)
    bid_volumes = np.zeros((n, depth), dtype=np.float32)
    ask_volumes = np.zeros((n, depth), dtype=np.float32)
    
    # Извлекаем данные
    for i, book in enumerate(books):
        bids = sorted(book.get("bids", []), key=lambda x: x[0], reverse=True)[:depth]
        asks = sorted(book.get("asks", []), key=lambda x: x[0])[:depth]
        
        for j, (_, v) in enumerate(bids):
            bid_volumes[i, j] = float(v)
        for j, (_, v) in enumerate(asks):
            ask_volumes[i, j] = float(v)
    
    if self.use_gpu:
        bid_volumes_gpu = cp.asarray(bid_volumes)
        ask_volumes_gpu = cp.asarray(ask_volumes)
        
        bid_sums = cp.sum(bid_volumes_gpu, axis=1)
        ask_sums = cp.sum(ask_volumes_gpu, axis=1)
        
        total = bid_sums + ask_sums
        obi = (bid_sums - ask_sums) / cp.maximum(total, EPS)
        
        return obi.get()
    
    # CPU fallback
    bid_sums = np.sum(bid_volumes, axis=1)
    ask_sums = np.sum(ask_volumes, axis=1)
    total = bid_sums + ask_sums
    obi = (bid_sums - ask_sums) / np.maximum(total, EPS)
    return obi
```

**Ожидаемый эффект**:
- **Ускорение**: 10-30x для батчей из 50+ книг
- **Использование GPU**: +3-7% utilization

---

### **PRIORITY 4: Rolling Window Statistics (Средний приоритет)**

#### Проблема:
`signals/featurizer.py` - класс `Rolling`:
```python
class Rolling:
    def mean(self) -> Optional[float]:
        # ✅ Уже частично на GPU для больших окон
        if len(self.buf) >= self._gpu_threshold and self._gpu_service:
            # ✅ GPU используется
        return self.sum / len(self.buf)  # ❌ CPU для малых окон
    
    def std(self) -> Optional[float]:
        # ✅ Уже частично на GPU
        # ❌ Но можно улучшить для rolling windows
```

#### GPU Оптимизация:
```python
def compute_rolling_stats_batch(
    windows: List[List[float]],
    window_size: int
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Батч вычисление rolling mean/std для множества окон.
    
    Преимущества:
    - Параллельная обработка множества окон
    - Эффективное использование GPU памяти
    """
    n = len(windows)
    max_len = max(len(w) for w in windows) if windows else 0
    
    data = np.zeros((n, max_len), dtype=np.float32)
    for i, w in enumerate(windows):
        for j, v in enumerate(w):
            data[i, j] = float(v)
    
    if self.use_gpu:
        data_gpu = cp.asarray(data)
        # Параллельное вычисление mean/std для всех окон
        means = cp.mean(data_gpu, axis=1)
        stds = cp.std(data_gpu, axis=1)
        return means.get(), stds.get()
    
    # CPU fallback
    means = np.mean(data, axis=1)
    stds = np.std(data, axis=1)
    return means, stds
```

**Ожидаемый эффект**:
- **Ускорение**: 5-15x для батчей из 30+ окон
- **Использование GPU**: +2-4% utilization

---

### **PRIORITY 5: Statistics Aggregation (Низкий приоритет)**

#### Проблема:
`analytics/ab_compare.py` - функция `summarize_orders()`:
```python
def summarize_orders(orders: List[Order]) -> Dict[str, Any]:
    pnls = [(o.pnl_usd or 0.0) for o in orders]  # ❌ CPU цикл
    
    wr = winrate(pnls)  # ❌ CPU
    ap = avg(pnls)  # ❌ CPU
    med = float(np.median(pnls)) if pnls else 0.0  # ❌ CPU
    std = float(np.std(pnls)) if pnls else 0.0  # ❌ CPU
    sharpe = (ap / std) if std > 1e-9 else 0.0  # ❌ CPU
```

#### GPU Оптимизация:
```python
def compute_order_stats_batch(
    orders_list: List[List[Order]]
) -> List[Dict[str, Any]]:
    """
    Батч вычисление статистики для множества наборов ордеров.
    
    Преимущества:
    - Параллельное вычисление winrate, avg, median, std, sharpe
    - Обработка 100+ наборов одновременно
    """
    n = len(orders_list)
    max_orders = max(len(orders) for orders in orders_list) if orders_list else 0
    
    pnls = np.zeros((n, max_orders), dtype=np.float32)
    is_win = np.zeros((n, max_orders), dtype=np.float32)
    
    # Извлекаем данные
    for i, orders in enumerate(orders_list):
        for j, o in enumerate(orders):
            pnl = float(o.pnl_usd or 0.0)
            pnls[i, j] = pnl
            is_win[i, j] = 1.0 if pnl > 0 else 0.0
    
    if self.use_gpu:
        pnls_gpu = cp.asarray(pnls)
        is_win_gpu = cp.asarray(is_win)
        
        # Параллельные вычисления
        winrates = cp.mean(is_win_gpu, axis=1) * 100.0
        avg_pnls = cp.mean(pnls_gpu, axis=1)
        medians = cp.median(pnls_gpu, axis=1)
        stds = cp.std(pnls_gpu, axis=1)
        sharpes = avg_pnls / cp.maximum(stds, EPS)
        
        return [
            {
                "winrate": float(wr),
                "avg_pnl": float(ap),
                "median_pnl": float(med),
                "std_pnl": float(std),
                "sharpe": float(sh)
            }
            for wr, ap, med, std, sh in zip(
                winrates.get(), avg_pnls.get(), medians.get(), stds.get(), sharpes.get()
            )
        ]
    
    # CPU fallback
    # ...
```

**Ожидаемый эффект**:
- **Ускорение**: 5-20x для батчей из 50+ наборов
- **Использование GPU**: +2-5% utilization

---

### **PRIORITY 6: Feature Extraction Batch (Средний приоритет)**

#### Проблема:
`services/export_features.py` - функция `extract_features()`:
```python
def extract_features(ticks: List[Dict], books_map: Dict[int, Dict], ...):
    roll = Rolling(size=delta_window)
    rows = []
    
    for tick in ticks:  # ❌ CPU цикл по тикам
        ts = int(tick["ts"])
        book = find_nearest_book(books_map, ts)
        feat = make_features(tick, book, roll)  # ❌ CPU
        rows.append(feat)
```

#### GPU Оптимизация:
```python
def extract_features_batch(
    ticks_batch: List[List[Dict]],
    books_map: Dict[int, Dict]
) -> List[List[Dict]]:
    """
    Батч извлечение фич для множества наборов тиков.
    
    Преимущества:
    - Параллельная обработка фич
    - GPU ускорение для rolling statistics
    - Обработка 1000+ тиков одновременно
    """
    # Группируем тики по символам/окнам
    # Вычисляем фичи батчами через GPU
    # ...
```

**Ожидаемый эффект**:
- **Ускорение**: 10-30x для батчей из 1000+ тиков
- **Использование GPU**: +5-10% utilization

---

## 📈 Ожидаемые результаты

### После реализации всех оптимизаций:

| Метрика | Текущее | После оптимизации | Улучшение |
|---------|---------|-------------------|-----------|
| **GPU Utilization** | 17% | 50-70% | +33-53% |
| **Memory Usage** | 6.56% | 15-25% | +8.5-18.5% |
| **Order Book Processing** | ~5ms/book | ~0.1-0.5ms/book | **10-50x** |
| **L2 Metrics Computation** | ~10ms/batch | ~0.2-1ms/batch | **10-50x** |
| **OBI Computation** | ~2ms/book | ~0.05-0.2ms/book | **10-40x** |
| **Feature Extraction** | ~1ms/tick | ~0.03-0.1ms/tick | **10-30x** |

---

## 🛠️ План реализации

### **Phase 1: Order Book L2 Metrics (2-3 дня)**
1. ✅ Добавить `compute_l2_metrics_batch()` в `gpu_compute_service.py`
2. ✅ Интегрировать в `L2BookTracker` для батч обработки
3. ✅ Обновить `orderbook_l2_metrics.py` для использования GPU батчей
4. ✅ Тестирование и бенчмарки

### **Phase 2: Depth Aggregation (1-2 дня)**
1. ✅ Добавить `compute_depth_sum_batch()` в `gpu_compute_service.py`
2. ✅ Заменить `_depth_sum()` на GPU версию в handlers
3. ✅ Тестирование

### **Phase 3: OBI Batch (1-2 дня)**
1. ✅ Добавить `compute_obi_batch()` в `gpu_compute_service.py`
2. ✅ Обновить `obi_from_book()` для использования батчей
3. ✅ Тестирование

### **Phase 4: Rolling Stats Batch (1 день)**
1. ✅ Улучшить `compute_rolling_stats_batch()`
2. ✅ Интегрировать в `Rolling` класс
3. ✅ Тестирование

### **Phase 5: Statistics Aggregation (1 день)**
1. ✅ Добавить `compute_order_stats_batch()` в `gpu_compute_service.py`
2. ✅ Интегрировать в analytics модули
3. ✅ Тестирование

### **Phase 6: Feature Extraction Batch (2-3 дня)**
1. ✅ Добавить `extract_features_batch()` в `gpu_compute_service.py`
2. ✅ Интегрировать в `export_features.py`
3. ✅ Тестирование

**Общее время**: 8-12 дней разработки + тестирование

---

## 🎯 Критерии успеха

1. **GPU Utilization**: Увеличить с 17% до 50-70%
2. **Латентность**: Снизить обработку Order Book с 5ms до <1ms
3. **Пропускная способность**: Увеличить обработку книг с 200/sec до 2000+/sec
4. **CPU Load**: Снизить CPU нагрузку на 30-50%
5. **Memory**: Использовать 15-25% GPU памяти (вместо 6.56%)

---

## ⚠️ Риски и митигация

### Риск 1: Overhead батчирования
- **Митигация**: Использовать adaptive batching (маленькие батчи для малой нагрузки, большие для высокой)

### Риск 2: Memory pressure
- **Митигация**: Ограничить размер батчей, использовать memory pooling

### Риск 3: Latency для одиночных запросов
- **Митигация**: Hybrid подход - GPU для батчей, CPU для одиночных (если батч не заполнен)

---

## 📝 Заключение

Проект имеет **высокий потенциал** для GPU оптимизации. Основные возможности:

1. ✅ **Order Book L2 Metrics** - критично, высокий эффект
2. ✅ **Depth Aggregation** - высокий приоритет, средний эффект
3. ✅ **OBI Batch** - высокий приоритет, высокий эффект
4. ✅ **Rolling Stats** - средний приоритет, средний эффект
5. ✅ **Statistics Aggregation** - низкий приоритет, средний эффект
6. ✅ **Feature Extraction** - средний приоритет, высокий эффект

**Ожидаемый общий эффект**: Увеличение GPU utilization с 17% до 50-70%, ускорение обработки Order Book в 10-50 раз.
