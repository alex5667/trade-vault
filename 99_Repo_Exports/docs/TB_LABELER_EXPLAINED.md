# TB Labeler (v10.1) - Подробное объяснение

## 🎯 Цель

TB Labeler создает **метки (labels)** для обучения ML моделей на основе реального движения цены после момента принятия решения, а не на основе закрытых позиций.

**Проблема, которую решает:**
- Старые методы использовали `POSITION_CLOSED` события → задержка, пропуски, неточность
- TB Labeler использует **ticks из Redis Stream** → точное движение цены в реальном времени

---

## 📊 Что такое Triple-Barrier (TB)?

**Triple-Barrier** — это метод маркировки, который определяет исход сделки по трем барьерам:

1. **TP (Take Profit)** — верхний барьер прибыли
2. **SL (Stop Loss)** — нижний барьер убытка  
3. **TIMEOUT** — временной барьер (горизонт)

### Визуализация для LONG позиции:

```
Entry Price
    |
    |     TP (Take Profit)
    |     ────────────────
    |    /
    |   /  ← Цена движется вверх
    |  /
    | /    ← MFE (Max Favorable Excursion) - максимальная прибыль
    |/
    |\    ← MAE (Max Adverse Excursion) - максимальный просадка
    | \
    |  \   ← Цена движется вниз
    |   \
    |    ────────────────
    |     SL (Stop Loss)
    |
    └───────────────────────→ Время
    ts0                  ts0 + horizon
```

---

## 🔄 Как работает TB Labeler (v10.1)

### Архитектура: 2-фазная обработка

#### **Фаза 1: Enqueue (Постановка в очередь)**

**Источник:** `signals:of:inputs` stream (OF inputs с индикаторами)

**Процесс:**
1. Worker читает новые сообщения из `signals:of:inputs` через consumer group
2. Для каждого сигнала:
   - Проверяет наличие `sid`, `symbol`, `ts_ms`, `direction`
   - Проверяет дедупликацию (`tb:done:{sid}`)
   - Сохраняет job в Redis:
     - `tb:job:{sid}` — JSON с данными сигнала (TTL 2h)
     - `tb:jobs:due` (ZSET) — очередь с временем обработки

**Время обработки (due_ms):**
```
due_ms = ts_ms + max(HORIZONS) + TB_SLACK_MS
```
- `max(HORIZONS)` = 300000ms (5 минут) — максимальный горизонт
- `TB_SLACK_MS` = 15000ms (15 сек) — запас времени после горизонта

**Пример:**
- Сигнал пришел в `ts_ms = 1000000`
- `due_ms = 1000000 + 300000 + 15000 = 1315000`
- Job будет обработан после `1315000ms`

---

#### **Фаза 2: Process Due (Обработка готовых jobs)**

**Триггер:** Job становится "due" (время `due_ms` наступило)

**Процесс для каждого due job:**

1. **Загрузка job из Redis:**
   ```python
   job = {
       "sid": "abc123",
       "symbol": "BTCUSDT",
       "ts_ms": 1000000,
       "direction": "LONG",
       "indicators": {
           "stop_bps": 50.0,
           "atr_bps": 100.0,
           ...
       }
   }
   ```

2. **Загрузка тиков из Redis Stream:**
   ```python
   stream = "stream:tick_BTCUSDT"
   start_ms = ts0  # время сигнала
   end_ms = ts0 + max(HORIZONS)  # ts0 + 300000ms
   
   ticks = fetch_ticks_window(r_ticks, stream, start_ms, end_ms)
   # Результат: [(ts1, price1), (ts2, price2), ...]
   ```

3. **Определение барьеров (TP/SL):**
   ```python
   # Приоритет: stop_bps > atr_bps > fallback
   if stop_bps > 0:
       tp_bps = TP_K_ATR * stop_bps  # например, 1.0 * 50 = 50 bps
       sl_bps = SL_K_ATR * stop_bps  # например, 1.0 * 50 = 50 bps
   elif atr_bps > 0:
       tp_bps = TP_K_ATR * atr_bps
       sl_bps = SL_K_ATR * atr_bps
   else:
       tp_bps = FALLBACK_TP_BPS  # 30 bps
       sl_bps = FALLBACK_SL_BPS  # 30 bps
   ```

4. **Вычисление Triple-Barrier для каждого горизонта:**
   
   Горизонты: `[60000, 180000, 300000]` ms (1мин, 3мин, 5мин)
   
   Для каждого горизонта `h_ms`:
   ```python
   barrier_stats(
       ts0=ts0,                    # время сигнала
       direction="LONG",            # направление
       entry_px=entry_px,          # цена входа (первый тик >= ts0)
       path=ticks,                 # путь цены [(ts, px), ...]
       b=Barriers(tp_bps, sl_bps), # барьеры
       h_ms=180000,                # горизонт (3 минуты)
       adv_max=1.2                 # порог adverse_proxy
   )
   ```

5. **Логика barrier_stats (ядро алгоритма):**
   
   ```python
   # Инициализация
   label = "TIMEOUT"  # по умолчанию
   hit_ms = ts0 + h_ms
   mae = 0.0  # максимальная просадка (adverse)
   mfe = 0.0  # максимальная прибыль (favorable)
   
   # Проход по тикам
   for ts, px in path:
       if ts < ts0 or ts > ts0 + h_ms:
           continue
       
       # Вычисляем signed return (в bps)
       ret_bps = signed_ret_bps(direction, entry_px, px)
       # Для LONG: ret_bps > 0 если px > entry_px
       # Для SHORT: ret_bps > 0 если px < entry_px
       
       # Обновляем MAE/MFE
       if ret_bps > mfe:
           mfe = ret_bps  # новая максимальная прибыль
       if ret_bps < mae:
           mae = ret_bps  # новая максимальная просадка
       
       # Проверка барьеров
       if ret_bps >= tp_bps:
           label = "TP"  # Take Profit сработал
           hit_ms = ts
           break
       if ret_bps <= -sl_bps:
           label = "SL"  # Stop Loss сработал
           hit_ms = ts
           break
   
   # Вычисление метрик
   mae_mag = abs(mae)  # абсолютная просадка
   mfe_mag = max(0.0, mfe)  # абсолютная прибыль
   
   # adverse_proxy = риск-скорректированное качество
   adverse_proxy = mae_mag / mfe_mag if mfe_mag > 0 else mae_mag
   
   # R-multiple (нормализованная доходность)
   r_mult = ret_bps / scale_bps  # scale_bps = stop_bps или atr_bps
   
   # y_edge = бинарная метка для обучения
   # 1 если TP сработал И adverse_proxy <= 1.2 (низкий риск)
   y_edge = 1 if (label == "TP" and adverse_proxy <= 1.2) else 0
   ```

6. **Сохранение результата:**
   
   ```python
   payload = {
       "sid": "abc123",
       "symbol": "BTCUSDT",
       "ts_ms": 1000000,
       "direction": "LONG",
       "primary": {
           "label": "TP",           # TP | SL | TIMEOUT | NO_TICKS
           "hit_ms": 1000180000,    # когда сработал барьер
           "ret_bps": 52.3,         # доходность в bps
           "r_mult": 1.046,         # R-multiple
           "mae_bps": 8.2,          # максимальная просадка
           "mfe_bps": 58.5,         # максимальная прибыль
           "adverse_proxy": 0.14,   # mae/mfe = 8.2/58.5
           "y_edge": 1              # 1 = хороший сигнал
       },
       "horizons": {
           "60000": {...},   # результаты для 1 мин
           "180000": {...}, # результаты для 3 мин (primary)
           "300000": {...}  # результаты для 5 мин
       },
       "meta": {
           "tp_bps": 50.0,
           "sl_bps": 50.0,
           "exec_cost_r": 0.15,     # стоимость исполнения в R
           "util_r": 0.896          # utility = r_mult - exec_cost_r
       }
   }
   
   # Публикация в Redis Stream
   r.xadd("labels:tb", {"payload": json.dumps(payload)})
   ```

---

## 📈 Ключевые метрики

### 1. **MAE (Max Adverse Excursion)**
- Максимальная просадка от цены входа
- Для LONG: минимальное значение `ret_bps` (самый низкий момент)
- Для SHORT: максимальное значение `ret_bps` (самый высокий момент)

### 2. **MFE (Max Favorable Excursion)**
- Максимальная прибыль от цены входа
- Для LONG: максимальное значение `ret_bps`
- Для SHORT: минимальное значение `ret_bps`

### 3. **adverse_proxy**
- Риск-скорректированное качество сигнала
- Формула: `mae_mag / mfe_mag` (если mfe > 0)
- Низкое значение (< 1.2) = хороший сигнал (маленькая просадка относительно прибыли)

### 4. **R-multiple (r_mult)**
- Нормализованная доходность
- Формула: `ret_bps / scale_bps`
- `scale_bps` = `stop_bps` или `atr_bps` (базовая единица риска)
- `r_mult = 2.0` означает прибыль в 2x от размера стоп-лосса

### 5. **y_edge (бинарная метка)**
- `y_edge = 1` если:
  - `label == "TP"` (Take Profit сработал)
  - И `adverse_proxy <= TB_ADV_MAX` (1.2 по умолчанию)
- Используется как целевая переменная для обучения ML

### 6. **util_r (utility)**
- Полезность после учета стоимости исполнения
- Формула: `util_r = r_mult - exec_cost_r`
- `exec_cost_r = (spread_bps + expected_slippage_bps) / scale_bps`

---

## ⚙️ Конфигурация

### ENV переменные:

```bash
# Streams
OF_INPUTS_STREAM=signals:of:inputs      # входной stream
TB_LABELS_STREAM=labels:tb              # выходной stream
TB_TICK_STREAM_PREFIX=stream:tick_      # префикс для tick streams

# Горизонты (в миллисекундах)
TB_HORIZONS_MS=60000,180000,300000      # 1мин, 3мин, 5мин
TB_PRIMARY_H_MS=180000                   # primary горизонт (3 мин)

# Барьеры
TB_TP_K_ATR=1.0                          # множитель для TP
TB_SL_K_ATR=1.0                          # множитель для SL
TB_FALLBACK_TP_BPS=30                   # fallback TP (если нет stop/atr)
TB_FALLBACK_SL_BPS=30                   # fallback SL

# Качество
TB_ADV_MAX=1.2                           # максимальный adverse_proxy для y_edge=1

# Тайминги
TB_SLACK_MS=15000                        # запас времени после горизонта
TB_JOB_TTL_SEC=7200                     # TTL для job (2 часа)
```

---

## 🔄 Жизненный цикл сигнала

```
1. Сигнал приходит в signals:of:inputs
   ↓
2. TB Labeler читает сигнал (consumer group)
   ↓
3. Enqueue: создает job, ставит в очередь (tb:jobs:due)
   ↓
4. Ожидание: job ждет до due_ms = ts_ms + horizon + slack
   ↓
5. Process Due: когда due_ms наступило
   ↓
6. Загрузка тиков из stream:tick_{SYMBOL}
   ↓
7. Вычисление Triple-Barrier для каждого горизонта
   ↓
8. Публикация результата в labels:tb
   ↓
9. Маркировка как done (tb:done:{sid}) для дедупликации
```

---

## 🎯 Почему это лучше, чем POSITION_CLOSED?

### Старый метод (POSITION_CLOSED):
- ❌ Зависит от реального исполнения
- ❌ Пропускает сигналы, которые не были исполнены
- ❌ Задержка между сигналом и меткой
- ❌ Неточность из-за slippage, частичного исполнения

### TB Labeling (v10.1):
- ✅ Независим от исполнения (работает с любыми сигналами)
- ✅ Использует реальное движение цены (ticks)
- ✅ Мгновенная обработка после горизонта
- ✅ Точные метрики (MAE/MFE, adverse_proxy)
- ✅ Multi-horizon (несколько временных окон)

---

## 📊 Пример результата

**Входной сигнал:**
```json
{
  "sid": "btc_20240101_120000_long",
  "symbol": "BTCUSDT",
  "ts_ms": 1704110400000,
  "direction": "LONG",
  "indicators": {
    "stop_bps": 50.0,
    "atr_bps": 100.0,
    "spread_bps": 2.0,
    "expected_slippage_bps": 1.5
  }
}
```

**Результат TB Labeling (primary horizon 180s):**
```json
{
  "sid": "btc_20240101_120000_long",
  "primary": {
    "label": "TP",
    "hit_ms": 1704110580000,
    "ret_bps": 52.3,
    "r_mult": 1.046,
    "mae_bps": 8.2,
    "mfe_bps": 58.5,
    "adverse_proxy": 0.14,
    "y_edge": 1
  },
  "meta": {
    "tp_bps": 50.0,
    "sl_bps": 50.0,
    "exec_cost_r": 0.07,
    "util_r": 0.976
  }
}
```

**Интерпретация:**
- ✅ TP сработал через 180 секунд
- ✅ Прибыль: 52.3 bps (1.046 R)
- ✅ Просадка была небольшой (8.2 bps)
- ✅ Качество хорошее (adverse_proxy = 0.14 < 1.2)
- ✅ `y_edge = 1` → хороший сигнал для обучения

---

## 🚀 Производительность

- **Latency:** обработка происходит сразу после `due_ms` (минимальная задержка)
- **Throughput:** обрабатывает до 200 jobs за цикл
- **Memory:** использует Redis Streams (эффективное хранение)
- **Deduplication:** предотвращает повторную обработку через `tb:done:{sid}`

---

## 🔍 Мониторинг

```bash
# Проверить количество jobs в очереди
docker exec redis-worker-1 redis-cli ZCARD tb:jobs:due

# Проверить количество меток
docker exec redis-worker-1 redis-cli XLEN labels:tb

# Проверить последние метки
docker exec redis-worker-1 redis-cli XREVRANGE labels:tb + - COUNT 5
```

---

## 📝 Итоги

TB Labeler (v10.1) — это система маркировки сигналов на основе реального движения цены, которая:

1. **Читает сигналы** из `signals:of:inputs`
2. **Ставит в очередь** для обработки после горизонта
3. **Загружает тики** из `stream:tick_{SYMBOL}`
4. **Вычисляет Triple-Barrier** для каждого горизонта
5. **Публикует метки** в `labels:tb` для обучения ML

**Ключевое преимущество:** точные метки на основе реального движения цены, а не на основе исполнения сделок.

