# Touch-Level Tracker Integration (v2)

## Обзор

Реализован тонкий touch-level tracker v2 для анализа order flow на уровне best bid/ask.

**Улучшения v2:**
- Засчитывает Traded@touch даже если trade_price не ровно best (touch-band)
- Учитывает "снятие" best при ухудшении best-цены (ask поднялся / bid опустился)
- Остаётся "тонким" (только top1/top2/top3 из raw book)

## Что отслеживается

Для каждой стороны (BID и ASK) отдельно:

1. **Drop@touch (D)**: 
   - Падение видимого объёма на best level при той же цене
   - **v2**: Также учитывается "снятие" best при ухудшении цены (ask↑ / bid↓)
2. **Traded@touch (T)**: 
   - **v1**: Объём сделок, у которых trade_price ≈ best_price за окно W (500мс)
   - **v2**: Объём сделок в touch-band (в пределах N тиков от best) за окно W
3. **ρ = T / (D + eps)**: отношение traded к drop
4. **refill_lag**: время восстановления объёма после падения

## Теги

- `cancel`: D>0, но T≈0 (отмены без сделок)
- `depletion`: T>0, ρ не большой, refill не мгновенный (истощение)
- `refill`: ρ большой или refill_lag маленький (быстрое восстановление)
- `none`: нет активности
- `touch`: пограничный случай

## Компоненты

### 1. TouchLevelTracker v2 (`services/touch_level_tracker.py`)

Основной класс трекера:
- Хранит только best bid/ask (и top2/top3 для дебага)
- Считает Traded@touch и Drop@touch в rolling окне W
- Определяет теги refill/depletion/cancel

**Новые возможности v2:**
- `_is_touch_trade()` - проверка сделки в touch-band (не строго по best)
- Учёт "снятия best" при ухудшении цены (ask↑ / bid↓)
- Проверка свежести книги перед матчингом сделок

**Методы:**
- `on_book()` - обновление при изменении стакана (v2: учитывает снятие best)
- `on_trade()` - обновление при сделках (v2: использует touch-band)
- `get_last()` - получение последних статистик

### 2. Интеграция в BaseOrderFlowHandler

**Инициализация:**
- Включается через `TOUCH_LEVEL_ENABLED=true`
- Настраивается через переменные окружения

**Обработка:**
- `_process_book()`: кормит трекер сразу (не ждёт L2 flush)
- `_process_tick()`: кормит сделки для traded@touch

### 3. Поля в SignalContext

Добавлены поля:
```python
touch_ts: int = 0
touch_age_ms: int = 0
touch_is_stale: bool = True

touch_bid_tag: str = "none"
touch_bid_rho: float = 0.0
touch_bid_traded_w: float = 0.0
touch_bid_drop_w: float = 0.0
touch_bid_refill_lag_ms: int = 0

touch_ask_tag: str = "none"
touch_ask_rho: float = 0.0
touch_ask_traded_w: float = 0.0
touch_ask_drop_w: float = 0.0
touch_ask_refill_lag_ms: int = 0
```

### 4. Проброс в indicators/audit

Все поля доступны в:
- `indicators` словаре сигнала
- `audit_payload` для отладки

## Настройки

### Переменные окружения

```bash
# Включение трекера
TOUCH_LEVEL_ENABLED=true

# Параметры окна
TOUCH_WINDOW_MS=500          # Окно для rolling sum (мс)
TOUCH_TAU_REFILL_MS=250      # Порог для быстрого refill (мс)
TOUCH_RECOVER_FRAC=0.90      # Доля восстановления для refill (0.9 = 90%)

# Пороги для тегов
TOUCH_RHO_REFILL_MIN=1.5     # Минимальный ρ для refill
TOUCH_RHO_DEPLETION_MAX=1.5  # Максимальный ρ для depletion

# Staleness
TOUCH_MAX_STALE_MS=1500      # Максимальный возраст touch данных (мс)

# v2: Touch-band и свежесть книги
TOUCH_MAX_TOUCH_TICKS=1      # Максимальное количество тиков от best для touch-trade
TOUCH_BOOK_FRESH_MS=250      # Максимальный возраст книги для матчинга сделок к touch (мс)
```

## Использование

### 1. Включение

Установите в `docker-compose.yml`:
```yaml
- TOUCH_LEVEL_ENABLED=true
```

Перезапустите контейнер:
```bash
docker-compose up -d python-worker
```

### 2. Просмотр метрик

Метрики доступны в:
- `indicators` словаре каждого сигнала
- `audit_payload` для отладки

Пример:
```python
{
    "touch_bid_tag": "depletion",
    "touch_bid_rho": 0.8,
    "touch_ask_tag": "refill",
    "touch_ask_rho": 2.1,
    ...
}
```

### 3. Фильтрация сигналов (опционально)

Пример простого фильтра для breakout:

```python
if not ctx.touch_is_stale:
    if impulse_side == "LONG" and ctx.touch_ask_tag == "refill":
        return False  # LONG breakout: не хотим ASK refill
    if impulse_side == "SHORT" and ctx.touch_bid_tag == "refill":
        return False  # SHORT breakout: не хотим BID refill
```

## Мониторинг

### Проверка работы

1. **Логи:**
   При включении трекера в логах должно появиться:
   ```
   ✅ Touch-level tracker enabled (window=500ms)
   ```

2. **Audit payload:**
   Проверьте `touch_bid_tag` и `touch_ask_tag` в indicators:
   - Должны быть значения: `none`, `cancel`, `depletion`, `refill`, `touch`
   - `touch_is_stale` должен быть `false` при активной торговле

3. **Метрики:**
   - `touch_bid_rho` и `touch_ask_rho` - должны быть >= 0
   - `touch_bid_traded_w` и `touch_ask_traded_w` - объём сделок на touch
   - `touch_bid_drop_w` и `touch_ask_drop_w` - объём падений на touch

## Следующие шаги

1. **Сбор метрик:**
   - Включить `TOUCH_LEVEL_ENABLED=true`
   - Наблюдать за метриками в audit_payload
   - Проверить частоту появления refill vs depletion на событиях breakout/extreme

2. **Анализ:**
   - Есть ли рассинхрон (`touch_is_stale=true`)?
   - Как часто появляется refill vs depletion?
   - Коррелируют ли теги с качеством сигналов?

3. **Фильтрация (когда метрики стабильны):**
   - Добавить простой фильтр для breakout (см. пример выше)
   - Расширить фильтрацию для других типов сигналов

## Примечания

- Трекер работает независимо от L2 batching (кормится сразу при получении book)
- Использует только top3 уровней стакана (легковесный)
- Rolling window автоматически обновляется при новых данных
- Staleness проверяется аналогично L2 метрикам

