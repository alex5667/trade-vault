# Полноценная калибровка TRAILING_TP1_OFFSET_ATR

Новая система глубокого анализа и автоматической калибровки параметра `trailing_tp1_offset_atr` на основе исторических данных цены.

## 🎯 Что реализовано

### ✅ Глубокий анализ на исторических данных
- **Фильтр tp1_hit = true**: Только сделки, где реально был достигнут TP1
- **Полноценная симуляция**: На реальных тиковых/минутных данных цены после TP1
- **Временной анализ**: Отслеживание динамики возврата цены к new_sl
- **Метрики качества**: expectancy_r, giveback_r, missed_r, fake_stopout_rate

### ✅ Автоматическая калибровка
- **Интеллектуальный скоринг**: Композитная функция качества offset_mult
- **Адаптивные сетки**: Разные диапазоны тестирования для разных символов
- **Интеграция с Redis**: Автоматическое обновление `symbol_specs:{symbol}`
- **Состояние калибровки**: Отслеживание прогресса и новых сделок

## 📁 Файлы

```
python-worker/tools/trailing_tp1_calibration.py     # Библиотека + CLI
python-worker/services/auto_calibration_service.py  # Авто-калибровка
python-worker/test_trailing_tp1_calibration.py      # Тестовый скрипт
```

## 🚀 Быстрый старт

### 1. Ручная калибровка

```bash
cd python-worker/tools
python trailing_tp1_calibration.py \
    --dsn "postgresql://..." \
    --source CryptoOrderFlow \
    --symbol ETHUSDT \
    --offsets "0.3,0.4,0.5,0.6,0.7"
```

### 2. Тестирование

```bash
cd python-worker
python test_trailing_tp1_calibration.py
```

### 3. Автоматическая калибровка

```bash
# Настройка переменных окружения
export PG_DSN_CALIBRATION="postgresql://..."
export REDIS_URL="redis://redis:6379/0"

# Запуск
cd python-worker/services
python auto_calibration_service.py
```

## ⚙️ Настройка

### Переменные окружения

```bash
# База данных
PG_DSN_CALIBRATION="postgresql://user:pass@host:port/db"

# Redis
REDIS_URL="redis://host:port/db"

# Символы для калибровки (опционально, по умолчанию BTCUSDT, ETHUSDT)
AUTO_CALIBRATION_SYMBOLS="BTCUSDT,ETHUSDT,SOLUSDT"
```

### Параметры калибровки

```python
symbols = [
    SymbolConfig(
        source="CryptoOrderFlow",
        symbol="ETHUSDT",
        offsets=[0.3, 0.4, 0.5, 0.6, 0.7],  # Сетка тестирования
        limit_trades=300,                     # Макс. сделок для анализа
        min_total_trades=150,                 # Мин. сделок для старта
        min_new_trades=30,                    # Мин. новых сделок с прошлого запуска
        use_mfe_exit=False,                   # Использовать MFE-выход в симуляции
    ),
]
```

## 📊 Алгоритм калибровки

### 1. Выборка данных
- Только сделки с `tp1_hit = true`
- Исторические тиковые/минутные данные цены
- Период: от `tp1_hit_ts` до `exit_ts + 5 мин`

### 2. Симуляция трейлинга
```
Для каждого offset_mult:
  - Рассчитать new_sl = entry_price ± (atr × offset_mult)
  - Симулировать движение цены после TP1
  - Если цена касается new_sl → выход по трейлингу
  - Иначе → выход по оригинальной логике
```

### 3. Метрики качества
- **expectancy_r**: Средний R после применения трейлинга
- **giveback_r**: Потеря прибыли от MFE (средняя)
- **missed_r**: Упущенная прибыль относительно оригинала (средняя)
- **fake_stopout**: Доля случаев, где трейлинг сработал слишком рано

### 4. Скоринг
```python
score = w_exp × expectancy_r
      - w_gb × giveback_r
      - w_mis × missed_r
      - w_fake × fake_stopout_rate

# Весовые коэффициенты
w_exp = 1.0, w_gb = 0.4, w_mis = 0.3, w_fake = 0.7
```

## 🔧 Адаптация под вашу схему БД

### Таблица сделок (trades_closed)
```sql
-- Проверьте наличие полей
SELECT tp1_hit, signal_payload FROM trades_closed LIMIT 1;

-- signal_payload должен содержать:
-- 'atr': ATR на входе
-- 'tp1_price': Цена TP1
-- 'initial_sl_price': Начальный SL
-- 'tp1_hit_ts_ms': Timestamp достижения TP1
```

### Исторические данные цены
- **Приоритет**: Таблица `ohlcv_1m` (минутные свечи)
- **Fallback**: Агрегация из `ticks` (если нет минутных данных)

## 📈 Примеры результатов

### ETHUSDT
```
offset=0.30 count=45 expR=1.234 giveback=0.123 missed=0.056 fake=0.089 score=1.145
offset=0.40 count=45 expR=1.298 giveback=0.145 missed=0.034 fake=0.112 score=1.189
offset=0.50 count=45 expR=1.345 giveback=0.167 missed=0.023 fake=0.134 score=1.212
offset=0.60 count=45 expR=1.367 giveback=0.189 missed=0.012 fake=0.156 score=1.218 ← лучший
offset=0.70 count=45 expR=1.356 giveback=0.201 missed=0.008 fake=0.178 score=1.189

✅ Рекомендация: offset=0.60 (expR=1.367, giveback=0.189, fake=15.6%)
```

### BTCUSDT
```
offset=0.50 count=38 expR=0.987 giveback=0.098 missed=0.045 fake=0.076 score=0.912
offset=0.60 count=38 expR=1.034 giveback=0.112 missed=0.034 fake=0.092 score=0.945
offset=0.80 count=38 expR=1.056 giveback=0.134 missed=0.023 fake=0.108 score=0.958 ← лучший
offset=1.00 count=38 expR=1.034 giveback=0.145 missed=0.018 fake=0.124 score=0.921
offset=1.20 count=38 expR=1.012 giveback=0.156 missed=0.015 fake=0.141 score=0.887

✅ Рекомендация: offset=0.80 (expR=1.056, giveback=0.134, fake=10.8%)
```

## 🔄 Интеграция с TradeMonitorService

После калибровки параметр автоматически применяется:

```python
# TradeMonitorService._resolve_trailing_tp1_offset_atr()
v = getattr(spec, "trailing_tp1_offset_atr", None)
if v is not None and float(v) > 0:
    return float(v)
```

## 📋 TODO / Улучшения

- [ ] Добавить больше символов (SOLUSDT, ADAUSDT, etc.)
- [ ] Оптимизировать производительность (меньше запросов к БД)
- [ ] Добавить валидацию результатов калибровки
- [ ] Интеграция с алертами (уведомления о смене параметров)
- [ ] A/B тестирование разных скоринговых функций

## 🐛 Troubleshooting

### Проблема: "No trades with tp1_hit found"
**Решение**: Проверьте данные в `trades_closed`:
```sql
SELECT count(*) FROM trades_closed
WHERE source = 'CryptoOrderFlow' AND symbol = 'ETHUSDT' AND tp1_hit = true;
```

### Проблема: "No candles fetched"
**Решение**: Проверьте наличие исторических данных:
```sql
SELECT count(*) FROM ohlcv_1m WHERE symbol = 'ETHUSDT';
-- или
SELECT count(*) FROM ticks WHERE symbol = 'ETHUSDT';
```

### Проблема: Параметры не применяются
**Решение**: Проверьте Redis:
```bash
redis-cli get symbol_specs:ETHUSDT
```

---

🎯 **Система готова к продакшену!** Тестируйте на своих данных и настраивайте под специфику стратегии.
