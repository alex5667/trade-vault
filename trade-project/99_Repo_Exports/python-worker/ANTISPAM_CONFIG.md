# 🛡️ Антиспам конфигурация для XAUUSD сигналов

## 📊 Текущие настройки

Все 3 сервиса обработки XAUUSD используют **антиспам режим 60 секунд (1 минута)**.

### Сервисы

| Сервис                         | Файл                                      | Интервал | ENV переменная            |
| ------------------------------ | ----------------------------------------- | -------- | ------------------------- |
| **OrderFlow Handler (legacy)** | `handlers/xau_orderflow_handler.py`       | 60 сек   | `XAU_MIN_SIGNAL_INTERVAL` |
| **OrderFlow Handler V2**       | `handlers/xauusd_orderflow_handler_v2.py` | 60 сек   | `XAU_MIN_SIGNAL_INTERVAL` |
| **Aggregated Hub V2**          | `aggregated_signal_hub_v2.py`             | 60 сек   | `HUB_MIN_SIG_INT_SEC`     |

---

## 🎯 Как работает

### Логика антиспама

```python
# В каждом сервисе
if current_timestamp - last_signal_timestamp < min_signal_interval_sec * 1000:
    # Пропускаем сигнал - слишком рано
    return
```

**Результат:**

- ✅ Максимум 1 сигнал в минуту от каждого сервиса
- ✅ Предотвращает спам одинаковых сигналов
- ✅ Снижает нагрузку на Telegram и MT5

---

## ⚙️ Изменение интервала

### Метод 1: Через переменные окружения (рекомендуется)

```bash
# В docker-compose.yml или .env
export XAU_MIN_SIGNAL_INTERVAL=60      # OrderFlow Handler
export HUB_MIN_SIG_INT_SEC=60          # Aggregated Hub

# Перезапуск
docker-compose restart scanner-signal-hub scanner-python-worker scanner-multi-orderflow
```

### Метод 2: Через код (требует пересборки)

**xau_orderflow_handler.py:**

```python
"min_signal_interval_sec": int(os.getenv("XAU_MIN_SIGNAL_INTERVAL", "60")),
```

**aggregated_signal_hub_v2.py:**

```python
min_signal_interval_sec: int = int(os.getenv("HUB_MIN_SIG_INT_SEC", "60"))
```

**xauusd_orderflow_handler_v2.py:**

Использует `instrument_config.py`:

```python
min_signal_interval_sec: int = 60  # В OrderFlowConfig
```

---

## 📋 Рекомендуемые значения

| Режим                    | Интервал | Использование                                      |
| ------------------------ | -------- | -------------------------------------------------- |
| **Агрессивный**          | 30 сек   | Высоковолатильный рынок, много возможностей        |
| **Стандартный**          | 60 сек   | ✅ **Текущий** - Баланс между частотой и качеством |
| **Консервативный**       | 120 сек  | Спокойный рынок, высокое качество сигналов         |
| **Очень консервативный** | 180 сек  | Только лучшие возможности                          |

---

## 🔍 Проверка работы

### 1. Проверка текущих настроек

```bash
# Проверить настройки в логах при запуске
docker logs scanner-signal-hub 2>&1 | grep "Min signal interval"
```

**Должно показать:**

```
Min signal interval: 60s
```

### 2. Мониторинг частоты сигналов

```bash
# Следить за сигналами в реальном времени
redis-cli XREVRANGE "notify:telegram" + - COUNT 20

# Проверить временные метки - разница должна быть >= 60 секунд
```

### 3. Python скрипт проверки

```python
import redis
import time

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Получить последние 10 сигналов
messages = r.xrevrange("notify:telegram", count=10)

print("📊 Последние сигналы:\n")

timestamps = []
for msg_id, fields in messages:
    # Извлекаем timestamp из ID (формат: timestamp-sequence)
    ts_str = msg_id.split('-')[0]
    ts = int(ts_str) / 1000  # Конвертируем в секунды

    symbol = fields.get('symbol', 'N/A')
    direction = fields.get('direction', 'N/A')

    print(f"• {time.strftime('%H:%M:%S', time.localtime(ts))} - {symbol} {direction}")
    timestamps.append(ts)

# Проверка интервалов
print("\n⏱️  Интервалы между сигналами:")
for i in range(len(timestamps) - 1):
    interval = timestamps[i] - timestamps[i+1]
    status = "✅" if interval >= 60 else "⚠️"
    print(f"{status} {interval:.0f} сек")
```

---

## 🧪 Тестирование

### Быстрый тест

```bash
# 1. Перезапустить сервисы
make restart

# 2. Следить за сигналами
watch -n 5 'redis-cli XLEN notify:telegram'

# 3. Проверить временные метки
redis-cli XREVRANGE notify:telegram + - COUNT 5
```

**Ожидаемый результат:**

- Сигналы появляются не чаще 1 раз в минуту
- В логах нет "spam" сообщений

---

## 🔧 Переопределение через ENV

### docker-compose.yml

```yaml
services:
  scanner-signal-hub:
    environment:
      - HUB_MIN_SIG_INT_SEC=60 # Aggregated Hub

  scanner-python-worker:
    environment:
      - XAU_MIN_SIGNAL_INTERVAL=60 # OrderFlow Handler (legacy)

  scanner-multi-orderflow:
    environment:
      - XAU_MIN_SIGNAL_INTERVAL=60 # OrderFlow Handler V2
```

### .env файл

```bash
# Антиспам для XAUUSD сигналов (секунды)
HUB_MIN_SIG_INT_SEC=60
XAU_MIN_SIGNAL_INTERVAL=60
```

---

## 📈 Влияние на производительность

### До изменения (180 сек для Hub)

- Aggregated Hub: ~20 сигналов/час
- OrderFlow V2: ~60 сигналов/час
- **Итого:** ~80 сигналов/час

### После изменения (60 сек для всех)

- Aggregated Hub: ~60 сигналов/час
- OrderFlow V2: ~60 сигналов/час
- **Итого:** ~120 сигналов/час

**Но:** Дублирующиеся сигналы (от разных сервисов в один момент) будут фильтроваться на уровне TradeMonitor или MT5.

---

## ⚠️ Важно

1. **Антиспам работает ПО СЕРВИСАМ:**

   - Каждый сервис независимо отслеживает свой `last_signal_ts`
   - Если 2 сервиса генерят сигнал одновременно → оба пройдут

2. **Дополнительная фильтрация:**

   - Используйте дедупликацию в TradeMonitor
   - Настройте фильтры на уровне go-gateway
   - Используйте threshold tuning (Analytics v2.0)

3. **Мониторинг:**
   - Следите за количеством сигналов в час
   - Проверяйте качество (winrate) через Signal Performance Tracker
   - Анализируйте ROC/AUC метрики

---

## 📚 Связанная документация

- `python-worker/services/README_SIGNAL_TRACKER.md` - Signal Performance Tracker
- `python-worker/analytics/ANALYTICS_V2_README.md` - Threshold Tuning
- `telegram-worker/TELEGRAM_CHANNELS_FIX.md` - Telegram каналы

---

**Антиспам режим активирован для всех сервисов!** 🛡️
