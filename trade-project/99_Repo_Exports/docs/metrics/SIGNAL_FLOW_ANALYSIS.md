# 🔍 Анализ потока сигналов - Signal Performance Tracker

## ❌ ПРОБЛЕМА НАЙДЕНА!

**signal_performance_tracker.py** НЕ получает сигналы от **aggregated_signal_hub_v2.py**

---

## 📊 Таблица потоков данных

### signal_performance_tracker.py ЧИТАЕТ из:

| Stream                      | Источник                                            | Статус        |
| --------------------------- | --------------------------------------------------- | ------------- |
| `signals:orderflow:XAUUSD`  | xau_orderflow_handler + xauusd_orderflow_handler_v2 | ✅            |
| `signals:ta:XAUUSD`         | signal-generator                                    | ✅            |
| `notify:telegram`           | все компоненты                                      | ✅            |
| `signals:aggregated:XAUUSD` | -                                                   | ❌ НЕ ЧИТАЕТ! |

**Код:** `signal_performance_tracker.py:223-227`

```python
for strategy in self.strategies:  # ["orderflow", "ta"]
    for symbol in self.symbols:
        stream_name = f"signals:{strategy}:{symbol}"
        signal_streams.append(stream_name)

# ✅ ДОБАВЛЯЕМ notify:telegram для OrderFlow сигналов
signal_streams.append("notify:telegram")
```

---

### Компоненты ПУБЛИКУЮТ в:

#### 1. **aggregated_signal_hub_v2.py**

**Через:** `FilteredSignalWriter.write_and_push()`

| Stream                         | Строка кода                   | Формат        |
| ------------------------------ | ----------------------------- | ------------- |
| ❌ `signals:aggregated:XAUUSD` | filtered_signal_writer.py:187 | JSON payload  |
| ✅ `notify:telegram`           | filtered_signal_writer.py:179 | Redis payload |

**Проблема:** Публикует в `signals:aggregated:XAUUSD`, но **tracker НЕ ЧИТАЕТ этот stream!**

---

#### 2. **xau_orderflow_handler.py** (legacy)

| Stream                        | Строка кода                    | Формат           |
| ----------------------------- | ------------------------------ | ---------------- |
| ✅ `signals:orderflow:XAUUSD` | xau_orderflow_handler.py:1013  | `{"data": json}` |
| ✅ `notify:telegram`          | xau_orderflow_handler.py:992   | Redis payload    |
| `signals:audit:XAUUSD`        | xau_orderflow_handler.py:1029+ | Audit            |

**Статус:** ✅ Tracker ЧИТАЕТ `signals:orderflow:XAUUSD`

---

#### 3. **xauusd_orderflow_handler_v2.py**

**Через:** `BaseOrderFlowHandler._publish_signal()`

| Stream                        | Строка кода                   | Формат           |
| ----------------------------- | ----------------------------- | ---------------- |
| ✅ `signals:orderflow:XAUUSD` | base_orderflow_handler.py:782 | `{"data": json}` |
| ✅ `notify:telegram`          | base_orderflow_handler.py:765 | Redis payload    |

**Статус:** ✅ Tracker ЧИТАЕТ `signals:orderflow:XAUUSD`

---

#### 4. **signal-generator/signal_generator.py**

| Stream                 | Строка кода             | Формат           |
| ---------------------- | ----------------------- | ---------------- |
| ✅ `signals:ta:XAUUSD` | signal_generator.py:598 | `{"data": json}` |
| ✅ `notify:telegram`   | signal_generator.py:618 | Redis payload    |
| `signals:audit:XAUUSD` | signal_generator.py:649 | Audit            |

**Статус:** ✅ Tracker ЧИТАЕТ `signals:ta:XAUUSD`

---

## 🔧 РЕШЕНИЕ

### Вариант 1: ✅ РЕКОМЕНДУЕТСЯ - Добавить "aggregated" в strategies

**Изменить:** `signal_performance_tracker.py`

```python
# ДО (строка 103):
self.strategies = streams_cfg.get("strategies", ["orderflow", "ta"])

# ПОСЛЕ:
self.strategies = streams_cfg.get("strategies", ["orderflow", "ta", "aggregated"])
```

**ENV переменная:**

```yaml
STRATEGIES=orderflow,ta,aggregated # Добавить "aggregated"
```

**Преимущества:**

- ✅ Минимальное изменение (1 строка)
- ✅ Консистентная архитектура
- ✅ Легко расширяется для других стратегий

---

### Вариант 2: Изменить aggregated_signal_hub_v2 чтобы публиковал в другой stream

**Изменить:** `core/filtered_signal_writer.py:187`

```python
# ДО:
signal_stream = f"signals:aggregated:{symbol}"

# ПОСЛЕ (выбрать один):
signal_stream = f"signals:orderflow:{symbol}"  # Если больше OrderFlow
# ИЛИ
signal_stream = f"signals:ta:{symbol}"  # Если больше TA
```

**Недостатки:**

- ❌ Смешивание разных типов сигналов в одном stream
- ❌ Сложнее различить источник сигнала
- ❌ Не масштабируется

---

## 📋 Детальная диагностика

### Текущая ситуация:

```
┌────────────────────────────────────────────────────────┐
│         aggregated_signal_hub_v2.py                    │
│    (FilteredSignalWriter.write_and_push)               │
└─────────────────────┬──────────────────────────────────┘
                      │
         ┌────────────┴────────────┐
         ▼                         ▼
┌──────────────────┐     ┌─────────────────────┐
│ notify:telegram  │     │ signals:aggregated: │  ❌ НЕ ЧИТАЕТСЯ!
│                  │     │      XAUUSD         │
└────────┬─────────┘     └─────────────────────┘
         │
         │ ✅ ЧИТАЕТСЯ tracker'ом
         ▼
┌──────────────────────────────────────────────────┐
│    signal_performance_tracker.py                 │
│    Consumer group: signal-tracker-group          │
└──────────────────────────────────────────────────┘
```

### После исправления (Вариант 1):

```
┌────────────────────────────────────────────────────────┐
│         aggregated_signal_hub_v2.py                    │
│    (FilteredSignalWriter.write_and_push)               │
└─────────────────────┬──────────────────────────────────┘
                      │
         ┌────────────┴────────────┐
         ▼                         ▼
┌──────────────────┐     ┌─────────────────────┐
│ notify:telegram  │     │ signals:aggregated: │  ✅ ТЕПЕРЬ ЧИТАЕТСЯ!
│                  │     │      XAUUSD         │
└────────┬─────────┘     └──────────┬──────────┘
         │                          │
         │ ✅                       │ ✅
         ▼                          ▼
┌──────────────────────────────────────────────────┐
│    signal_performance_tracker.py                 │
│    strategies: ["orderflow", "ta", "aggregated"] │
│    Consumer group: signal-tracker-group          │
└──────────────────────────────────────────────────┘
```

---

## 🔍 Проверка текущего состояния

### 1. Проверить какие streams существуют:

```bash
docker exec scanner-redis-worker-1 redis-cli KEYS "signals:*"
```

Ожидаемый вывод:

```
signals:orderflow:XAUUSD
signals:ta:XAUUSD
signals:aggregated:XAUUSD  ← Этот stream создается, но не читается!
signals:audit:XAUUSD
```

### 2. Проверить consumer groups:

```bash
docker exec scanner-redis-worker-1 redis-cli XINFO GROUPS signals:aggregated:XAUUSD
```

Вывод покажет:

```
name
signal-tracker-group  ← НЕ БУДЕТ! (group не создана)
```

### 3. Проверить длину streams:

```bash
docker exec scanner-redis-worker-1 redis-cli XLEN signals:aggregated:XAUUSD
docker exec scanner-redis-worker-1 redis-cli XLEN signals:orderflow:XAUUSD
docker exec scanner-redis-worker-1 redis-cli XLEN signals:ta:XAUUSD
```

---

## ✅ Рекомендуемые действия (Senior Developer)

### Шаг 1: Добавить "aggregated" в strategies

**Файл:** `python-worker/services/signal_performance_tracker.py`
**Строка:** 103

```python
self.strategies = streams_cfg.get("strategies", ["orderflow", "ta", "aggregated"])
```

### Шаг 2: Обновить docker-compose.yml

**Файл:** `docker-compose.yml`
**Секция:** `signal-performance-tracker`

```yaml
environment:
  - STRATEGIES=orderflow,ta,aggregated # ← Добавить aggregated
```

### Шаг 3: Обновить config файл (если используется)

**Файл:** `python-worker/config/signal_tracker_config.json`

```json
{
	"streams": {
		"symbols": ["XAUUSD"],
		"strategies": ["orderflow", "ta", "aggregated"]
	}
}
```

### Шаг 4: Перезапустить tracker

```bash
docker-compose restart signal-performance-tracker
```

### Шаг 5: Проверить логи

```bash
docker logs -f scanner-signal-tracker | grep -E "(aggregated|Consumer group)"
```

Ожидаемый вывод:

```
✅ Consumer group created for signals:aggregated:XAUUSD
Listening to 3 signal streams:
   - signals:orderflow:XAUUSD
   - signals:ta:XAUUSD
   - signals:aggregated:XAUUSD  ← ДОЛЖЕН ПОЯВИТЬСЯ!
   - notify:telegram
```

---

## 📊 Summary

### Сейчас:

- ❌ `aggregated_signal_hub_v2` публикует в `signals:aggregated:XAUUSD`
- ❌ `signal_performance_tracker` НЕ ЧИТАЕТ этот stream
- ✅ `signal_performance_tracker` ЧИТАЕТ `signals:orderflow:XAUUSD` и `signals:ta:XAUUSD`

### После исправления:

- ✅ `aggregated_signal_hub_v2` публикует в `signals:aggregated:XAUUSD`
- ✅ `signal_performance_tracker` ЧИТАЕТ все 3 strategy streams
- ✅ Все сигналы отслеживаются

---

**Дата:** 2025-11-05  
**Senior Developer + Trading Analyst**  
**Статус:** ❌ ПРОБЛЕМА НАЙДЕНА - ТРЕБУЕТСЯ ИСПРАВЛЕНИЕ
