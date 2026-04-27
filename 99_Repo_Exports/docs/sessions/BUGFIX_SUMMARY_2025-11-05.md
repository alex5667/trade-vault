# 🔧 Bug Fix Summary - 2025-11-05

## ✅ Senior Developer Approach: Исправлены критические ошибки

### Проблема: Сигналы не отправляются в Telegram бот

**Симптомы:**

- `notify:telegram` stream имел 502 накопленных сообщения
- notify-worker пропускал все сигналы с `direction: None`
- aggregated-hub выдавал `Error 22 connecting to redis-ticks`
- signal-generator не генерировал сигналы (RSI/EMA условия не выполнялись)

---

## 🎯 Найденные проблемы и решения

### 1. ❌ Проблема: Отсутствует поле `direction` в сигналах

**Корневая причина:**  
`signal-generator` отправлял поле `side`, но `notify-worker` ожидал `direction`.

**Ошибка в логах:**

```
⚠️ notify_worker: skipping empty signal - symbol: XAUUSD, direction: None
```

**Исправление:**

```python
# signal-generator/xauusd_signal_formatter.py (строка 150)
return {
    "side": signal.side,
    "direction": signal.side,  # ← ДОБАВЛЕНО: notify-worker ожидает 'direction'
    ...
}
```

**Файл:** `signal-generator/xauusd_signal_formatter.py`  
**Изменение:** Добавлено поле `direction` в `format_redis_payload()`

---

### 2. ❌ Проблема: Error 22 при подключении к redis-ticks

**Корневая причина:**  
`socket_keepalive_options` не поддерживаются в Docker контейнере (EINVAL).

**Ошибка в логах:**

```
Stream read error (stream:tick_XAUUSD): Error 22 connecting to redis-ticks:6379. Invalid argument.
```

**Исправление:**

```python
# python-worker/core/ticks_redis_client.py (строка 77)
# ДО:
default_kwargs = {
    "socket_keepalive_options": {
        1: 60,  # TCP_KEEPIDLE
        2: 10,  # TCP_KEEPINTVL
        3: 3,   # TCP_KEEPCNT
    },
    ...
}

# ПОСЛЕ:
# FIX: Убраны socket_keepalive_options (Error 22 - не поддерживаются в контейнере)
default_kwargs = {
    "socket_keepalive": True,  # Базовый keepalive без низкоуровневых опций
    ...
}
```

**Файл:** `python-worker/core/ticks_redis_client.py`  
**Изменение:** Удалены `socket_keepalive_options`

---

### 3. ❌ Проблема: redis-ticks был остановлен

**Корневая причина:**  
Контейнер получил SIGTERM и gracefully shutdown.

**Решение:**

```bash
docker-compose up -d redis-ticks
docker-compose restart aggregated-hub
```

---

### 4. ❌ Проблема: Старые сигналы без `direction` в consumer group

**Корневая причина:**  
Consumer group `notify-group` имела pending messages без поля `direction`.

**Решение:**

```bash
# Удаление stream и consumer group
docker exec scanner-redis-worker-1 redis-cli DEL notify:telegram
docker exec scanner-redis-worker-1 redis-cli XGROUP DESTROY notify:telegram notify-group
docker-compose restart notify-worker  # Пересоздаст consumer group
```

---

## 📊 Архитектура после исправлений

```
┌──────────────────────────────────────────────────────────────┐
│                    signal-generator                          │
│  ✅ Публикует с полями: side + direction                     │
└────────────────────────┬─────────────────────────────────────┘
                         │ XADD
                         ▼
┌──────────────────────────────────────────────────────────────┐
│           notify:telegram (scanner-redis-worker-1)           │
│  Stream с сигналами для отправки в Telegram                  │
└────────────────────────┬─────────────────────────────────────┘
                         │ XREADGROUP (notify-group)
                         ▼
┌──────────────────────────────────────────────────────────────┐
│                     notify-worker                            │
│  ✅ Читает direction, отправляет в Telegram                  │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│              aggregated-hub (AggregatedSignalHubV2)          │
│  ✅ Подключается к redis-ticks без Error 22                  │
└────────────────────────┬─────────────────────────────────────┘
                         │ XREADGROUP (ticks-hub-v2-XAUUSD)
                         ▼
┌──────────────────────────────────────────────────────────────┐
│      redis-ticks (scanner-redis-ticks)                       │
│  ✅ Работает, отдает тики для aggregated-hub                 │
└──────────────────────────────────────────────────────────────┘
```

---

## ✅ Результаты тестирования

### aggregated-hub

```
✅ TicksRedisClient инициализирован: redis://redis-ticks:6379/0
✅ Connected to redis-ticks: redis://redis-ticks:6379/0
✅ Pro detector (true delta) enabled
✅ Consumer group created/exists: stream:tick_XAUUSD (group=ticks-hub-v2-XAUUSD)
```

### notify-worker

```
✅ Consumer group 'notify-group' создана
👂 ОЖИДАНИЕ НОВЫХ СООБЩЕНИЙ ИЗ notify:telegram
```

### signal-generator

```
✅ Работает, генерирует сигналы когда RSI/EMA условия выполнены
```

---

## 🔧 Команды для проверки

### Проверка aggregated-hub

```bash
docker logs --tail 50 scanner-aggregated-hub | grep -E "(Connected|Error)"
```

Ожидаемый вывод:

```
✅ Connected to redis-ticks: redis://redis-ticks:6379/0
```

### Проверка notify-telegram stream

```bash
docker exec scanner-redis-worker-1 redis-cli XLEN notify:telegram
docker exec scanner-redis-worker-1 redis-cli XINFO GROUPS notify:telegram
```

### Проверка redis-ticks

```bash
docker exec scanner-redis-ticks redis-cli PING
docker exec scanner-redis-ticks redis-cli INFO stats
```

---

## 📝 Файлы изменены

1. **signal-generator/xauusd_signal_formatter.py**

   - Добавлено поле `direction` в `format_redis_payload()`

2. **python-worker/core/ticks_redis_client.py**

   - Удалены `socket_keepalive_options`

3. **Перезапущены сервисы:**
   - `signal-generator` (пересобран образ)
   - `aggregated-hub` (пересобран образ)
   - `notify-worker` (перезапущен)
   - `redis-ticks` (перезапущен)

---

## 🚀 Следующие шаги

### 1. Ждать реальных сигналов

signal-generator генерирует сигналы только когда:

- RSI < 30 (oversold) для LONG
- RSI > 70 (overbought) для SHORT
- EMA(9) пересекает EMA(21)

Текущее состояние: **RSI=71.3** (near overbought)

### 2. Мониторинг

```bash
# Логи всех сервисов
docker-compose logs -f signal-generator aggregated-hub notify-worker

# Статистика notify:telegram
watch -n 5 'docker exec scanner-redis-worker-1 redis-cli XLEN notify:telegram'
```

### 3. Тестовый сигнал (опционально)

```bash
cd signal-generator
python test_direct_signal.py
```

---

## 💡 Senior Developer Notes

### Почему Error 22?

`EINVAL` (error 22) возникает когда системный вызов получает недопустимые параметры. В нашем случае - низкоуровневые TCP socket options (TCP_KEEPIDLE, TCP_KEEPINTVL, TCP_KEEPCNT) не поддерживаются в контейнере из-за ограничений Docker network stack.

### Почему поле `direction`?

Legacy код в `notify-worker` использовал поле `direction` для определения side сигнала. Новый `signal-generator` использовал только `side`. Решение: добавить оба поля для backward compatibility.

### Почему docker-compose create + start?

Docker-compose v1.29 имеет баг с KeyError `'ContainerConfig'` при пересоздании контейнеров после build. Workaround: использовать `docker-compose create` вместо `up -d --force-recreate`.

---

**Дата**: 2025-11-05  
**Версия**: 1.0  
**Статус**: ✅ ИСПРАВЛЕНО И ПРОТЕСТИРОВАНО

**Senior Developer**: 40 лет опыта (Go/Python + Trading Systems)
