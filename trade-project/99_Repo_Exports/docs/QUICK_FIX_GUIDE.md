# ⚡ Быстрое руководство по исправлению Signal Tracker

## 🎯 Что было исправлено

### Основная проблема

**Signal Performance Tracker** должен был отправлять статистику каждые 3 часа, но **не работал вообще**.

### Найденные баги

1. ❌ Сервис не был запущен (отсутствовал в docker-compose.yml)
2. ❌ Неправильная загрузка конфигурации
3. ❌ Отсутствие обработки ошибок
4. ❌ Нет команд управления в Makefile

### Что исправлено

- ✅ Добавлен сервис в docker-compose.yml
- ✅ Создан конфиг-файл signal_tracker_config.json
- ✅ Исправлен код загрузки конфигурации
- ✅ Добавлено детальное логирование
- ✅ Добавлены Makefile команды
- ✅ Создан диагностический скрипт

---

## 🚀 Запуск исправленной системы

### Вариант 1: Полный перезапуск (рекомендуется)

```bash
# 1. Остановить все сервисы
make down

# 2. Запустить систему
make up-bg

# 3. Проверить статус трекера
make tracker-status
```

### Вариант 2: Добавить только новый сервис

```bash
# Запустить только signal-performance-tracker
docker-compose up -d signal-performance-tracker

# Проверить статус
make tracker-status
```

---

## 🔍 Проверка работы

### Шаг 1: Проверка всех 3 сервисов XAUUSD

```bash
make check-xauusd-services
```

**Ожидаемый результат:**

```
═══════════════════════════════════════════════════════════════
  ЧАСТЬ 1: Проверка контейнеров
═══════════════════════════════════════════════════════════════

🔍 Проверка: Multi-Symbol OrderFlow Handler
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ Контейнер запущен
   Статус: running
   Health: healthy
   Restarts: 0

🔍 Проверка: Aggregated Hub V2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ Контейнер запущен
   Статус: running
   Health: healthy
   Restarts: 0

🔍 Проверка: Signal Performance Tracker
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ Контейнер запущен
   Статус: running
   Health: healthy
   Restarts: 0

   📋 Последние логи:
      🚀 Запуск Signal Performance Tracker...
      ✅ Redis подключение установлено
      ✅ Все потоки запущены
      📊 Система мониторинга активна

═══════════════════════════════════════════════════════════════
  ИТОГОВАЯ СВОДКА
═══════════════════════════════════════════════════════════════

📊 Запущено сервисов: 4 из 4
✅ Все сервисы работают!
✅ Данные поступают корректно!

📊 Периодическая статистика будет отправлена через 3 часа
📊 Ежедневная сводка будет отправлена в 00:00 UTC
```

### Шаг 2: Проверка логов трекера

```bash
make tracker-logs
```

**Что вы должны увидеть:**

```
🔧 Загрузка конфигурации из: /app/python-worker/config/signal_tracker_config.json
✅ Конфигурация загружена из файла
📊 Символы: ['XAUUSD']
📊 Стратегии: ['orderflow', 'aggregated-hub']
📊 Периодические отчеты: True
📊 Интервал отчетов: 3ч
🚀 Запуск Signal Performance Tracker...
✅ Redis подключение установлено
🔧 Инициализация компонентов...
✅ Signal Performance Tracker инициализирован
🚀 Запуск Signal Performance Tracker...
✅ Все потоки запущены
🔄 Запущен цикл обработки сигналов
🔄 Запущен цикл обработки тиков
🔄 Запущен цикл периодических задач

# Каждые 60 секунд:
📊 Статус: Uptime 60s | Signals 5 | Ticks 450 | Open 1 | Closed 2 | Errors 0
📊 Статус: Uptime 120s | Signals 12 | Ticks 950 | Open 2 | Closed 4 | Errors 0
```

### Шаг 3: Проверка Redis streams

```bash
# Проверка сигналов orderflow
docker exec scanner-redis redis-cli XLEN signals:orderflow:XAUUSD

# Проверка тиков
docker exec scanner-redis redis-cli XLEN stream:tick_XAUUSD

# Проверка статистики
docker exec scanner-redis redis-cli KEYS "stats:*"
```

---

## 📊 Когда ожидать статистику

### Первая периодическая статистика

**Время**: Через 3 часа после запуска трекера

**Что будет в Telegram:**

```
📊 Периодическая сводка (3ч)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Strategy: orderflow
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
├─ Всего сделок: 25
├─ Прибыльных: 18 (72.0%)
├─ Убыточных: 7 (28.0%)
└─ Общий P&L: +$523.40

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Strategy: aggregated-hub
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
├─ Всего сделок: 15
├─ Прибыльных: 11 (73.3%)
├─ Убыточных: 4 (26.7%)
└─ Общий P&L: +$342.10

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ИТОГО
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Всего сделок: 40
Win Rate: 72.5%
Общий P&L: +$865.50
```

### Ежедневная статистика

**Время**: Каждый день в 00:00 UTC

**Содержание**: Детальный отчет за весь день

---

## 🛠️ Новые команды Makefile

### Статус трекера

```bash
make tracker-status
```

Показывает:

- Запущен ли контейнер
- Статус (running/stopped/restarting)
- Health status
- Количество перезапусков
- Последние 30 строк логов

### Логи трекера

```bash
make tracker-logs
```

Следит за логами в реальном времени (Ctrl+C для выхода)

### Перезапуск трекера

```bash
make tracker-restart
```

Перезапускает только Signal Performance Tracker (без остановки других сервисов)

### Проверка всех сервисов XAUUSD

```bash
make check-xauusd-services
```

Комплексная проверка:

- ✅ Статус всех 4 контейнеров
- ✅ Health checks
- ✅ Логи
- ✅ Redis streams
- ✅ Redis keys
- ✅ Telegram credentials
- ✅ Итоговая сводка

---

## 🔧 Настройка конфигурации

### Изменение интервала отчетов

Отредактируйте `python-worker/config/signal_tracker_config.json`:

```json
{
	"reporting": {
		"periodic_summary_enabled": true,
		"periodic_summary_interval_hours": 1 // Изменить с 3 на 1 час
	}
}
```

Перезапустите:

```bash
make tracker-restart
```

### Добавление новых символов

```json
{
	"streams": {
		"symbols": ["XAUUSD", "BTCUSD", "ETHUSD"], // Добавить символы
		"strategies": ["orderflow", "aggregated-hub"]
	}
}
```

### Отключение ежедневных отчетов

```json
{
	"reporting": {
		"daily_summary_enabled": false, // Отключить
		"periodic_summary_enabled": true
	}
}
```

---

## ⚠️ Troubleshooting

### Проблема: Ошибка Docker Compose Profiles

**Симптом**:

```
ERROR: Service "multi-symbol-orderflow" was pulled in as a dependency
but is not enabled by the active profiles.
```

**Причина**: Несовпадение профилей между зависимыми сервисами

**Решение**: ✅ УЖЕ ИСПРАВЛЕНО в docker-compose.yml

Добавлен профиль `default` к `signal-performance-tracker`:

```yaml
signal-performance-tracker:
  profiles:
    - default
```

**Что делать**:

```bash
# Просто запустите систему заново
make down
make up-bg
```

Подробнее: [HOTFIX_PROFILES.md](HOTFIX_PROFILES.md)

---

### Проблема: Трекер не запускается

```bash
# Проверьте логи
make tracker-logs

# Типичные ошибки:
# 1. Redis не подключен
# 2. Telegram credentials отсутствуют
# 3. Конфиг-файл не найден
```

**Решение:**

```bash
# Проверьте Redis
docker exec scanner-signal-tracker redis-cli -h scanner-redis ping

# Проверьте Telegram credentials
docker exec scanner-signal-tracker env | grep TELEGRAM

# Проверьте конфиг
docker exec scanner-signal-tracker cat /app/python-worker/config/signal_tracker_config.json
```

### Проблема: Нет сигналов

```bash
# Проверьте, что orderflow генерирует сигналы
docker exec scanner-redis redis-cli XLEN signals:orderflow:XAUUSD

# Проверьте aggregated-hub
docker exec scanner-redis redis-cli XLEN signals:aggregated-hub:XAUUSD

# Если 0, проверьте сервисы:
make orderflow-status
make hub-status
```

### Проблема: Нет отчетов в Telegram

```bash
# Проверьте credentials
docker exec scanner-signal-tracker env | grep TELEGRAM_BOT_TOKEN
docker exec scanner-signal-tracker env | grep TELEGRAM_CHAT_ID

# Если пусто или None, добавьте в docker-compose.yml переменные
# Или создайте .env файл:
cat > .env << EOF
TELEGRAM_BOT_TOKEN=your_token
TELEGRAM_CHAT_ID=your_chat_id
EOF

# Перезапустите
docker-compose down
docker-compose up -d
```

### Проблема: Трекер показывает 0 сигналов

```bash
# Проверьте consumer group
docker exec scanner-redis redis-cli XINFO GROUPS signals:orderflow:XAUUSD

# Если группа signal-tracker-group имеет большой lag, сбросьте:
docker exec scanner-redis redis-cli XGROUP SETID signals:orderflow:XAUUSD signal-tracker-group $

# Перезапустите трекер
make tracker-restart
```

---

## 📈 Мониторинг работы

### Real-time мониторинг

```bash
# Терминал 1: Логи трекера
make tracker-logs

# Терминал 2: Статус каждые 30 сек
watch -n 30 "make tracker-status"

# Терминал 3: Redis streams
watch -n 10 "docker exec scanner-redis redis-cli XLEN signals:orderflow:XAUUSD"
```

### Проверка через 3 часа

```bash
# Через 3 часа после запуска проверьте:

# 1. Логи трекера (должна быть запись об отправке)
make tracker-logs | grep "Отправка периодической сводки"

# 2. Telegram (должно прийти сообщение)

# 3. Redis статистика
docker exec scanner-redis redis-cli KEYS "stats:*"
```

---

## 📊 Ожидаемое поведение

### Сразу после запуска

```
[00:00] 🚀 Запуск Signal Performance Tracker...
[00:00] ✅ Redis подключение установлено
[00:00] 🔧 Инициализация компонентов...
[00:00] ✅ Signal Performance Tracker инициализирован
[00:00] 📊 Отслеживаемые символы: ['XAUUSD']
[00:00] 📊 Отслеживаемые стратегии: ['orderflow', 'aggregated-hub']
[00:00] 🔄 Запущен цикл обработки сигналов
[00:00] 🔄 Запущен цикл обработки тиков
[00:00] 🔄 Запущен цикл периодических задач
```

### Каждую минуту

```
[00:01] 📊 Статус: Uptime 60s | Signals 3 | Ticks 250 | Open 1 | Closed 0 | Errors 0
[00:02] 📊 Статус: Uptime 120s | Signals 7 | Ticks 510 | Open 2 | Closed 1 | Errors 0
[00:03] 📊 Статус: Uptime 180s | Signals 10 | Ticks 780 | Open 1 | Closed 3 | Errors 0
```

### Через 3 часа (10800 секунд)

```
[03:00] 📊 Отправка периодической сводки (интервал: 3ч)...
[03:00] 📊 Периодическая сводка отправлена в Telegram
```

### В 00:00 UTC каждый день

```
[00:00] 📊 Отправка ежедневной сводки...
[00:00] 📊 Ежедневная сводка отправлена в Telegram
```

---

## 🎓 Архитектура решения

### 3 сервиса обработки XAUUSD

#### 1. Multi-Symbol OrderFlow Handler

**Контейнер**: `scanner-multi-orderflow`

**Функции:**

- Читает тики из `stream:tick_XAUUSD`
- Анализирует Order Flow (delta, OBI, clusters, speed)
- Генерирует сигналы → `signals:orderflow:XAUUSD`

**Выход:**

```json
{
	"symbol": "XAUUSD",
	"side": "LONG",
	"confidence": 0.75,
	"entry": 2055.5,
	"sl": 2050.0,
	"tp1": 2060.0
}
```

#### 2. Aggregated Hub V2

**Контейнер**: `scanner-aggregated-hub`

**Функции:**

- Читает `signals:orderflow:XAUUSD` и `signals:ta:XAUUSD`
- Комбинирует с weighted confidence
- Применяет фильтры (cooldown, anti-dither)
- Отправляет в Go Gateway → `POST /orders/push`
- Уведомления → `notify:telegram`

**Выход:**

- HTTP POST в Go Gateway
- Сообщения в Telegram Bot
- Запись в `signals:aggregated-hub:XAUUSD` (опционально)

#### 3. Signal Performance Tracker (НОВЫЙ!)

**Контейнер**: `scanner-signal-tracker`

**Функции:**

- Читает все сигналы (`orderflow`, `aggregated-hub`, etc.)
- Создает виртуальные позиции
- Обновляет позиции по тикам
- Собирает статистику (win rate, P&L, drawdown)
- **Отправляет отчеты каждые 3 часа в Telegram**

**Выход:**

- Статистика в Redis: `stats:{strategy}:{symbol}:{tf}`
- Отчеты в Telegram
- Логирование метрик

### Data Flow

```
Ticks → OrderFlow Handler → signals:orderflow:XAUUSD
                                      │
                                      ├─→ Aggregated Hub → Gateway → Orders
                                      │                       │
                                      │                       └─→ Telegram Bot
                                      │
                                      └─→ Signal Tracker → Stats → Telegram Reports
                                                │                    (каждые 3ч)
                                                └─→ Redis (stats:*)
```

---

## 📝 Следующие шаги

### 1. Запустите систему

```bash
make down
make up-bg
```

### 2. Проверьте все сервисы

```bash
make check-xauusd-services
```

### 3. Следите за логами

```bash
make tracker-logs
```

### 4. Дождитесь первой статистики (через 3 часа)

**Вы получите в Telegram:**

- Статистику по всем стратегиям
- Win rate
- P&L
- Количество сделок

### 5. Если проблемы - смотрите секцию Troubleshooting

---

## ✅ Checklist

- [ ] Система остановлена (`make down`)
- [ ] Telegram credentials установлены
- [ ] Система запущена (`make up-bg`)
- [ ] Проверен статус трекера (`make tracker-status`)
- [ ] Проверены логи (`make tracker-logs`)
- [ ] Проверены сервисы XAUUSD (`make check-xauusd-services`)
- [ ] Все 4 сервиса запущены (multi-orderflow, aggregated-hub, signal-tracker, tick-ingest)
- [ ] Данные поступают в Redis streams
- [ ] Ожидание первой статистики (3 часа)

---

## 🎉 Готово!

Система исправлена и полностью готова к работе.

**Ключевые улучшения:**

- ✅ Signal Tracker теперь запущен
- ✅ Статистика будет отправляться каждые 3 часа
- ✅ Детальное логирование всех операций
- ✅ Удобные команды управления
- ✅ Диагностический скрипт

**Если возникнут проблемы:**

1. Запустите `make check-xauusd-services`
2. Проверьте логи `make tracker-logs`
3. Смотрите troubleshooting в этом документе

---

_Исправлено: 3 ноября 2025_  
_Senior Go/Python Developer + Senior Trading Systems Analyst_  
_40+ лет совместного опыта_ 🚀
