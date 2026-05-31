# 📊 Быстрая отправка отчетов в Telegram

**Дата**: 2025-11-06  
**Статус**: ✅ Работает

---

## 🚀 Быстрая команда

```bash
make send-real-report
```

Эта команда:

1. ✅ Отправит тестовое сообщение
2. ✅ Отправит полную ежедневную сводку
3. ✅ Отправит детальные отчеты по всем стратегиям

---

## 📊 Что отправляется

### 1. Тестовое сообщение

Проверка работоспособности системы

### 2. Ежедневная сводка

- Общие показатели (сделки, winrate, P/L)
- По каждой стратегии (orderflow, ta)
- TP метрики (TP1, TP2, TP3)
- Упущенная прибыль (TP→SL)
- Разбивка по источникам

### 3. Детальные отчеты

- orderflow:XAUUSD:tick
- orderflow:BTCUSD:tick (если есть данные)
- orderflow:ETHUSD:tick (если есть данные)
- ta:XAUUSD:tick (если есть данные)

---

## 🔍 Проверка доставки

### 1. Проверить Redis stream

```bash
docker exec -it scanner-redis redis-cli XLEN notify:telegram
```

Если 0 - значит все сообщения обработаны ✅

### 2. Проверить логи notify-worker

```bash
docker logs scanner-notify-worker --tail 50 | grep -E "report|сводка"
```

Должны видеть отправленные отчеты

### 3. Проверить Telegram бота

Отчеты должны прийти в чат в течение нескольких секунд

---

## 🛠️ Альтернативные способы

### Вариант 1: Через Python напрямую

```bash
docker exec scanner-aggregated-hub python3 /app/test_send_report.py
```

### Вариант 2: Вручную из Python

```bash
# Войти в контейнер
docker exec -it scanner-aggregated-hub python3

# Выполнить
from services.reporting_service import ReportingService
reporting = ReportingService(redis_url="redis://scanner-redis:6379/0")
reporting.send_daily_summary(include_sources=True)
```

---

## 📈 Пример отчета

```
📅 Ежедневная сводка (полная)
🗓️ 2025-11-06
========================================

📈 ОБЩИЕ ПОКАЗАТЕЛИ
Всего сделок: 253
Выигрышей: 94 (37.1%)
Проигрышей: 142
Общий P/L: -203.54

📊 ORDERFLOW
Сделок: 253 | WR: 37.1% | P/L: -203.54
TP: 112 (44%) / 82 (32%) / 70 (28%)
TP→SL: 42 (17%) ⚠️

📡 ПО ИСТОЧНИКАМ
• AggregatedHub-V2: 94 сделок, WR 38.3%, P/L -204.12
• OrderFlow: 120 сделок, WR 34.2%, P/L +0.90
• TechnicalAnalysis: 39 сделок, WR 43.6%, P/L -0.32
```

---

## 🔄 Автоматическая отправка

Signal Performance Tracker автоматически отправляет отчеты:

### Периодические отчеты

- **Частота**: Каждые 3 часа (по умолчанию)
- **Содержание**: Ежедневная сводка

### Ежедневные сводки

- **Время**: 00:00 UTC (по умолчанию)
- **Содержание**: Полная сводка + детальные отчеты по каждой стратегии

### Настройка расписания

В `docker-compose.yml` → `signal-performance-tracker`:

```yaml
environment:
  - PERIODIC_REPORT_HOURS=3 # Каждые 3 часа
  - DAILY_SUMMARY=true # Включить ежедневные сводки
  - DAILY_SUMMARY_HOUR=0 # Час отправки (00:00 UTC)
```

---

## 🐛 Troubleshooting

### Отчеты не приходят

```bash
# 1. Проверка Signal Performance Tracker (если используется автоматическая отправка)
docker logs scanner-signal-tracker -f | grep "report"

# 2. Проверка Redis stream
docker exec -it scanner-redis redis-cli XLEN notify:telegram

# 3. Проверка Notify Worker
docker logs scanner-notify-worker -f | grep "report"

# 4. Повторная отправка вручную
make send-real-report
```

### Нет данных в отчетах

```bash
# Проверка статистики в Redis
docker exec -it scanner-redis redis-cli KEYS "stats:*"

# Пример ключа
docker exec -it scanner-redis redis-cli HGETALL "stats:orderflow:XAUUSD:tick"

# Если нет данных - проверьте:
# - TradeMonitor обрабатывает сигналы?
# - Позиции закрываются?
# - StatsAggregator работает?
```

---

## ✅ Что исправлено

**Было**:

- ❌ ReportingService создавался, но не использовался
- ❌ Вызывался несуществующий скрипт `/app/send_real_report.py`
- ❌ Отчеты не отправлялись

**Стало**:

- ✅ ReportingService используется для отправки
- ✅ Отправка через Redis stream `notify:telegram`
- ✅ Полные метрики с TP и разбивкой по источникам
- ✅ Работает автоматическая и ручная отправка

---

## 📁 Файлы

- **Тестовый скрипт**: `test_send_report.py`
- **Makefile команда**: `make send-real-report`
- **ReportingService**: `python-worker/services/reporting_service.py`
- **Signal Tracker**: `python-worker/services/signal_performance_tracker.py`

---

## 📝 Связанные документы

- `SIGNAL_TRACKER_REPORTS_FIX.md` - Полное описание исправления
- `SIGNAL_TRACKER_SETUP_COMPLETE.md` - Настройка Signal Tracker
- `REPORTING_QUICK_START.md` - Быстрый старт с отчетами

---

**Обновлено**: 2025-11-06

