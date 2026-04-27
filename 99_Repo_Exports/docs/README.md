# 📊 Scanner Infrastructure - Trading Analysis Platform

## Обзор проекта

**Scanner Infrastructure** - это высокопроизводительная платформа для анализа рыночных данных в реальном времени, генерации торговых сигналов и автоматизации торговых стратегий. Система построена на микросервисной архитектуре с использованием Go и Python.

### Ключевые возможности

- 🚀 **Real-time данные**: Получение данных с Binance WebSocket API для всех таймфреймов (1m-1y)
- 📈 **Order Flow анализ**: Анализ потока ордеров, дельты объемов, OBI (Order Book Imbalance)
- 🤖 **Генерация сигналов**: Технический анализ (EMA, RSI, MACD, ATR) + Order Flow
- 📱 **Telegram интеграция**: Получение сигналов от 40+ каналов и отправка уведомлений
- 🎯 **Мультисимвольная поддержка**: XAUUSD, BTC, ETH и другие символы
- 📊 **Monitoring**: Prometheus + Grafana для мониторинга системы
- 🔄 **Redis**: Высокопроизводительное хранилище данных с репликацией
- 🐳 **Docker**: Полная контейнеризация всех сервисов

### Архитектура

```
┌─────────────────────────────────────────────────────────────────┐
│                     BINANCE WebSocket API                        │
└────────────────────────────┬────────────────────────────────────┘
                             │
         ┌───────────────────┴───────────────────┐
         │                                       │
┌────────▼────────┐                    ┌────────▼────────┐
│  Go Workers     │                    │  MT5 Adapter    │
│  (10 workers)   │                    │  (Wine/HTTP)    │
│  All timeframes │                    │  Tick Ingest    │
└────────┬────────┘                    └────────┬────────┘
         │                                       │
         └───────────────────┬───────────────────┘
                             │
                    ┌────────▼────────┐
                    │  Redis Cluster  │
                    │  (3 instances)  │
                    │  - Main (6379)  │
                    │  - Worker-1     │
                    │  - Worker-2     │
                    └────────┬────────┘
                             │
         ┌───────────────────┼───────────────────┐
         │                   │                   │
┌────────▼────────┐ ┌────────▼────────┐ ┌───────▼────────┐
│ Python Workers  │ │ Signal Gen      │ │ Telegram       │
│ - Order Flow    │ │ - TA Analysis   │ │ - Listener     │
│ - Multi-Symbol  │ │ - ATR Calc      │ │ - Parser       │
│ - OHLC Agg      │ │ - Aggregator    │ │ - Notifier     │
└────────┬────────┘ └────────┬────────┘ └───────┬────────┘
         │                   │                   │
         └───────────────────┼───────────────────┘
                             │
                    ┌────────▼────────┐
                    │  Go Gateway     │
                    │  - Order Queue  │
                    │  - TG Bot       │
                    │  - API Server   │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │   Paper/Live    │
                    │   Executor      │
                    └─────────────────┘
```

## 🚀 Быстрый старт

### Предварительные требования

- Docker 20.10+
- Docker Compose 2.0+
- 16GB RAM (минимум), 32GB рекомендуется
- 4 CPU cores (минимум)
- 50GB свободного места на диске

### Установка и запуск

1. **Клонирование репозитория**

```bash
git clone <repository-url>
cd scanner_infra
```

2. **Настройка переменных окружения**

```bash
# Создать файл telegram-worker/.env
cp telegram-worker/.env.example telegram-worker/.env

# Заполнить обязательные параметры:
# - TELEGRAM_API_ID
# - TELEGRAM_API_HASH
# - TELEGRAM_PHONE
# - TELEGRAM_BOT_TOKEN (для уведомлений)
# - TELEGRAM_CHAT_ID
```

3. **Запуск системы**

```bash
# Запуск всех сервисов
make up

# Или в фоновом режиме
make up-bg

# Проверка статуса
make status
```

4. **Проверка работоспособности**

```bash
# Полная диагностика
make full-status

# Проверка Redis
make redis-stats

# Логи конкретного сервиса
make signal-logs
make gateway-logs
make telegram-logs
```

### Первый запуск

После запуска система автоматически:

1. ✅ Инициализирует Redis кластер (20-30 сек)
2. ✅ Запускает 10 Go workers для получения данных (40 сек)
3. ✅ Подключается к Binance WebSocket
4. ✅ Начинает накапливать рыночные данные
5. ✅ Запускает Python workers для анализа (60+ сек)
6. ✅ Подключается к Telegram каналам (25 сек)

**Первые сигналы появятся через 2-3 минуты после запуска.**

## 📋 Основные команды

### Управление системой

```bash
make up              # Запуск всех сервисов
make down            # Остановка всех сервисов
make restart         # Перезапуск
make rebuild         # Пересборка и перезапуск
make clean           # Полная очистка (удаляет volumes!)
```

### Мониторинг

```bash
make status          # Статус всех контейнеров
make logs            # Все логи в реальном времени
make health          # Проверка здоровья сервисов
make diagnose        # Диагностика проблем
make full-status     # Полный статус системы
```

### По сервисам

```bash
make signal-status   # Статус signal-generator
make hub-status      # Статус signal-hub
make gateway-status  # Статус go-gateway
make telegram-status # Статус telegram-worker
make redis-stats     # Статистика Redis
```

### Управление символами (динамическое)

```bash
make symbol-add SYMBOL=BTCUSD      # Добавить символ
make symbol-remove SYMBOL=BTCUSD   # Удалить символ
make symbol-list                   # Список активных символов
make symbol-status                 # Статус обработчиков
```

## 🔧 Конфигурация

### Основные файлы конфигурации

- `docker-compose.yml` - главная конфигурация Docker
- `redis-*.conf` - конфигурации Redis
- `prometheus.yml` - конфигурация Prometheus
- `Makefile` - команды управления системой

### Порты сервисов

| Сервис         | Порт      | Описание                  |
| -------------- | --------- | ------------------------- |
| Redis Main     | 6379      | Основной Redis            |
| Tick Ingest    | 8087      | HTTP API для MT5 (legacy) |
| Py-OBI Service | 8088      | OBI анализ и графики      |
| Go Gateway     | 8090      | Order queue, Telegram bot |
| Prometheus     | 9090      | Метрики                   |
| Grafana        | 3001      | Дашборды                  |
| Go Workers     | 2112-2121 | Prometheus metrics        |

## 📚 Структура проекта

```
scanner_infra/
├── go-worker/          # Go workers для WebSocket + Redis
├── go-gateway/         # Go API gateway + Order routing
├── python-worker/      # Python обработчики сигналов
│   ├── core/          # Ядро системы
│   ├── handlers/      # Обработчики данных
│   ├── services/      # Вспомогательные сервисы
│   └── analytics/     # Аналитика и метрики
├── telegram-worker/    # Telegram интеграция
├── py-obi/            # OBI анализ (Order Book Imbalance)
├── regime-worker/     # Определение рыночного режима
├── signal-generator/  # Генерация TA сигналов
├── hub/               # Агрегация сигналов
├── scripts/           # Утилиты и скрипты
└── config/            # Конфигурационные файлы
```

## 🎯 Основные сервисы

### 1. Go Workers (10 контейнеров)

- Получение данных с Binance WebSocket
- Обработка всех таймфреймов: 1m, 5m, 15m, 1h, 4h, 1d, 1w, 1M, 3M, 1y
- Публикация в Redis streams (`candles:data`)
- Prometheus метрики

### 2. Python Workers

- **Order Flow Handler**: Анализ потока ордеров, дельта, OBI
- **Multi-Symbol Handler**: Мультисимвольная обработка (XAUUSD, BTC, ETH)
- **OHLC Aggregator**: Дневные OHLC из тиков для Pivot уровней
- **ATR Worker**: Вычисление ATR из свечей

### 3. Signal Generator

- Технический анализ (EMA, RSI, MACD, ATR)
- Генерация сигналов на основе индикаторов
- Публикация в `signals:ta:XAUUSD`

### 4. Aggregated Hub

- Комбинирование Order Flow + TA сигналов
- Взвешенный scoring с учетом confidence
- Фильтрация по уровням значимости
- Отправка в Go Gateway

### 5. Go Gateway

- Order queue management
- Telegram notifications
- REST API для внешних систем
- Paper trading executor

### 6. Telegram Worker

- Прослушивание 40+ каналов
- Парсинг торговых сигналов
- Публикация в Redis
- Multi-threaded обработка

### 7. Redis Cluster

- **Main (6379)**: Основное хранилище
- **Worker-1**: Candles + Signals
- **Worker-2**: Backup + Distribution
- AOF + RDB persistence

## 📊 Потоки данных

### 1. Рыночные данные

```
Binance WS → Go Workers → Redis (candles:data) → Python Workers
```

### 2. Order Flow

```
MT5 Ticks → Tick Ingest → Redis (stream:tick_*) → Order Flow Handler → Signals
```

### 3. Сигналы

```
Order Flow + TA → Aggregated Hub → Go Gateway → Telegram Bot / Paper Executor
```

### 4. Telegram

```
Channels → Telegram Worker → Redis (signal:telegram:raw) → Parser → Notify Worker → Bot
```

## 🔍 Мониторинг

### Grafana Dashboards

```
http://localhost:3001
Login: admin
Password: admin
```

### Prometheus Metrics

```
http://localhost:9090
```

### Redis Monitoring

```bash
# Статистика
make redis-stats

# Память
make redis-memory

# Все ключи
docker exec scanner-redis redis-cli KEYS '*'
```

## 🧪 Тестирование

```bash
# Проверка API endpoints
make api-test

# Тест Go Gateway
make gateway-test

# Тест сигналов
make signal-test

# Тест уведомлений
make notification-test
```

## 🆘 Troubleshooting

### Система не запускается

```bash
make diagnose           # Диагностика
make emergency-fix      # Автоматическое исправление
make clean && make up   # Полная переустановка
```

### Нет сигналов

```bash
make signal-status      # Проверка генератора
make redis-stats        # Проверка данных в Redis
make check-signals      # Проверка качества сигналов
```

### Высокое использование памяти

```bash
make redis-memory       # Проверка Redis
make monitor-resources  # Мониторинг ресурсов
make optimize-redis     # Оптимизация Redis
```

### Telegram не подключается

```bash
make telegram-status    # Статус Telegram worker
make telegram-logs      # Логи
# Проверить .env файл в telegram-worker/
```

## 🔧 Последние обновления (3 ноября 2025)

### ⚡ ИСПРАВЛЕНО: Signal Performance Tracker

**Проблема**: Сервис анализа сигналов не работал, статистика не отправлялась каждые 3 часа.

**Решение**:

- ✅ Добавлен сервис в docker-compose.yml
- ✅ Исправлен код загрузки конфигурации
- ✅ Создан конфиг-файл
- ✅ Добавлены команды управления в Makefile
- ✅ Созданы диагностические инструменты

**Быстрый старт**:

```bash
make down && make up-bg      # Перезапустить систему
make tracker-status          # Проверить трекер
make check-xauusd-services   # Проверить все 3 сервиса XAUUSD
make test-tracker-telegram   # Тест отправки в Telegram
```

**Подробнее**: [COMPLETE_FIX_REPORT.md](COMPLETE_FIX_REPORT.md) | [QUICK_FIX_GUIDE.md](QUICK_FIX_GUIDE.md)

---

## 📖 Документация

### 📚 Полная документация (рекомендуется)

**Новая подробная документация в 5 файлах** → [documentation/](documentation/)

- **[01_OVERVIEW.md](documentation/01_OVERVIEW.md)** - Обзор проекта, архитектура, быстрый старт
- **[02_SERVICES.md](documentation/02_SERVICES.md)** - Детальное описание всех 30+ сервисов
- **[03_CONFIGURATION.md](documentation/03_CONFIGURATION.md)** - Все конфигурации (Docker, Redis, Env)
- **[04_DEVELOPMENT.md](documentation/04_DEVELOPMENT.md)** - Примеры кода, интеграции, backtest
- **[05_OPERATIONS.md](documentation/05_OPERATIONS.md)** - Операции, мониторинг, troubleshooting

### 📄 Дополнительная документация

- [ARCHITECTURE.md](ARCHITECTURE.md) - Детальная архитектура и поток данных
- [CONFIGURATION.md](CONFIGURATION.md) - Все конфигурации (Redis, Docker, сервисы)
- [SERVICES.md](SERVICES.md) - Подробное описание всех модулей и сервисов
- [EXAMPLES.md](EXAMPLES.md) - Примеры кода и использования

### 🔧 Исправления и обновления

- [COMPLETE_FIX_REPORT.md](COMPLETE_FIX_REPORT.md) - Полный отчет об исправлении Signal Tracker
- [QUICK_FIX_GUIDE.md](QUICK_FIX_GUIDE.md) - Быстрая инструкция по запуску
- [FIX_SUMMARY.md](FIX_SUMMARY.md) - Краткая сводка изменений

## 🔐 Безопасность

- Все пароли и токены хранятся в `.env` файлах
- Redis доступен только внутри Docker сети (кроме порта 6379)
- Telegram сессии зашифрованы
- HTTPS рекомендуется для production

## 📝 Лицензия

MIT License

## 🤝 Поддержка

Для вопросов и предложений создавайте Issues в репозитории.

---

**Made with ❤️ by Scanner Infrastructure Team**
