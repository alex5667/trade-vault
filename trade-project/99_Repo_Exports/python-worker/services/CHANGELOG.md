# Changelog - Signal Performance Tracker

## [1.0.0] - 2025-11-02

### ✨ Добавлено

#### Статистика по источникам сигналов ⭐

- **Разбивка по источникам**: OrderFlow, AggregatedHub-V2, TechnicalAnalysis
- **Двойная статистика**: общая + по каждому источнику отдельно
- **API для работы с источниками**:
  - `StatsAggregator.get_strategy_sources()` - список источников
  - `StatsAggregator.get_stats_by_source()` - статистика по источнику
  - `ReportingService.get_sources_summary()` - сводка по всем источникам
- **Автоматическое включение** в отчёты и уведомления
- **Redis индексы** для быстрого поиска по источникам

#### Периодические сводки каждые 3 часа ⭐

- **Автоматические уведомления** в Telegram каждые 3 часа
- **Настраиваемый интервал** через конфиг или ENV vars
- **Включает разбивку по источникам**
- Формат: `🗓 Итоги за 3ч` с деталями по стратегиям и источникам

#### Основные компоненты

- **Trade Monitor Service** (`services/trade_monitor.py`)

  - Отслеживание виртуальных позиций по сигналам
  - Частичное закрытие на TP1 (50%), TP2 (30%), TP3 (20%)
  - Расчёт уровней на основе ATR
  - Логирование всех событий в Redis Streams

- **Stats Aggregator** (`services/stats_aggregator.py`)

  - Статические методы для эффективной работы с Redis
  - Атомарные операции через Redis pipeline
  - Подсчёт метрик: WinRate, P/L, TP hit rates
  - Агрегация по стратегиям/символам/таймфреймам

- **Reporting Service** (`services/reporting_service.py`)

  - API для получения отчётов и статистики
  - Telegram уведомления (интеграция с существующей системой)
  - Ежедневные сводки (00:00 UTC)
  - **Периодические сводки каждые 3 часа** ⭐
  - Постраничная выборка сделок
  - Экспорт в JSON

- **Signal Performance Tracker** (`services/signal_performance_tracker.py`)
  - Главный оркестратор системы
  - Consumer groups для надёжной обработки Redis Streams
  - Graceful shutdown (SIGINT/SIGTERM)
  - Мониторинг в реальном времени

#### Скрипты и утилиты

- `run_performance_tracker.py` - standalone запуск с поддержкой ENV vars
- `services/example_usage.py` - 6 примеров использования API

#### Документация

- `README_SIGNAL_TRACKER.md` - полная документация
- `INTEGRATION_GUIDE.md` - руководство по интеграции
- `NOTIFICATION_INTEGRATION.md` - настройка Telegram уведомлений
- `DEPLOYMENT.md` - развёртывание (standalone/docker/systemd)
- `SOURCE_STATISTICS.md` - работа со статистикой по источникам ⭐
- `QUICKSTART_SOURCES.md` - быстрый старт с источниками ⭐
- `CHANGELOG.md` - история изменений

#### Примеры и утилиты

- `example_sources_analysis.py` - 7 примеров анализа источников ⭐

#### Конфигурация

- `config/signal_tracker_config.json` - основной конфиг
- Поддержка переменных окружения
- Приоритет: ENV vars → JSON config → defaults

### 🎯 Особенности

#### Частичное закрытие позиций

- **TP1**: 50% позиции при R:R = 1.0
- **TP2**: 30% позиции при R:R = 2.0
- **TP3**: 20% позиции при R:R = 3.0

#### Redis схема данных

```
Streams:
  signals:{strategy}:{symbol}     - входящие сигналы
  stream:tick_{symbol}            - тиковые данные
  events:trades                   - события (OPEN/TP/SL/CLOSE)
  trades:closed                   - закрытые сделки

Hashes:
  signal:{id}                                - исходный сигнал
  order:{id}                                 - данные позиции
  stats:{strategy}:{symbol}:{tf}             - общая статистика
  stats:{strategy}:{symbol}:{tf}:{source}    - статистика по источнику ⭐

Lists:
  closed:{strategy}:{symbol}:{tf}            - ID сделок для пагинации
  closed:{strategy}:{symbol}:{tf}:{source}   - ID сделок по источнику ⭐

Sets:
  stats:strategies                           - список стратегий
  stats:symbols:{strategy}                   - символы по стратегии
  stats:tfs:{strategy}:{symbol}              - таймфреймы
  stats:sources:{strategy}:{symbol}:{tf}     - источники сигналов ⭐
```

#### Уведомления Telegram

**По умолчанию:**

- ❌ Уведомления при каждой сделке: **ВЫКЛЮЧЕНЫ** (избежание спама)
- ✅ Ежедневные сводки: **ВКЛЮЧЕНЫ** (00:00 UTC)
- ✅ Периодические сводки: **ВКЛЮЧЕНЫ** (каждые 3 часа) ⭐

**Формат периодической сводки (каждые 3 часа):**

```
🗓 Итоги за 3ч

• orderflow: 12 сделок, WR 75.0%, P/L +89.40

📊 По источникам:
  • OrderFlow: 5 сделок, WR 80.0%, P/L +45.20
  • AggregatedHub-V2: 5 сделок, WR 60.0%, P/L +34.10
  • TechnicalAnalysis: 2 сделки, WR 100.0%, P/L +10.10
```

#### Интеграция

- ✅ Совместимость с существующей Telegram инфраструктурой
  - telegram-worker (Python)
  - bot-nest (Node.js)
  - improved_notifier
  - notify-bridge (FastAPI)
- ✅ Использование существующих Redis Streams
- ✅ Consumer groups для масштабирования
- ✅ Следование архитектуре python-worker

### 🚀 Развёртывание

#### Способы запуска

1. **Standalone**: `python run_performance_tracker.py`
2. **Docker Compose**: `docker-compose up -d signal-performance-tracker`
3. **Systemd**: `systemctl start signal-tracker`
4. **Python API**: `SignalPerformanceTracker(config).run_forever()`

#### Переменные окружения

```bash
# Базовые
REDIS_HOST=scanner-redis-worker-1
REDIS_PORT=6379
SYMBOLS=XAUUSD,BTCUSD
STRATEGIES=orderflow

# Telegram
TELEGRAM_BOT_TOKEN=your_token
TELEGRAM_CHAT_ID=your_chat_id
NOTIFY_ON_TRADE_CLOSE=false

# Отчёты
DAILY_SUMMARY=true
DAILY_SUMMARY_HOUR=0
PERIODIC_SUMMARY=true         # ⭐ Новое
PERIODIC_SUMMARY_HOURS=3      # ⭐ Новое
```

### 📊 Метрики

#### Базовые

- Total Trades
- Wins / Losses / Breakevens
- WinRate (%)
- Total P/L (валюта)
- Average P/L

#### TP метрики

- TP1/TP2/TP3 Hits
- TP1/TP2/TP3 Rate (%)

#### Временные

- Average Duration
- Min/Max Duration
- TP Latency (планируется)

#### Расширенные (планируется)

- Profit Factor
- Sharpe Ratio
- Max Drawdown
- Recovery Factor
- Precision/Recall
- ROC curves
- Signal Decay

### 🔧 API

```python
from services.signal_performance_tracker import SignalPerformanceTracker
from services.stats_aggregator import StatsAggregator
from services.reporting_service import ReportingService
from core.redis_client import get_redis

# Запуск
tracker = SignalPerformanceTracker(config)
tracker.start()

# Статистика (статические методы)
redis_client = get_redis()
stats = StatsAggregator.get_stats(redis_client, "orderflow", "XAUUSD", "tick")

# Отчёты
reporting = ReportingService()
reporting.send_daily_summary()
reporting.notify_periodic_summary(stats, period="3ч")

# Статус
status = tracker.get_status()
```

### 🐛 Исправления

- Корректная обработка позиций с частичным закрытием
- Атомарные операции Redis для избежания race conditions
- Graceful shutdown без потери данных
- Избежание дублирования уведомлений через существующую систему

### ⚡ Оптимизации

- Статические методы StatsAggregator (без создания экземпляров)
- Redis pipeline для batch операций
- Connection pooling для Redis
- Индексирование позиций по символам
- Кэширование статистики (опционально)

### 📝 Известные ограничения

- Виртуальные позиции (не реальная торговля)
- Не учитывается slippage
- Не учитываются комиссии
- Предполагается мгновенное исполнение

### 🔮 Roadmap

#### v1.1.0 (планируется)

- [ ] WebSocket API для real-time обновлений
- [ ] Web Dashboard (React)
- [ ] Графики производительности (Plotly)
- [ ] Backtesting на исторических данных

#### v1.2.0 (планируется)

- [ ] ML-анализ качества сигналов
- [ ] Precision/Recall метрики
- [ ] ROC curves
- [ ] Signal Decay анализ
- [ ] Экспорт в ClickHouse/TimescaleDB

#### v2.0.0 (планируется)

- [ ] А/B тестирование стратегий
- [ ] Автоматическая оптимизация параметров
- [ ] Multi-strategy portfolio optimization
- [ ] Risk management integration

### 👥 Контрибьюторы

Создано для проекта scanner_infra

### 📄 Лицензия

Внутренний проект

---

## Примечания к версионированию

Версионирование следует Semantic Versioning (SemVer):

- MAJOR: несовместимые изменения API
- MINOR: новая функциональность с обратной совместимостью
- PATCH: исправления багов с обратной совместимостью
