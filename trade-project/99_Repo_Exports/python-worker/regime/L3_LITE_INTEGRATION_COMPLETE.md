# ✅ L3-Lite Metrics System - Полная Интеграция

## 🎯 Выполненные Задачи

### 1. ✅ Подключение L3-Lite потока в обработчике

**Реализовано:**
- Добавлен L3-Lite стрим в `BaseOrderFlowHandler`
- Методы `_parse_l3_event()` и `_process_l3_event()` для обработки событий
- Интеграция с `CryptoOrderFlowHandler.on_l3_event()`

**Код:**
```python
# В BaseOrderFlowHandler
self.l3_stream = os.getenv(f"{symbol}_L3_STREAM") or f"stream:l3_{symbol}"
streams = [self.tick_stream, self.book_stream, self.l3_stream]

# Обработка событий
elif m.stream == self.l3_stream:
    l3_event = self._parse_l3_event(m.fields)
    if l3_event:
        self._process_l3_event(l3_event)
```

### 2. ✅ Настройка порогов под специфику инструментов

**Реализовано:**
- `CryptoConfScorerConfig` с поддержкой symbol-specific настроек
- Разные пороги для BTCUSDT, ETHUSDT, ADAUSDT, SOLUSDT и др.
- Конфигурация через ENV или программно

**Пример конфигураций:**
```python
# BTCUSDT - жесткие пороги для ликвидного инструмента
"l3_spread_max_ok_bps": 3.0,
"l3_spread_hard_limit_bps": 15.0,
"l3_cancel_to_trade_soft": 1.5,

# ADAUSDT - мягкие пороги для менее ликвидного
"l3_spread_max_ok_bps": 8.0,
"l3_spread_hard_limit_bps": 25.0,
"l3_cancel_to_trade_soft": 2.5,
```

### 3. ✅ Мониторинг качества сигналов с L3-метриками

**Реализовано:**
- `SignalQualityMonitor` для отслеживания качества
- Запись сигналов и результатов в реальном времени
- Расчет корреляций L3-метрик с результатами
- Генерация отчетов и алертов

**Метрики мониторинга:**
- Win Rate по символу/семейству
- Средние L3-confidence, spread, OBI
- Корреляции: L3 vs Win Rate, Spread vs Win Rate, OBI vs Win Rate

**API:**
```python
# Запись сигнала
monitor.record_signal(signal_id, symbol, family, ctx, raw_score, final_score)

# Запись результата
monitor.record_result(signal_id, pnl_r)

# Отчет
report = monitor.get_quality_report(symbol="BTCUSDT")
alerts = monitor.get_alerts()
```

### 4. ✅ Добавление новых метрик при необходимости

**Реализовано:**
- **Microprice Velocity**: скорость изменения микропрайса (bps/сек)
- **Queue Pressure**: комбинированная метрика давления на очередь
- **Market Depth Imbalance**: несбалансированность глубины книги

**Новые метрики:**
```python
@dataclass
class L3LiteFeatures:
    # Существующие + новые
    microprice_velocity_bps: float = 0.0      # скорость изменения
    queue_pressure_bid: float = 0.0           # давление на bid
    queue_pressure_ask: float = 0.0           # давление на ask
    market_depth_imbalance: float = 0.0       # имбаланс глубины
```

## 🏗️ Архитектура Системы

### Компоненты

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   L3-Lite       │ => │  L3LiteMetrics   │ => │   SignalContext  │
│   Stream        │    │  Aggregator      │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                            │
┌─────────────────┐    ┌──────────────────┐           │
│ CryptoConf      │ <= │  Quality         │    ┌─────────────────┐
│ Scorer          │    │  Monitor         │ <= │   Trade Results │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### Поток Данных

1. **L3-Lite Events** → `L3LiteMetricsAggregator`
2. **Book Updates** → `L3LiteMetricsAggregator`
3. **L3 Features** → `SignalContext` → `CryptoConfScorer`
4. **Signals** → `SignalQualityMonitor`
5. **Results** → `SignalQualityMonitor`

## 📊 L3-Mетрики (Полный Набор)

### Core Metrics
- **Cancel/Trade Ratios**: `cancel_to_trade_bid/ask_5s/20s`
- **Microprice Shift**: `microprice_shift_bps_20`
- **Spread**: `spread_bps`
- **OBI**: `obi_5`, `obi_20`, `obi_50`
- **OBI Persistence**: `obi_persistence_score`

### Extended Metrics
- **Microprice Velocity**: `microprice_velocity_bps`
- **Queue Pressure**: `queue_pressure_bid/ask`
- **Market Depth Imbalance**: `market_depth_imbalance`

## 🔧 Конфигурация

### Переменные Окружения

```bash
# L3-Lite конфигурация
L3_SPREAD_MAX_OK_BPS=5.0
L3_SPREAD_HARD_LIMIT_BPS=20.0
L3_CANCEL_TO_TRADE_SOFT=2.0
L3_CANCEL_TO_TRADE_HARD=5.0
L3_MP_DRIFT_MAX_BPS=5.0

# Symbol-specific (через код)
# См. CryptoConfScorerConfig.symbol_configs
```

### Программная Конфигурация

```python
from regime import CryptoConfScorer, CryptoConfScorerConfig

cfg = CryptoConfScorerConfig()
# Доступны все настройки через cfg.symbol_configs

scorer = CryptoConfScorer(cfg)
confidence = scorer(signal_context, symbol="BTCUSDT")
```

## 📈 Мониторинг и Аналитика

### Отчеты о Качестве

```python
from regime import SignalQualityMonitor

monitor = SignalQualityMonitor()

# Отчет по всем символам
report = monitor.get_quality_report()

# Отчет по конкретному символу
report = monitor.get_quality_report(symbol="BTCUSDT")

# Алерты о проблемах
alerts = monitor.get_alerts()
```

### Пример Отчета

```
📊 Signal Quality Report
==================================================
🔸 BTCUSDT:crypto_orderflow
   Signals: 150 total, 120 with results
   Win Rate: 65.0%
   Avg Raw Score: 2.3
   Avg Final Score: 2.8
   Avg L3 Confidence: 0.4
   Avg Spread: 4.2 bps
   Avg OBI-5: 0.15
   Correlations:
     L3 vs Win Rate: 0.35
     Spread vs Win Rate: -0.28
     OBI vs Win Rate: 0.42
```

## 🚀 Использование

### В CryptoOrderFlowHandler

```python
class CryptoOrderFlowHandler(BaseOrderFlowHandler):
    def __init__(self, symbol):
        super().__init__(symbol)
        self.l3_agg = L3LiteMetricsAggregator()
        self.conf_scorer = CryptoConfScorer()
        self.quality_monitor = SignalQualityMonitor()

    def on_l3_event(self, event):
        self.l3_agg.on_l3_event(event)

    def on_book_update(self, snapshot):
        self.l3_agg.on_book_update(snapshot)

    # SignalContext автоматически наполняется L3-метриками
    # Confidence рассчитывается с учетом L3
```

### Запуск Демо

```bash
cd python-worker
python -m regime.l3_lite_demo
```

## 🎯 Результат

### ✅ Достигнутые Цели

1. **L3-Lite поток полностью интегрирован** в обработчик сигналов
2. **Symbol-specific пороги** позволяют гибко настраивать фильтры
3. **Мониторинг качества** предоставляет полную аналитику сигналов
4. **Расширенный набор метрик** улучшает качество скоринга

### 📈 Улучшения Качества

- **Лучшая фильтрация** сигналов по микроструктуре рынка
- **Адаптивные пороги** для разных инструментов
- **Раннее обнаружение** проблем с качеством
- **Подробная аналитика** для оптимизации стратегий

### 🔄 Следующие Шаги

1. **Сбор исторических данных** L3-Lite для обучения
2. **Тюнинг порогов** на основе результатов мониторинга
3. **Интеграция с ML-моделями** для предсказания качества
4. **Добавление новых метрик** по мере необходимости

## 📚 Документация

- `L3_LITE_README.md` - детальное описание системы
- `l3_lite_example.py` - примеры использования
- `l3_lite_demo.py` - полная демонстрация

Система L3-Lite метрик готова к production использованию! 🚀✨
