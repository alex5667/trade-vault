# EV Gate Tools & Optimizations

Набор инструментов для мониторинга, анализа и оптимизации EV (Expected Value) gate системы.

## 📁 Структура

```
tools/
├── analyz e_ev_stats.py       # Анализ статистики P(hit TP1) из Redis
├── monitor_ev_gate.py         # Real-time мониторинг логов
└── telegram_ev_commands.py    # Telegram bot команды

dashboards/
└── ev_gate_dashboard.json     # Grafana dashboard config
```

---

## 🔧 Инструменты

### 1. EV Statistics Analyzer

**Описание:** Анализирует накопленную статистику P(hit TP1) из Redis и предоставляет рекомендации по тюнингу.

**Использование:**
```bash
# Базовый анализ
python3 tools/analyze_ev_stats.py

# С кастомными параметрами
python3 tools/analyze_ev_stats.py --redis-url redis://localhost:6379/0 --min-trades 50

# Экспорт в CSV
python3 tools/analyze_ev_stats.py --export /tmp/ev_stats.csv
```

**Вывод:**
- Overall P(hit TP1) stats (mean, median, range)
- Breakdown by signal kind
- Breakdown by symbol
- Breakdown by regime
- Tuning recommendations

**Пример:**
```
📊 Dataset: 45 valid keys (min_trades >= 10)

🎯 Overall P(hit TP1):
   Mean:   0.582
   Median: 0.570
   Range:  [0.420, 0.710]
   StDev:  0.065

📈 By Signal Kind:
   breakout        → mean=0.595, median=0.590 (n=18)
   absorption      → mean=0.550, median=0.545 (n=12)

💡 Recommendations:
   ✅ Median P(TP1) = 0.570 >= current p_min = 0.550
```

---

### 2. Real-time EV Gate Monitor

**Описание:** Мониторит логи в реальном времени с красивым форматированием.

**Использование:**
```bash
# Default контейнер
python3 tools/monitor_ev_gate.py

# Кастомный контейнер
python3 tools/monitor_ev_gate.py --container scanner-crypto-orderflow-2
```

**Вывод:**
```
[10:45:23]
🚫 EV VETO
   Signal: breakout LONG on BTCUSDT
   P(TP1): 0.580 (min=0.550, n=45, src=ema)
   Levels: TP1=50.0bps, SL=30.0bps
   EV:     17.4bps < threshold=24.0bps
   Formula: 0.58 × 50.0 - 0.42 × 30.0 = 17.4bps
```

---

### 3. Telegram Bot Commands

**Описание:** Команды для мониторинга через Telegram бота.

**Интеграция:**
```python
from tools.telegram_ev_commands import register_ev_commands

# В вашем existing telegram bot:
register_ev_commands(bot, redis_client)
```

**Команды:**
- `/ev_stats` - статистика P(hit TP1)
- `/ev_health` - health check EV gate
- `/ev_tune [symbol]` - рекомендации по тюнингу

**Пример:**
```
User: /ev_stats

Bot:
📊 EV Gate Statistics

Total keys: 52
Valid keys: 45

Overall P(TP1):
  Mean: 0.582
  Median: 0.570
  Range: [0.420, 0.710]

By Kind:
  breakout: 0.595 (n=18)
  absorption: 0.550 (n=12)
```

---

### 4. Grafana Dashboard

**Описание:** Pre-configured Grafana dashboard для визуализации метрик.

**Установка:**
```bash
# Import в Grafana
curl -X POST http://localhost:3000/api/dashboards/db \
  -H "Content-Type: application/json" \
  -d @dashboards/ev_gate_dashboard.json
```

**Метрики:**
- EV Gate Veto Rate (по kind/symbol)
- P(hit TP1) Distribution
- EV (bps) at Veto (heatmap)
- EV Stats Sample Count
- Veto Reasons Breakdown (pie chart)
- EV Gate Evaluation Latency (p95, p99)
- EV Stats Age (staleness check)

---

## 🚀 Optimizations

### 1. Per-Kind P_min

**Описание:** Разные пороги вероятности для разных типов сигналов.

**Конфигурация:**
```bash
# В docker-compose.yml
EDGE_EV_P_MIN=0.55                # Default
EDGE_EV_P_MIN_BREAKOUT=0.58       # Breakout строже
EDGE_EV_P_MIN_ABSORPTION=0.52     # Absorption мягче
EDGE_EV_P_MIN_EXTREME=0.60        # Extreme очень строгий
```

**Rationale:**
- `breakout` - более шумный, требует выше вероятность
- `absorption` - более качественный, можно быть мягче
- `extreme` - редкий но сильный, очень строго

**Когда использовать:**
- У вас разная win rate по different signal types
- Хотите fine-tune каждый kind independently

---

### 2. Dynamic K (Volatility-Adjusted)

**Описание:** Автоматически повышает K (строгость) в периоды высокой волатильности.

**Конфигурация:**
```bash
# В docker-compose.yml
EDGE_EV_DYNAMIC_K_ENABLED=1
EDGE_EV_DYNAMIC_K_ATR_MULT=0.5
```

**Formula:**
```
K_dynamic = K_base * (1 + atr_mult * normalized_atr)
```

**Пример:**
- K_base = 2.0
- ATR = 2.5 (high volatility)
- atr_mult = 0.5
- → K_dynamic = 2.0 * (1 + 0.5 * 0.625) = 2.625

**Rationale:**
- Высокая волатильность → больше риск → строже filter
- Низкая волатильность → меньше риск → мягче filter

**Когда использовать:**
- Crypto markets с резкими изменениями volatility
- Хотите автоматическую адаптацию к market conditions

---

## 📊 Workflow

### Cold Start (0-2 часа)
```bash
# 1. Проверить deployment
docker ps --filter "name=crypto-orderflow"

# 2. Мониторить warm-up
python3 tools/monitor_ev_gate.py

# 3. Проверять появление статистики
docker exec redis redis-cli --scan --pattern "ev:tp1:*"
```

### Steady State (после 24 часов)
```bash
# 1. Анализ статистики
python3 tools/analyze_ev_stats.py

# 2. Tuning если нужно
# Если median P < p_min -> снизить EDGE_EV_P_MIN
# Если median P >> p_min -> можно поднять

# 3. Экспорт для backtesting
python3 tools/analyze_ev_stats.py --export /data/ev_stats_$(date +%Y%m%d).csv
```

### Production Monitoring
```bash
# 1. Grafana dashboard
# -> ev_gate_dashboard.json

# 2. Telegram alerts
/ev_health   # Ежедневно
/ev_stats    # Еженедельно

# 3. Periodic analysis
# Cron: 0 0 * * 0 python3 tools/analyze_ev_stats.py --export /data/weekly_stats.csv
```

---

## 🧪 Testing

### Test analyzer
```bash
cd /home/alex/front/trade/scanner_infra
python3 -c "from tools import analyze_ev_stats; print('✓ Import OK')"
```

### Test monitor
```bash
python3 tools/monitor_ev_gate.py --help
```

### Test telegram commands
```python
import redis
from tools.telegram_ev_commands import get_ev_stats

r = redis.from_url("redis://localhost:6379/0", decode_responses=True)
stats = get_ev_stats(r, min_trades=5)
print(stats)
```

---

## 📖 Related Documentation

- [EV Gate Monitoring Guide](../.gemini/antigravity/brain/66a00799-ba0b-49e1-8f40-668c308ad7fc/ev_gate_monitoring_guide.md)
- [Deployment Summary](../.gemini/antigravity/brain/66a00799-ba0b-49e1-8f40-668c308ad7fc/deployment_summary.md)

---

## 🆘 Troubleshooting

**Issue: analyzer returns "No data"**
```bash
# Check Redis
docker exec redis redis-cli KEYS "ev:tp1:*"

# If empty -> trades not closing or EV_TP1_ENABLED=0
docker logs scanner-crypto-orderflow 2>&1 | grep "ev_tp1"
```

**Issue: monitor shows nothing**
```bash
# Check if EV gate is active
docker exec scanner-crypto-orderflow env | grep EDGE_EXPECTED_MOVE_MODE
# Should be: EDGE_EXPECTED_MOVE_MODE=ev

# Check logs manually
docker logs scanner-crypto-orderflow --tail 100 | grep -i "gate"
```

**Issue: Telegram commands fail**
```bash
# Check Redis connection
python3 -c "import redis; r=redis.from_url('redis://localhost:6379/0'); r.ping(); print('OK')"

# Check bot token
echo $TELEGRAM_BOT_TOKEN
```

---

Created: 2025-12-28  
Author: EV Gate Integration Team
