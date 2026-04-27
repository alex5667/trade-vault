# 🎯 XAUUSD Signal Generator

**Professional signal generation system based on technical analysis**

## 📊 Features

### Technical Indicators

- **EMA Crossover** (9/21) - Trend detection
- **RSI** (14) - Momentum & overbought/oversold
- **MACD** - Trend confirmation
- **ATR** (14) - Dynamic SL/TP calculation

### Strategy Logic

#### LONG Signal:

- EMA(9) crosses above EMA(21), OR
- EMA(9) > EMA(21) AND MACD histogram bullish
- RSI > 30 and < 60 (not overbought)

#### SHORT Signal:

- EMA(9) crosses below EMA(21), OR
- EMA(9) < EMA(21) AND MACD histogram bearish
- RSI < 70 and > 40 (not oversold)

### Risk Management

- **ATR-based SL/TP**: Adapts to volatility
- **Position sizing**: Configurable lot size
- **Signal cooldown**: Minimum 15 min between signals
- **Stop Loss**: 1.5 × ATR
- **Take Profits**: 2×, 3×, 4× ATR

---

## 🚀 Quick Start

### 1. Standalone (без Docker):

```bash
cd signal-generator

# Install dependencies
pip install -r requirements.txt

# Configure
cp .env.example .env
nano .env  # Edit settings

# Run
export $(cat .env | xargs)
python signal_generator.py
```

### 2. With Docker:

```bash
# Build
docker build -t signal-generator .

# Run
docker run --rm \
  --name signal-generator \
  --network scanner_infra_scanner-network \
  -e GATEWAY_URL=http://scanner-go-gateway:8090 \
  -e SYMBOL=XAUUSD \
  -e DEFAULT_LOT=0.01 \
  signal-generator
```

### 3. Docker Compose Integration:

Add to `docker-compose.yml`:

```yaml
signal-generator:
  build: ./signal-generator
  container_name: scanner-signal-generator
  restart: unless-stopped
  networks:
    - scanner-network
  environment:
    - GATEWAY_URL=http://scanner-go-gateway:8090
    - OBI_SERVICE_URL=http://py-obi-service:8088
    - SYMBOL=XAUUSD
    - TIMEFRAME=M5
    - CHECK_INTERVAL=60
    - EMA_FAST=9
    - EMA_SLOW=21
    - RSI_PERIOD=14
    - ATR_PERIOD=14
    - ATR_SL_MULTIPLIER=1.5
    - ATR_TP_MULTIPLIERS=2.0,3.0,4.0
    - DEFAULT_LOT=0.01
  depends_on:
    go-gateway:
      condition: service_healthy
  logging:
    driver: 'json-file'
    options:
      max-size: '10m'
      max-file: '3'
```

---

## ⚙️ Configuration

### Environment Variables

| Variable                      | Default                 | Description                                                      |
| ----------------------------- | ----------------------- | ---------------------------------------------------------------- |
| `GATEWAY_URL`                 | `http://127.0.0.1:8090` | go-gateway API endpoint                                          |
| `OBI_SERVICE_URL`             | `http://127.0.0.1:8088` | py-obi service (for tick data)                                   |
| `SYMBOL`                      | `XAUUSD`                | Trading symbol                                                   |
| `TIMEFRAME`                   | `M5`                    | Timeframe (M1, M5, M15, H1)                                      |
| `CHECK_INTERVAL`              | `60`                    | Check for signals every N seconds                                |
| **Strategy**                  |                         |                                                                  |
| `EMA_FAST`                    | `9`                     | Fast EMA period                                                  |
| `EMA_SLOW`                    | `21`                    | Slow EMA period                                                  |
| `RSI_PERIOD`                  | `14`                    | RSI period                                                       |
| `RSI_OVERSOLD`                | `30`                    | RSI oversold level                                               |
| `RSI_OVERBOUGHT`              | `70`                    | RSI overbought level                                             |
| **Risk**                      |                         |                                                                  |
| `ATR_PERIOD`                  | `14`                    | ATR period                                                       |
| `ATR_SL_MULTIPLIER`           | `1.5`                   | SL = ATR × this                                                  |
| `ATR_TP_MULTIPLIERS`          | `2.0,3.0,4.0`           | TP levels (ATR × each)                                           |
| `DEFAULT_LOT`                 | `0.01`                  | Position size                                                    |
| `MAX_LOT`                     | `0.1`                   | Maximum position size                                            |
| `RISK_PERCENT`                | `5.0`                   | Risk per trade (% of account)                                    |
| **Position Tracking**         |                         |                                                                  |
| `ENABLE_POSITION_TRACKING`    | `false`                 | Allow multiple signals (false = unlimited, true = one at a time) |
| `MAX_POSITION_DURATION_HOURS` | `2.0`                   | Auto-reset position tracking after N hours (if enabled)          |

---

## 📊 Output Example

```
2025-10-27 21:45:00 | INFO     | Analyzing 100 candles on M5 timeframe
2025-10-27 21:45:00 | INFO     | Price: 2763.50 | EMA(9): 2762.30 | EMA(21): 2761.10 | RSI: 55.3 | ATR: 3.20
2025-10-27 21:45:00 | INFO     | 🔔 LONG SIGNAL: EMA bullish crossover; RSI favorable (55.3); MACD bullish
============================================================
SIGNAL GENERATED: LONG XAUUSD
Reason: EMA bullish crossover; RSI favorable (55.3); MACD bullish
Entry: MARKET | SL: 2758.70 | TPs: [2769.90, 2773.10, 2776.30]
============================================================
2025-10-27 21:45:00 | INFO     | Sending signal to http://scanner-go-gateway:8090/orders/enqueue
2025-10-27 21:45:00 | INFO     | ✅ Signal sent successfully: {'queued': 1, 'sid': 'signal-XAUUSD-1730060700'}
```

---

## 🔍 Monitoring

### Check Logs:

```bash
# Docker Compose
docker compose logs -f signal-generator

# Standalone Docker
docker logs -f scanner-signal-generator

# Standalone Python
tail -f signal_generator.log
```

### Health Check:

```bash
# Check if running
docker ps | grep signal-generator

# Check recent signals
docker logs scanner-signal-generator --tail 50 | grep "SIGNAL GENERATED"
```

---

## 🎯 Testing

### Manual Test:

```python
# test_signal.py
import requests

signal = {
    "sid": "test-manual-001",
    "symbol": "XAUUSD",
    "side": "LONG",
    "lot": 0.01,
    "sl": 2758.50,
    "tp_levels": [2773.50, 2778.50, 2783.50]
}

resp = requests.post("http://127.0.0.1:8090/orders/enqueue", json=signal)
print(resp.json())
```

---

## 📈 Strategy Optimization

### Conservative (Lower risk):

```bash
ATR_SL_MULTIPLIER=2.0
ATR_TP_MULTIPLIERS=2.5,3.5,5.0
DEFAULT_LOT=0.01
```

### Aggressive (Higher risk):

```bash
ATR_SL_MULTIPLIER=1.0
ATR_TP_MULTIPLIERS=1.5,2.0,3.0
DEFAULT_LOT=0.02
```

### Scalping (M1):

```bash
TIMEFRAME=M1
EMA_FAST=5
EMA_SLOW=13
CHECK_INTERVAL=30
ATR_SL_MULTIPLIER=1.0
ATR_TP_MULTIPLIERS=1.2,1.5,2.0
```

---

## 🔧 Troubleshooting

### No signals generated:

- Check tick data: `curl http://127.0.0.1:8088/healthz`
- Verify TickBridge is running in MT5
- Check indicator values in logs
- Reduce CHECK_INTERVAL for more frequent checks

### Signals not reaching MT5:

- Check go-gateway: `curl http://127.0.0.1:8090/healthz`
- Verify OrderExecutor is attached to chart
- Check Docker network connectivity

### High CPU usage:

- Increase CHECK_INTERVAL (e.g., 120 seconds)
- Reduce historical periods in candle building
- Use higher timeframe (M15, H1)

---

## 📚 Architecture

```
Signal Generator
    ↓ (fetch tick data)
py-obi-service
    ↓ (build candles)
Technical Analysis
    ↓ (EMA, RSI, MACD, ATR)
Signal Decision
    ↓ (LONG/SHORT)
Calculate SL/TP (ATR-based)
    ↓ (HTTP POST)
go-gateway
    ↓ (queue + Telegram)
OrderExecutor (MT5)
    ↓
Position Opened!
```

---

## 📝 Notes

- **Production Ready**: Proper error handling, logging, graceful shutdown
- **No OBI Required**: Works without Order Book data
- **Flexible**: Easy to add new indicators or strategies
- **Scalable**: Can run multiple instances for different symbols
- **Observable**: Detailed logging for monitoring and debugging

---

## 🚀 Next Steps

1. **Backtest** the strategy with historical data
2. **Paper trade** before going live
3. **Monitor performance** and adjust parameters
4. **Add more strategies** (Bollinger Bands, Fibonacci, etc.)
5. **Implement ML** for signal filtering

---

**Created by: Scanner Infrastructure Team**  
**Version: 1.0**  
**Date: October 27, 2025**
