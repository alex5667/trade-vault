# 💡 Examples and Usage Patterns

## Обзор

Этот документ содержит практические примеры использования системы, код интеграций, и паттерны разработки.

---

## 🚀 Quick Start Examples

### Пример 1: Запуск полной системы

```bash
# 1. Клонирование и переход в директорию
cd scanner_infra

# 2. Настройка Telegram
cat > telegram-worker/.env << EOF
TELEGRAM_API_ID=your_api_id
TELEGRAM_API_HASH=your_api_hash
TELEGRAM_PHONE=+1234567890
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
EOF

# 3. Запуск системы
make up

# 4. Проверка статуса
make full-status

# 5. Просмотр логов
make logs

# 6. Мониторинг Redis
make redis-stats
```

### Пример 2: Добавление нового символа (динамически)

```bash
# Добавить Bitcoin
make symbol-add SYMBOL=BTCUSD

# Проверить статус
make symbol-status

# Удалить символ
make symbol-remove SYMBOL=BTCUSD

# Установить список символов
make symbol-set SYMBOLS='XAUUSD BTCUSD ETHUSD'

# Показать активные символы
make symbol-list
```

### Пример 3: Мониторинг и диагностика

```bash
# Полная диагностика системы
make full-system-check

# Проверка конкретного сервиса
make signal-status
make hub-status
make gateway-status

# Логи конкретного сервиса
make signal-logs
make hub-logs
make telegram-logs

# Redis статистика
make redis-stats
make redis-memory

# Проверка сигналов
make check-signals
```

---

## 📊 Redis Examples

### Пример 1: Чтение candles из Redis

```python
import redis
import json

# Подключение к Redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Чтение последних 10 candles
messages = r.xrevrange('candles:data', count=10)

for msg_id, data in messages:
    candle = {
        'symbol': data['symbol'],
        'timeframe': data['timeframe'],
        'open': float(data['open']),
        'high': float(data['high']),
        'low': float(data['low']),
        'close': float(data['close']),
        'volume': float(data['volume']),
        'timestamp': int(data['open_time'])
    }

    print(f"{candle['symbol']} {candle['timeframe']}: "
          f"O={candle['open']:.2f} H={candle['high']:.2f} "
          f"L={candle['low']:.2f} C={candle['close']:.2f}")
```

### Пример 2: Consumer Group для обработки candles

```python
import redis
import time

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Создание consumer group
try:
    r.xgroup_create('candles:data', 'my-group', id='0', mkstream=True)
except redis.exceptions.ResponseError as e:
    print(f"Group already exists: {e}")

# Чтение из stream с consumer group
consumer_name = 'consumer-1'

while True:
    # Читаем новые сообщения
    messages = r.xreadgroup(
        'my-group',
        consumer_name,
        {'candles:data': '>'},
        count=10,
        block=1000  # Block for 1 second
    )

    for stream, msg_list in messages:
        for msg_id, data in msg_list:
            # Обработка candle
            process_candle(data)

            # Подтверждение обработки
            r.xack('candles:data', 'my-group', msg_id)

    time.sleep(0.1)
```

### Пример 3: Публикация сигнала в Redis

```python
import redis
import json
import time

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Формирование сигнала
signal = {
    "symbol": "XAUUSD",
    "side": "LONG",
    "entry": 2055.50,
    "sl": 2050.00,
    "tp1": 2060.00,
    "tp2": 2065.00,
    "tp3": 2070.00,
    "confidence": 0.85,
    "timestamp": time.time(),
    "features": {
        "delta_zscore": 3.5,
        "obi": 0.6,
        "speed": 0.02
    }
}

# Публикация в stream
stream_id = r.xadd(
    'signals:orderflow:XAUUSD',
    signal,
    maxlen=1000  # Храним последние 1000 сигналов
)

print(f"Signal published: {stream_id}")

# Также можно сохранить как hash для быстрого доступа
r.hset('signal:latest:XAUUSD', mapping=signal)
r.expire('signal:latest:XAUUSD', 3600)  # TTL 1 час
```

### Пример 4: Получение ATR из Redis

```python
import redis
import json

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Получение ATR для символа и таймфрейма
atr_key = 'ta:last:atr:XAUUSD:1m'
atr_data = r.get(atr_key)

if atr_data:
    atr_obj = json.loads(atr_data)

    print(f"Symbol: {atr_obj['symbol']}")
    print(f"Timeframe: {atr_obj['timeframe']}")
    print(f"ATR: {atr_obj['atr']:.4f}")
    print(f"Timestamp: {atr_obj['timestamp']}")
else:
    print("ATR not found")

# Использование ATR для расчета SL/TP
entry = 2055.50
atr_value = atr_obj['atr']
sl_multiplier = 1.5
tp_multipliers = [2.0, 3.0, 4.0]

sl = entry - (atr_value * sl_multiplier)  # Для LONG
tp1 = entry + (atr_value * tp_multipliers[0])
tp2 = entry + (atr_value * tp_multipliers[1])
tp3 = entry + (atr_value * tp_multipliers[2])

print(f"\nEntry: {entry:.2f}")
print(f"SL: {sl:.2f} ({entry - sl:.2f} points)")
print(f"TP1: {tp1:.2f} ({tp1 - entry:.2f} points)")
print(f"TP2: {tp2:.2f} ({tp2 - entry:.2f} points)")
print(f"TP3: {tp3:.2f} ({tp3 - entry:.2f} points)")
```

---

## 🔧 Integration Examples

### Пример 1: Отправка тиков в систему (MT5 EA)

**MQL5 код для Expert Advisor**:

```mql5
// MT5 Expert Advisor - Tick Publisher
#property strict

// API endpoint
string API_URL = "http://localhost:8087/tick";

// Символ
string SYMBOL = "XAUUSD";

// Интервал отправки (миллисекунды)
int SEND_INTERVAL = 100;  // 100ms = 10 тиков в секунду

datetime lastSendTime = 0;

//+------------------------------------------------------------------+
//| Expert tick function                                              |
//+------------------------------------------------------------------+
void OnTick()
{
    // Проверка интервала
    datetime currentTime = TimeCurrent();
    if(currentTime - lastSendTime < SEND_INTERVAL / 1000)
        return;

    lastSendTime = currentTime;

    // Получение данных
    double bid = SymbolInfoDouble(SYMBOL, SYMBOL_BID);
    double ask = SymbolInfoDouble(SYMBOL, SYMBOL_ASK);
    double last = SymbolInfoDouble(SYMBOL, SYMBOL_LAST);
    long volume = SymbolInfoInteger(SYMBOL, SYMBOL_VOLUME);

    // Формирование JSON
    string json = "{";
    json += "\"symbol\":\"" + SYMBOL + "\",";
    json += "\"time\":\"" + TimeToString(currentTime, TIME_DATE|TIME_SECONDS) + "\",";
    json += "\"bid\":" + DoubleToString(bid, 2) + ",";
    json += "\"ask\":" + DoubleToString(ask, 2) + ",";
    json += "\"last\":" + DoubleToString(last, 2) + ",";
    json += "\"volume\":" + IntegerToString(volume) + ",";
    json += "\"flags\":6";
    json += "}";

    // Отправка HTTP POST
    char post[], result[];
    StringToCharArray(json, post, 0, StringLen(json));

    string headers = "Content-Type: application/json\r\n";

    int res = WebRequest(
        "POST",
        API_URL,
        headers,
        5000,  // Timeout 5 seconds
        post,
        result,
        headers
    );

    if(res == 200)
    {
        Print("Tick sent successfully");
    }
    else
    {
        Print("Error sending tick: ", res);
    }
}
```

### Пример 2: Получение ордеров из системы (MT5 EA)

```mql5
// MT5 Expert Advisor - Order Receiver
#property strict

string GATEWAY_URL = "http://localhost:8090/orders/poll";
string CONFIRM_URL = "http://localhost:8090/orders/confirm";

int POLL_INTERVAL = 1000;  // 1 second

datetime lastPollTime = 0;

//+------------------------------------------------------------------+
//| Timer function                                                    |
//+------------------------------------------------------------------+
void OnTimer()
{
    PollOrders();
}

//+------------------------------------------------------------------+
//| Poll orders from gateway                                          |
//+------------------------------------------------------------------+
void PollOrders()
{
    char result[];
    string headers;

    int res = WebRequest(
        "GET",
        GATEWAY_URL,
        NULL,
        NULL,
        5000,
        result,
        headers
    );

    if(res != 200)
    {
        Print("Error polling orders: ", res);
        return;
    }

    // Парсинг JSON ответа
    string json = CharArrayToString(result);

    // Упрощенный парсинг (в реальности нужна библиотека для JSON)
    if(StringFind(json, "\"orders\":[") >= 0)
    {
        // Есть ордера для исполнения
        ProcessOrders(json);
    }
}

//+------------------------------------------------------------------+
//| Process orders                                                    |
//+------------------------------------------------------------------+
void ProcessOrders(string json)
{
    // Парсинг ордеров из JSON
    // (упрощенный пример, в реальности нужна JSON библиотека)

    // Пример ордера
    string orderID = ExtractValue(json, "order_id");
    string symbol = ExtractValue(json, "symbol");
    string side = ExtractValue(json, "side");
    double entry = StringToDouble(ExtractValue(json, "entry"));
    double sl = StringToDouble(ExtractValue(json, "sl"));
    double tp1 = StringToDouble(ExtractValue(json, "tp1"));
    double lot = StringToDouble(ExtractValue(json, "lot"));

    // Исполнение ордера
    int ticket = 0;

    if(side == "LONG")
    {
        ticket = OrderSend(
            symbol,
            OP_BUY,
            lot,
            Ask,
            3,
            sl,
            tp1,
            "Scanner Signal",
            0,
            0,
            clrGreen
        );
    }
    else if(side == "SHORT")
    {
        ticket = OrderSend(
            symbol,
            OP_SELL,
            lot,
            Bid,
            3,
            sl,
            tp1,
            "Scanner Signal",
            0,
            0,
            clrRed
        );
    }

    if(ticket > 0)
    {
        Print("Order executed: ", ticket);

        // Подтверждение исполнения
        ConfirmOrder(orderID, ticket);
    }
    else
    {
        Print("Error executing order: ", GetLastError());
    }
}

//+------------------------------------------------------------------+
//| Confirm order execution                                           |
//+------------------------------------------------------------------+
void ConfirmOrder(string orderID, int ticket)
{
    string json = "{";
    json += "\"order_id\":\"" + orderID + "\",";
    json += "\"ticket\":" + IntegerToString(ticket) + ",";
    json += "\"status\":\"filled\"";
    json += "}";

    char post[], result[];
    StringToCharArray(json, post, 0, StringLen(json));

    string headers = "Content-Type: application/json\r\n";

    int res = WebRequest(
        "POST",
        CONFIRM_URL,
        headers,
        5000,
        post,
        result,
        headers
    );

    if(res == 200)
    {
        Print("Order confirmed");
    }
}
```

### Пример 3: Python скрипт для мониторинга сигналов

```python
#!/usr/bin/env python3
"""
Signal Monitor - Real-time мониторинг сигналов из Redis
"""

import redis
import json
import time
from datetime import datetime
from colorama import Fore, Style, init

init(autoreset=True)

# Подключение к Redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Streams для мониторинга
STREAMS = {
    'signals:orderflow:XAUUSD': '$',
    'signals:ta:XAUUSD': '$'
}

print(f"{Fore.CYAN}=== Signal Monitor Started ==={Style.RESET_ALL}\n")

while True:
    try:
        # Чтение из streams
        messages = r.xread(STREAMS, block=1000, count=10)

        for stream_name, msg_list in messages:
            for msg_id, data in msg_list:
                # Обновление last ID
                STREAMS[stream_name] = msg_id

                # Определение типа сигнала
                signal_type = "OrderFlow" if "orderflow" in stream_name else "TA"

                # Извлечение данных
                symbol = data.get('symbol', 'N/A')
                side = data.get('side', 'N/A')
                confidence = float(data.get('confidence', 0))
                entry = float(data.get('entry', 0))

                # Цветовое кодирование
                color = Fore.GREEN if side == "LONG" else Fore.RED

                # Вывод
                timestamp = datetime.now().strftime('%H:%M:%S')
                print(f"{Fore.YELLOW}[{timestamp}]{Style.RESET_ALL} "
                      f"{Fore.CYAN}{signal_type:10s}{Style.RESET_ALL} "
                      f"{color}{side:5s}{Style.RESET_ALL} "
                      f"{symbol:10s} @ {entry:8.2f} "
                      f"Conf: {confidence:.2%}")

                # Детали
                if 'features' in data:
                    features = json.loads(data['features'])
                    print(f"  └─ Features: {features}")

    except KeyboardInterrupt:
        print(f"\n{Fore.CYAN}=== Signal Monitor Stopped ==={Style.RESET_ALL}")
        break

    except Exception as e:
        print(f"{Fore.RED}Error: {e}{Style.RESET_ALL}")
        time.sleep(1)
```

### Пример 4: Webhook для получения уведомлений

```python
#!/usr/bin/env python3
"""
Webhook Server - Получение уведомлений от системы
"""

from flask import Flask, request, jsonify
import logging

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

@app.route('/webhook/signal', methods=['POST'])
def receive_signal():
    """Прием торгового сигнала"""
    try:
        data = request.json

        logging.info(f"Signal received: {data['side']} {data['symbol']} @ {data['entry']}")

        # Обработка сигнала
        # Например, отправка в другую систему
        process_signal(data)

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        logging.error(f"Error processing signal: {e}")
        return jsonify({"status": "error", "message": str(e)}), 400

@app.route('/webhook/notification', methods=['POST'])
def receive_notification():
    """Прием уведомлений"""
    try:
        data = request.json

        logging.info(f"Notification: {data.get('message', '')}")

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        logging.error(f"Error processing notification: {e}")
        return jsonify({"status": "error", "message": str(e)}), 400

def process_signal(signal):
    """Обработка сигнала (пример)"""
    # Здесь можно:
    # - Отправить в Telegram
    # - Записать в БД
    # - Отправить в внешнюю систему
    # - Выполнить торговый ордер
    pass

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
```

---

## 🔨 Development Examples

### Пример 1: Создание нового OrderFlow Handler

```python
#!/usr/bin/env python3
"""
Custom OrderFlow Handler для нового символа
"""

from handlers.base_orderflow_handler import BaseOrderFlowHandler
import numpy as np

class SOLUSDOrderFlowHandler(BaseOrderFlowHandler):
    """OrderFlow Handler для Solana (SOLUSD)"""

    def __init__(self, redis_client, config):
        super().__init__(redis_client, config)

        self.symbol = "SOLUSD"

        # Custom параметры для SOL
        self.delta_z_threshold = 2.0  # Ниже чем XAUUSD (крипта)
        self.obi_threshold = 0.4
        self.weak_progress_atr = 0.2  # Выше чем XAUUSD
        self.min_signal_interval = 30  # Чаще сигналы

        # Crypto-specific
        self.volatility_multiplier = 1.5  # Крипта волатильнее

    def calculate_custom_confidence(self, features):
        """Кастомный расчет confidence для SOL"""
        base_confidence = super().calculate_confidence(features)

        # Учет волатильности крипты
        volatility_factor = min(features.get('volatility', 0) / 0.1, 1.5)

        adjusted_confidence = base_confidence * volatility_factor

        return min(adjusted_confidence, 1.0)

    def apply_filters(self, signal):
        """Дополнительные фильтры для SOL"""
        # Базовые фильтры
        if not super().apply_filters(signal):
            return False

        # Фильтр по времени (избегать азиатской сессии для крипты)
        hour = datetime.now(timezone.utc).hour
        if 2 <= hour <= 6:  # Низкая ликвидность
            return False

        # Фильтр по объему
        if signal.get('volume', 0) < 1000:
            return False

        return True

# Использование
if __name__ == '__main__':
    import redis

    r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    config = {
        'tick_stream': 'stream:tick_SOLUSD',
        'book_stream': 'stream:book_SOLUSD',
        'signal_stream': 'signals:orderflow:SOLUSD'
    }

    handler = SOLUSDOrderFlowHandler(r, config)
    handler.start()
```

### Пример 2: Создание custom индикатора

```python
#!/usr/bin/env python3
"""
Custom Technical Indicator - VWAP (Volume Weighted Average Price)
"""

import numpy as np
from collections import deque

class VWAPIndicator:
    """Volume Weighted Average Price"""

    def __init__(self, period=20):
        self.period = period
        self.prices = deque(maxlen=period)
        self.volumes = deque(maxlen=period)
        self.vwap = None

    def update(self, price, volume):
        """Обновление VWAP новым значением"""
        self.prices.append(price)
        self.volumes.append(volume)

        if len(self.prices) < self.period:
            return None

        # VWAP = SUM(Price * Volume) / SUM(Volume)
        prices_array = np.array(self.prices)
        volumes_array = np.array(self.volumes)

        self.vwap = np.sum(prices_array * volumes_array) / np.sum(volumes_array)

        return self.vwap

    def get_signal(self, current_price):
        """Генерация сигнала на основе VWAP"""
        if self.vwap is None:
            return None

        deviation = (current_price - self.vwap) / self.vwap

        # Если цена сильно ниже VWAP - BUY
        if deviation < -0.02:  # -2%
            return "LONG", abs(deviation)

        # Если цена сильно выше VWAP - SELL
        elif deviation > 0.02:  # +2%
            return "SHORT", abs(deviation)

        return None, 0

# Использование с real-time данными
class VWAPStrategy:
    def __init__(self, redis_client):
        self.redis = redis_client
        self.vwap = VWAPIndicator(period=20)

    def run(self):
        """Основной цикл стратегии"""
        while True:
            # Получение последнего тика
            tick = self.get_latest_tick()

            if tick:
                # Обновление VWAP
                vwap_value = self.vwap.update(
                    tick['price'],
                    tick['volume']
                )

                if vwap_value:
                    # Проверка сигнала
                    signal, confidence = self.vwap.get_signal(tick['price'])

                    if signal:
                        self.publish_signal(signal, tick['price'], confidence)

            time.sleep(0.1)

    def get_latest_tick(self):
        """Получение последнего тика из Redis"""
        messages = self.redis.xrevrange('stream:tick_XAUUSD', count=1)

        if messages:
            msg_id, data = messages[0]
            return {
                'price': float(data['last']),
                'volume': float(data['volume'])
            }

        return None

    def publish_signal(self, side, price, confidence):
        """Публикация сигнала"""
        signal = {
            'symbol': 'XAUUSD',
            'side': side,
            'entry': price,
            'confidence': confidence,
            'indicator': 'VWAP',
            'timestamp': time.time()
        }

        self.redis.xadd('signals:vwap:XAUUSD', signal)

        print(f"VWAP Signal: {side} @ {price:.2f}, Conf: {confidence:.2%}")
```

### Пример 3: Backtest framework

```python
#!/usr/bin/env python3
"""
Backtest Framework для тестирования стратегий
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class Backtest:
    def __init__(self, initial_balance=10000):
        self.initial_balance = initial_balance
        self.balance = initial_balance
        self.equity = initial_balance

        self.positions = []
        self.trades = []

        self.stats = {
            'total_trades': 0,
            'winning_trades': 0,
            'losing_trades': 0,
            'total_pnl': 0,
            'max_drawdown': 0,
            'sharpe_ratio': 0
        }

    def load_data(self, filename):
        """Загрузка исторических данных"""
        self.data = pd.read_parquet(filename)
        print(f"Loaded {len(self.data)} ticks from {filename}")

    def run_strategy(self, strategy):
        """Запуск стратегии на исторических данных"""
        print("Running backtest...")

        for idx, row in self.data.iterrows():
            # Обновление открытых позиций
            self.update_positions(row)

            # Генерация сигнала стратегией
            signal = strategy.analyze(row)

            if signal:
                self.open_position(signal, row)

        # Закрытие всех открытых позиций
        self.close_all_positions(self.data.iloc[-1])

        # Расчет статистики
        self.calculate_stats()

        return self.stats

    def open_position(self, signal, tick):
        """Открытие позиции"""
        position = {
            'id': len(self.positions),
            'symbol': signal['symbol'],
            'side': signal['side'],
            'entry': tick['price'],
            'sl': signal['sl'],
            'tp': signal['tp'],
            'lot': signal['lot'],
            'open_time': tick['timestamp'],
            'status': 'open'
        }

        self.positions.append(position)
        print(f"Position opened: {position['side']} @ {position['entry']:.2f}")

    def update_positions(self, tick):
        """Обновление позиций текущей ценой"""
        for pos in self.positions:
            if pos['status'] != 'open':
                continue

            # Проверка SL/TP
            current_price = tick['price']

            if pos['side'] == 'LONG':
                if current_price <= pos['sl']:
                    self.close_position(pos, current_price, tick['timestamp'], 'SL')
                elif current_price >= pos['tp']:
                    self.close_position(pos, current_price, tick['timestamp'], 'TP')

            else:  # SHORT
                if current_price >= pos['sl']:
                    self.close_position(pos, current_price, tick['timestamp'], 'SL')
                elif current_price <= pos['tp']:
                    self.close_position(pos, current_price, tick['timestamp'], 'TP')

    def close_position(self, position, close_price, close_time, reason):
        """Закрытие позиции"""
        # Расчет P&L
        if position['side'] == 'LONG':
            pnl = (close_price - position['entry']) * position['lot'] * 100
        else:
            pnl = (position['entry'] - close_price) * position['lot'] * 100

        # Обновление баланса
        self.balance += pnl

        # Сохранение trade
        trade = {
            **position,
            'close_price': close_price,
            'close_time': close_time,
            'pnl': pnl,
            'reason': reason
        }

        self.trades.append(trade)
        position['status'] = 'closed'

        print(f"Position closed: {reason} @ {close_price:.2f}, P&L: ${pnl:.2f}")

    def close_all_positions(self, last_tick):
        """Закрытие всех открытых позиций"""
        for pos in self.positions:
            if pos['status'] == 'open':
                self.close_position(pos, last_tick['price'], last_tick['timestamp'], 'EOD')

    def calculate_stats(self):
        """Расчет статистики бэктеста"""
        if not self.trades:
            return

        df = pd.DataFrame(self.trades)

        self.stats['total_trades'] = len(df)
        self.stats['winning_trades'] = len(df[df['pnl'] > 0])
        self.stats['losing_trades'] = len(df[df['pnl'] < 0])
        self.stats['total_pnl'] = df['pnl'].sum()

        # Win rate
        self.stats['win_rate'] = self.stats['winning_trades'] / self.stats['total_trades']

        # Average win/loss
        wins = df[df['pnl'] > 0]['pnl']
        losses = df[df['pnl'] < 0]['pnl']

        self.stats['avg_win'] = wins.mean() if len(wins) > 0 else 0
        self.stats['avg_loss'] = losses.mean() if len(losses) > 0 else 0

        # Profit factor
        total_wins = wins.sum() if len(wins) > 0 else 0
        total_losses = abs(losses.sum()) if len(losses) > 0 else 1
        self.stats['profit_factor'] = total_wins / total_losses if total_losses > 0 else 0

        # Max drawdown
        equity_curve = df['pnl'].cumsum() + self.initial_balance
        running_max = equity_curve.expanding().max()
        drawdown = (equity_curve - running_max) / running_max
        self.stats['max_drawdown'] = drawdown.min()

        # Sharpe ratio (упрощенный)
        returns = df['pnl'] / self.initial_balance
        self.stats['sharpe_ratio'] = returns.mean() / returns.std() if returns.std() > 0 else 0

    def print_report(self):
        """Вывод отчета"""
        print("\n" + "="*50)
        print("BACKTEST RESULTS")
        print("="*50)
        print(f"Initial Balance: ${self.initial_balance:,.2f}")
        print(f"Final Balance: ${self.balance:,.2f}")
        print(f"Total P&L: ${self.stats['total_pnl']:,.2f}")
        print(f"Return: {(self.balance / self.initial_balance - 1) * 100:.2f}%")
        print()
        print(f"Total Trades: {self.stats['total_trades']}")
        print(f"Winning Trades: {self.stats['winning_trades']}")
        print(f"Losing Trades: {self.stats['losing_trades']}")
        print(f"Win Rate: {self.stats['win_rate']:.2%}")
        print()
        print(f"Average Win: ${self.stats['avg_win']:.2f}")
        print(f"Average Loss: ${self.stats['avg_loss']:.2f}")
        print(f"Profit Factor: {self.stats['profit_factor']:.2f}")
        print()
        print(f"Max Drawdown: {self.stats['max_drawdown']:.2%}")
        print(f"Sharpe Ratio: {self.stats['sharpe_ratio']:.2f}")
        print("="*50)

# Использование
if __name__ == '__main__':
    # Создание бэктеста
    bt = Backtest(initial_balance=10000)

    # Загрузка данных
    bt.load_data('data/xau_ticks.parquet')

    # Создание стратегии
    from strategies import MyStrategy
    strategy = MyStrategy()

    # Запуск
    stats = bt.run_strategy(strategy)

    # Отчет
    bt.print_report()
```

---

## 📊 Monitoring Examples

### Пример 1: Prometheus query examples

```promql
# Среднее количество сообщений в секунду по всем Go workers
rate(binance_ws_messages_total[1m])

# Latency публикации в Redis (99th percentile)
histogram_quantile(0.99, rate(redis_publish_duration_seconds_bucket[5m]))

# Количество активных WebSocket соединений
sum(binance_ws_connected)

# Сгенерированные OrderFlow сигналы за последний час
increase(orderflow_signals_generated_total[1h])

# Средний Z-score дельты
avg(orderflow_delta_zscore)

# Использование памяти Redis
redis_used_memory_bytes / 1024 / 1024 / 1024  # В GB

# Количество подключенных клиентов к Redis
redis_connected_clients

# Длина Redis streams
redis_stream_length{stream="candles:data"}
```

### Пример 2: Grafana Dashboard JSON (пример панели)

```json
{
	"dashboard": {
		"title": "Scanner Infrastructure - Main Dashboard",
		"panels": [
			{
				"id": 1,
				"title": "Binance Messages Rate",
				"type": "graph",
				"targets": [
					{
						"expr": "rate(binance_ws_messages_total[1m])",
						"legendFormat": "{{worker}}"
					}
				]
			},
			{
				"id": 2,
				"title": "OrderFlow Signals",
				"type": "graph",
				"targets": [
					{
						"expr": "increase(orderflow_signals_generated_total[5m])",
						"legendFormat": "{{symbol}}"
					}
				]
			},
			{
				"id": 3,
				"title": "Redis Memory Usage",
				"type": "gauge",
				"targets": [
					{
						"expr": "redis_used_memory_bytes / redis_max_memory_bytes * 100"
					}
				]
			}
		]
	}
}
```

---

## 🐛 Debugging Examples

### Пример 1: Debug скрипт для проверки потока данных

```python
#!/usr/bin/env python3
"""
Debug Script - Проверка всего data flow
"""

import redis
import time
import json

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

def check_candles():
    """Проверка candles stream"""
    print("Checking candles stream...")

    messages = r.xrevrange('candles:data', count=10)

    if not messages:
        print("❌ No candles found!")
        return False

    print(f"✅ Found {len(messages)} candles")

    # Группировка по таймфреймам
    timeframes = {}
    for msg_id, data in messages:
        tf = data['timeframe']
        timeframes[tf] = timeframes.get(tf, 0) + 1

    print(f"   Timeframes: {timeframes}")
    return True

def check_ticks():
    """Проверка tick stream"""
    print("\nChecking tick stream...")

    messages = r.xrevrange('stream:tick_XAUUSD', count=10)

    if not messages:
        print("❌ No ticks found!")
        return False

    print(f"✅ Found {len(messages)} ticks")

    # Последний тик
    last_tick = messages[0][1]
    print(f"   Last tick: {last_tick['last']} @ {last_tick['time']}")

    return True

def check_signals():
    """Проверка signals stream"""
    print("\nChecking signals stream...")

    of_signals = r.xrevrange('signals:orderflow:XAUUSD', count=10)
    ta_signals = r.xrevrange('signals:ta:XAUUSD', count=10)

    print(f"   OrderFlow signals: {len(of_signals)}")
    print(f"   TA signals: {len(ta_signals)}")

    if len(of_signals) == 0 and len(ta_signals) == 0:
        print("❌ No signals found!")
        return False

    print("✅ Signals found")
    return True

def check_atr():
    """Проверка ATR values"""
    print("\nChecking ATR values...")

    atr_key = 'ta:last:atr:XAUUSD:1m'
    atr_data = r.get(atr_key)

    if not atr_data:
        print("❌ No ATR data!")
        return False

    atr_obj = json.loads(atr_data)
    print(f"✅ ATR: {atr_obj['atr']:.4f}")

    return True

def check_order_book():
    """Проверка order book"""
    print("\nChecking order book...")

    book_data = r.hgetall('book:levels:XAUUSD')

    if not book_data:
        print("❌ No order book data!")
        return False

    print(f"✅ Order book has {len(book_data)} fields")
    return True

def main():
    print("="*50)
    print("DATA FLOW DEBUG SCRIPT")
    print("="*50 + "\n")

    results = {
        'Candles': check_candles(),
        'Ticks': check_ticks(),
        'Signals': check_signals(),
        'ATR': check_atr(),
        'Order Book': check_order_book()
    }

    print("\n" + "="*50)
    print("SUMMARY")
    print("="*50)

    for component, status in results.items():
        icon = "✅" if status else "❌"
        print(f"{icon} {component}")

    all_ok = all(results.values())

    if all_ok:
        print("\n🎉 All checks passed!")
    else:
        print("\n⚠️ Some checks failed. Check services status.")

if __name__ == '__main__':
    main()
```

---

**Примеры покрывают**:

- ✅ Quick start и базовое использование
- ✅ Работу с Redis (чтение, запись, consumer groups)
- ✅ Интеграцию с MT5 через MQL5
- ✅ Python скрипты для мониторинга и разработки
- ✅ Создание custom handlers и индикаторов
- ✅ Backtest framework
- ✅ Prometheus queries и Grafana dashboards
- ✅ Debug скрипты

Используйте эти примеры как отправную точку для своих интеграций!
