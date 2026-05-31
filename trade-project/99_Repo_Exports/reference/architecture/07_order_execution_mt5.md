# Этап 7: Исполнение ордеров (Order Execution)

## Что это и зачем?
Сигнал в Redis — это ещё не сделка. Его нужно "материализовать": отправить ордер на реальную биржу или в брокерский терминал. Это самый ответственный шаг — ошибки здесь стоят денег.

В системе реализованы три пути исполнения:
1. **MT5 (MetaTrader 5)** — через MQL5 советники (Forex/CFD брокеры)
2. **Binance REST API** — прямое исполнение на крипто-бирже
3. **Paper Trade (Симулятор)** — для тестирования без реальных денег

---

## 1. Архитектура MT5 Bridge

MetaTrader 5 работает на Windows, а весь наш стек — на Linux. Мост выглядит так:
```
Python (Linux) → Redis "orders:queue:mt5" → MT5 Expert Advisor (Windows) → Broker
```

```python
# Запись ордера в очередь (из Python-воркера):
order = {
    "signal_id": "4fac31a...",
    "action": "OPEN",
    "symbol": "BTCUSD",        # Символ в нотации брокера
    "side": "BUY",
    "entry_price": 64500.5,
    "sl_price": 64000.0,
    "tp1_price": 65500.0,
    "risk_pct": 1.0,           # % риска от депозита
    "comment": "CryptoOrderFlow:breakout:88.5%",
}
await redis.xadd("orders:queue:mt5", {"data": json.dumps(order)}, maxlen=10000)
```

---

## 2. OrderExecutorAdvanced.mq5 — Робот в MetaTrader

Файл `scanner_infra/mt5/OrderExecutorAdvanced.mq5`. Работает через OnTimer (вызывается каждые N ms).

### Расчет Лота (Money Management)
```mql5
double CalculateLotSize(string symbol, double sl_price, double risk_pct)
{
    double balance = AccountInfoDouble(ACCOUNT_BALANCE);
    double risk_amount = balance * (risk_pct / 100.0);       // $ риска
    double entry = SymbolInfoDouble(symbol, SYMBOL_ASK);
    double sl_dist = MathAbs(entry - sl_price);              // Расстояние до стопа
    double pip_value = SymbolInfoDouble(symbol, SYMBOL_TRADE_TICK_VALUE);
    double point = SymbolInfoDouble(symbol, SYMBOL_POINT);
    
    // Формула: лот = риск / (пипы_стопа * стоимость_пипа)
    double lot = risk_amount / ((sl_dist / point) * pip_value);
    double step = SymbolInfoDouble(symbol, SYMBOL_VOLUME_STEP);
    return MathMax(MathFloor(lot / step) * step, 
                   SymbolInfoDouble(symbol, SYMBOL_VOLUME_MIN));
}
```

### Retry loop для реквотов
```mql5
bool PlaceMarketOrder(string symbol, string side, double lot, double sl, double tp)
{
    MqlTradeRequest req = {};
    MqlTradeResult res = {};
    req.action = TRADE_ACTION_DEAL;
    req.type = (side == "BUY") ? ORDER_TYPE_BUY : ORDER_TYPE_SELL;
    req.volume = lot;
    req.sl = sl;
    req.tp = tp;
    
    for (int attempt = 0; attempt < 3; attempt++) {
        if (OrderSend(req, res) && res.retcode == TRADE_RETCODE_DONE)
            return true;
        if (res.retcode == TRADE_RETCODE_REQUOTE) {
            req.price = SymbolInfoDouble(symbol, 
                (side == "BUY") ? SYMBOL_ASK : SYMBOL_BID);
            Sleep(100);
            continue;
        }
        if (res.retcode == TRADE_RETCODE_CONNECTION) { Sleep(1000); continue; }
        return false;
    }
    return false;
}
```

---

## 3. Binance REST API Executor (Python)

```python
class BinanceExecutor:
    def _sign(self, params: dict) -> str:
        query = "&".join(f"{k}={v}" for k, v in params.items())
        return hmac.new(self.api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    
    async def place_order(self, signal: dict) -> dict:
        params = {
            "symbol": signal["symbol"],
            "side": signal["side"],
            "type": "MARKET",
            "quantity": self._calc_quantity(signal),
            "timestamp": int(time.time() * 1000),
        }
        params["signature"] = self._sign(params)
        
        async with aiohttp.ClientSession() as session:
            response = await session.post(
                f"{self.base_url}/fapi/v1/order",
                params=params,
                headers={"X-MBX-APIKEY": self.api_key}
            )
            result = await response.json()
        
        if "orderId" in result:
            await self._place_stop_loss(signal)  # Ставим SL отдельным запросом
            return result
        raise Exception(f"Binance error: {result}")
```

---

## 4. Paper Trading (Симулятор)

```python
# Включается через: CRYPTO_PAPER_SHADOW_ENABLED=1
class PaperTradeSimulator:
    async def simulate_open(self, signal: dict) -> str:
        # Симулируем проскальзывание
        slip = 1.0003 if signal["side"] == "BUY" else 0.9997
        fill_price = signal["entry_price"] * slip
        
        await db.execute("""
            INSERT INTO paper_trades (signal_id, symbol, side, fill_price, 
                                      sl_price, tp1_price, status, opened_at)
            VALUES ($1, $2, $3, $4, $5, $6, 'OPEN', NOW())
        """, signal["signal_id"], signal["symbol"], signal["side"],
            fill_price, signal["sl_price"], signal["tp1_price"])
        return signal["signal_id"]
    
    async def tick_update(self, symbol: str, current_price: float) -> None:
        """На каждом тике: закрываем если выбило по SL или TP."""
        trades = await db.fetch("""
            SELECT * FROM paper_trades WHERE symbol=$1 AND status='OPEN'
        """, symbol)
        
        for trade in trades:
            if trade["side"] == "BUY":
                if current_price <= trade["sl_price"]:
                    await self._close(trade["signal_id"], current_price, "stop_loss")
                elif current_price >= trade["tp1_price"]:
                    await self._close(trade["signal_id"], current_price, "tp1_hit")
```
