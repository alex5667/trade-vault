# Этап 8: Пост-Трейд и Динамический Риск-менеджмент

## Что это и зачем?
Открытие позиции — только начало. 80% успеха в трейдинге — это управление уже открытой сделкой. Статичные TP/SL — примитивный подход. Наша система динамически адаптирует защитные уровни под поведение рынка.

Задействованные компоненты:
- `post-sl-analyzer` (Python-сервис)
- `sl-quantile-aggregator` (микросервис ML-стопов)
- Встроенная логика в `OrderExecutorAdvanced.mq5`

---

## 1. Trade Monitor (Мониторинг открытых позиций)

```python
# Из post-sl-analyzer (упрощенно):
class TradeMonitor:
    async def monitor_loop(self) -> None:
        """Каждые N секунд опрашивает брокера и обновляет стейт позиций."""
        while True:
            await asyncio.sleep(float(os.getenv("MONITOR_INTERVAL_SEC", "5")))
            
            positions = await self.broker.get_open_positions()
            
            for pos in positions:
                # Обновляем запись в базе
                await self.db.execute("""
                    UPDATE open_positions 
                    SET current_price=$1, 
                        floating_pnl=$2, 
                        max_favorable=$3, 
                        updated_at=NOW()
                    WHERE signal_id=$4
                """, pos.current_price, pos.floating_pnl,
                    max(pos.max_favorable, pos.current_price if pos.side == "BUY" 
                        else -pos.current_price),
                    pos.signal_id)
                
                # Запускаем логику трейлинга
                await self._check_trailing(pos)
```

---

## 2. Break Even и Trailing Stop (Динамический Трейлинг)

```python
# Логика управления позицией:
class TrailingManager:
    async def _check_trailing(self, pos: Position) -> None:
        """Проверяет условие перехода в безубыток и трейлинга."""
        
        # --- Фаза 1: Break Even ---
        # Условие: цена достигла TP1 (первой цели)
        tp1_price = pos.tp1_price
        be_activated = getattr(pos, "be_activated", False)
        
        if not be_activated:
            if (pos.side == "BUY" and pos.current_price >= tp1_price) or \
               (pos.side == "SELL" and pos.current_price <= tp1_price):
                
                commission_bps = float(os.getenv("TAKER_FEE_BPS", "4.0"))
                
                # Новый SL = цена входа + компенсация комиссии
                if pos.side == "BUY":
                    new_sl = pos.entry_price * (1 + commission_bps / 10000)
                else:
                    new_sl = pos.entry_price * (1 - commission_bps / 10000)
                
                await self.broker.modify_sl(pos.ticket, new_sl)
                await self.db.set_field(pos.signal_id, "be_activated", True)
                
                logger.info(
                    "Break Even activated: %s new_sl=%.2f", 
                    pos.signal_id, new_sl
                )
        
        # --- Фаза 2: Trailing Stop (после Break Even) ---
        else:
            # Phase 2.6: Trailing Surface A/B Canary
            # Вычисляется безусловно (unconditional telemetry) для аналитики
            canary_decision = should_apply_trailing_surface(symbol=pos.symbol, sid=pos.sid)
            trailing_surface = build_trailing_surface(pos.signal_payload, pos.atr, default_mult)
            
            # Сохраняем в payload для BatchTradeWriter (запишется при закрытии)
            pos.signal_payload["meta"]["trailing_canary_decision"] = canary_decision
            pos.signal_payload["meta"]["trailing_surface_diagnostic"] = trailing_surface

            # Применяем локальный фоллбек (или отправляем во внешний оркестратор)
            trail_offset = trailing_surface.get("baseline_offset_distance_px")
            if canary_decision.get("should_apply"):
                trail_offset = trailing_surface.get("selected_offset_distance_px", trail_offset)
            
            if pos.side == "BUY":
                # "Ползущий" стоп = максимум позиции - trail_offset
                ideal_sl = pos.max_favorable - trail_offset
                if ideal_sl > pos.current_sl:  # Только вверх (никогда вниз)
                    await self.broker.modify_sl(pos.ticket, ideal_sl)
            
            # Для SELL: аналогично но зеркально
```

**Ключевая философия**: Stop Loss можно только **двигать в сторону прибыли**. Никогда обратно. Это называется "ratchet" (храповик).

---

## 3. SLQ — Адаптивный Stop Loss (ML для стопов)

Вместо константы `1.5 * ATR`, сервис `sl-quantile-aggregator` смотрит в историю:

```python
# Из sl-quantile-aggregator (упрощенно):
class SLQuantileAggregator:
    """
    Строит распределение Maximum Adverse Excursion (MAE) — 
    "насколько глубоко уходила сделка в минус перед тем как выстрелить".
    """
    
    async def compute_adaptive_mult(self, symbol: str, kind: str) -> float:
        window_days = int(os.getenv("SLQ_WINDOW", "30"))
        
        # Получаем MAE всех прибыльных сделок за N дней
        rows = await self.db.fetch("""
            SELECT 
                ABS(min_floating_pnl / entry_price) AS mae_pct
            FROM closed_trades
            WHERE symbol = $1 
              AND kind = $2
              AND pnl > 0          -- Только прибыльные!
              AND closed_at > NOW() - INTERVAL '$3 days'
            ORDER BY mae_pct
        """, symbol, kind, window_days)
        
        if len(rows) < 20:  # Мало данных — используем дефолт
            return float(os.getenv("DEFAULT_SL_ATR_MULT", "1.5"))
        
        mae_values = [row["mae_pct"] for row in rows]
        
        # Q90: 90% прибыльных сделок не выходили за этот "просадку"
        q90_pct = np.percentile(mae_values, 90)
        
        # Конвертируем % просадки в множитель ATR
        current_atr = await self.get_atr(symbol)
        if current_atr <= 0:
            return 1.5
        
        current_price = await self.get_price(symbol)
        atr_pct = current_atr / current_price  # ATR как % от цены
        
        adaptive_mult = q90_pct / atr_pct
        
        # Ограничиваем разумными значениями
        adaptive_mult = np.clip(
            adaptive_mult,
            float(os.getenv("SLQ_MULT_MIN", "0.8")),
            float(os.getenv("SLQ_MULT_MAX", "3.0"))
        )
        
        logger.info(
            "SLQ update: %s kind=%s q90=%.3f%% atr_pct=%.3f%% mult=%.2f",
            symbol, kind, q90_pct * 100, atr_pct * 100, adaptive_mult
        )
        
        return adaptive_mult

# Использование при создании сигнала:
atr_mult = await slq.compute_adaptive_mult(symbol, kind)
sl_price = entry_price - (atr * atr_mult)  # Адаптивный стоп!
```

**Что это дает?** Если BTC начинает "дышать" шире (высокая волатильность), Q90 автоматически вырастет, и стопы станут дальше — меньше случайных выбиваний. Если рынок успокоился — стопы подтянутся ближе.

---

## 4. Slippage Feedback Loop (Петля обратной связи по проскальзыванию)

```python
# После закрытия каждой сделки:
class SlippageTracker:
    async def record_fill(self, signal_id: str, fill_price: float) -> None:
        """Записывает реальное исполнение и считает проскальзывание."""
        signal = await self.db.get_signal(signal_id)
        
        signal_price = signal["entry_price"]
        
        # Проскальзывание в базисных пунктах
        slippage_bps = abs(fill_price - signal_price) / signal_price * 10_000
        
        # EMA проскальзывания (exponential moving average)
        prev_ema = await self.redis.get(f"slippage_ema:{signal['symbol']}")
        alpha = 0.1  # Коэффициент сглаживания
        new_ema = (float(prev_ema or slippage_bps) * (1 - alpha) + slippage_bps * alpha)
        
        await self.redis.set(
            f"slippage_ema:{signal['symbol']}", 
            str(new_ema),
            ex=86400  # Обновляем каждый день
        )
        
        # Логируем и экспортируем в Prometheus
        slippage_bps_hist.labels(symbol=signal["symbol"]).observe(slippage_bps)
```

**Edge Cost Gate** читает `slippage_ema:{symbol}` при следующей оценке сигнала. Если проскальзывание выросло (плохая ликвидность) — Gate автоматически заблокирует торговлю по паре.

---

## 5. PostgreSQL — База аналитики

```sql
-- Структура таблицы закрытых сделок:
CREATE TABLE closed_trades (
    signal_id        TEXT PRIMARY KEY,
    symbol           TEXT NOT NULL,
    kind             TEXT NOT NULL,       -- 'breakout', 'absorption', etc.
    side             TEXT NOT NULL,       -- 'BUY' / 'SELL'
    entry_price      NUMERIC NOT NULL,
    exit_price       NUMERIC,
    sl_price         NUMERIC,
    tp1_price        NUMERIC,
    fill_price       NUMERIC,             -- Реальная цена исполнения
    pnl              NUMERIC,             -- Прибыль/убыток в USD
    pnl_bps          NUMERIC,             -- PnL в базисных пунктах
    max_mae_pct      NUMERIC,             -- Maximum Adverse Excursion (%)
    confidence       NUMERIC,             -- Уверенность модели
    ml_confirm_p     NUMERIC,             -- Вероятность L2 модели
    slippage_bps     NUMERIC,             -- Проскальзывание
    close_reason     TEXT,               -- 'stop_loss', 'tp1_hit', 'manual'
    opened_at        TIMESTAMPTZ,
    closed_at        TIMESTAMPTZ,
    source           TEXT DEFAULT 'CryptoOrderFlow'
);

-- Индекс для быстрых запросов SLQ:
CREATE INDEX ON closed_trades (symbol, kind, closed_at) WHERE pnl > 0;

-- Индекс для дашбордов (по времени):
CREATE INDEX ON closed_trades (closed_at DESC);
```

### Ключевые метрики дашборда (вычисляются из этой таблицы):
```sql
-- Sharpe Ratio за последние 30 дней:
SELECT 
    symbol,
    AVG(pnl_bps) / NULLIF(STDDEV(pnl_bps), 0) AS sharpe_daily
FROM closed_trades
WHERE closed_at > NOW() - INTERVAL '30 days'
GROUP BY symbol
ORDER BY sharpe_daily DESC;

-- Profit Factor (отношение прибылей к убыткам):
SELECT 
    SUM(CASE WHEN pnl > 0 THEN pnl ELSE 0 END) /
    NULLIF(ABS(SUM(CASE WHEN pnl < 0 THEN pnl ELSE 0 END)), 0) AS profit_factor
FROM closed_trades
WHERE closed_at > NOW() - INTERVAL '7 days';

-- Win Rate по типу сигнала:
SELECT 
    kind,
    COUNT(*) FILTER (WHERE pnl > 0)::float / COUNT(*) AS win_rate,
    AVG(pnl_bps) AS avg_pnl_bps
FROM closed_trades
GROUP BY kind;
```
