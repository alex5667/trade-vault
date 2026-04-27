# Этап 6: Публикация и Диспетчеризация (Signal Dispatch)

## Что это и зачем?
Сигнал прошел все проверки. Теперь его нужно:
1. Оформить в финальный формат (JSON payload).
2. Защитить от дублирования (одна сделка, а не 50).
3. Отправить сразу в несколько мест: боевой очереди, аналитике, Telegram.

Всё это делает `AsyncSignalPublisher` + `OrderFlowStrategy.publish_signal()`.

---

## 1. Унифицированный Signal Payload

Все сигналы оформляются в единую структуру (DTO). Это важно: каждый потребитель (MT5 эксперт, Telegram бот, аналитика) работает с одним и тем же форматом.

```json
{
  "signal_id": "4fac31a2e3bc...",    // SHA1 хэш (стабильный для replay-тестов)
  "symbol": "BTCUSDT",
  "kind": "breakout",                // Тип сигнала
  "side": "BUY",
  "entry_price": 64500.5,            // Цена входа
  "sl_price": 64000.0,               // Stop Loss
  "tp1_price": 65500.0,              // Take Profit 1 (первая цель)
  "tp2_price": 66500.0,              // Take Profit 2 (вторая цель, если есть)
  "confidence": 88.5,                // Уверенность модели (%)
  "ts_ms": 1700000000000,            // Время сигнала в миллисекундах
  "venue": "binance_futures",        // Биржа
  "source": "CryptoOrderFlow",       // Источник
  "meta": {
    "sl_mode": "ATR",                // Режим стопа
    "sl_atr_mult": 1.5,              // Множитель ATR
    "regime": "trend_up",            // Режим рынка
    "delta_z_threshold": 3.1,        // Параметры конфига
    "dq_flags": [],                  // Флаги качества данных
    "ml_confirm_p": 0.73             // Вероятность от L2 модели
  }
}
```

### Как генерируется signal_id?
```python
# Из handlers/crypto_orderflow_handler.py (реальный код)
def _stable_signal_id(self, payload: dict) -> str:
    """
    Стабильный ID для replay-тестов и дедупликации.
    """
    ts = int(payload.get("ts", 0))
    bucket_ms = int(os.getenv("OUTBOX_SEM_DEDUP_BUCKET_MS", "1000"))
    ts_bucket = (ts // bucket_ms) * bucket_ms  # Округляем до секунды
    
    # Точность цены тоже округляем (избегаем дублей при флуктуациях)
    lvl = float(payload.get("level_price", 0.0))
    lvl_r = round(lvl, 8)  # 8 знаков для крипты
    
    # Строим "мета-ключ"
    base = f"{payload['symbol']}|{payload['kind']}|{payload['side']}|{ts_bucket}|{lvl_r}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()
```

---

## 2. Семантическая Дедупликация (Outbox Semantic Dedup)

**Проблема**: Во время сильного движения детектор активируется на каждом тике. Без защиты — одна "идея" превратится в 50 ордеров за минуту.

**Решение**: временное и семантическое ограничение.

```python
# Логика деда в publish pipeline:
async def _publish_with_dedup(self, payload: dict) -> bool:
    """Возвращает True если сигнал опубликован, False если дедуп отбил."""
    
    # 1. Ключ дедупликации (по символу и стороне)
    symbol = payload["symbol"]
    side = payload["side"]
    kind = payload["kind"]
    dedup_key = f"signal_dedup:{symbol}:{side}:{kind}"
    
    # 2. Попытка установить блокировку в Redis (SETNX = SET if Not eXist)
    cooldown_sec = int(os.getenv("MIN_SIGNAL_INTERVAL_SEC", "60"))
    
    acquired = await redis.set(dedup_key, "1", nx=True, ex=cooldown_sec)
    # nx=True → записываем только если ключа нет
    # ex=60   → ключ автоматически удалится через 60 секунд
    
    if not acquired:
        # Ключ уже есть → значит этот сигнал уже торговался недавно
        logger.debug("Dedup hit for %s:%s (cooldown=%ds)", symbol, side, cooldown_sec)
        return False
    
    # 3. Семантический дедуп через хэш payload'а
    signal_id = self._stable_signal_id(payload)
    bucket_key = f"signal_bucket:{signal_id}"
    
    seen = await redis.get(bucket_key)
    if seen:
        return False  # Точно такой же сигнал уже был
    
    await redis.setex(bucket_key, 5, "1")  # 5 секунд "памяти"
    return True  # Прошло всё!
```

---

## 3. AsyncSignalPublisher — роутинг по потокам

```python
# Из services/async_signal_publisher.py
class AsyncSignalPublisher:
    """
    Публикует сигналы в несколько Redis Streams асинхронно.
    Использует внутреннюю retry-очередь при временных ошибках Redis.
    """
    def __init__(self, redis_client, source: str):
        self.r = redis_client
        self.source = source
        self._retry_queue = asyncio.Queue(maxsize=1000)  # Буфер при сбоях
    
    async def publish(self, signal: dict, symbol: str) -> None:
        payload_str = json.dumps(signal, ensure_ascii=False)
        
        tasks = [
            # 1. Главный поток всех сигналов (для аналитики, ML Replay)
            self._xadd_safe("signals:crypto:raw", {"data": payload_str}),
            
            # 2. Per-symbol поток (для подписчиков конкретной монеты)
            self._xadd_safe(f"signals:cryptoorderflow:{symbol}", {"data": payload_str}),
            
            # 3. Боевая очередь (только если tradeable=True)
            self._xadd_safe("orders:queue:mt5", {"data": payload_str}) 
                if signal.get("tradeable") else asyncio.sleep(0),
            
            # 4. Telegram уведомление (с троттлингом)
            self._publish_telegram(signal, symbol),
        ]
        
        # Запускаем всё параллельно (не ждём по очереди)
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _xadd_safe(self, stream: str, data: dict, maxlen: int = 100000) -> None:
        """Безопасная запись с MAXLEN (ограничение длины стрима)."""
        try:
            await self.r.xadd(
                stream, 
                data,
                maxlen=maxlen,        # Максимум 100K сообщений в стриме
                approximate=True      # ~maxlen (более производительно)
            )
        except RedisError as e:
            # Временная ошибка — складируем в retry-очередь
            await self._retry_queue.put((stream, data))
            log_silent_error(e, "publish_error", stream, "xadd_safe")
```

---

## 4. Telegram Уведомления (Throttling)

```python
# Не спамим в Telegram: берём каждый N-й сигнал
async def _publish_telegram(self, signal: dict, symbol: str) -> None:
    every_n = int(os.getenv("CRYPTO_NOTIFY_SIGNAL_EVERY_N", 
                            os.getenv("NOTIFY_SIGNAL_EVERY_N", "1")))
    
    # Инкремент счётчика сигналов по символу
    self._signal_counters[symbol] = self._signal_counters.get(symbol, 0) + 1
    
    if self._signal_counters[symbol] % every_n != 0:
        return  # Пропускаем, не наш "черёд"
    
    # Форматируем красивое сообщение
    message = (
        f"🚀 {signal['kind'].upper()} {signal['symbol']}\n"
        f"Side: {signal['side']}\n"
        f"Entry: {signal['entry_price']:.2f}\n"
        f"SL: {signal['sl_price']:.2f} | TP1: {signal['tp1_price']:.2f}\n"
        f"Confidence: {signal['confidence']:.1f}%"
    )
    
    # Кладём в notify-стрим (бот читает его)
    await self.notify_client.xadd(
        self.notify_stream,  # "notify:telegram"
        {"message": message, "signal_id": signal["signal_id"]},
        maxlen=10000
    )
```

---

## 5. DecisionTrace — Диагностический след

Технически, каждый VETO не просто теряется - у него есть полный след:

```python
def _publish_trace_diag_best_effort(self, ctx: Any, *, reason: str) -> None:
    """
    Публикует полную диагностику отклоненного сигнала.
    Не влияет на торговлю (tradeable=False всегда).
    """
    if not os.getenv("DECISION_TRACE_DIAG_STREAM"):
        return  # Отключено
    
    payload = {
        "type": "diagnostic",
        "tradeable": False,           # Это НЕ торговый ордер
        "reason": reason,             # Например: "smt_diverged"
        "symbol": ctx.symbol,
        "kind": getattr(ctx, "kind", ""),
        "confidence": getattr(ctx, "confidence", 0.0),
        "trace": {
            "delta_z": getattr(ctx, "z_delta", 0.0),
            "obi_score": getattr(ctx, "obi_avg", 0.0),
            "spread_bps": getattr(ctx, "spread_bps", 0.0),
            "regime": getattr(ctx, "regime", "unknown"),
            "book_age_ms": ...,
        },
        "ts_ms": int(time.time() * 1000),
    }
    
    # В стрим диагностики (никогда не используется для ордеров)
    redis_client.xadd(
        "stream:signals:diagnostics",
        {"data": json.dumps(payload)},
        maxlen=50000,
        approximate=True
    )
```

---

## 6. Throttled Logging (Защита от переполнения логов)

```python
# Из handlers/crypto_orderflow/utils/log_sampler.py
class LogSamplerFactory:
    """
    Фабрика выборочных логгеров.
    Чтобы не захлебнуться в миллионах строк.
    """
    @staticmethod
    def get_sampler(key: str, every_n: int = 1000) -> "LogSampler":
        # Каждый N-й вызов реально логируется
        ...

# Использование в critic path:
sampler = LogSamplerFactory.get_sampler("TICK_PROCESS", every_n=1000)
if sampler.should_log(symbol):
    logger.debug("Processing tick: price=%.2f z=%.3f", tick["price"], z_delta)
# 999 из 1000 раз → logger.debug НЕ вызывается
# CPU и IO не расходуются
```
