# Этап 2: Предварительная обработка (Python Workers)

## Что это и зачем?
Python Worker — следующее звено пайплайна. Он "подписан" на Redis Streams (написанные Go Worker'ами) и начинает читать тики. Но перед тем как пустить сырые данные в мозг системы, необходимо их **очистить и проверить**.

Представьте: фармацевт (Python) принимает от курьера (Go) партию таблеток. Прежде чем выдать пациенту — нужно проверить срок годности, убедиться что это не фейк, и не выдавать одно и то же лекарство дважды.

Точка входа: `python-worker/services/crypto_orderflow_service.py`, класс `CryptoOrderflowService`.

---

## 1. Жизненный цикл сервиса (Service Lifecycle)

При старте `CryptoOrderflowService`:

```python
class CryptoOrderflowService:
    async def run_forever(self) -> None:
        # 1. Подключаем publisher и config loader к Redis
        self.publisher.r = self.main
        self.publisher.start()
        
        # 2. Загружаем конфиги ML Gate (5 попыток с ретраем)
        for _ in range(5):
            await self.of_engine.ml_gate.refresh_async(self.main)
            ...
        
        # 3. Грузим символы из Redis и запускаем воркеры
        await self.load_dynamic_symbols()
        
        # 4. Запускаем фоновые задачи
        self._refresh_task = asyncio.create_task(self._refresh_loop())    # обновление конфигов
        self._ml_gate_bg_task = asyncio.create_task(self._maintain_ml_gate_loop())  # обновление ML Gate
        self._burst_task = asyncio.create_task(self._burst_flush_loop())  # выброс burst-сигналов
        self._supervisor_task = asyncio.create_task(self._supervisor_loop())  # надзор за воркерами
```

Каждые `CRYPTO_OF_REFRESH_SEC=30` секунд конфиги символов и список обновляется из Redis.

---

## 2. Потребление тиков (Consumer Loop)

Для каждого символа запускается **отдельная асинхронная задача** `consume_ticks(symbol)`.

```python
async def consume_ticks(self, symbol: str) -> None:
    # Redis Streams Consumer Group
    helper = AsyncRedisStreamHelper(self.ticks, group, self.consumer_id_ticks)
    await helper.ensure_group(stream)  # создаем группу если нет
    
    while not self._shutdown:
        # Блокирующее чтение пачки тиков
        messages = await helper.read(
            {stream: ">"},          # ">" = только новые (непрочитанные)
            count=200,              # batch_size = 200 тиков за раз
            block=250,              # ждать 250ms если нет данных
        )
        for stream_name, entries in messages:
            for msg_id, fields in entries:
                tick = _parse_tick_payload(raw)  # парсим поля
                await self._process_tick(runtime, tick, msg_id)
```

**Что такое Consumer Group?**
Redis Streams Consumer Group — это механизм, который позволяет нескольким процессам читать один поток без дублирования. Каждое сообщение выдается только одному консьюмеру. Если консьюмер упал и не подтвердил (ACK) — сообщение переходит в **PEL (Pending Entries List)** и будет отдано другому.

---

## 3. Tick Time Policy: Временная гигиена (Hygiene)

Это один из важнейших компонентов. Биржи иногда присылают тики с опозданием на 30-60 секунд (особенно во время флэша / сквиза). Если принять такой "старый" тик как "свежий" — вся аналитика (ATR, CVD) будет посчитана неправильно.

```python
# КРИТИЧЕСКИЕ ПАРАМЕТРЫ (из .env / docker-compose)
TICK_TIME_MAX_PAST_MS = 120000     # Тик старше 2х минут = отбросить
TICK_TIME_MAX_FUTURE_MS = 5000     # Тик "из будущего" на 5+ сек = аномалия
TICK_TIME_MAX_REORDER_MS = 1500    # Опоздание на 1.5 сек = OK, терпим

# Чтение настроек в коде (из crypto_orderflow_service.py):
self._max_ts_skew_ms = int(os.getenv("CRYPTO_OF_MAX_TS_SKEW_MS", str(6 * 3600_000)))
```

**Как работает логика:**
```python
ingest_ts_ms = int(time.time() * 1000)   # Wall-clock (время сервера)
event_ts_ms = tick.get("ts_ms", 0)       # Метаданные биржи

lag = ingest_ts_ms - event_ts_ms          # Разница

if lag > TICK_TIME_MAX_PAST_MS:
    # Тик слишком старый — это "просроченные таблетки"
    ticks_dropped_total.labels(symbol=symbol, reason="stale").inc()
    continue  # Пропускаем

if event_ts_ms > ingest_ts_ms + TICK_TIME_MAX_FUTURE_MS:
    # Тик из "будущего" — NTP рассинхрон
    ticks_dropped_total.labels(symbol=symbol, reason="future").inc()
    continue
```

**Механизм Карантина (Freeze)**:
Если `BAD_TIME_TRIGGER_STREAK=3` тика подряд нарушают правила, символ "замораживается":
- Дальнейшая обработка сигналов **останавливается** на `BAD_TIME_STATE_FREEZE_MS=15000` миллисекунд.
- Плохие тики уходят в стрим диагностики `stream:tick_time:quarantine`.
- Восстановление происходит только когда придут `BAD_TIME_RECOVERY_OK_STREAK=5` хороших тиков подряд.

---

## 4. Дедупликация (Deduplication)

Биржа иногда присылает один и тот же трейд несколько раз (Retry-storm в API). Дубль может "перекосить" CVD (купили дважды вместо одного).

```python
# Включение (из .env):
TICK_DEDUPE_ENABLE=1
TICK_DEDUP_WINDOW=4096  # Размер окна памяти (последние 4096 сообщений)

# Под капотом (грубо):
def _compute_tick_uid(tick: dict) -> str:
    """Строим уникальный ключ для дедупликации"""
    return f"{tick['price']}|{tick['qty']}|{tick['side']}|{tick['ts_ms']}"

# В consume_ticks:
uid = _compute_tick_uid(tick)
if uid in self._seen_ticks_window:
    tick_dedup_drop_total.labels(symbol=symbol).inc()
    continue  # Дубль — отбрасываем молча
self._seen_ticks_window.add(uid)
```

---

## 5. Unknown Side Policy (Неизвестный инициатор)

В агрегированных трейдах (`@aggTrade`) иногда нет флага `is_buyer_maker` (кто "ударил" по стакану). Если мы угадаем неправильно — CVD мгновенно перекосится вправо или влево.

```python
# Настройка (из docker-compose-crypto-orderflow.yml):
CRYPTO_OF_UNKNOWN_SIDE_POLICY=ignore_delta  # Учитываем в объеме, но НЕ в CVD

# Реализация:
POLICY_MAP = {
    "ignore_delta": lambda tick: tick.update({"counted_in_delta": False}),
    "drop": lambda tick: None,  # Полностью удалить тик
    "random": lambda tick: tick.update({"side": random.choice(["B", "S"])}),
}
```

1% таких тиков сэмплируется (`TICK_SIDE_QUARANTINE_SAMPLE=0.01`) и публикуется в `stream:tick_side:quarantine` — для оффлайн анализа качества данных.

---

## 6. Supervisor и Resilience (Надежность)

Что если `consume_ticks(BTCUSDT)` упала с ошибкой? Вместо перезапуска контейнера — работает **Supervisor Loop**:

```python
async def _supervisor_loop(self) -> None:
    """
    Проверяет каждые 5 секунд, живы ли воркеры.
    Если задача "умерла" — перезапускает её.
    """
    interval = float(os.getenv("CRYPTO_OF_SUPERVISOR_INTERVAL_SEC", "5"))
    max_restarts = int(os.getenv("CRYPTO_OF_SUPERVISOR_MAX_RESTARTS", "10"))
    window_sec = float(os.getenv("CRYPTO_OF_SUPERVISOR_WINDOW_SEC", "300"))
    
    while not self._shutdown:
        await asyncio.sleep(interval)
        for symbol, (t_tick, t_book) in list(self.symbol_tasks.items()):
            if t_tick.done():  # Задача завершилась (значит, упала)
                # Проверяем: не слишком ли много рестартов (Restart Storm)?
                if len(restart_history) > max_restarts:
                    # Слишком много сбоев — останавливаем символ
                    await self._stop_symbol(symbol)
                else:
                    # Перезапускаем
                    new_task = asyncio.create_task(self.consume_ticks(symbol))
```

**Защита от "Restart Storm"**: Если символ падает 10 раз за 5 минут — его останавливают. Это предотвращает бесконечный цикл краша → перезапуска → краша, который съедает ресурсы.

---

## 7. Calibration Bootstrap
Перед тем как воркер начинает обрабатывать тики, он загружает "прошлые данные" (калибровки):

```python
async def bootstrap_task():
    async with self._bootstrap_sem:  # Максимум 10 символов калибруются одновременно
        await asyncio.wait_for(
            self.calib_svc.ensure_loaded(runtime), 
            timeout=2.0  # 2 секунды максимум
        )
        runtime.ready = True  # Теперь воркер готов к работе
```

Это нужно чтобы воркер знал исторические квантили волатильности (ATR percentiles) и не генерировал мусорные сигналы в первые секунды после старта.
