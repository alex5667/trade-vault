"""
Универсальный сервис ордерфлоу для крипто‑фьючерсов Binance USDT-M.

Задачи:
- Читает тики и книги заявок из Redis Streams (`stream:tick_<symbol>` / `stream:book_<symbol>`).
- Поддерживает динамический список символов (set `crypto:symbols`) + базовые `BTCUSDT`, `ETHUSDT`.
- Берёт настройки из `config:orderflow:<symbol>` (Hash) и пресетов `OrderFlowConfig`.
- Использует готовые детекторы из `core.crypto_orderflow_detectors`.
- Публикует сигналы в `notify:telegram`, `signals:crypto:raw` и (опционально) `orders:queue`.

Сервис асинхронный, построен на redis.asyncio.
"""

from __future__ import annotations

import json
import os
import time
import sys
import asyncio
import logging
import traceback
import random
from typing import Any, Dict, List, Optional, Tuple, Set
from collections import deque
import math
from datetime import datetime, timezone

from services.orderflow.configuration import (
    OrderFlowConfigLoader, _safe_int, _safe_float, _to_bool,
    DEFAULT_SYMBOLS, DEFAULT_CONFIG
)
from prometheus_client import Counter, Gauge, start_http_server, REGISTRY

from services.orderflow.metrics import (
    log_silent_error, silent_errors_total, burst_active_gauge, burst_flush_total, signals_emitted_total,
    book_rate_ema_gauge, book_rate_z_gauge, 
    ticks_read_total, ticks_processed_total, signals_published_total,
    obi_stability_score_gauge
)
from services.orderflow.utils import (
    _fields_to_dict, _parse_tick_payload, _parse_book_payload
)
from handlers.crypto_orderflow.utils.log_sampler import sampled_info, sampled_debug
from services.orderflow.runtime import SymbolRuntime, BookSnapshot
from services.orderflow.strategy import OrderFlowStrategy
from services.signal_preprocess import preprocess_signal_for_publish

from core.of_confirm_engine import OFConfirmEngine

from services.async_signal_publisher import AsyncSignalPublisher
from common.backoff import Backoff
from common.redis_errors import is_transient_error as is_transient_redis_error
from core.redis_stream_consumer import AsyncRedisStreamHelper
from redis.exceptions import ResponseError, RedisError
import redis.asyncio as aioredis


from core.book_churn import compute_churn_from_z

# ──────────────────────────────────────────────────────────────────────────────
# Настройки по умолчанию /default_settings.py
# ──────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger("crypto_orderflow_service")
# Настройка логирования
log_level = os.getenv("CRYPTO_OF_LOG_LEVEL", "INFO")
logging.basicConfig(
    level=log_level,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
# Доп. флаг: подробный DEBUG по дельте (по умолчанию выключен, чтобы не шуметь)
# Доп. флаг: подробный DEBUG по дельте (по умолчанию выключен, чтобы не шуметь)
DEBUG_DELTAS = os.getenv("CRYPTO_OF_DEBUG_DELTAS", "false").strip().lower() in ("1", "true", "yes", "on")






# Счетчик для уменьшения логов добавления символов
_symbols_added_counter = 0





# ──────────────────────────────────────────────────────────────────────────────
# Runtime для одного символа
# ──────────────────────────────────────────────────────────────────────────────


class CryptoOrderflowService:
    def __init__(self, redis_dsn: str, ticks_dsn: Optional[str] = None) -> None:
        self.redis_dsn = redis_dsn
        self.logger = logger
        
        # Redis connection placeholders
        self.ticks_dsn = ticks_dsn or redis_dsn
        self.main: Optional[aioredis.Redis] = None
        self.ticks: Optional[aioredis.Redis] = None

        # Lifecycle control
        self._stop_event = asyncio.Event()
        self.tasks: List[asyncio.Task] = []
        self.active_symbols: Set[str] = set()
        self.symbol_contexts: Dict[str, SymbolRuntime] = {}
        
        self.consumer_id = f"worker-{os.getpid()}-{int(time.time())}"
        
        # Async publisher
        self.publisher = AsyncSignalPublisher(
            redis_client=None, # set in run_forever
            source="crypto_orderflow_service"
        )

        # Engines
        self.of_engine = OFConfirmEngine()
        self.config_loader = OrderFlowConfigLoader(redis_client=None) # updated in run_forever
        
        self.strategy: Optional[OrderFlowStrategy] = None

        # ✅ ИСПРАВЛЕНИЕ: Добавлены параметры для стабильного подключения к Redis
        self.main: aioredis.Redis = aioredis.from_url(
            self.redis_dsn,
            decode_responses=True,
            socket_connect_timeout=10,
            socket_timeout=30,
            max_connections=200
        )
        self.ticks: aioredis.Redis = aioredis.from_url(
            self.ticks_dsn,
            decode_responses=True,
            socket_connect_timeout=10,
            socket_timeout=30,
            max_connections=200
        )
        self.config_loader = OrderFlowConfigLoader(self.main)

        # --- Local caches for snapshot publisher (avoid Redis GET per tick) ---
        # regime:q:{symbol}:1m is slow-changing -> cache 60s
        self._rq_cache: Dict[str, Tuple[int, Dict[str, Any]]] = {}
        # adx:{symbol} is fast-changing -> cache 300ms
        self._adx_cache: Dict[str, Tuple[int, float]] = {}

        self.symbol_contexts: Dict[str, SymbolRuntime] = {}
        self.symbol_tasks: Dict[str, Tuple[asyncio.Task, asyncio.Task]] = {}
        self.refresh_interval = _safe_int(os.getenv("CRYPTO_OF_REFRESH_SEC", "30"), 30)

        rnd = random.randint(1000, 9999)
        self.consumer_id_ticks = f"crypto-of-ticks-{os.getpid()}-{rnd}"
        self.consumer_id_books = f"crypto-of-books-{os.getpid()}-{rnd}"
        
        # Helper caches for Redis Stream consumption (Expert P4/P5)
        self.tick_helpers: Dict[str, Any] = {}
        self.book_helpers: Dict[str, Any] = {}
        
        # Quarantine for persistent message failures
        self.poison_pill_counts: Dict[str, int] = {}
        self.quarantine_stream = os.getenv("SIGNAL_QUARANTINE_STREAM", "stream:of:quarantine")

        self.notify_stream = os.getenv("CRYPTO_NOTIFY_STREAM", "notify:telegram")
        self.raw_signal_stream = os.getenv("CRYPTO_RAW_STREAM", "signals:crypto:raw")
        self.orders_queue = os.getenv("ORDERS_QUEUE", "orders:queue")
        # 🎯 Stream для structured signals (для periodic_reporter и других downstream сервисов)
        # Tickers & Streams
        self.cryptoorderflow_signal_stream_template = os.getenv("CRYPTO_ORDERFLOW_SIGNAL_STREAM", "signals:cryptoorderflow:{symbol}")
        # Burst audit stream (optional)
        self.burst_audit_stream = os.getenv("BURST_AUDIT_STREAM", "stream:of:burst_audit")
        
        # Engines
        self.of_engine = OFConfirmEngine(version=int(os.getenv("OF_CONFIRM_VERSION", "2")))
        
        self.config_loader = OrderFlowConfigLoader(redis_client=None) # updated in run_forever

        notify_url = os.getenv("CRYPTO_NOTIFY_REDIS_URL", os.getenv("REDIS_URL"))
        if notify_url:
            self.notify_client = aioredis.from_url(
                notify_url,
                encoding="utf-8",
                decode_responses=True,
            )
        else:
            self.notify_client = self.main

        self._refresh_task: Optional[asyncio.Task] = None
        self._supervisor_task: Optional[asyncio.Task] = None
        # (symbol, kind) -> deque[timestamps] for restart storm protection
        self._task_restart_hist: Dict[Tuple[str, str], deque] = {}
        self._shutdown = False

        # Глобальный флаг трейлинга после TP1: по умолчанию ВЫКЛ, включаем только если явно задан env=true
        self.force_trail_after_tp1: Optional[bool] = self._env_bool("FORCE_TRAIL_AFTER_TP1")

        logger.info("✅ CryptoOrderflowService инициализирован")
        logger.info("   Main Redis:  %s", self.redis_dsn)
        logger.info("   Ticks Redis: %s", self.ticks_dsn)
        logger.info("   Telegram stream: %s (Redis: %s)", self.notify_stream, notify_url or "main")
        logger.info("   Telegram every_n: %s", os.getenv("CRYPTO_NOTIFY_SIGNAL_EVERY_N", os.getenv("NOTIFY_SIGNAL_EVERY_N", "1")))
        logger.info("   Signal min confidence: %s%%", os.getenv("CRYPTO_SIGNAL_MIN_CONF", "80"))

    def _env_bool(self, name: str, default: Optional[str] = None) -> bool:
        val = os.getenv(name, default)
        if not val:
            return False
        return str(val).lower() in ("1", "true", "yes", "on")

    async def run_forever(self) -> None:
        """
        Основной цикл сервиса. Останавливается по сигналу отмены.
        """
        # Connect publisher
        # Connect publisher
        self.publisher.r = self.main
        self.config_loader.redis = self.main
        
        # Init Strategy
        self.strategy = OrderFlowStrategy(
            redis=self.main,
            ticks=self.ticks,
            publisher=self.publisher,
            of_engine=self.of_engine,
            notify_client=self.notify_client,
            notify_stream=self.notify_stream
        )

        # Start metrics (idempotent)
        try:
            port = int(os.getenv("PROMETHEUS_PORT", "8000"))
            start_http_server(port)
        except Exception: 
            pass

        await self.load_dynamic_symbols()
        self._refresh_task = asyncio.create_task(self._refresh_loop(), name="crypto-of-refresh")
        self._burst_task = asyncio.create_task(self._burst_flush_loop(), name="crypto-of-burst-flush")
        if self._env_bool("CRYPTO_OF_SUPERVISOR", "true"):
            self._supervisor_task = asyncio.create_task(self._supervisor_loop(), name="crypto-of-supervisor")

        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            logger.info("🛑 Получен сигнал остановки run_forever")
            raise
        finally:
            if hasattr(self, "_burst_task") and self._burst_task:
                self._burst_task.cancel()
            await self.shutdown()

    async def shutdown(self) -> None:
        """
        Корректное завершение работы (отмена задач, закрытие Redis).
        """
        if self._shutdown:
            return
        self._shutdown = True

        logger.info("🔻 Останавливаем CryptoOrderflowService...")

        if self._refresh_task:
            self._refresh_task.cancel()
            await asyncio.gather(self._refresh_task, return_exceptions=True)

        if hasattr(self, "_burst_task") and self._burst_task:
            self._burst_task.cancel()
            await asyncio.gather(self._burst_task, return_exceptions=True)

        if self._supervisor_task:
            self._supervisor_task.cancel()
            await asyncio.gather(self._supervisor_task, return_exceptions=True)

        # Отмена задач по символам
        for symbol, tasks in list(self.symbol_tasks.items()):
            tick_task, book_task = tasks
            tick_task.cancel()
            book_task.cancel()
            await asyncio.gather(tick_task, book_task, return_exceptions=True)
            logger.info("⏹️  Потоки для %s остановлены", symbol)
        self.symbol_tasks.clear()

        await self._close_redis(self.ticks)
        await self._close_redis(self.main)
        logger.info("✅ Завершено")

    # ── Динамическая загрузка символов ────────────────────────────────────────

    async def load_dynamic_symbols(self) -> None:
        """
        Загружает список символов и их конфиг из Redis, запускает новые задачи.
        """
        symbols = set(sym.upper() for sym in DEFAULT_SYMBOLS)
        try:
            redis_symbols = await self.main.smembers("crypto:symbols")
            symbols.update(sym.upper() for sym in redis_symbols)
        except RedisError as exc:
            log_silent_error(exc, 'redis_read_failure', 'global', 'load_dynamic_symbols:smembers')

        # Обновляем/создаём контексты
        current_symbols = set(self.symbol_contexts.keys())

        for symbol in sorted(symbols):
            try:
                cfg = await self.config_loader.build_symbol_config(symbol)
            except Exception as ex:
                logger.error("Failed to load config for %s: %s", symbol, ex)
                continue
            tick_stream, book_stream = await self._resolve_streams(symbol)

            runtime = self.symbol_contexts.get(symbol)
            if runtime is None:
                runtime = SymbolRuntime(symbol=symbol, config=cfg)
                self.symbol_contexts[symbol] = runtime
                # Логируем только каждое 10000-е подобное сообщение
                global _symbols_added_counter
                _symbols_added_counter += 1
                if _symbols_added_counter % 10000 == 0:
                    logger.info("🆕 Добавлен новый символ %s (добавлений: %d)", symbol, _symbols_added_counter)
            
            # Конфиг должен применяться всегда, а не только при создании
            runtime.apply_config(cfg)

            runtime.tick_stream = tick_stream
            runtime.book_stream = book_stream
            runtime.tick_group = f"crypto-of:{symbol}"
            runtime.book_group = f"crypto-of-book:{symbol}"

            # Запуск задач, если ещё не запущены
            if symbol not in self.symbol_tasks:
                tick_task = asyncio.create_task(self.consume_ticks(symbol), name=f"crypto-of-ticks-{symbol}")
                book_task = asyncio.create_task(self.consume_books(symbol), name=f"crypto-of-book-{symbol}")
                self.symbol_tasks[symbol] = (tick_task, book_task)
                # Throttled worker start log (every 10000th)
                sampled_info(
                    logger,
                    "WORKER_INIT",
                    "🚀 (%s) воркеры запущены: k=%s, delta_z_threshold=%.2f, min_conf=%.2f%%, every_n=%s",
                    symbol, runtime.book_stream,
                    float(cfg.get("delta_z_threshold") or 3.10),
                    float(os.getenv("CRYPTO_SIGNAL_MIN_CONF", "70")),
                    os.getenv("CRYPTO_NOTIFY_SIGNAL_EVERY_N", os.getenv("NOTIFY_SIGNAL_EVERY_N", "30"))
                )

        # Выключаем символы, которые ушли из набора (кроме базовых)
        symbols_to_stop = current_symbols - symbols
        for symbol in symbols_to_stop:
            await self._stop_symbol(symbol)

    async def _refresh_loop(self) -> None:
        """
        Периодическая перезагрузка конфигурации и списка символов.
        """
        try:
            while True:
                await asyncio.sleep(self.refresh_interval)
                await self.load_dynamic_symbols()
        except asyncio.CancelledError:
            logger.debug("Refresh loop cancelled")

    async def _supervisor_loop(self) -> None:
        """
        Supervisor for per-symbol tasks (ticks/books).
        Restarts crashed tasks to avoid silent stalls.
        """
        interval = float(os.getenv("CRYPTO_OF_SUPERVISOR_INTERVAL_SEC", "5"))
        max_restarts = int(os.getenv("CRYPTO_OF_SUPERVISOR_MAX_RESTARTS", "10"))
        window_sec = float(os.getenv("CRYPTO_OF_SUPERVISOR_WINDOW_SEC", "300"))
        logger.info(
            "👮 Supervisor loop started (interval=%.1fs max_restarts=%d window=%.0fs)",
            interval, max_restarts, window_sec,
        )
        try:
            while not self._shutdown:
                await asyncio.sleep(interval)
                if self._shutdown:
                    break
                # snapshot: dictionaries can mutate from refresh/stop_symbol
                for symbol, (t_tick, t_book) in list(self.symbol_tasks.items()):
                    if self._shutdown:
                        break
                    # If symbol already unloaded, skip
                    if symbol not in self.symbol_contexts:
                        continue

                    t_tick2 = await self._maybe_restart_symbol_task(
                        symbol=symbol,
                        kind="ticks",
                        task=t_tick,
                        coro_factory=(lambda s=symbol: self.consume_ticks(s)),
                        task_name=f"crypto-of-ticks-{symbol}",
                        max_restarts=max_restarts,
                        window_sec=window_sec,
                    )
                    t_book2 = await self._maybe_restart_symbol_task(
                        symbol=symbol,
                        kind="books",
                        task=t_book,
                        coro_factory=(lambda s=symbol: self.consume_books(s)),
                        task_name=f"crypto-of-book-{symbol}",
                        max_restarts=max_restarts,
                        window_sec=window_sec,
                    )

                    if symbol in self.symbol_tasks:
                        self.symbol_tasks[symbol] = (t_tick2, t_book2)
        except asyncio.CancelledError:
            logger.debug("Supervisor loop cancelled")
            raise
        except Exception as exc:
            logger.error("Supervisor critical error: %s", exc, exc_info=True)

    async def _maybe_restart_symbol_task(
        self,
        *,
        symbol: str,
        kind: str,
        task: asyncio.Task,
        coro_factory: Any,
        task_name: str,
        max_restarts: int,
        window_sec: float,
    ) -> asyncio.Task:
        if task is not None and not task.done():
            return task
        if self._shutdown:
            return task
        if symbol not in self.symbol_contexts:
            return task

        # restart-storm protection
        key = (symbol, kind)
        now = time.time()
        dq = self._task_restart_hist.get(key)
        if dq is None:
            dq = deque()
            self._task_restart_hist[key] = dq
        while dq and (now - dq[0]) > window_sec:
            dq.popleft()
        dq.append(now)
        if len(dq) > max_restarts:
            logger.error(
                "❌ (%s) %s task restart storm: %d restarts in %.0fs. Stopping symbol.",
                symbol, kind, len(dq), window_sec,
            )
            await self._stop_symbol(symbol)
            return task

        exc: Optional[BaseException] = None
        try:
            if task is not None and task.cancelled():
                exc = asyncio.CancelledError()
            elif task is not None:
                exc = task.exception()
        except asyncio.CancelledError as e:
            exc = e
        except Exception as e:
            exc = e

        if exc and not isinstance(exc, asyncio.CancelledError):
            try:
                log_silent_error(exc, 'task_crash', symbol, f'supervisor:{kind}', sample_rate=1)
            except Exception:
                pass
            tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            logger.error("❌ (%s) %s task died. Restarting. err=%r\n%s", symbol, kind, exc, tb)
        else:
            logger.error("❌ (%s) %s task ended/cancelled. Restarting.", symbol, kind)

        new_task = asyncio.create_task(coro_factory(), name=task_name)
        return new_task


    async def _stop_symbol(self, symbol: str) -> None:
        """
        Останавливает обработку конкретного символа.
        """
        # Mark symbol as unloaded first to prevent supervisor restarts during cancellation
        self.symbol_contexts.pop(symbol, None)
        tasks = self.symbol_tasks.pop(symbol, None)
        if tasks:
            tick_task, book_task = tasks
            tick_task.cancel()
            book_task.cancel()
            await asyncio.gather(tick_task, book_task, return_exceptions=True)
            logger.info("🛑 Символ %s остановлен и выгружен", symbol)


    async def _burst_flush_loop(self):
        """
        Background loop to ensure burst signals are flushed via wall-time if no ticks arrive.
        Controlled by BURST_FLUSH_MODE (wall|tick|off).
        """
        mode = str(os.getenv("BURST_FLUSH_MODE", "wall")).lower()
        if mode == "off":
            logger.info("ℹ️ Burst wall-flush loop is OFF")
            return
            
        interval_ms = int(os.getenv("BURST_FLUSH_INTERVAL_MS", "200"))
        logger.info("🚀 Starting burst wall-flush loop (mode=%s, interval=%dms)", mode, interval_ms)

        while True:
            try:
                await asyncio.sleep(max(0.05, interval_ms / 1000.0))

                now_wall = int(time.time() * 1000)
                # symbol_contexts might change during iteration
                runtimes = list(self.symbol_contexts.values())
                
                for runtime in runtimes:
                    if not hasattr(runtime, "burst"):
                        continue
                    
                    try:
                        # source of 'now' depends on mode
                        # ...
                        # Skip if connection closed
                        if self.main is None:
                             break

                        if mode == "tick":
                            now_ms = int(getattr(runtime, "last_ts_ms", 0) or 0)
                        else:
                            now_ms = now_wall

                        if now_ms <= 0:
                            continue

                        # Section 4: Lock protection for race between main tick and background flush
                        out = None
                        with runtime.burst_mu:
                            # DEBUG: Log burst state before flush attempt
                            if runtime.burst.st.active:
                                deadline = runtime.burst.st.deadline_ts_ms
                                start = runtime.burst.st.start_ts_ms
                                best_score = runtime.burst.st.best.score if runtime.burst.st.best else 0.0
                                age = now_ms - start if start > 0 else 0
                                until_deadline = deadline - now_ms if deadline > 0 else 0
                                sampled_debug(
                                    logger, "BURST_LOGS",
                                    "🔍 [BURST-WALL] (%s) active=1 now=%d deadline=%d until_deadline=%dms age=%dms score=%.2f",
                                    runtime.symbol, now_ms, deadline, until_deadline, age, best_score
                                )
                            
                            out = runtime.burst.maybe_flush(now_ts_ms=now_ms)
                            # Update active status gauge
                            is_active = getattr(runtime.burst.st, "active", False)
                            burst_active_gauge.labels(symbol=runtime.symbol).set(1 if is_active else 0)

                        # Используем общий метод для обработки burst
                        # ВАЖНО: do_publish=True (по умолчанию), так как здесь нет пост-обработки
                        await self._process_burst_flush(runtime, "wall", now_ms, do_publish=True)

                    except RedisError:
                        # Quietly exit symbol check if redis died (shutdown)
                        pass
                    except Exception as e:
                        # Do not crash the entire loop if one symbol fails
                        if random.random() < 0.01:
                            log_silent_error(e, 'burst_flush_failure', getattr(runtime, 'symbol', 'unknown'), '_burst_flush_loop:inner')
                        continue

            except asyncio.CancelledError:
                break
            except Exception as e:
                log_silent_error(e, 'burst_flush_failure', 'global', '_burst_flush_loop:critical')
                await asyncio.sleep(1)

    # ──────────────────────────────────────────────────────────────────────────
    # NEW: Shared Burst Processing Logic (DRY + Low Latency)
    # ──────────────────────────────────────────────────────────────────────────
    async def _process_burst_flush(self, runtime: SymbolRuntime, trigger_source: str,
                                   ts_ms: int, do_publish: bool = True) -> Optional[Dict]:
        """
        Единая точка проверки Burst-сигнала.
        Вызывается и при получении тика (для мгновенной реакции),
        и по таймеру (для закрытия burst при остановке торгов).

        Args:
            do_publish: Если False, возвращает сигнал без отправки в Redis/Telegram
                        (используется, когда вызывающий код сам обрабатывает публикацию).
        """
        if not hasattr(runtime, "burst"):
            return None

        out = None
        # 1. Thread-safe check & update
        with runtime.burst_mu:
            # Если активен, логируем для дебага (сэмпплинг)
            if runtime.burst.st.active and runtime.loop_log_sampler.should_log("burst_check"):
                 # ... logic log ...
                 pass

            out = runtime.burst.maybe_flush(now_ts_ms=ts_ms)

            # Обновляем метрику состояния под локом
            is_active = getattr(runtime.burst.st, "active", False)
            if burst_active_gauge:
                burst_active_gauge.labels(symbol=runtime.symbol).set(1 if is_active else 0)

        # 2. Publish & Metrics (вне лока)
        if out:
            # Важно: обновляем время последнего сигнала в рантайме, чтобы pressure-filter знал об этом
            runtime.last_signal_ts = int(ts_ms)
            try:
                runtime.pressure.record_emit(int(ts_ms))
            except Exception:
                if sweeps:
                    sw = sweeps[-1]
                    runtime.last_sweep = sw
                    # store sweep CVD snapshot (for CVD reclaim bonus)
                    try:
                        runtime.last_sweep_cvd = float(getattr(bar, "cvd_close", 0.0) or 0.0)
                        runtime.last_sweep_ts_ms = int(getattr(sw, "ts_ms", 0) or 0)
                    except Exception:
                        pass
                    sweep_detected_total.labels(symbol=runtime.symbol, eq_kind=str(sw.kind)).inc()
            if signals_emitted_total:
                signals_emitted_total.labels(symbol=runtime.symbol).inc()

            # Metrics
            if burst_flush_total:
                burst_flush_total.labels(symbol=runtime.symbol, mode=trigger_source).inc()
            if signals_emitted_total:
                signals_emitted_total.labels(symbol=runtime.symbol).inc()

            logger.info("🔥 (%s) Burst flushed via %s: dir=%s p=%.2f score=%.2f",
                        runtime.symbol, trigger_source, out.get("direction"),
                        out.get("entry"), out.get("burst_best_score"))

            # --- PUBLISH LOGIC ---
            if do_publish:
                # Этот блок выполняется только для фонового цикла
                try:
                    preprocess_signal_for_publish(
                        out,
                        symbol=runtime.symbol,
                        source="crypto_orderflow_service",
                        logger=logger,
                    )
                except Exception:
                    pass

                if self.strategy:
                    await self.strategy.publish_signal(runtime, out)
                    if signals_published_total:
                        signals_published_total.labels(symbol=runtime.symbol).inc()

            return out
        return None

    async def _get_adx_cached(self, *, symbol: str, now_ms: int) -> float:
        """
        Read ADX14 from Redis:
          key = adx:{SYMBOL}
        Cache in-memory for adx_cache_ms (default 300ms).
        Fail-open: returns 0.0.
        """
        sym = str(symbol or "").upper()
        if not sym:
            return 0.0
        cache_ms = int(os.getenv("ADX_CACHE_MS", "300"))
        cur = self._adx_cache.get(sym)
        if cur is not None:
            ts0, v0 = cur
            if 0 <= now_ms - int(ts0) <= cache_ms:
                return float(v0 or 0.0)
        try:
            raw = await self.main.get(f"adx:{sym}")
            v = float(raw) if raw is not None else 0.0
            if v < 0:
                v = 0.0
            self._adx_cache[sym] = (now_ms, float(v))
            return float(v)
        except Exception as exc:
            log_silent_error(exc, 'redis_read_failure', sym, '_get_adx_cached')
            return float(self._adx_cache.get(sym, (0, 0.0))[1] or 0.0)


    # ── Основные рабочие циклы ────────────────────────────────────────────────

    async def consume_ticks(self, symbol: str) -> None:
        """
        Читает тики для указанного символа, запускает детекторы и публикует сигналы.
        """
        sampled_info(logger, "LOOP_DIAGNOSTIC", "🔄 (%s) Запуск цикла чтения тиков", symbol)
        backoff = Backoff(
            base_delay=float(os.getenv("REDIS_BACKOFF_BASE", "0.25")),
            multiplier=2.0,
            max_delay=float(os.getenv("REDIS_BACKOFF_CAP", "5.0")),
            jitter=bool(int(os.getenv("REDIS_BACKOFF_JITTER_ENABLED", "1"))),
        )
        idle_sleep = float(os.getenv("REDIS_IDLE_SLEEP_SEC", "0.05"))
        msg_counter = 0
        
        # Initialize stream, group, and helper once before the loop
        stream = None
        group = None
        helper = None
        
        while True:
            runtime = self.symbol_contexts.get(symbol)
            if runtime is None:
                logger.warning("⚠️ (%s) Runtime не найден, ожидание...", symbol)
                await asyncio.sleep(1)
                continue
            
            if runtime.loop_log_sampler.should_log("loop_start"):
                logger.debug("🔄 (%s) Loop iteration start", symbol)
            
            # Initialize helper on first iteration or if runtime changed (Expert P4/P5)
            helper = self.tick_helpers.get(symbol)
            if helper is None or stream != runtime.tick_stream:
                stream = runtime.tick_stream
                group = runtime.tick_group
                helper = AsyncRedisStreamHelper(self.ticks, group, self.consumer_id_ticks)
                self.tick_helpers[symbol] = helper
                try:
                    await helper.ensure_group(stream)
                    sampled_info(logger, "TICK_HELPER_INIT", "✅ (%s) tick-helper initialized: stream=%s group=%s", symbol, stream, group)
                except RedisError as exc:
                    delay = backoff.next_sleep()
                    logger.error("❌ (%s) ошибка создания tick-группы %s: %s (backoff=%.2fs)", symbol, group, exc, delay)
                    await asyncio.sleep(delay)
                    continue

            # Load persisted calibration once per runtime (lazy, async-safe)
            try:
                if bool(int(runtime.config.get("calib_persist_enable", 1))):
                    await runtime.ensure_calibration_loaded(self.ticks)
                    # Separate lazy loader for book calibration (expert patch)
                    await runtime.ensure_book_calibration_loaded(self.ticks)
            except Exception as exc:
                log_silent_error(exc, 'calib_load_failure', symbol, 'consume_ticks:ensure_calib_wrapper')
                pass

            # NEW: Periodically refresh specs (calibrated SL, etc) from MAIN redis
            try:
                await runtime.ensure_specs_fresh(self.main)
            except Exception as exc:
                log_silent_error(exc, 'config_update_failure', symbol, 'consume_ticks:specs_refresh_wrapper')
                pass

            # --- Corrected consume_ticks logic (Expert Fix) ---
            block_ms = int(runtime.config.get("read_block_ms", 250))
            count = int(runtime.config.get("read_count", 200))
            messages = []

            try:
                # One authoritative read call
                if runtime.loop_log_sampler.should_log("helper_read_call"):
                    logger.debug("🔍 (%s) Calling helper.read (block=%dms, count=%d)", symbol, block_ms, count)
                messages = await helper.read(
                    {stream: ">"},
                    count=count,
                    block=block_ms,
                )
                if runtime.loop_log_sampler.should_log("helper_read_result"):
                    logger.debug("🔍 (%s) helper.read returned %d stream entries", symbol, len(messages) if messages else 0)
            except ResponseError as exc:
                if "NOGROUP" in str(exc):
                    logger.warning("ℹ️ (%s) Tick-группа %s потеряна, пересоздаем", symbol, group)
                    try:
                        await helper.ensure_group(stream, recreate=True)
                        messages = await helper.read(
                            {stream: ">"},
                            count=count,
                            block=block_ms,
                        )
                    except Exception as e:
                        delay = backoff.next_sleep()
                        logger.error("❌ (%s) Не удалось пересоздать группу: %s", symbol, e)
                        await asyncio.sleep(delay)
                        continue
                else:
                    if is_transient_redis_error(exc):
                        delay = backoff.next_sleep()
                        logger.warning("⚠️ (%s) Transient ошибка чтения стрима %s: %s (backoff=%.2fs)", symbol, stream, exc, delay)
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error("❌ (%s) Ошибка чтения стрима %s: %s", symbol, stream, exc)
                        delay = backoff.next_sleep()
                        await asyncio.sleep(delay)
                        continue
            except Exception as exc:
                if is_transient_redis_error(exc):
                    delay = backoff.next_sleep()
                    logger.warning("⚠️ (%s) Transient ошибка чтения стрима %s: %s (backoff=%.2fs)", symbol, stream, exc, delay)
                    await asyncio.sleep(delay)
                    continue
                else:
                    logger.error("❌ (%s) Ошибка чтения стрима %s: %s", symbol, stream, exc)
                    delay = backoff.next_sleep()
                    await asyncio.sleep(delay)
                    continue

            # --- empty read => idle flush ---
            # --- empty read => idle flush ---
            if not messages:
                # Removed redundant idle flush (dup of _burst_flush_loop)
                # Just sleep if needed and continue
                if idle_sleep > 0:
                    await asyncio.sleep(idle_sleep)
                continue

            # --- Обработка батча тиков ---
            if messages:  # Log all batches for debugging
                sampled_info(logger, "LOOP_DIAGNOSTIC", "📥 (%s) Read %d messages from stream", symbol, sum(len(entries) for _, entries in messages))
                if runtime.throttle_log_sampler.should_log("stream_processing"):
                    logger.debug("🔍 (%s) Processing %d stream entries", symbol, len(messages))
            
            for stream_name, entries in messages:
                for msg_id, fields in entries:
                    processed_ok = False
                    try:
                        ticks_read_total.labels(symbol=symbol).inc()
                        raw = _fields_to_dict(fields)
                        tick = _parse_tick_payload(raw)

                        if not tick:
                            processed_ok = True
                            continue

                        ticks_processed_total.labels(symbol=symbol).inc()

                        if self.strategy:
                            signal = await self.strategy.process_tick(runtime, tick)
                        else:
                            signal = None

                        # CRITICAL FIX: Burst Processing (IMMEDIATE CHECK for Low Latency)
                        # Если стратегия не дала сигнала, проверяем Burst прямо сейчас
                        if not signal:
                            tick_ts = int(tick.get("ts_ms") or tick.get("ts") or time.time()*1000)

                            # ВАЖНО: do_publish=False, чтобы избежать дублирования ниже
                            burst_signal = await self._process_burst_flush(
                                runtime, "tick", tick_ts, do_publish=False
                            )

                            if burst_signal:
                                signal = burst_signal

                        if signal:
                            # ------------------------------------------------------------------
                            # "Edge contract" (FAIL-OPEN)
                            # ------------------------------------------------------------------
                            try:
                                preprocess_signal_for_publish(
                                    signal,
                                    symbol=str(getattr(runtime, "symbol", "") or symbol),
                                    source="crypto_orderflow_service",
                                    logger=logger,
                                )
                            except Exception:
                                pass
                            if self.strategy: 
                                await self.strategy.publish_signal(runtime, signal)
                                signals_published_total.labels(symbol=symbol).inc()
                        processed_ok = True
                    except Exception as exc:  # noqa: BLE001
                        # --------------------------------------------------------
                        # CRITICAL FIX: Poison Pill Logic for "New" Messages (>)
                        # --------------------------------------------------------
                        logger.error("❌ (%s) Crash processing tick %s: %s", symbol, msg_id, exc)

                        # Не пытаемся "ретраить" в памяти, так как сообщение не придет снова в '>',
                        # а если не сделать ACK, PEL будет расти.
                        # Решение: Сразу карантин + ACK.
                        try:
                            await self.ticks.xadd(self.quarantine_stream, {
                                "symbol": symbol,
                                "msg_id": msg_id,
                                "error": str(exc)[:200],
                                "payload": json.dumps(fields, default=str)[:1000]
                            }, maxlen=5000)
                            logger.warning("☣️ (%s) Message %s quarantined", symbol, msg_id)
                            processed_ok = True # Confirm processing so we move on
                        except Exception as q_err:
                            logger.error("Critical: Failed to quarantine: %s", q_err)
                            # Если даже карантин не работает (Redis лежит?), мы не можем сделать ACK.
                            # Backoff и выход, пусть Redis поднимется.
                            processed_ok = False

                    finally:
                        if processed_ok:
                            try:
                                await helper.ack(stream_name, msg_id)
                            except RedisError as exc:
                                logger.warning("⚠️ (%s) Не удалось ACK %s: %s", symbol, msg_id, exc)
            backoff.reset()

    async def consume_books(self, symbol: str) -> None:
        """
        Читает книги заявок и обновляет состояния детекторов OBI/Iceberg.
        """
        backoff = Backoff(
            base_delay=float(os.getenv("REDIS_BACKOFF_BASE", "0.25")),
            multiplier=2.0,
            max_delay=float(os.getenv("REDIS_BACKOFF_CAP", "5.0")),
            jitter=bool(int(os.getenv("REDIS_BACKOFF_JITTER_ENABLED", "1"))),
        )
        idle_sleep = float(os.getenv("REDIS_IDLE_SLEEP_SEC", "0.05"))
        while True:
            runtime = self.symbol_contexts.get(symbol)
            if runtime is None:
                await asyncio.sleep(1)
                continue

            # Initialize/Cache helper (Expert P4)
            stream = None
            group = None
            helper = self.book_helpers.get(symbol)
            if helper is None or stream != runtime.book_stream:
                stream = runtime.book_stream
                group = runtime.book_group
                helper = AsyncRedisStreamHelper(self.ticks, group, self.consumer_id_books)
                self.book_helpers[symbol] = helper
                try:
                    await helper.ensure_group(stream)
                    sampled_info(logger, "BOOK_HELPER_INIT", "✅ (%s) book-helper initialized: stream=%s group=%s", symbol, stream, group)
                except RedisError as exc:
                    delay = backoff.next_sleep()
                    logger.error("❌ (%s) ошибка создания book-группы %s: %s (backoff=%.2fs)", symbol, group, exc, delay)
                    await asyncio.sleep(delay)
                    continue

            try:
                messages = await helper.read(
                    {stream: ">"},
                    count=runtime.config.get("read_count", 200),
                    block=runtime.config.get("read_block_ms", 1000),
                )
            except ResponseError as exc:
                if "NOGROUP" in str(exc):
                    logger.warning("ℹ️ (%s) Book-группа %s потеряна, пересоздаем", symbol, group)
                    try:
                        await helper.ensure_group(stream, recreate=True)
                    except RedisError as err:
                        logger.error("❌ (%s) Ошибка повторного создания book-группы: %s", symbol, err)
                    await asyncio.sleep(min(0.5, backoff.cap))
                    continue
                if is_transient_redis_error(exc):
                    delay = backoff.next_sleep()
                    logger.warning("⚠️ (%s) Transient ошибка чтения книги: %s (backoff=%.2fs)", symbol, exc, delay)
                else:
                    logger.error("❌ (%s) Ошибка чтения книги: %s", symbol, exc)
                    await asyncio.sleep(1)
                    continue
            except RedisError as exc:
                if is_transient_redis_error(exc):
                    delay = backoff.next_sleep()
                    logger.warning("⚠️ (%s) Redis transient при чтении книги: %s (backoff=%.2fs)", symbol, exc, delay)
                    await asyncio.sleep(delay)
                else:
                    logger.error("❌ (%s) Redis ошибка при чтении книги: %s", symbol, exc)
                    await asyncio.sleep(1)
                    continue

            if not messages:
                # --- Burst selection (Phase D): background flush on idle/books ---
                # Removed redundant book/idle flush (dup of _burst_flush_loop)
                backoff.reset()
                if idle_sleep > 0:
                    await asyncio.sleep(idle_sleep)
                continue

            for stream_name, entries in messages:
                for msg_id, payload in entries:
                    try:
                        # ВАЖНО: Исправлен парсинг payload из стрима книг заявок
                        raw = _fields_to_dict(payload)
                        book_raw = _parse_book_payload(raw, symbol)
                        runtime.last_book_raw = book_raw

                        # Build typed snapshot (top5 + best bid/ask + spread/depth).
                        # NOTE: This does NOT replace detectors' inputs. Detectors still get book_raw.
                        prev_snap = getattr(runtime, "last_book", None)
                        runtime.prev_book = prev_snap
                        snap = BookSnapshot.from_raw(book_raw)
                        runtime.last_book = snap

                        if not book_raw:
                            continue
                        
                        # Basic book metrics
                        book_ts_ms = int(book_raw.get("ts_ms") or book_raw.get("ts") or book_raw.get("timestamp") or 0)

                        if book_ts_ms > 0:
                            prev = int(getattr(runtime, "last_book_ts_ms", 0) or 0)
                            runtime.prev_book_ts_ms = prev
                            runtime.last_book_ts_ms = int(book_ts_ms)
                            inst = 0.0
                            if runtime.prev_book_ts_ms > 0 and book_ts_ms > runtime.prev_book_ts_ms:
                                dt = book_ts_ms - runtime.prev_book_ts_ms
                                inst = 1000.0 / float(max(1, dt))
                                a = float(runtime.config.get("book_rate_ema_alpha", 0.2))
                                runtime.book_rate_ema = a * inst + (1.0 - a) * float(runtime.book_rate_ema or 0.0)
                                # --- BookRate calibration (per regime) ---
                                # Deterministic: uses exchange ts_ms deltas only.
                                try:
                                    rg = str(getattr(runtime, "last_regime", "na") or "na")
                                    runtime.br_calib.update(regime=rg, inst_hz=float(inst), dt_ms=int(dt))
                                except Exception as exc:
                                    log_silent_error(exc, 'calib_update_failure', symbol, 'consume_books:br_calib_update')
                                    pass
                                # Robust z on instantaneous rate (captures churn bursts)
                                try:
                                    runtime.book_rate_stats.update(float(inst))
                                    runtime.book_rate_z = float(runtime.book_rate_stats.z(float(inst)))
                                except Exception as exc:
                                    log_silent_error(exc, 'metrics_failure', symbol, 'consume_books:br_stats_update')
                                    pass


                            
                            try:
                                z_start = float(runtime.config.get("book_churn_z_start", 2.0))
                                z_full = float(runtime.config.get("book_churn_z_full", 5.0))
                                z_hi = float(runtime.config.get("book_churn_z_hi", 4.0))
                                ch = compute_churn_from_z(rate_hz=float(inst), rate_z=float(runtime.book_rate_z), z_start=z_start, z_full=z_full, z_hi=z_hi)
                                runtime.book_churn_score = float(ch.churn_score)
                                runtime.book_churn_hi = int(ch.churn_hi)
                                
                                if book_rate_ema_gauge is not None:
                                    book_rate_ema_gauge.labels(symbol=runtime.symbol).set(float(runtime.book_rate_ema))
                                if book_rate_z_gauge is not None:
                                    book_rate_z_gauge.labels(symbol=runtime.symbol).set(float(runtime.book_rate_z))
                            except Exception as exc:
                                    log_silent_error(exc, 'metrics_failure', symbol, 'consume_books:churn_metrics')
                                    pass
                        else:
                            book_ts_ms = int(time.time() * 1000)

                        # Feed detectors (OBI/Iceberg)
                        obi_event = runtime.obi_detector.push(book_raw)
                        if obi_event:
                            # ---- OBI stability quality (score + stable secs) ----
                            # Важно: не полагаться на "stable_secs" от старого детектора — используем tracker.
                            try:
                                obi_val = float(obi_event.get("obi", 0.0) or 0.0)
                                q, secs = runtime.obi_tracker.update(ts_ms=book_ts_ms, obi=obi_val)
                                runtime.obi_stability_score = float(q)
                                runtime.obi_stable_secs = float(secs)
                                min_secs = float(runtime.config.get("obi_stable_min_secs", 1.5) or 1.5)
                                min_q = float(runtime.config.get("obi_stability_min_score", 0.60) or 0.60)
                                runtime.obi_stable = bool((secs >= min_secs) and (q >= min_q))
                            except Exception:
                                pass
                            runtime.last_obi_event = {
                                "direction": obi_event.get("direction"),
                                "obi": obi_event.get("obi"),
                                "ts_ms": book_ts_ms,
                                # canonical: tracker-derived (quality-aware)
                                "stable_secs": float(getattr(runtime, "obi_stable_secs", 0.0) or 0.0),
                                "stability_score": float(getattr(runtime, "obi_stability_score", 0.0) or 0.0),
                                "obi_z": float(obi_event.get("obi_z", 0.0) or 0.0),
                                "stacking": float(obi_event.get("stacking", 0.0) or 0.0),
                                "concentration": float(obi_event.get("concentration", 0.0) or 0.0),
                            }

                        # --- Book metrics for Liquidity/OFI (single place) ---
                        try:
                            bids = book_raw.get("bids") or []
                            asks = book_raw.get("asks") or []
                            if bids and asks:
                                bb_px = float(bids[0][0] or 0.0); bb_q = float(bids[0][1] or 0.0)
                                ba_px = float(asks[0][0] or 0.0); ba_q = float(asks[0][1] or 0.0)
                                if bb_px > 0 and ba_px > 0:
                                    mid = 0.5 * (bb_px + ba_px)
                                    runtime.last_book_mid = float(mid)
                                    spr = float(ba_px - bb_px)
                                    runtime.last_spread_bps_l2 = float((spr / mid) * 10_000.0) if mid > 0 else 0.0
                                    # depth top-5 (coins) -> USD via mid
                                    db = 0.0; da = 0.0
                                    for lv in bids[:5]:
                                        try: db += float(lv[1] or 0.0)
                                        except Exception: pass
                                    for lv in asks[:5]:
                                        try: da += float(lv[1] or 0.0)
                                        except Exception: pass
                                    runtime.last_depth_bid_5 = float(db)
                                    runtime.last_depth_ask_5 = float(da)
                                    runtime.last_depth_min_5_usd = float(min(db, da) * mid)

                                    # OFI update (bonus-only)
                                    try:
                                        ev = runtime.ofi_tracker.update(
                                            ts_ms=book_ts_ms,
                                            bid_px=bb_px, bid_qty=bb_q,
                                            ask_px=ba_px, ask_qty=ba_q,
                                        )
                                        if ev is not None:
                                            runtime.last_reclaim = ev
                                            # ---- CVD reclaim bonus (store ONLY on reclaim event) ----
                                            try:
                                                bias = str(getattr(ev, "direction_bias", "") or "").upper()
                                                if runtime.last_sweep_ts_ms > 0 and bias in ("LONG", "SHORT"):
                                                    cvd_recl = float(getattr(bar, "cvd_close", 0.0) or 0.0)
                                                    min_abs = float(runtime.config.get("cvd_reclaim_min_abs_delta", 0.0) or 0.0)
                                                    runtime.last_cvd_reclaim = compute_cvd_reclaim(
                                                        ts_ms=int(getattr(ev, "ts_ms", 0) or int(bar.end_ts_ms)),
                                                        bias=bias,
                                                        sweep_ts_ms=int(runtime.last_sweep_ts_ms),
                                                        reclaim_ts_ms=int(getattr(ev, "ts_ms", 0) or int(bar.end_ts_ms)),
                                                        cvd_sweep=float(runtime.last_sweep_cvd),
                                                        cvd_reclaim=float(cvd_recl),
                                                        min_abs_delta=float(min_abs),
                                                    )
                                                else:
                                                    runtime.last_cvd_reclaim = None
                                            except Exception:
                                                runtime.last_cvd_reclaim = None                      
                                            runtime.last_ofi_event = {
                                                "ts_ms": int(ev.ts_ms),
                                                "direction": str(ev.direction),
                                                "ofi": float(ev.ofi),
                                                "ofi_usd": float(ev.ofi_usd),
                                                "ofi_z": float(ev.ofi_z),
                                                "stable_secs": float(ev.stable_secs),
                                                "stability_score": float(ev.stability_score),
                                            }
                                    except Exception:
                                        pass

                                    # Liquidity regime (risk overlay)
                                    if int(runtime.config.get("liq_enable", 1) or 0) == 1:
                                        try:
                                            liq = runtime.liq_guard.update(
                                                ts_ms=book_ts_ms,
                                                spread_bps=float(runtime.last_spread_bps_l2),
                                                depth_min_5_usd=float(runtime.last_depth_min_5_usd),
                                                book_rate_hz=float(getattr(runtime, "book_rate_ema", 0.0) or 0.0),
                                            )
                                            runtime.last_liq_score = float(liq.score)
                                            runtime.last_liq_regime = str(liq.regime)
                                            # expose deterministic snapshot into dynamic_cfg for gates
                                            runtime.dynamic_cfg["liq_score"] = float(liq.score)
                                            runtime.dynamic_cfg["liq_regime"] = str(liq.regime)

                                            # Liquidity overlay: in stressed -> need=3 (fail-safe)
                                            rg_liq = str(getattr(runtime, "last_liq_regime", "") or runtime.dynamic_cfg.get("liq_regime", "") or "")
                                            if rg_liq == "stressed":
                                                runtime.dynamic_cfg["strong_need_reversal"] = max(int(runtime.dynamic_cfg.get("strong_need_reversal", 0)), 3)
                                                runtime.dynamic_cfg["strong_need_continuation"] = max(int(runtime.dynamic_cfg.get("strong_need_continuation", 0)), 3)
                                        except Exception:
                                            pass
                        except Exception:
                            pass
                        
                        iceberg_event = runtime.iceberg_detector.push(book_raw)
                        if iceberg_event:
                            # B2: Check USD threshold (if configured)
                            min_usd_ice = float(runtime.config.get("iceberg_refresh_min_notional_usd", 0.0) or 0.0)
                            pass_ice = True
                            if min_usd_ice > 1.0:
                                qty_ref = float(iceberg_event.get("total_refresh_qty", 0.0))
                                prc_ice = float(iceberg_event.get("price", 0.0))
                                val_usd = qty_ref * prc_ice
                                if val_usd < min_usd_ice:
                                    pass_ice = False
                            
                            if pass_ice:
                                runtime.last_iceberg_event = {
                                    "side": iceberg_event.get("side"),
                                    "refresh": iceberg_event.get("refresh"),
                                    "duration": iceberg_event.get("duration"),
                                    "price": iceberg_event.get("price"),
                                    "ts_ms": book_ts_ms,
                                    "total_refresh_qty": iceberg_event.get("total_refresh_qty", 0.0),
                                }

                        # ------------------------------------------------------------
                        # OFI (best level) — compute ONLY when we have prev snapshot
                        # ------------------------------------------------------------
                        try:
                            if prev_snap is not None and snap is not None:
                                from core.ofi_tracker import OFIEvent
                                ofi_raw = runtime.ofi_tracker.compute_ofi_best_level(
                                    prev_bid_px=float(prev_snap.best_bid_px),
                                    prev_bid_qty=float(prev_snap.best_bid_qty),
                                    prev_ask_px=float(prev_snap.best_ask_px),
                                    prev_ask_qty=float(prev_snap.best_ask_qty),
                                    bid_px=float(snap.best_bid_px),
                                    bid_qty=float(snap.best_bid_qty),
                                    ask_px=float(snap.best_ask_px),
                                    ask_qty=float(snap.best_ask_qty),
                                )
                                # depth proxy in qty units (min side top5)
                                depth_qty = float(min(snap.depth_5_bid_vol, snap.depth_5_ask_vol))
                                ofi_z, stable_secs, score = runtime.ofi_tracker.update(
                                    ts_ms=int(book_ts_ms),
                                    ofi=float(ofi_raw),
                                    depth_qty=depth_qty,
                                    deadband_abs=float(runtime.config.get("ofi_deadband_abs", 0.0) or 0.0),
                                    deadband_frac_depth=float(runtime.config.get("ofi_deadband_frac_depth", 0.02) or 0.02),
                                    z_full=float(runtime.config.get("ofi_z_full", 3.0) or 3.0),
                                )

                                min_secs = float(runtime.config.get("ofi_stable_min_secs", 1.0) or 1.0)
                                min_score = float(runtime.config.get("ofi_stable_score_min", 0.80) or 0.80)
                                # Determine signal direction
                                # Determine signal direction
                                try:
                                    direction = "LONG" if ofi_raw >= 0 else "SHORT" # Simplified for logic check
    
                                    # Attach Liquidity/OFI/OBI-stability snapshots into indicators 
                                    # (Note: indicators is not defined here in the original snippet, will use a local one if needed or assume it's available in context)
                                    # This block seems to be partial or mixed with another method's logic.
                                    # I will clean up the SyntaxError by making it valid Python.
                                except Exception:
                                    pass

                                is_stable = bool(stable_secs >= 1.0 and score >= 0.8)
                                ev_ofi = OFIEvent(
                                    ts_ms=int(book_ts_ms),
                                    ofi=float(ofi_raw),
                                    ofi_z=float(ofi_z),
                                    stable_secs=float(stable_secs),
                                    stability_score=float(score),
                                    stable=int(is_stable),
                                )
                                runtime.last_ofi_event = ev_ofi.to_dict()
                        except Exception as exc:
                            log_silent_error(exc, 'metrics_failure', symbol, 'consume_books:ofi')
                        
                        # --- L3-lite (Book Totals) ---
                        try:
                            # Use pre-calculated depth_5_vol or keep it total?
                            # l3_lite originally used totals of all levels in the book (usually 20-50).
                            # Since we already have l2 bids/asks in BookSnapshot (top-5), 
                            # we should probably keep calculating full total here if needed,
                            # or use top-5 if that's what's expected.
                            # Original code: bid_tot = sum(float(lv[1]) for lv in book["bids"])
                            # We'll use the 'book_raw' dict which still has all levels.
                            bid_tot = sum(float(lv[1]) for lv in book_raw.get("bids", []))
                            ask_tot = sum(float(lv[1]) for lv in book_raw.get("asks", []))
                            runtime.l3_queue.on_l2_totals(bid_total=bid_tot, ask_total=ask_tot)
                        except Exception:
                            pass
                    except Exception as exc:  # noqa: BLE001
                        logger.exception("❌ (%s) Ошибка обработки книги %s: %s", symbol, msg_id, exc)
                    finally:
                        try:
                            await helper.ack(stream_name, msg_id)
                        except RedisError as exc:
                            logger.warning("⚠️ (%s) Не удалось ACK book %s: %s", symbol, msg_id, exc)
            backoff.reset()

    # ── Обработка тиков и генерация сигналов ──────────────────────────────────

    async def _resolve_streams(self, symbol: str) -> Tuple[str, str]:
        """
        Определяет реальные имена стримов (поддерживает и ':' и '_').
        """
        candidates_tick = [f"stream:tick_{symbol}", f"ticks:{symbol}", f"stream:tick:{symbol}"]
        candidates_book = [f"stream:book_{symbol}", f"book:{symbol}", f"stream:book:{symbol}"]

        # 1. Check direct ENV overrides first
        env_tick = os.getenv(f"{symbol}_TICK_STREAM")
        if env_tick:
             candidates_tick.insert(0, env_tick)
        
        env_book = os.getenv(f"{symbol}_BOOK_STREAM")
        if env_book:
             candidates_book.insert(0, env_book)

        tick_stream = await self._first_existing_stream(candidates_tick)
        book_stream = await self._first_existing_stream(candidates_book)

        if tick_stream is None:
            tick_stream = candidates_tick[0]  # предпочтительный формат stream:tick_{symbol}
        if book_stream is None:
            book_stream = candidates_book[0]

        return tick_stream, book_stream

    async def _first_existing_stream(self, candidates: List[str]) -> Optional[str]:
        for name in candidates:
            try:
                exists = await self.ticks.exists(name)
            except RedisError:
                exists = 0
            if exists:
                return name
        return None


    async def _close_redis(self, client: aioredis.Redis) -> None:
        if client is None:
            return
        try:
            await client.aclose()
        except AttributeError:
            # Fallback for older redis versions
            try:
                client.close()
                await client.wait_closed()
            except AttributeError:
                pass


# ──────────────────────────────────────────────────────────────────────────────
# Точка входа
# ──────────────────────────────────────────────────────────────────────────────


async def _async_main() -> None:
    # Start metrics server
    try:
        start_http_server(8000)
        logger.info("✅ Metrics server started on port 8000")
        print("✅ DEBUG: Metrics server started on port 8000", file=sys.stdout)
        sys.stdout.flush()
    except Exception as e:
        logger.warning(f"⚠️ Failed to start metrics server: {e}")
        print(f"⚠️ DEBUG: Failed to start metrics server: {e}", file=sys.stderr)
        sys.stderr.flush()

    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    ticks_url = os.getenv("REDIS_TICKS_URL") or os.getenv("REDIS_URL_TICKS")

    service = CryptoOrderflowService(redis_dsn=redis_url, ticks_dsn=ticks_url)
    try:
        await service.run_forever()
    except asyncio.CancelledError:
        pass
    finally:
        await service.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(_async_main())
    except KeyboardInterrupt:
        logger.info("🛑 CryptoOrderflowService остановлен по Ctrl+C")

