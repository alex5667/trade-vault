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
import hashlib
import os
import time
import sys
import asyncio
import logging
import traceback
import random
from typing import Any, Dict, List, Optional, Tuple, Set
from collections import deque, OrderedDict
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
    obi_stability_score_gauge, drain_forced_cancel_total,
    worker_lag_ms_gauge, processing_time_us, redis_errors_total,
    tick_dedup_dropped_total, redis_pel_pending_gauge, redis_pel_claim_total,
    tick_uid_missing_total, tick_trade_id_missing_total, ticks_deduped_total, redis_pel_claimed_total,
    ticks_quarantined_total, ticks_schema_invalid_total, tick_ts_missing_total, tick_ts_clamped_total,
    ticks_unknown_side_policy_total, ticks_unknown_side_quarantine_published_total, ticks_dropped_total
)
from services.orderflow.utils import (
    _fields_to_dict, _parse_tick_payload, _parse_book_payload, redis_stream_id_ts_ms, _compute_tick_uid
)
from services.orderflow.tick_dedup import TickDeduper, tick_uid
from handlers.crypto_orderflow.utils.log_sampler import sampled_info, sampled_debug, LogSamplerFactory
from services.orderflow.runtime import SymbolRuntime, BookSnapshot
from services.orderflow.strategy import OrderFlowStrategy
from services.signal_preprocess import preprocess_signal_for_publish
from services.persistence_manager import get_persistence_manager
from services.orderflow.calibration_repo import CalibrationRepository
from services.orderflow.calibration_service import CalibrationService

from core.of_confirm_engine import OFConfirmEngine

from services.async_signal_publisher import AsyncSignalPublisher
from common.backoff import Backoff
from common.redis_errors import is_transient_error as is_transient_redis_error
from core.redis_stream_consumer import AsyncRedisStreamHelper
from redis.exceptions import ResponseError, RedisError
import redis.asyncio as aioredis

# Tick time policy enforcement
from common.tick_time import TickTimeGuard, TickTimePolicy
from common.time_quarantine import BadTimeQuarantine, BadTimeQuarantinePolicy

# Unknown-side policy
from services.orderflow.side_policy import (
    normalize_unknown_side_policy, is_unknown_side_tick, deterministic_sample
)


from core.book_churn import compute_churn_from_z
from core.cvd_reclaim import compute_cvd_reclaim

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
            source="CryptoOrderFlow"  # ✅ FIX: Use canonical source name
        )

        # Engines
        self.of_engine = OFConfirmEngine()
        self.config_loader = OrderFlowConfigLoader(redis_client=None) # updated in run_forever
        
        self.strategy: Optional[OrderFlowStrategy] = None

        # ✅ ИСПРАВЛЕНИЕ: Параметризация и оптимизация пула соединений
        main_max = int(os.getenv("REDIS_MAIN_MAX_CONNECTIONS", "200"))
        ticks_max = int(os.getenv("REDIS_TICKS_MAX_CONNECTIONS", "600"))
        conn_to = float(os.getenv("REDIS_SOCKET_CONNECT_TIMEOUT", "5"))
        sock_to = float(os.getenv("REDIS_SOCKET_TIMEOUT", "15"))
        hc_iv = int(os.getenv("REDIS_HEALTHCHECK_INTERVAL", "30"))

        self.main: aioredis.Redis = aioredis.from_url(
            self.redis_dsn,
            decode_responses=True,
            socket_connect_timeout=conn_to,
            socket_timeout=sock_to,
            socket_keepalive=True,
            health_check_interval=hc_iv,
            max_connections=main_max,
        )

        # Если стримы на одном Redis, используем один пул для экономии соединений
        if str(self.ticks_dsn) == str(self.redis_dsn):
            self.ticks = self.main
            logger.info("🔗 Using shared Redis client for main and ticks (max_conn=%d)", main_max)
        else:
            self.ticks: aioredis.Redis = aioredis.from_url(
                self.ticks_dsn,
                decode_responses=True,
                socket_connect_timeout=conn_to,
                socket_timeout=sock_to,
                socket_keepalive=True,
                health_check_interval=hc_iv,
                max_connections=ticks_max,
            )
            logger.info("🔗 Using separate Redis clients: main (max=%d), ticks (max=%d)", main_max, ticks_max)

        self.config_loader = OrderFlowConfigLoader(self.main)

        # PersistenceManager (PG) injectable into SymbolRuntime for testability
        try:
            self.pm = get_persistence_manager()
        except Exception as exc:
            logger.error("Failed to init PersistenceManager: %s", exc, exc_info=True)
            self.pm = None

        # Calibration Service (SRP Phase A)
        self.calib_repo = CalibrationRepository(redis_ticks=self.ticks, pm=self.pm)
        self.calib_svc = CalibrationService(repo=self.calib_repo)

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
        self._tick_uid_cache_by_symbol: Dict[str, OrderedDict[str, int]] = {}
        self._tick_uid_cache_size: int = int(os.getenv("TICK_UID_DEDUPE_CACHE_SIZE", "4096") or 4096)
        
        # Quarantine for persistent message failures
        self.poison_pill_counts: Dict[str, int] = {}
        self.quarantine_stream = os.getenv("SIGNAL_QUARANTINE_STREAM", "stream:of:quarantine")

        # Tick quarantine (bad schema/time/exception)
        self.tick_quarantine_enable = os.getenv("TICK_QUARANTINE_ENABLE", "1").lower() in {"1", "true", "yes"}
        self.tick_quarantine_stream = os.getenv("TICK_QUARANTINE_STREAM", "stream:tick:quarantine")
        self.tick_quarantine_maxlen = int(os.getenv("TICK_QUARANTINE_MAXLEN", "50000") or 50000)
        self.tick_quarantine_sample_rate = float(os.getenv("TICK_QUARANTINE_SAMPLE_RATE", "0.05") or 0.05)

        # Unknown-side tick policy (prevents implicit BUY bias)
        self._unknown_side_policy = normalize_unknown_side_policy(os.getenv("CRYPTO_OF_UNKNOWN_SIDE_POLICY"))
        self._unknown_side_quarantine_stream = os.getenv("TICK_SIDE_QUARANTINE_STREAM", "stream:tick_side:quarantine")
        self._unknown_side_quarantine_sample = float(os.getenv("TICK_SIDE_QUARANTINE_SAMPLE", "0.01") or 0.01)
        self._unknown_side_quarantine_maxlen = int(os.getenv("TICK_SIDE_QUARANTINE_MAXLEN", "20000") or 20000)

        # Tick dedup (protect against duplicates / retries)
        self.tick_dedup_enable = os.getenv("TICK_DEDUP_ENABLE", "1").lower() in {"1", "true", "yes"}
        self.tick_dedup_max_items = int(os.getenv("TICK_DEDUP_MAX_ITEMS", "20000") or 20000)
        self.tick_dedup_max_age_ms = int(os.getenv("TICK_DEDUP_MAX_AGE_MS", "180000") or 180000)
        self._tick_dedupers: Dict[str, TickDeduper] = {}

        # Tick time policy (event-time hygiene before strategy)
        self.tick_time_max_future_ms = int(os.getenv("TICK_TIME_MAX_FUTURE_MS", "5000") or 5000)
        self.tick_time_max_past_ms = int(os.getenv("TICK_TIME_MAX_PAST_MS", "120000") or 120000)
        self.tick_time_max_reorder_ms = int(os.getenv("TICK_TIME_MAX_REORDER_MS", "1500") or 1500)
        self.tick_time_allow_soft_reorder = os.getenv("TICK_TIME_ALLOW_SOFT_REORDER", "1").lower() in {"1", "true", "yes"}
        self.tick_time_clamp_soft_future = os.getenv("TICK_TIME_CLAMP_SOFT_FUTURE", "1").lower() in {"1", "true", "yes"}
        
        # Tick time policy enforcement (from next_step_6_tick_time_policy_enforce_autotune)
        self.tick_time_policy = TickTimePolicy(
            max_future_ms=self.tick_time_max_future_ms,
            max_past_ms=self.tick_time_max_past_ms,
            max_reorder_ms=self.tick_time_max_reorder_ms,
            clamp_soft_future=self.tick_time_clamp_soft_future,
            allow_soft_reorder=self.tick_time_allow_soft_reorder,
        )
        
        # Per-symbol tick time guards and quarantine
        self.tick_time_guards: Dict[str, TickTimeGuard] = {}
        self.tick_time_quarantines: Dict[str, BadTimeQuarantine] = {}
        
        # Quarantine policy
        self.tick_time_quarantine_policy = BadTimeQuarantinePolicy(
            hard_drop_streak_threshold=int(os.getenv("BAD_TIME_TRIGGER_STREAK", "3") or 3),
            score_threshold=float(os.getenv("BAD_TIME_TRIGGER_SCORE", "3.0") or 3.0),
            hard_drop_score=float(os.getenv("BAD_TIME_HARD_PENALTY", "1.0") or 1.0),
            soft_event_score=float(os.getenv("BAD_TIME_SOFT_PENALTY", "0.2") or 0.2),
            ok_decay=float(os.getenv("BAD_TIME_DECAY_PER_OK", "0.1") or 0.1),
            quarantine_ttl_ms=int(os.getenv("BAD_TIME_QUARANTINE_MS", "60000") or 60000),
            state_freeze_ttl_ms=int(os.getenv("BAD_TIME_STATE_FREEZE_MS", "15000") or 15000),
        )
        
        # Tick time observability
        self.tick_time_observe_enable = os.getenv("TICK_TIME_OBSERVE_ENABLE", "1").lower() in {"1", "true", "yes"}
        self.tick_time_stream_enable = os.getenv("TICK_TIME_STREAM_ENABLE", "0").lower() in {"1", "true", "yes"}
        self.tick_time_stream_key = os.getenv("TICK_TIME_STREAM_KEY", "metrics:tick_time")
        self.tick_time_stream_sample = float(os.getenv("TICK_TIME_STREAM_SAMPLE", "0.01") or 0.01)
        self.tick_time_stream_maxlen = int(os.getenv("TICK_TIME_STREAM_MAXLEN", "200000") or 200000)

        # Max timestamp skew for event time coercion (ms)
        self._max_ts_skew_ms = int(os.getenv("TICK_TIME_MAX_SKEW_MS", "5000") or 5000)

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
        self._burst_task: Optional[asyncio.Task] = None
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
        logger.info("   Telegram every_n: %s", os.getenv("CRYPTO_NOTIFY_SIGNAL_EVERY_N", "100"))
        logger.info("   Signal min confidence: %s%%", os.getenv("CRYPTO_SIGNAL_MIN_CONF", "70"))

    def _env_bool(self, name: str, default: Optional[str] = None) -> bool:
        val = os.getenv(name, default)
        if not val:
            return False
        return str(val).lower() in ("1", "true", "yes", "on")

    def _should_sample_tick_quarantine(self, uid: str) -> bool:
        if not self.tick_quarantine_enable:
            return False
        r = float(self.tick_quarantine_sample_rate or 0.0)
        if r >= 1.0:
            return True
        if r <= 0.0:
            return False
        try:
            h = int(hashlib.sha1(uid.encode("utf-8")).hexdigest()[:8], 16)
            return (h % 10000) < int(r * 10000)
        except Exception:
            import random
            return random.random() < r

    def _should_sample_unknown_side(self, key_ms: int) -> bool:
        try:
            return deterministic_sample(int(key_ms), float(self._unknown_side_quarantine_sample))
        except Exception:
            return False

    async def _quarantine_unknown_side_tick(
        self, *, symbol: str, msg_id: str, tick: Dict[str, Any],
        raw_fields: Dict[str, Any], reason: str = "unknown_side"
    ) -> None:
        try:
            if not self.ticks:
                return
            key_ms = _safe_int(tick.get("event_ts_ms") or tick.get("ts_ms") or 0)
            if not self._should_sample_unknown_side(key_ms):
                return

            payload = {
                "symbol": str(symbol),
                "reason": str(reason),
                "policy": str(self._unknown_side_policy),
                "msg_id": str(msg_id),
                "tick_uid": str(tick.get("tick_uid") or ""),
                "event_ts_ms": str(_safe_int(tick.get("event_ts_ms") or 0)),
                "ts_source": str(tick.get("ts_source") or ""),
                "side": str(tick.get("side") or ""),
                "side_conf": str(tick.get("side_conf") or ""),
                "side_raw": str(tick.get("side_raw") or ""),
                "is_buyer_maker": str(tick.get("is_buyer_maker") if tick.get("is_buyer_maker") is not None else ""),
                "trade_id": str(tick.get("trade_id") or ""),
                "price": str(tick.get("price") or ""),
                "qty": str(tick.get("qty") or tick.get("volume") or ""),
            }
            try:
                raw_keys = sorted(list(raw_fields.keys()))
                payload["raw_keys"] = ",".join(raw_keys[:32])
            except Exception:
                pass

            await self.ticks.xadd(
                self._unknown_side_quarantine_stream,
                payload,
                maxlen=int(self._unknown_side_quarantine_maxlen),
                approximate=True,
            )

            try:
                ticks_unknown_side_quarantine_published_total.labels(symbol=str(symbol), reason=str(reason)).inc()
            except Exception:
                pass
        except Exception:
            return  # fail-open

    async def _quarantine_tick(
        self,
        *,
        symbol: str,
        msg_id: str,
        tick: Dict[str, Any],
        reason: str,
        indicators: Optional[Dict[str, Any]] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Best-effort quarantine write for problematic ticks."""
        try:
            ticks_quarantined_total.labels(symbol=symbol, reason=str(reason)).inc()
        except Exception:
            pass

        uid = str(tick.get("_uid") or tick_uid(tick))
        if not self._should_sample_tick_quarantine(uid):
            return

        try:
            now_ms = int(time.time() * 1000)
        except Exception:
            now_ms = 0

        def _dumps(obj: Any, limit: int = 4096) -> str:
            try:
                s = json.dumps(obj, ensure_ascii=False, default=str)
            except Exception:
                s = str(obj)
            if len(s) > limit:
                return s[:limit] + "...(truncated)"
            return s

        payload: Dict[str, Any] = {
            "kind": "tick",
            "symbol": symbol,
            "reason": str(reason),
            "msg_id": str(msg_id),
            "uid": uid,
            "ts_ms": _safe_int(tick.get("ts_ms") or 0),
            "event_ts_ms": _safe_int(tick.get("event_ts_ms") or 0),
            "written_at_ms": now_ms,
            "tick": _dumps(tick),
        }
        if indicators:
            payload["indicators"] = _dumps(indicators)
        if extra:
            payload["extra"] = _dumps(extra)
        try:
            await self.ticks.xadd(
                self.tick_quarantine_stream,
                payload,
                maxlen=self.tick_quarantine_maxlen,
                approximate=True,
            )
        except Exception:
            return

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
            calib_svc=self.calib_svc,
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

        if self._supervisor_task:
            self._supervisor_task.cancel()
            await asyncio.gather(self._supervisor_task, return_exceptions=True)

        if self._refresh_task:
            self._refresh_task.cancel()
            await asyncio.gather(self._refresh_task, return_exceptions=True)

        if hasattr(self, "_burst_task") and self._burst_task:
            self._burst_task.cancel()
            await asyncio.gather(self._burst_task, return_exceptions=True)

        # Drain mode: let workers finish current iteration to reduce PEL growth
        drain_timeout = float(os.getenv("CRYPTO_OF_DRAIN_TIMEOUT_SEC", "10"))
        logger.info("🔻 Draining symbol workers (timeout=%.1fs)...", drain_timeout)

        # Track tasks with their metadata for reporting
        task_to_meta = {}
        for symbol, (t_tick, t_book) in list(self.symbol_tasks.items()):
            if t_tick and not t_tick.done():
                task_to_meta[t_tick] = (symbol, "ticks")
            if t_book and not t_book.done():
                task_to_meta[t_book] = (symbol, "books")

        if task_to_meta:
            all_tasks = list(task_to_meta.keys())
            done, pending = await asyncio.wait(all_tasks, timeout=drain_timeout)
            
            if pending:
                logger.warning("⚠️ Drain timeout: forcing cancel of %d task(s)", len(pending))
                for t in pending:
                    t.cancel()
                    sym, kind = task_to_meta[t]
                    if drain_forced_cancel_total:
                        drain_forced_cancel_total.labels(symbol=sym, kind=kind).inc()
                
                await asyncio.gather(*pending, return_exceptions=True)

        self.symbol_tasks.clear()

        if self.ticks is not None:
            if self.ticks is self.main:
                await self._close_redis(self.main)
                self.main = None
                self.ticks = None
            else:
                await self._close_redis(self.ticks)
                await self._close_redis(self.main)
                self.main = None
                self.ticks = None
        elif self.main is not None:
             await self._close_redis(self.main)
             self.main = None
        logger.info("✅ Завершено")


    # ── Динамическая загрузка символов ────────────────────────────────────────

    async def load_dynamic_symbols(self) -> None:
        """
        Загружает список символов и их конфиг из Redis, запускает новые задачи.
        """
        if self._shutdown:
            return
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
                runtime.pm = getattr(self, 'pm', None)
                # Inject Redis client for metrics (best-effort, safe if None)
                runtime.redis_client = getattr(self, 'main', None)
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

            # Запуск задач, если ещё не запущены (или упали)
            tasks = self.symbol_tasks.get(symbol)
            if not tasks:
                # EAGER BOOTSTRAP: Load calibrations BEFORE starting tasks
                try:
                    # Bounded concurrency for bootstrap (limit 10)
                    sem = asyncio.Semaphore(10)
                    async def bootstrap_task():
                        async with sem:
                            try:
                                await asyncio.wait_for(self.calib_svc.ensure_loaded(runtime), timeout=2.0)
                            except Exception as exc:
                                log_silent_error(exc, 'bootstrap_timeout', symbol, 'load_dynamic_symbols:bootstrap')
                            runtime.ready = True
                    
                    asyncio.create_task(bootstrap_task())
                except Exception:
                    runtime.ready = True # fail-open

                tick_task = asyncio.create_task(self.consume_ticks(symbol), name=f"crypto-of-ticks-{symbol}")
                book_task = asyncio.create_task(self.consume_books(symbol), name=f"crypto-of-book-{symbol}")
                self.symbol_tasks[symbol] = (tick_task, book_task)
            else:
                t_tick, t_book = tasks
                if (t_tick and t_tick.done()) or (t_book and t_book.done()):
                    logger.error("❌ (%s) Detected dead tasks in load_dynamic_symbols; will restart via supervisor", symbol)
                # Throttled worker start log (every 10000th)
                sampled_info(
                    logger,
                    "WORKER_INIT",
                    "🚀 (%s) воркеры запущены: k=%s, delta_z_threshold=%.2f, min_conf=%.2f%%, every_n=%s",
                    symbol, runtime.book_stream,
                    float(cfg.get("delta_z_threshold") or 3.10),
                    float(os.getenv("CRYPTO_SIGNAL_MIN_CONF", "70")),
                    os.getenv("CRYPTO_NOTIFY_SIGNAL_EVERY_N", "100")
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
            while not self._shutdown:
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

        last_alive_log = 0.0
        while not self._shutdown:
            try:
                await asyncio.sleep(max(0.05, interval_ms / 1000.0))
                now_s = time.time()
                if now_s - last_alive_log > 60:
                    # Sample every 10000th message
                    burst_loop_sampler = LogSamplerFactory.get_sampler("BURST_LOOP_ALIVE", 10000)
                    if burst_loop_sampler.should_log("burst_loop_alive"):
                        logger.info("💓 Burst flush loop alive. Active symbols=%d mode=%s", len(self.symbol_contexts), mode)
                    last_alive_log = now_s

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

                        # Используем общий метод для обработки burst.
                        # Внутри _process_burst_flush уже вызывается maybe_flush под блокировкой
                        # и обновляются соответствующие метрики (gauge).
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
        async with runtime.burst_mu:
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
            except Exception as exc:
                log_silent_error(exc, 'pressure_record_failure', runtime.symbol, '_process_burst_flush:record_emit')

            # Metrics
            if burst_flush_total:
                burst_flush_total.labels(symbol=runtime.symbol, mode=trigger_source).inc()
            if signals_emitted_total:
                signals_emitted_total.labels(symbol=runtime.symbol).inc()

            # Sample every 10000th message
            burst_flush_sampler = LogSamplerFactory.get_sampler("BURST_FLUSH", 10000)
            if burst_flush_sampler.should_log(f"burst_flush_{runtime.symbol}"):
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

    def _dedupe_tick(self, runtime: Any, tick: Dict[str, Any]) -> bool:
        """Check if tick is duplicate. Returns True if duplicate (should skip)."""
        if not tick:
            return False
        
        # Check if dedupe is enabled
        if os.getenv("TICK_DEDUPE_MAXLEN", "20000").strip() in ("0", "false", "off"):
            return False
        
        maxlen = int(os.getenv("TICK_DEDUPE_MAXLEN", "20000") or 20000)
        if maxlen <= 0:
            return False
        
        # Get tick_uid (preferred) or fallback to trade_id/stream_id
        uid = tick.get("tick_uid") or tick.get("trade_id") or tick.get("stream_id") or ""
        if not uid:
            return False
        
        uid_str = str(uid)
        
        # Initialize dedupe structures if needed
        dq = getattr(runtime, "_tick_dedupe_dq", None)
        st = getattr(runtime, "_tick_dedupe_set", None)
        if dq is None or st is None:
            dq = deque(maxlen=maxlen)
            st = set()
            setattr(runtime, "_tick_dedupe_dq", dq)
            setattr(runtime, "_tick_dedupe_set", st)
        
        # Check if already seen
        if uid_str in st:
            if tick_dedup_dropped_total:
                tick_dedup_dropped_total.labels(symbol=runtime.symbol, reason="duplicate").inc()
            return True
        
        # Add to dedupe structures
        dq.append(uid_str)
        st.add(uid_str)
        
        # Trim if over maxlen (deque should handle this, but be safe)
        while len(dq) > maxlen:
            old = dq.popleft()
            st.discard(old)
        
        return False

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
        
        # PEL recovery timer (persists across loop iterations)
        pel_every_sec = float(os.getenv("REDIS_PEL_RECOVERY_EVERY_SEC", "5") or 5)
        pel_min_idle_ms = int(os.getenv("REDIS_PEL_MIN_IDLE_MS", "5000") or 5000)
        pel_claim_count = int(os.getenv("REDIS_PEL_CLAIM_COUNT", "100") or 100)
        pel_start_id = str(os.getenv("REDIS_PEL_START_ID", "0-0") or "0-0")
        pel_last_check = 0.0
        dedupe_size = int(self._tick_uid_cache_size or 4096)
        
        while not self._shutdown:
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

            # 1. Calibration loading (SRP Phase A - Service-based)
            await self.calib_svc.ensure_loaded(runtime)
            
            # 2. Daily candle warmup (TODO: move to service later)

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
                # Periodic PEL recovery: claim pending messages (stuck after crash/restart)
                claimed_entries = []
                now_ts = time.time()
                if pel_every_sec > 0 and (now_ts - pel_last_check) >= pel_every_sec:
                    pel_last_check = now_ts
                    try:
                        pending = await helper.pending_len(stream)
                        if pending and pending > 0:
                            next_id, claimed_msgs = await helper.claim_pending(
                                stream,
                                min_idle_ms=pel_min_idle_ms,
                                count=min(pel_claim_count, int(pending)),
                                start_id=pel_start_id,
                            )
                            if claimed_msgs:
                                redis_pel_claimed_total.labels(symbol=symbol, kind="tick").inc(len(claimed_msgs))
                                # Convert StreamMsg to (msg_id, fields) format
                                claimed_entries = [(msg.msg_id, msg.fields) for msg in claimed_msgs]
                    except Exception as exc:
                        log_silent_error(exc, "pel_recovery_failed", symbol, "consume_ticks")
                        claimed_entries = []

                # One authoritative read call
                if runtime.loop_log_sampler.should_log("helper_read_call"):
                    logger.debug("🔍 (%s) Calling helper.read (block=%dms, count=%d)", symbol, block_ms, count)
                messages = await helper.read(
                    {stream: ">"},
                    count=count,
                    block=block_ms,
                )

                if claimed_entries:
                    # prepend claimed pending messages before new ones to reduce staleness
                    messages = [(stream, claimed_entries)] + (messages or [])
                if runtime.loop_log_sampler.should_log("helper_read_result"):
                    logger.debug("🔍 (%s) helper.read returned %d stream entries", symbol, len(messages) if messages else 0)
            except ResponseError as exc:
                if redis_errors_total: redis_errors_total.labels(op="read_ticks", symbol=symbol).inc()
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
                if redis_errors_total: redis_errors_total.labels(op="read_ticks", symbol=symbol).inc()
                if is_transient_redis_error(exc):
                    delay = backoff.next_sleep()
                    logger.warning("⚠️ (%s) Transient ошибка чтения стрима %s: %s (backoff=%.2fs)", symbol, str(exc), delay)
                    await asyncio.sleep(delay)
                    continue
                else:
                    logger.error("❌ (%s) Критическая ошибка чтения стрима %s: %s", symbol, stream, exc)
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

                        # Normalize msg_id and enforce symbol consistency
                        try:
                            msg_id_s = msg_id.decode("utf-8") if isinstance(msg_id, (bytes, bytearray)) else str(msg_id)
                        except Exception:
                            msg_id_s = str(msg_id)

                        tick_symbol = str(tick.get("symbol") or "")
                        if tick_symbol and tick_symbol != symbol:
                            try:
                                ticks_schema_invalid_total.labels(symbol=symbol, field="symbol_mismatch").inc()
                            except Exception:
                                pass
                            await self._quarantine_tick(
                                symbol=symbol,
                                msg_id=msg_id_s,
                                tick=tick,
                                reason="symbol_mismatch",
                                extra={"tick_symbol": tick_symbol},
                            )
                            processed_ok = True
                            continue

                        tick["symbol"] = symbol
                        tick["_msg_id"] = msg_id_s

                        # Единая модель времени (event/ingest/process)
                        ingest_ts_ms = int(time.time() * 1000)
                        tick["ingest_ts_ms"] = ingest_ts_ms

                        # Stream metadata (for deterministic replay / observability)
                        tick["stream_id"] = str(msg_id)
                        tick["stream_ms"] = int(self._msgid_ms(str(msg_id)) or 0)

                        # Deterministic event-time (prefer payload ts if sane; else Redis msg_id ms)
                        now_ms = ingest_ts_ms
                        payload_ts_ms = 0
                        try:
                            payload_ts_ms = _safe_int(tick.get("ts_ms") or tick.get("event_ts_ms") or 0)
                        except Exception:
                            payload_ts_ms = 0
                        tick["payload_ts_ms"] = int(payload_ts_ms or 0)

                        event_ts_ms, ts_source = self._coerce_event_ts_ms(
                            msg_id=str(msg_id),
                            payload_ts_ms=int(payload_ts_ms or 0),
                            now_ms=int(now_ms),
                        )
                        tick["ts_source"] = str(ts_source)
                        tick["event_ts_ms"] = int(event_ts_ms)
                        tick["ts_ms"] = int(event_ts_ms)  # legacy compatibility

                        # process_ts_ms перед фактическим process_tick
                        process_ts_ms = int(time.time() * 1000)
                        tick["process_ts_ms"] = process_ts_ms

                        # Basic schema validation
                        bad_field = None
                        try:
                            price = float(tick.get("price") or 0.0)
                            if not (price > 0.0):
                                bad_field = "price"
                        except Exception:
                            bad_field = "price"
                        if bad_field is None:
                            try:
                                qty = float(tick.get("qty") or 0.0)
                                if qty < 0.0:
                                    bad_field = "qty"
                            except Exception:
                                bad_field = "qty"
                        if bad_field is None:
                            side = str(tick.get("side") or "").upper()
                            if side not in ("BUY", "SELL", "UNKNOWN"):
                                bad_field = "side"
                        if bad_field is not None:
                            try:
                                ticks_schema_invalid_total.labels(symbol=symbol, field=bad_field).inc()
                            except Exception:
                                pass
                            await self._quarantine_tick(symbol=symbol, msg_id=msg_id_s, tick=tick, reason=f"bad_{bad_field}")
                            processed_ok = True
                            continue

                        # Tick dedup (best-effort). Drop duplicates to avoid double-counting delta/volume.
                        try:
                            uid = _compute_tick_uid(
                                symbol=str(tick.get("symbol") or symbol),
                                trade_id=tick.get("trade_id"),
                                ts_ms=_safe_int(tick.get("ts_ms") or 0),
                                price_src=raw.get("price") or raw.get("p") or raw.get("last") or raw.get("mid"),
                                qty_src=raw.get("qty") or raw.get("q") or raw.get("volume"),
                                side=str(tick.get("side") or ""),
                                is_buyer_maker=tick.get("is_buyer_maker"),
                                stream_id=str(msg_id),
                            )
                            tick["tick_uid"] = uid
                        except Exception:
                            uid = tick_uid(tick)
                        tick["_uid"] = uid

                        try:
                            now_ms = int(time.time() * 1000)
                        except Exception:
                            now_ms = 0

                        # Tick time policy enforcement (next_step_6_tick_time_policy_enforce_autotune)
                        # Initialize guard and quarantine for this symbol if needed
                        guard = self.tick_time_guards.get(symbol)
                        if guard is None:
                            guard = TickTimeGuard(self.tick_time_policy)
                            self.tick_time_guards[symbol] = guard
                        
                        quarantine = self.tick_time_quarantines.get(symbol)
                        if quarantine is None:
                            def _inc_metric(name: str, delta: int = 1) -> None:
                                try:
                                    # Try to increment Prometheus metrics if available
                                    pass  # Metrics can be added here if needed
                                except Exception:
                                    pass
                            quarantine = BadTimeQuarantine(policy=self.tick_time_quarantine_policy, inc=_inc_metric)
                            self.tick_time_quarantines[symbol] = quarantine
                        
                        # Check if processing should be suppressed due to quarantine
                        if quarantine.should_suppress_processing(now_ms):
                            processed_ok = True
                            continue
                        
                        # Apply tick time policy
                        raw_ts = tick.get("ts_ms") or tick.get("ts") or 0
                        sanitize_result = guard.sanitize_ts_ms(raw_ts, now_ms=now_ms)
                        
                        if sanitize_result is None:
                            # Failed to parse timestamp
                            await self._quarantine_tick(
                                symbol=symbol,
                                msg_id=msg_id_s,
                                tick=tick,
                                reason="drop_missing",
                                extra={"raw_ts": raw_ts},
                            )
                            processed_ok = True
                            continue
                        
                        if sanitize_result.drop_reason:
                            # Hard drop - update quarantine and quarantine tick
                            quarantine.on_hard_drop(sanitize_result.drop_reason, now_ms)
                            await self._quarantine_tick(
                                symbol=symbol,
                                msg_id=msg_id_s,
                                tick=tick,
                                reason=sanitize_result.drop_reason,
                                extra=sanitize_result.to_meta(),
                            )
                            processed_ok = True
                            continue
                        
                        # Update tick timestamp if it was clamped
                        if sanitize_result.ts_ms != raw_ts:
                            tick["ts_ms"] = sanitize_result.ts_ms
                            tick["ts"] = sanitize_result.ts_ms
                        
                        # Track soft events for quarantine scoring
                        if sanitize_result.flags:
                            for flag in sanitize_result.flags:
                                quarantine.on_soft_event(flag)
                        
                        # Successful tick - update quarantine state
                        quarantine.on_ok_tick()
                        
                        # Update runtime watermark
                        t_ms = sanitize_result.ts_ms
                        if hasattr(runtime, "last_ts_ms"):
                            runtime.last_ts_ms = max(int(getattr(runtime, "last_ts_ms", 0) or 0), int(t_ms))
                        
                        # Optional: publish tick time observability metrics
                        if self.tick_time_observe_enable and self.tick_time_stream_enable:
                            try:
                                import random
                                if random.random() < self.tick_time_stream_sample:
                                    meta = sanitize_result.to_meta()
                                    meta["symbol"] = symbol
                                    await self.ticks.xadd(
                                        self.tick_time_stream_key,
                                        {"payload": json.dumps(meta)},
                                        maxlen=self.tick_time_stream_maxlen,
                                    )
                            except Exception:
                                pass

                        # In-memory dedup
                        if self.tick_dedup_enable:
                            d = self._tick_dedupers.get(symbol)
                            if d is None:
                                d = TickDeduper(max_items=self.tick_dedup_max_items, max_age_ms=self.tick_dedup_max_age_ms)
                                self._tick_dedupers[symbol] = d
                            if d.seen(uid, now_ms):
                                try:
                                    ticks_dedup_dropped_total.labels(symbol=symbol, mode="uid").inc()
                                except Exception:
                                    pass
                                processed_ok = True
                                continue

                        # Unknown-side policy: prevents implicit BUY bias and enables quarantine/drop options.
                        if is_unknown_side_tick(tick):
                            try:
                                ticks_unknown_side_policy_total.labels(
                                    symbol=str(symbol), policy=str(self._unknown_side_policy)
                                ).inc()
                            except Exception:
                                pass

                            pol = str(self._unknown_side_policy or "ignore_delta")
                            if pol in ("drop", "quarantine"):
                                try:
                                    ticks_dropped_total.labels(symbol=symbol, reason=f"unknown_side_{pol}").inc()
                                except Exception:
                                    pass

                                if pol == "quarantine":
                                    await self._quarantine_unknown_side_tick(
                                        symbol=str(symbol),
                                        msg_id=str(msg_id),
                                        tick=tick,
                                        raw_fields=fields,
                                        reason="unknown_side",
                                    )
                                processed_ok = True
                                continue

                        # worker_lag_ms observability
                        try:
                            ts_ms = _safe_int(tick.get("ts_ms") or 0)
                            if ts_ms > 0 and worker_lag_ms_gauge:
                                worker_lag_ms_gauge.labels(symbol=symbol).set(float(int(time.time()*1000) - ts_ms))
                        except Exception:
                            pass

                        # Track lag and update last_seen_tick_ts (if parse succeeded)
                        if tick:
                            try:
                                runtime.last_seen_tick_ts = tick.get("ts_ms", tick.get("ts", 0))
                            except Exception:
                                pass

                        ticks_processed_total.labels(symbol=symbol).inc()

                        if self.strategy:
                            t0 = time.perf_counter_ns()
                            signal = await self.strategy.process_tick(runtime, tick)
                            try:
                                if processing_time_us:
                                    dt_us = (time.perf_counter_ns() - t0) / 1000.0
                                    processing_time_us.labels(symbol=symbol).observe(float(dt_us))
                            except Exception:
                                pass
                        else:
                            signal = None

                        # CRITICAL FIX: Burst Processing (IMMEDIATE CHECK for Low Latency)
                        # Если стратегия не дала сигнала, проверяем Burst прямо сейчас
                        if not signal:
                            tick_ts = _safe_int(tick.get("ts_ms") or tick.get("ts") or time.time()*1000)

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
                                    source="CryptoOrderFlow",  # ✅ FIX: Use canonical source name
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
                            try:
                                msg_id_s = msg_id.decode("utf-8") if isinstance(msg_id, (bytes, bytearray)) else str(msg_id)
                            except Exception:
                                msg_id_s = str(msg_id)
                            await self._quarantine_tick(
                                symbol=symbol,
                                msg_id=msg_id_s,
                                tick={"symbol": symbol, "ts_ms": redis_stream_id_ts_ms(msg_id_s), "fields": _fields_to_dict(fields)},
                                reason="exception",
                                extra={"error": str(exc)[:200]},
                            )
                            logger.warning("☣️ (%s) Tick message %s quarantined: %s", symbol, msg_id_s, str(exc)[:120])
                            processed_ok = True  # Confirm processing so we move on
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
                                if redis_errors_total: redis_errors_total.labels(op="ack_ticks", symbol=symbol).inc()
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
        
        # PEL recovery timer (persists across loop iterations)
        pel_every_sec = float(os.getenv("REDIS_PEL_RECOVERY_EVERY_SEC", "5") or 5)
        pel_min_idle_ms = int(os.getenv("REDIS_PEL_MIN_IDLE_MS", "5000") or 5000)
        pel_claim_count = int(os.getenv("REDIS_PEL_CLAIM_COUNT", "100") or 100)
        pel_start_id = str(os.getenv("REDIS_PEL_START_ID", "0-0") or "0-0")
        pel_last_check = 0.0
        
        while not self._shutdown:
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
                # Periodic PEL recovery: claim pending messages (stuck after crash/restart)
                claimed_entries = []
                now_ts = time.time()
                if pel_every_sec > 0 and (now_ts - pel_last_check) >= pel_every_sec:
                    pel_last_check = now_ts
                    try:
                        pending = await helper.pending_len(stream)
                        if pending and pending > 0:
                            next_id, claimed_msgs = await helper.claim_pending(
                                stream,
                                min_idle_ms=pel_min_idle_ms,
                                count=min(pel_claim_count, int(pending)),
                                start_id=pel_start_id,
                            )
                            if claimed_msgs:
                                redis_pel_claimed_total.labels(symbol=symbol, kind="book").inc(len(claimed_msgs))
                                # Convert StreamMsg to (msg_id, fields) format
                                claimed_entries = [(msg.msg_id, msg.fields) for msg in claimed_msgs]
                    except Exception as exc:
                        log_silent_error(exc, "pel_recovery_failed", symbol, "consume_books")
                        claimed_entries = []

                messages = await helper.read(
                    {stream: ">"},
                    count=runtime.config.get("read_count", 200),
                    block=runtime.config.get("read_block_ms", 1000),
                )

                if claimed_entries:
                    messages = [(stream, claimed_entries)] + (messages or [])
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

                        # Always persist raw OBI snapshot for p_cluster (even when no stable event fires)
                        try:
                            raw_snap = runtime.obi_detector.snapshot()
                            if raw_snap:
                                runtime.last_obi_snapshot = raw_snap
                        except Exception:
                            pass

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


    def _msgid_ms(self, msg_id: str) -> int:
        """Parse Redis Stream entry id (e.g. '1700000000000-0') -> epoch ms."""
        return redis_stream_id_ts_ms(msg_id)

    def _coerce_event_ts_ms(self, *, msg_id: str, payload_ts_ms: int, now_ms: int) -> Tuple[int, str]:
        """Choose deterministic event time + provenance label.

        Order:
          1) payload_ts_ms if sane (within max skew)
          2) Redis stream msg_id ms
          3) wall clock (last resort)

        Returns:
          (event_ts_ms, ts_source) where ts_source ∈ {payload, msg_id, now}.
        """
        ts = _safe_int(payload_ts_ms or 0)
        if ts > 0 and abs(int(now_ms) - ts) <= int(self._max_ts_skew_ms):
            return int(ts), "payload"
        mid = self._msgid_ms(msg_id)
        if mid > 0:
            return int(mid), "msg_id"
        return int(now_ms), "now"

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

