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
from services.orderflow.metric_labels import TickMetricLimiter, _parse_allowlist, should_emit

import time
import asyncio
from utils.task_manager import safe_create_task

import logging
import traceback
import random
from typing import Any, Dict, List, Optional, Tuple, Set
from collections import deque

from services.orderflow.configuration import (
    OrderFlowConfigLoader, _safe_int, DEFAULT_SYMBOLS
)
from prometheus_client import start_http_server

from services.orderflow.metrics import (
    log_silent_error, burst_active_gauge, burst_flush_total, signals_emitted_total,
    ticks_read_total, ticks_processed_total, signals_published_total,
    drain_forced_cancel_total,
    worker_lag_ms_gauge, worker_lag_ms_p50_gauge, worker_lag_ms_p95_gauge, worker_lag_ms_p99_gauge,
    processing_time_us, redis_errors_total,
    ticks_dropped_total, pel_autoclaim_total, tick_dedup_drop_total,
    ticks_unknown_side_policy_total, ticks_unknown_side_quarantine_published_total,
    ticks_ts_source_total,
    tick_unknown_side_ema_gauge, tick_ts_source_now_ema_gauge, tick_ts_source_stream_id_ema_gauge,
    tick_event_stream_skew_abs_ema_ms_gauge, tick_event_age_abs_ema_ms_gauge,
    tick_ingest_process_ms, tick_ingest_e2e_delay_ms
)
from services.orderflow.tick_quality_ema import TickQualityEMA
from services.orderflow.utils import (
    _fields_to_dict, _parse_tick_payload, _compute_tick_uid
)
from services.orderflow.side_policy import (
    normalize_unknown_side_policy, is_unknown_side_tick, deterministic_sample
)
from handlers.crypto_orderflow.utils.log_sampler import sampled_info, LogSamplerFactory
from services.orderflow.runtime import SymbolRuntime
from services.orderflow.strategy import OrderFlowStrategy
from services.signal_preprocess import preprocess_signal_for_publish
from services.persistence_manager import get_persistence_manager
from services.orderflow.calibration_repo import CalibrationRepository
from services.orderflow.calibration_service import CalibrationService
from common.metrics2 import LagTracker
from health_metrics import HealthMetrics
import time as _time

try:
    # Step 16 (optional): collapse label cardinality
    from services.orderflow.metric_labels import symbol_label as _symbol_label
except Exception:
    _symbol_label = None


from core.of_confirm_engine import OFConfirmEngine

from services.async_signal_publisher import AsyncSignalPublisher
from common.backoff import Backoff
from common.redis_errors import is_transient_error as is_transient_redis_error
from core.redis_stream_consumer import AsyncRedisStreamHelper
from redis.exceptions import ResponseError, RedisError, ConnectionError
import redis.asyncio as aioredis

try:
    # P3: deterministic Redis/data-quality veto — blocks publishing when infra is degraded
    from services.redis_dq_policy import RedisDQSnapshot, RedisDQThresholds, evaluate_redis_dq
except Exception:
    try:
        from redis_dq_policy import RedisDQSnapshot, RedisDQThresholds, evaluate_redis_dq
    except Exception:  # pragma: no cover
        RedisDQSnapshot = RedisDQThresholds = evaluate_redis_dq = None  # type: ignore

try:
    # P4: unified risk policy engine — per-trade sizing + tier policy + exposure caps
    from services.risk.risk_policy_engine import (
        PortfolioPosition, PortfolioRiskInput, PortfolioRiskLimits, evaluate_portfolio_risk,
        infer_symbol_tier, RISK_DENY_HARD, RISK_DENY_SOFT, RISK_FORCE_FLATTEN,
    )
except Exception:
    try:
        from risk.risk_policy_engine import (
            PortfolioPosition, PortfolioRiskInput, PortfolioRiskLimits, evaluate_portfolio_risk,
            infer_symbol_tier, RISK_DENY_HARD, RISK_DENY_SOFT, RISK_FORCE_FLATTEN,
        )
    except Exception:
        # Fall back to legacy portfolio_risk_engine (old API, no infer_symbol_tier)
        try:
            from services.risk.portfolio_risk_engine import (
                PortfolioPosition, PortfolioRiskInput, PortfolioRiskLimits, evaluate_portfolio_risk,
                RISK_DENY_HARD, RISK_DENY_SOFT, RISK_FORCE_FLATTEN,
            )
            infer_symbol_tier = None  # type: ignore
        except Exception:
            try:
                from risk.portfolio_risk_engine import (
                    PortfolioPosition, PortfolioRiskInput, PortfolioRiskLimits, evaluate_portfolio_risk,
                    RISK_DENY_HARD, RISK_DENY_SOFT, RISK_FORCE_FLATTEN,
                )
                infer_symbol_tier = None  # type: ignore
            except Exception:  # pragma: no cover
                PortfolioPosition = PortfolioRiskInput = PortfolioRiskLimits = evaluate_portfolio_risk = infer_symbol_tier = None  # type: ignore
                RISK_DENY_HARD = "DENY_HARD"
                RISK_DENY_SOFT = "DENY_SOFT"
                RISK_FORCE_FLATTEN = "FORCE_FLATTEN"

try:
    # P4.5: SQL audit sink for risk decisions (fail-open: publish path not blocked by DB outages)
    from services.risk.risk_audit_sql import RiskAuditSqlSink
except Exception:
    try:
        from risk.risk_audit_sql import RiskAuditSqlSink
    except Exception:  # pragma: no cover
        RiskAuditSqlSink = None  # type: ignore

try:
    from services.quarantine_denylist import check_signal_against_quarantine_cache
except Exception:
    try:
        from quarantine_denylist import check_signal_against_quarantine_cache
    except Exception:  # pragma: no cover
        check_signal_against_quarantine_cache = None  # type: ignore




# ──────────────────────────────────────────────────────────────────────────────
# Настройки по умолчанию /default_settings.py
# ──────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger("crypto_orderflow_service")


def ensure_audit_chain_fields(signal: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure a stable signal/execution chain contract before publish (P5).

    The execution journal needs explicit IDs to join signal production with the
    execution lifecycle and the closed-trade analytics layer.  Upstream signal
    producers may only provide `decision_id`, so we materialize stable
    `signal_id` and `execution_plan_id` here before the payload leaves the
    publisher boundary.
    """
    if not isinstance(signal, dict):
        return signal
    decision_id = str(signal.get('decision_id') or signal.get('id') or '').strip()
    signal_id = str(signal.get('signal_id') or decision_id or signal.get('sid') or '').strip()
    execution_plan_id = str(signal.get('execution_plan_id') or decision_id or signal_id or '').strip()
    if signal_id:
        signal['signal_id'] = signal_id
    if execution_plan_id:
        signal['execution_plan_id'] = execution_plan_id
    if decision_id:
        signal['decision_id'] = decision_id
    signal.setdefault('audit_chain_ver', 'p5_execution_audit_v1')
    return signal



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





def _utc_epoch_ms() -> int:
    """Canonical wall-clock timestamp for emitted payloads (epoch ms, UTC)."""
    return int(time.time() * 1000)


def _mono_ms() -> int:
    """Monotonic timestamp used only for local latency deltas."""
    return int(_time.monotonic() * 1000)


def _safe_latency_delta_ms(start_mono_ms: int, end_mono_ms: int) -> int:
    try:
        return max(0, int(end_mono_ms) - int(start_mono_ms))
    except Exception:
        return 0


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

        # Lifecycle control — asyncio.Event must be created inside a running loop.
        # Initialized lazily in run_forever() to allow __init__ to be called from a plain Thread.
        self._stop_event: Optional[asyncio.Event] = None
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
        # Each symbol uses 2 blocking xreadgroup calls (ticks + books) that hold connections
        # Formula: min_connections = (symbols_count * 2) + overhead (config, publish, etc)
        # Defaults increased to handle 15+ symbols (15*2=30 + overhead ~50-100 = safe margin at 512/1024)
        # Increased further to handle Redis LOADING state where connections may be held longer
        self.main_max = int(os.getenv("REDIS_MAIN_MAX_CONNECTIONS", "1024"))
        self.ticks_max = int(os.getenv("REDIS_TICKS_MAX_CONNECTIONS", "2048"))
        notify_max = int(os.getenv("REDIS_NOTIFY_MAX_CONNECTIONS", "64"))
        conn_to = float(os.getenv("REDIS_SOCKET_CONNECT_TIMEOUT", "5"))
        sock_to = float(os.getenv("REDIS_SOCKET_TIMEOUT", "15"))
        hc_iv = int(os.getenv("REDIS_HEALTHCHECK_INTERVAL", "30"))

        self.main: aioredis.Redis = aioredis.from_url(
            self.redis_dsn,
            decode_responses=True,
            socket_connect_timeout=conn_to,
            socket_timeout=sock_to,
            socket_keepalive=True,
            health_check_interval=0,  # ✅ Disable to prevent additional connections
            max_connections=self.main_max,
        )

        # Если стримы на одном Redis, используем один пул для экономии соединений
        if str(self.ticks_dsn) == str(self.redis_dsn):
            self.ticks = self.main
            logger.info(
                "🔗 Using shared Redis client for main and ticks (max_conn=%d). "
                "Each symbol uses ~2 blocking connections (ticks+books), plan accordingly.",
                self.main_max
            )
        else:
            self.ticks: aioredis.Redis = aioredis.from_url(
                self.ticks_dsn,
                decode_responses=True,
                socket_connect_timeout=conn_to,
                socket_timeout=sock_to,
                socket_keepalive=True,
                health_check_interval=0,  # ✅ Disable to prevent additional connections
                max_connections=self.ticks_max,
            )
            logger.info(
                "🔗 Using separate Redis clients: main (max=%d), ticks (max=%d). "
                "Each symbol uses ~2 blocking connections (ticks+books), plan accordingly.",
                self.main_max, self.ticks_max
            )
        
        # ✅ WARNING: Validate connection pool size
        # Each symbol needs 2 connections (ticks + books), plus overhead for config, publish, etc.
        # Estimate max symbols: (ticks_max - overhead) / 2
        overhead = 50  # Config, publish, metrics, etc.
        max_supported_symbols = max(0, (self.ticks_max - overhead) // 2)
        if max_supported_symbols < 10:
            logger.warning(
                "⚠️ Connection pool may be too small: ticks_max=%d supports ~%d symbols. "
                "Consider increasing REDIS_TICKS_MAX_CONNECTIONS if you plan to run more symbols.",
                self.ticks_max, max_supported_symbols
            )
        else:
            logger.info(
                "✅ Connection pool configured: ticks_max=%d supports ~%d symbols (with %d overhead). "
                "Health check disabled to prevent additional connections.",
                self.ticks_max, max_supported_symbols, overhead
            )

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

        self.health_metrics = HealthMetrics(redis_url=self.redis_dsn, window_sec=5)

        # Production safety overlays (P3/P4): Redis DQ veto + portfolio-aware risk.
        # Rollout flags for P3/P4 gates. These allow operators to disable the
        # newer veto layers quickly without editing code during incident
        # mitigation or staged rollout.
        self.trade_dq_hard_veto_enable = str(os.getenv("TRADE_DQ_HARD_VETO_ENABLE", "1")).strip().lower() in {"1", "true", "yes", "on"}
        self.trade_risk_engine_v2_enable = str(os.getenv("TRADE_RISK_ENGINE_V2_ENABLE", "1")).strip().lower() in {"1", "true", "yes", "on"}
        self.redis_dq_thresholds = RedisDQThresholds.from_env() if (RedisDQThresholds and self.trade_dq_hard_veto_enable) else None
        self.portfolio_risk_limits = PortfolioRiskLimits.from_env() if (PortfolioRiskLimits and self.trade_risk_engine_v2_enable) else None
        # Set PORTFOLIO_RISK_HARD_VETO=0 to run risk engine in observe-only mode
        self.portfolio_risk_hard_veto = str(os.getenv("PORTFOLIO_RISK_HARD_VETO", "1")).strip().lower() in {"1", "true", "yes", "on"}
        # P4.5: SQL audit sink for risk decisions (fail-open)
        self.trade_risk_sql_audit_enable = str(os.getenv("TRADE_RISK_SQL_AUDIT_ENABLE", "1")).strip().lower() in {"1", "true", "yes", "on"}
        self.risk_audit_sql_sink = RiskAuditSqlSink.from_env() if (RiskAuditSqlSink and self.trade_risk_sql_audit_enable) else None
        self.exec_quarantine_denylist_enable = str(os.getenv("EXEC_QUARANTINE_DENYLIST_ENABLE", "1")).strip().lower() in {"1", "true", "yes", "on"}
        self.orders_quarantine_sids_key = str(os.getenv("ORDERS_QUARANTINE_SIDS_KEY", "orders:quarantine:state:sids")).strip() or "orders:quarantine:state:sids"
        self.quarantine_denylist_cache_ms = int(os.getenv("QUARANTINE_DENYLIST_CACHE_MS", "1000") or 1000)
        self._quarantine_sid_cache: Set[str] = set()
        self._quarantine_sid_cache_ts_ms: int = 0

        # --- Local caches for snapshot publisher (avoid Redis GET per tick) ---
        # regime:q:{symbol}:1m is slow-changing -> cache 60s
        self._rq_cache: Dict[str, Tuple[int, Dict[str, Any]]] = {}
        # adx:{symbol} is fast-changing -> cache 300ms
        self._adx_cache: Dict[str, Tuple[int, float]] = {}

        self.symbol_contexts: Dict[str, SymbolRuntime] = {}
        self.symbol_tasks: Dict[str, Tuple[asyncio.Task, asyncio.Task]] = {}
        self.refresh_interval = _safe_int(os.getenv("CRYPTO_OF_REFRESH_SEC", "30"), 30)
        
        # Lag trackers per symbol for worker_lag_ms percentiles
        self._lag_trackers: Dict[str, LagTracker] = {}
        # Counters for periodic percentile export (per symbol)
        self._lag_export_counters: Dict[str, int] = {}

        rnd = random.randint(1000, 9999)
        self.consumer_id_ticks = f"crypto-of-ticks-{os.getpid()}-{rnd}"
        self.consumer_id_books = f"crypto-of-books-{os.getpid()}-{rnd}"
        
        # Helper caches for Redis Stream consumption (Expert P4/P5)
        self.tick_helpers: Dict[str, Any] = {}
        self.book_helpers: Dict[str, Any] = {}
        
        # Quarantine for persistent message failures
        self.poison_pill_counts: Dict[str, int] = {}
        self.quarantine_stream = os.getenv("SIGNAL_QUARANTINE_STREAM", "stream:of:quarantine")

        # Unknown-side tick policy (prevents implicit BUY/SELL bias)
        self._unknown_side_policy = normalize_unknown_side_policy(os.getenv("CRYPTO_OF_UNKNOWN_SIDE_POLICY"))
        self._unknown_side_quarantine_stream = os.getenv("TICK_SIDE_QUARANTINE_STREAM", "stream:tick_side:quarantine")
        self._unknown_side_quarantine_sample = float(os.getenv("TICK_SIDE_QUARANTINE_SAMPLE", "0.01"))
        self._unknown_side_quarantine_maxlen = int(os.getenv("TICK_SIDE_QUARANTINE_MAXLEN", "20000"))

        self.notify_stream = os.getenv("CRYPTO_NOTIFY_STREAM", "notify:telegram")
        self.raw_signal_stream = os.getenv("CRYPTO_RAW_STREAM", "signals:crypto:raw")
        # MT5 orders queue. Binance uses orders:queue:binance (binance_executor).
        self.orders_queue = os.getenv("ORDERS_QUEUE_MT5") or os.getenv("ORDERS_QUEUE") or "orders:queue:mt5"
        # 🎯 Stream для structured signals (для periodic_reporter и других downstream сервисов)
        # Tickers & Streams
        self.cryptoorderflow_signal_stream_template = os.getenv("CRYPTO_ORDERFLOW_SIGNAL_STREAM", "signals:cryptoorderflow:{symbol}")
        # Burst audit stream (optional)
        self.burst_audit_stream = os.getenv("BURST_AUDIT_STREAM", "stream:of:burst_audit")
        
        # Engines
        from services.ml_confirm_gate import MLConfirmGate
        self.of_engine = OFConfirmEngine(
            version=int(os.getenv("OF_CONFIRM_VERSION", "2")),
            ml_gate=MLConfirmGate.from_env()
        )
        
        self.config_loader = OrderFlowConfigLoader(redis_client=None) # updated in run_forever

        notify_url = os.getenv("CRYPTO_NOTIFY_REDIS_URL", os.getenv("REDIS_URL"))
        if notify_url:
            # ✅ FIX: Set max_connections to prevent connection pool exhaustion
            self.notify_client = aioredis.from_url(
                notify_url,
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=conn_to,
                socket_timeout=sock_to,
                socket_keepalive=True,
                health_check_interval=0,  # ✅ Disable to prevent additional connections
                max_connections=notify_max,
            )
            logger.info("🔗 Using separate notify Redis client (max_conn=%d)", notify_max)
        else:
            self.notify_client = self.main
            logger.info("🔗 Using main Redis client for notifications")

        self._refresh_task: Optional[asyncio.Task] = None
        self._ml_gate_bg_task: Optional[asyncio.Task] = None
        self._burst_task: Optional[asyncio.Task] = None
        self._supervisor_task: Optional[asyncio.Task] = None
        # (symbol, kind) -> deque[timestamps] for restart storm protection
        self._task_restart_hist: Dict[Tuple[str, str], deque] = {}
        self._shutdown = False
        
        # Global bounded concurrency (do NOT create Semaphore per symbol)
        self._bootstrap_sem = asyncio.Semaphore(int(os.getenv("CRYPTO_OF_BOOTSTRAP_MAX_CONC", "10")))

        # Throughput / determinism controls
        self._ack_batch = int(os.getenv("CRYPTO_OF_ACK_BATCH", "200"))
        self._max_lag_ms = int(os.getenv("CRYPTO_OF_MAX_LAG_MS", "500"))
        # Safe default: OFF (enable explicitly once you are confident)
        self._drop_on_lag = self._env_bool("CRYPTO_OF_DROP_ON_LAG", "false")
        # If tick.ts_ms looks poisoned (too far from wall), fall back to Redis msg_id ms
        self._max_ts_skew_ms = int(os.getenv("CRYPTO_OF_MAX_TS_SKEW_MS", str(6 * 3600_000)))

        # PEL sweep state (optional)
        self._pel_cursor: Dict[Tuple[str, str], str] = {}
        self._pel_sweeper_task: Optional[asyncio.Task] = None

        # Глобальный флаг трейлинга после TP1: по умолчанию ВЫКЛ, включаем только если явно задан env=true
        self.force_trail_after_tp1: Optional[bool] = self._env_bool("FORCE_TRAIL_AFTER_TP1")

        logger.info("✅ CryptoOrderflowService инициализирован")
        logger.info("   Main Redis:  %s", self.redis_dsn)
        logger.info("   Ticks Redis: %s", self.ticks_dsn)
        logger.info("   Telegram stream: %s (Redis: %s)", self.notify_stream, notify_url or "main")
        logger.info("   Telegram every_n: %s", os.getenv("CRYPTO_NOTIFY_SIGNAL_EVERY_N", os.getenv("NOTIFY_SIGNAL_EVERY_N", "1")))
        logger.info("   Signal min confidence: %s%%", os.getenv("CRYPTO_SIGNAL_MIN_CONF", os.getenv("SIGNAL_MIN_CONF", "80")))

    def _env_bool(self, name: str, default: Optional[str] = None) -> bool:
        val = os.getenv(name, default)
        if not val:
            return False
        return str(val).lower() in ("1", "true", "yes", "on")

    async def run_forever(self) -> None:
        """
        Основной цикл сервиса. Останавливается по сигналу отмены.
        """
        # Connect publisher and start retry worker (must be inside event loop)
        self._stop_event = asyncio.Event()  # lazy init inside running event loop
        self.publisher.r = self.main
        self.publisher.start()
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

        # Start metrics server — respects PROMETHEUS_PORT env var (fallback: METRICS_PORT, then 8000)
        try:
            port = int(os.getenv("PROMETHEUS_PORT") or os.getenv("METRICS_PORT") or "8000")
            start_http_server(port)
            logger.info("✅ Metrics server started on port %d", port)
        except Exception as e:
            logger.error("❌ Failed to start metrics server on port %d: %s", port, e)

        # Initial ML config/model load (async) before blocking fast-path
        # Must run BEFORE load_dynamic_symbols so that backlog ticks don't hit ERR_NO_CFG
        if self.of_engine and getattr(self.of_engine, "ml_gate", None) and hasattr(self.of_engine.ml_gate, "refresh_async"):
            # Retry loop for resilience against DB/Redis startup race conditions
            for _ in range(5):
                try:
                    await self.of_engine.ml_gate.refresh_async(self.main)
                    if getattr(self.of_engine.ml_gate, "_cfg", None):
                        logger.info("✅ ML_CONFIRM_GATE successfully loaded config on startup")
                        break
                    logger.warning("ML_CONFIRM_GATE config not found yet (raw_payload possibly None), retrying in 2s...")
                    await asyncio.sleep(2.0)
                except Exception as e:
                    logger.error(f"Failed to async refresh ml_confirm_gate on startup: {e}")
                    await asyncio.sleep(2.0)

        if hasattr(self, 'health_metrics') and self.health_metrics:
            self.health_metrics.start_background_loop()

        await self.load_dynamic_symbols()
        self._refresh_task = safe_create_task(self._refresh_loop(), name="crypto-of-refresh")
        self._ml_gate_bg_task = safe_create_task(self._maintain_ml_gate_loop(), name="crypto-of-ml-gate-bg")
        self._burst_task = safe_create_task(self._burst_flush_loop(), name="crypto-of-burst-flush")
        if self._env_bool("CRYPTO_OF_SUPERVISOR", "true"):
            self._supervisor_task = safe_create_task(self._supervisor_loop(), name="crypto-of-supervisor")

        if self._env_bool("CRYPTO_OF_PEL_SWEEP", "false"):
            self._pel_sweeper_task = safe_create_task(self._pel_sweeper_loop(), name="crypto-of-pel-sweep")

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

        if getattr(self, "_pel_sweeper_task", None):
            self._pel_sweeper_task.cancel()
            await asyncio.gather(self._pel_sweeper_task, return_exceptions=True)

        if self._refresh_task:
            self._refresh_task.cancel()
            await asyncio.gather(self._refresh_task, return_exceptions=True)

        if getattr(self, "_ml_gate_bg_task", None):
            self._ml_gate_bg_task.cancel()
            await asyncio.gather(self._ml_gate_bg_task, return_exceptions=True)

        if hasattr(self, "_burst_task") and self._burst_task:
            self._burst_task.cancel()
            await asyncio.gather(self._burst_task, return_exceptions=True)
        
        # ✅ P0: Stop PEL sweeper
        if self._pel_sweeper_task:
            self._pel_sweeper_task.cancel()
            await asyncio.gather(self._pel_sweeper_task, return_exceptions=True)

        if hasattr(self, 'health_metrics') and self.health_metrics:
            self.health_metrics.stop()

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

        # ✅ FIX: Close notify_client if it's separate from main
        if hasattr(self, 'notify_client') and self.notify_client is not None:
            if self.notify_client is not self.main:
                await self._close_redis(self.notify_client)
            self.notify_client = None

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
            
        use_default_symbols = self._env_bool("CRYPTO_DEFAULT_SYMBOLS_ENABLED", "true")
        symbols = set(sym.upper() for sym in DEFAULT_SYMBOLS) if use_default_symbols else set()
        
        symbols_key = os.getenv("CRYPTO_SYMBOLS_SET_KEY", "crypto:symbols")
        try:
            redis_symbols = await self.main.smembers(symbols_key)
            symbols.update(sym.upper() for sym in redis_symbols)
        except RedisError as exc:
            log_silent_error(exc, 'redis_read_failure', 'global', 'load_dynamic_symbols:smembers')

        # Обновляем/создаём контексты
        current_symbols = set(self.symbol_contexts.keys())
        
        # Log connection pool usage estimate
        symbols_count = len(symbols)
        estimated_connections = symbols_count * 2  # ticks + books per symbol
        if symbols_count > 0 and estimated_connections > (self.ticks_max * 0.8):
            logger.warning(
                "⚠️ High connection pool usage estimate: %d symbols will use ~%d connections "
                "(ticks_max=%d). Consider increasing REDIS_TICKS_MAX_CONNECTIONS if you see 'Too many connections' errors.",
                symbols_count, estimated_connections, self.ticks_max
            )
        elif symbols_count > 0:
            logger.debug(
                "ℹ️ Connection pool estimate: %d symbols will use ~%d connections (ticks_max=%d, utilization=%.1f%%)",
                symbols_count, estimated_connections, self.ticks_max, (estimated_connections / self.ticks_max * 100) if self.ticks_max > 0 else 0
            )

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
                # Initialize lag tracker for percentiles
                self._lag_trackers[symbol] = LagTracker(
                    window=2048,
                    export_every_n=200,
                    metric_p50="worker_lag_ms_p50",
                    metric_p95="worker_lag_ms_p95",
                    metric_p99="worker_lag_ms_p99",
                    tags={"symbol": symbol}
                )
                self._lag_export_counters[symbol] = 0
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
                # ✅ P1: EAGER BOOTSTRAP: Load calibrations BEFORE starting tasks
                # Используем общий семафор на сервис вместо создания нового на каждый символ
                try:
                    async def bootstrap_task():
                        async with self._bootstrap_sem:
                            try:
                                await asyncio.wait_for(self.calib_svc.ensure_loaded(runtime), timeout=2.0)
                            except Exception as exc:
                                log_silent_error(exc, 'bootstrap_timeout', symbol, 'load_dynamic_symbols:bootstrap')
                            runtime.ready = True
                    
                    safe_create_task(bootstrap_task())
                except Exception:
                    runtime.ready = True # fail-open

                tick_task = safe_create_task(self.consume_ticks(symbol), name=f"crypto-of-ticks-{symbol}")
                book_task = safe_create_task(self.consume_books(symbol), name=f"crypto-of-book-{symbol}")
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
                    float(os.getenv("CRYPTO_SIGNAL_MIN_CONF", os.getenv("SIGNAL_MIN_CONF", "70"))),
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
            while not self._shutdown:
                await asyncio.sleep(self.refresh_interval)
                await self.load_dynamic_symbols()
        except asyncio.CancelledError:
            logger.debug("Refresh loop cancelled")

    async def _maintain_ml_gate_loop(self) -> None:
        """
        Periodically refresh MLConfirmGate configuration asynchronously.
        This prevents blocking Redis calls on the main loop during tick processing.
        """
        # Default TTL is 60s, so refresh every 30s to constitute a cache hit
        interval = 30.0
        logger.info("Starting ML Gate background refresh loop (interval=%.1fs)", interval)
        
        while not self._shutdown:
            try:
                gate = getattr(self.of_engine, "ml_gate", None)
                # Fast retry interval if config has not been successfully loaded
                cur_interval = 5.0 if gate and not getattr(gate, "_cfg", None) else interval
                
                # Wait first (gate is lazy-loaded, so might be None initially)
                await asyncio.sleep(cur_interval)
                
                if self._shutdown:
                     break
                
                # Access exposed property from OFConfirmEngine
                # Note: of_engine might not have ml_gate initialized if no build() calls happened yet,
                # or if ML_GATE is disabled.
                if gate and hasattr(gate, "refresh_async"):
                    t0 = time.time()
                    # Use self.main (AsyncRedis)
                    await gate.refresh_async(self.main)
                    dt = time.time() - t0
                    if dt > 0.5:
                        logger.warning("⚠️ ML gate async refresh took %.1fms", dt * 1000)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in ML gate refresh loop: %s", e)
                # Don't crash loop, just sleep and retry
                await asyncio.sleep(5.0)

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

        new_task = safe_create_task(coro_factory(), name=task_name)
        return new_task


    async def _stop_symbol(self, symbol: str) -> None:
        """
        Останавливает обработку конкретного символа.
        """
        # Mark symbol as unloaded first to prevent supervisor restarts during cancellation
        self.symbol_contexts.pop(symbol, None)

        # Cleanup caches to prevent leaks for unloaded symbols
        sym = str(symbol or "").upper()
        self.tick_helpers.pop(sym, None)
        self.book_helpers.pop(sym, None)
        self.poison_pill_counts.pop(sym, None)
        self._rq_cache.pop(sym, None)
        self._adx_cache.pop(sym, None)
        for kind in ("ticks", "books"):
            self._task_restart_hist.pop((sym, kind), None)
            self._pel_cursor.pop((sym, kind), None)

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

                        # NEW (P4): Periodic maintenance (overrides, etc.) moved from tick-path
                        if self.strategy:
                            await self.strategy.maintain_symbol(runtime)

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

                if self.strategy and await self._pre_publish_allows_signal(runtime, out):
                    await self.strategy.publish_signal(runtime, out)
                    if signals_published_total:
                        signals_published_total.labels(symbol=runtime.symbol).inc()

            return out
        return None

    def _build_redis_dq_snapshot(self, runtime: Any, *, now_ms: int) -> Optional["RedisDQSnapshot"]:
        """Build a RedisDQSnapshot from the runtime's current health counters.

        All attributes are read via getattr with safe defaults — this method
        never raises; it returns None if the DQ module is unavailable.
        """
        if RedisDQSnapshot is None:
            return None
        last_tick_ms = int(getattr(runtime, "last_tick_ts", 0) or 0)
        last_book_ms = int(getattr(runtime, "last_book_ts", 0) or 0)
        # Staleness = wall-clock minus last observed update
        queue_lag_ms = max(0, now_ms - last_tick_ms) if last_tick_ms > 0 else 0
        tick_staleness_ms = max(0, now_ms - last_tick_ms) if last_tick_ms > 0 else 0
        book_staleness_ms = max(0, now_ms - last_book_ms) if last_book_ms > 0 else 0
        outbox_backlog = 0
        try:
            # Read publisher internal queue depth if available
            pending = getattr(self.publisher, "_q", None)
            if pending is not None and hasattr(pending, "qsize"):
                outbox_backlog = int(pending.qsize())
        except Exception:
            outbox_backlog = 0
        return RedisDQSnapshot(
            symbol=str(getattr(runtime, "symbol", "") or "UNKNOWN"),
            queue_lag_ms=queue_lag_ms,
            tick_staleness_ms=tick_staleness_ms,
            book_staleness_ms=book_staleness_ms,
            redis_timeout_events=int(getattr(runtime, "redis_timeout_events", 0) or 0),
            negative_age_events=int(getattr(runtime, "negative_age_events", 0) or 0),
            xack_fail_events=int(getattr(runtime, "xack_fail_events", 0) or 0),
            outbox_backlog=outbox_backlog,
            stream_timeout_burst=int(getattr(runtime, "stream_timeout_burst", 0) or 0),
            force_hard_veto=bool(getattr(runtime, "force_hard_veto", False)),
        )

    def _build_portfolio_risk_input(self, runtime: Any, signal: Dict[str, Any]) -> Optional["PortfolioRiskInput"]:
        """Build portfolio risk input from signal payload and runtime state.

        Positions come from the signal's `portfolio_positions` field (list of dicts).
        Falls back to ENV ACCOUNT_DEPOSIT_USD for equity when not in signal.

        P4 additions:
          - stop_distance_bps  extracted from stop_distance_bps / sl_bps / stop_bps
          - volatility_bps     extracted from volatility_bps / atr_bps / realized_vol_bps
          - confidence         extracted from confidence / signal_confidence
          - maker_policy_requested  extracted from maker_policy_requested / prefer_maker / execution_policy
          - kill_switch        extracted from risk_kill_switch / kill_switch
          - tier               auto-inferred via infer_symbol_tier() if not in signal
        """
        if PortfolioRiskInput is None:
            return None
        positions_raw = signal.get("portfolio_positions") or []
        positions = []
        for p in positions_raw:
            if not isinstance(p, dict):
                continue
            try:
                positions.append(PortfolioPosition(
                    symbol=str(p.get("symbol") or ""),
                    notional_usd=float(p.get("notional_usd") or 0.0),
                    side=str(p.get("side") or "LONG"),
                    cluster=str(p.get("cluster") or "default"),
                    tier=str(p.get("tier") or "B"),
                ))
            except Exception:
                continue
        symbol = str(signal.get("symbol") or getattr(runtime, "symbol", "") or "")
        requested_notional = float(signal.get("planned_notional_usd") or signal.get("notional_usd") or 0.0)
        # Resolve tier: prefer explicit signal field, fall back to infer_symbol_tier (P4)
        tier = str(signal.get("symbol_tier") or signal.get("tier") or "").strip().upper()
        if (not tier or tier not in {"A", "B", "C"}) and infer_symbol_tier is not None:
            tier = infer_symbol_tier(symbol, self.portfolio_risk_limits)
        # P4: per-trade stop / volatility for risk sizing
        stop_distance_bps = float(
            signal.get("stop_distance_bps")
            or signal.get("planned_stop_distance_bps")
            or signal.get("sl_bps")
            or signal.get("stop_bps")
            or 0.0
        )
        volatility_bps = float(
            signal.get("volatility_bps")
            or signal.get("atr_bps")
            or signal.get("realized_vol_bps")
            or 0.0
        )
        confidence = float(signal.get("confidence") or signal.get("signal_confidence") or 0.0)
        maker_requested = bool(
            signal.get("maker_policy_requested")
            or signal.get("prefer_maker")
            or str(signal.get("execution_policy") or "").strip().upper() == "MAKER_FIRST"
        )
        return PortfolioRiskInput(
            symbol=symbol,
            cluster=str(signal.get("risk_cluster") or signal.get("cluster") or signal.get("symbol") or getattr(runtime, "symbol", "")),
            tier=tier or "B",
            requested_notional_usd=requested_notional,
            current_positions=positions,
            equity_usd=float(signal.get("account_equity_usd") or os.getenv("ACCOUNT_DEPOSIT_USD", "0") or 0.0),
            daily_pnl_pct=float(signal.get("daily_pnl_pct") or 0.0),
            stop_distance_bps=stop_distance_bps,
            volatility_bps=volatility_bps,
            spread_bps=float(signal.get("spread_bps") or 0.0),
            expected_slippage_bps=float(signal.get("expected_slippage_bps") or signal.get("slippage_bps") or 0.0),
            confidence=confidence,
            maker_policy_requested=maker_requested,
            infra_degraded=bool(signal.get("infra_degraded") or signal.get("dq_hard_veto")),
            high_vol=bool(signal.get("high_vol") or signal.get("regime_high_vol")),
            kill_switch=bool(signal.get("risk_kill_switch") or signal.get("kill_switch")),
        )


    async def _refresh_quarantine_sid_cache(self, *, now_ms: int) -> None:
        if not self.exec_quarantine_denylist_enable or not self.orders_quarantine_sids_key:
            return
        if (now_ms - int(self._quarantine_sid_cache_ts_ms or 0)) < int(self.quarantine_denylist_cache_ms or 0):
            return
        try:
            values = await self.main.smembers(self.orders_quarantine_sids_key)
            self._quarantine_sid_cache = {str(v) for v in (values or set()) if str(v)}
            self._quarantine_sid_cache_ts_ms = int(now_ms)
        except Exception:
            return

    def _persist_risk_decision_audit(self, *, signal: Dict[str, Any], risk_input: Any, risk_decision: Any) -> None:
        """Best-effort SQL mirror for risk decisions (P4.5).

        The publish path must remain available when SQL is unavailable, so every
        exception is swallowed after being logged once. Call after evaluate_portfolio_risk.
        """
        if not self.risk_audit_sql_sink:
            return
        try:
            import uuid
            decision_id = str(signal.get('decision_id') or signal.get('id') or uuid.uuid4().hex)
            signal['decision_id'] = decision_id
            self.risk_audit_sql_sink.record_decision(
                decision_id=decision_id,
                signal=signal,
                risk_input=risk_input,
                risk_decision=risk_decision,
            )
        except Exception as exc:
            logger.warning(
                "⚠️ (%s) Risk SQL audit write failed: %s",
                str(signal.get('symbol') or '?'), exc,
            )

    async def _pre_publish_allows_signal(self, runtime: Any, signal: Dict[str, Any]) -> bool:
        """Gate: returns True if the signal may be published, False if vetoed.

        Performs two checks in order:
          1. Redis DQ snapshot (hard veto blocks publish unconditionally)
          2. Portfolio risk (deny/force-flatten blocks publish if hard_veto mode is on)

        Side effects on signal dict:
          - Adds dq_snapshot, dq_level, dq_hard_veto keys (if DQ enabled)
          - Adds risk_snapshot, risk_level, risk_leverage_cap keys (if risk enabled)
          - Adds risk_decision_latency_ms, risk_clamp_ratio (P4.5)
          - May reduce planned_notional_usd if risk engine tightens the position

        Always fail-open: any unexpected exception returns True.
        """
        now_ms = _utc_epoch_ms()
        # P5: materialize audit chain IDs before publish so downstream systems can join
        signal = ensure_audit_chain_fields(signal)
        # P4.5: stamp timestamps early so audit sink always has them
        signal.setdefault('ts_event_ms', int(signal.get('ts_event_ms') or now_ms))
        signal['ts_publish_ms'] = int(now_ms)
        if self.exec_quarantine_denylist_enable and check_signal_against_quarantine_cache is not None:
            await self._refresh_quarantine_sid_cache(now_ms=now_ms)
            deny_decision = check_signal_against_quarantine_cache(signal, self._quarantine_sid_cache)
            signal['quarantine_snapshot'] = deny_decision.to_dict()
            if not deny_decision.allowed:
                signal['quarantine_denylist_hit'] = True
                signal['quarantine_sid'] = str(deny_decision.matched_sid)
                logger.warning(
                    "\U0001f6ab (%s) Quarantine denylist veto before publish: sid=%s candidates=%s",
                    getattr(runtime, 'symbol', '?'), deny_decision.matched_sid, deny_decision.candidates,
                )
                return False
        dq_decision = None
        if self.trade_dq_hard_veto_enable and self.redis_dq_thresholds and evaluate_redis_dq is not None:
            snap = self._build_redis_dq_snapshot(runtime, now_ms=now_ms)
            if snap is not None:
                dq_decision = evaluate_redis_dq(snap, self.redis_dq_thresholds)
                signal["dq_snapshot"] = dq_decision.to_dict()
                signal["dq_level"] = int(dq_decision.level)
                signal["dq_hard_veto"] = not bool(dq_decision.allow_trade_publish)
                if not dq_decision.allow_trade_publish:
                    logger.warning(
                        "\U0001f6ab (%s) Hard DQ veto before publish: reasons=%s snapshot=%s",
                        getattr(runtime, "symbol", "?"), dq_decision.reasons, dq_decision.snapshot,
                    )
                    return False
        # ── Portfolio risk check (P4: full tier-aware engine) ─────────────────
        if self.portfolio_risk_limits and evaluate_portfolio_risk is not None:
            risk_input = self._build_portfolio_risk_input(runtime, signal)
            if risk_input is not None:
                risk_decision = evaluate_portfolio_risk(risk_input, self.portfolio_risk_limits)
                signal["risk_snapshot"] = risk_decision.to_dict()
                signal["risk_level"] = str(risk_decision.level)
                signal["risk_leverage_cap"] = float(risk_decision.leverage_cap)
                # P4: propagate execution hints (tier policy) into the published signal
                signal["risk_tier"] = str(risk_decision.tier_policy.name)
                signal["risk_min_confidence_required"] = float(risk_decision.min_confidence_required)
                signal["risk_watchdog_timeout_ms"] = int(risk_decision.watchdog_timeout_ms)
                signal["risk_maker_policy_allowed"] = bool(risk_decision.maker_policy_allowed)
                signal["symbol_tier"] = str(risk_decision.tier_policy.name)
                signal["execution_policy"] = str(risk_decision.effective_execution_policy)
                # P4.5: propagate latency and clamp_ratio into the published signal for audit
                signal["risk_decision_latency_ms"] = float((risk_decision.snapshot or {}).get('decision_latency_ms') or 0.0)
                signal["risk_clamp_ratio"] = float((risk_decision.snapshot or {}).get('clamp_ratio') or 0.0)
                # P4.5: persist risk decision to SQL audit trail (fail-open)
                self._persist_risk_decision_audit(signal=signal, risk_input=risk_input, risk_decision=risk_decision)
                # Block publish if denied and hard veto is armed
                if risk_decision.level in {RISK_DENY_SOFT, RISK_DENY_HARD, RISK_FORCE_FLATTEN} and self.portfolio_risk_hard_veto:
                    logger.warning(
                        "\U0001f6ab (%s) Portfolio risk veto before publish: level=%s reasons=%s snapshot=%s",
                        getattr(runtime, "symbol", "?"), risk_decision.level, risk_decision.reasons, risk_decision.snapshot,
                    )
                    return False
                # P4: adjust notional to risk-engine output (shrink, not just deny)
                if risk_decision.allow_trade_publish and risk_decision.adjusted_notional_usd > 0:
                    signal["planned_notional_usd"] = float(risk_decision.adjusted_notional_usd)
        return True

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
        
        while not self._shutdown:
            runtime = self.symbol_contexts.get(symbol)
            if runtime is None:
                logger.warning("⚠️ (%s) Runtime не найден, ожидание...", symbol)
                await asyncio.sleep(1)
                continue
            
            if runtime.loop_log_sampler.should_log("loop_start"):
                logger.debug("🔄 (%s) Loop iteration start", symbol)
            
            # Initialize helper on first iteration or if runtime changed (Expert P4/P5)
            stream = runtime.tick_stream
            group = runtime.tick_group
            helper = self.tick_helpers.get(symbol)
            if helper is None:
                helper = AsyncRedisStreamHelper(self.ticks, group, self.consumer_id_ticks)
                self.tick_helpers[symbol] = helper
                try:
                    await helper.ensure_group(stream)
                    sampled_info(logger, "TICK_HELPER_INIT", "✅ (%s) tick-helper initialized: stream=%s group=%s", symbol, stream, group)
                except RedisError as exc:
                    delay = backoff.get_delay()
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

            tick_sample_rate = float(os.getenv("TICK_SAMPLE_RATE", "1.0"))

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
            except (ConnectionError, TimeoutError) as exc:
                # ✅ FIX: Handle connection pool exhaustion and timeouts
                error_str = str(exc)
                is_pool_exhausted = "Too many connections" in error_str or "Name or service not known" in error_str
                is_timeout = isinstance(exc, TimeoutError) or "Timeout" in error_str
                
                if is_pool_exhausted:
                    if redis_errors_total:
                        redis_errors_total.labels(op="read_ticks_pool_exhausted", symbol=symbol).inc()
                    # More aggressive backoff for pool exhaustion (min 3s, max 20s)
                    delay = max(3.0, min(backoff.get_delay() * 2.5, 20.0))
                    active_count = len(self.active_symbols)
                    # Calculate estimated connections needed
                    estimated_connections = active_count * 2  # ticks + books per symbol
                    # Calculate pool usage
                    pool_usage_pct = (estimated_connections / self.ticks_max * 100) if self.ticks_max > 0 else 0
                    logger.error(
                        "❌ (%s) Redis connection pool exhausted (main_max=%d, ticks_max=%d, active_symbols=%d, "
                        "estimated_connections=%d, pool_usage=%.1f%%). Error: %s. Retrying in %.2fs. "
                        "Consider increasing REDIS_TICKS_MAX_CONNECTIONS or reducing symbol count.",
                        symbol, self.main_max, self.ticks_max, active_count, estimated_connections, pool_usage_pct, error_str, delay
                    )
                    # Longer delay to allow connections to be released
                    await asyncio.sleep(delay)
                    # Reset backoff after pool exhaustion to avoid exponential growth
                    backoff.reset()
                    # Try to release any stale connections
                    try:
                        if hasattr(self.ticks, 'connection_pool'):
                            pool = self.ticks.connection_pool
                            if hasattr(pool, 'reset'):
                                pool.reset()
                    except Exception:
                        pass
                    continue
                elif is_timeout:
                    if redis_errors_total:
                        redis_errors_total.labels(op="read_ticks_timeout", symbol=symbol).inc()
                    delay = backoff.get_delay()
                    logger.warning("⚠️ (%s) Redis timeout reading ticks: %s (backoff=%.2fs)", symbol, error_str, delay)
                    await asyncio.sleep(delay)
                    continue
                else:
                    if redis_errors_total:
                        redis_errors_total.labels(op="read_ticks_connection", symbol=symbol).inc()
                    if is_transient_redis_error(exc):
                        delay = backoff.get_delay()
                        logger.warning("⚠️ (%s) Transient connection error: %s (backoff=%.2fs)", symbol, exc, delay)
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error("❌ (%s) Connection error: %s", symbol, exc)
                        delay = backoff.get_delay()
                        await asyncio.sleep(delay)
                        continue
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
                        delay = backoff.get_delay()
                        logger.error("❌ (%s) Не удалось пересоздать группу: %s", symbol, e)
                        await asyncio.sleep(delay)
                        continue
                else:
                    if is_transient_redis_error(exc):
                        delay = backoff.get_delay()
                        logger.warning("⚠️ (%s) Transient ошибка чтения стрима %s: %s (backoff=%.2fs)", symbol, stream, exc, delay)
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error("❌ (%s) Ошибка чтения стрима %s: %s", symbol, stream, exc)
                        delay = backoff.get_delay()
                        await asyncio.sleep(delay)
                        continue
            except Exception as exc:
                if redis_errors_total: redis_errors_total.labels(op="read_ticks", symbol=symbol).inc()
                if is_transient_redis_error(exc):
                    delay = backoff.get_delay()
                    logger.warning("⚠️ (%s) Transient ошибка чтения стрима %s: %s (backoff=%.2fs)", symbol, str(exc), delay)
                    await asyncio.sleep(delay)
                    continue
                else:
                    logger.error("❌ (%s) Критическая ошибка чтения стрима %s: %s", symbol, stream, exc)
                    delay = backoff.get_delay()
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
            
            sampled_ticks_dropped = 0
            for stream_name, entries in messages:
                ack_ids: List[str] = []
                entry_idx = 0
                for msg_id, fields in entries:
                    processed_ok = False
                    tick = None
                    
                    entry_idx += 1
                    if tick_sample_rate < 1.0:
                        if not deterministic_sample(entry_idx, tick_sample_rate):
                            # Drop tick due to sampling
                            ack_ids.append(msg_id)
                            sampled_ticks_dropped += 1
                            if ticks_dropped_total:
                                ticks_dropped_total.labels(symbol=symbol, reason="sampled").inc()
                            continue
                    
                    _t0 = _time.perf_counter()
                    try:
                        ticks_read_total.labels(symbol=symbol).inc()
                        raw = _fields_to_dict(fields)
                        tick = _parse_tick_payload(raw)

                        if not tick:
                            processed_ok = True
                            continue

                        # Единая модель времени (event/ingest/process)
                        ingest_ts_ms = _utc_epoch_ms()
                        tick["ingest_ts_ms"] = ingest_ts_ms
                        
                        # Deterministic event-time (prefer tick.ts_ms if sane; else Redis msg_id ms)
                        now_ms = ingest_ts_ms
                        payload_ts_ms = 0
                        try:
                            payload_ts_ms = _safe_int(tick.get("ts_ms") or tick.get("event_ts_ms") or 0)
                        except Exception:
                            payload_ts_ms = 0
                        event_ts_ms, ts_source = self._coerce_event_ts_ms(msg_id=msg_id, payload_ts_ms=payload_ts_ms, now_ms=now_ms)
                        tick["event_ts_ms"] = int(event_ts_ms)
                        tick["ts_ms"] = int(event_ts_ms)  # legacy compatibility
                        tick["ts_source"] = str(ts_source)

                        # ts_source observability: payload vs stream_id vs now
                        try:
                            if ticks_ts_source_total:
                                ticks_ts_source_total.labels(symbol=str(symbol), ts_source=str(ts_source)).inc()
                        except Exception:
                            pass

                        # Tick-quality EMAs (fast detection; complements rate()-based alerts).
                        # NOTE: deterministic: update by now_ms (monotonic per process).
                        unknown_side = False
                        try:
                            unknown_side = bool(is_unknown_side_tick(tick))
                        except Exception:
                            unknown_side = False

                        try:
                            if not hasattr(self, "_tick_quality_ema") or self._tick_quality_ema is None:
                                tau_ms = int(os.getenv("TICK_QUALITY_EMA_TAU_MS", "300000"))
                                self._tick_quality_ema = TickQualityEMA(tau_ms=tau_ms)

                            stream_ms = _safe_int(tick.get("stream_ms") or 0)
                            ev_ms = int(event_ts_ms or 0)
                            abs_skew_ms = abs(ev_ms - stream_ms) if stream_ms and ev_ms else 0
                            abs_age_ms = abs(int(now_ms) - ev_ms) if ev_ms else 0

                            ema = self._tick_quality_ema.update(
                                symbol=str(symbol),
                                ts_ms=int(now_ms),
                                unknown_side=1.0 if unknown_side else 0.0,
                                ts_source=str(ts_source),
                                abs_skew_ms=float(abs_skew_ms),
                                abs_age_ms=float(abs_age_ms),
                            )

                            # Step16: limit cardinality + throttle gauge emission (per label)
                            limiter = getattr(self, "_tick_metric_limiter", None)
                            if limiter is None:
                                allow = _parse_allowlist(os.getenv("TICK_QUALITY_SYMBOL_ALLOWLIST"))
                                mode = os.getenv("TICK_QUALITY_SYMBOL_LABEL_MODE", "collapse")
                                ema_min = int(os.getenv("TICK_QUALITY_EMA_UPDATE_MIN_MS", "250"))
                                self._tick_metric_limiter = TickMetricLimiter(allowlist=allow, mode=mode, ema_min_update_ms=ema_min)
                                self._tick_metric_last_emit_ms = {}
                                limiter = self._tick_metric_limiter

                            sym_label = limiter.label(str(symbol))
                            if sym_label is not None:
                                now_ms_int = int(now_ms)
                                last_ms = int(self._tick_metric_last_emit_ms.get(sym_label, 0))
                                if should_emit(now_ms_int, last_ms, int(limiter.ema_min_update_ms)):
                                    self._tick_metric_last_emit_ms[sym_label] = now_ms_int
                                    tick_unknown_side_ema_gauge.labels(symbol=sym_label).set(float(ema["unknown"]))
                                    tick_ts_source_now_ema_gauge.labels(symbol=sym_label).set(float(ema["ts_now"]))
                                    tick_ts_source_stream_id_ema_gauge.labels(symbol=sym_label).set(float(ema["ts_stream_id"]))
                                    tick_event_stream_skew_abs_ema_ms_gauge.labels(symbol=sym_label).set(float(ema["skew_abs_ms"]))
                                    tick_event_age_abs_ema_ms_gauge.labels(symbol=sym_label).set(float(ema["age_abs_ms"]))

                        except Exception:
                            pass
                        
                        # process_ts_ms перед фактическим process_tick
                        process_ts_ms = _utc_epoch_ms()
                        tick["process_ts_ms"] = process_ts_ms
                        
                        runtime.last_ts_ms = int(event_ts_ms)

                        # worker_lag_ms observability (+ pressure drop)
                        lag_ms = 0
                        try:
                            lag_ms = int(now_ms - int(event_ts_ms))
                            if lag_ms < 0:
                                lag_ms = 0
                            if worker_lag_ms_gauge:
                                worker_lag_ms_gauge.labels(symbol=symbol).set(float(lag_ms))
                            
                            # Update lag tracker for percentiles
                            lag_tracker = self._lag_trackers.get(symbol)
                            if lag_tracker:
                                lag_tracker.update(lag_ms)
                                # Export percentiles periodically (every 200 ticks)
                                export_counter = self._lag_export_counters.get(symbol, 0)
                                export_counter += 1
                                self._lag_export_counters[symbol] = export_counter
                                
                                if export_counter % 200 == 0:
                                    snap = lag_tracker.snapshot()
                                    if snap:
                                        try:
                                            if worker_lag_ms_p50_gauge:
                                                worker_lag_ms_p50_gauge.labels(symbol=symbol).set(snap.p50)
                                            if worker_lag_ms_p95_gauge:
                                                worker_lag_ms_p95_gauge.labels(symbol=symbol).set(snap.p95)
                                            if worker_lag_ms_p99_gauge:
                                                worker_lag_ms_p99_gauge.labels(symbol=symbol).set(snap.p99)
                                        except Exception:
                                            pass  # fail-open
                        except Exception:
                            lag_ms = 0

                        # Explicit pressure control: drop stale ticks to preserve real-time latency.
                        if self._drop_on_lag and int(lag_ms) > int(self._max_lag_ms):
                            if ticks_dropped_total:
                                ticks_dropped_total.labels(symbol=symbol, reason="lag").inc()
                            processed_ok = True
                            continue

                        # Tick dedup (best-effort). Drop duplicates to avoid double-counting delta/volume.
                        try:
                            uid = str(tick.get("tick_uid") or "")
                            if not uid:
                                uid = _compute_tick_uid(
                                    symbol=str(tick.get("symbol") or symbol),
                                    trade_id=tick.get("trade_id"),
                                    ts_ms=_safe_int(tick.get("ts_ms") or 0),
                                    price_src=raw.get("price") or raw.get("last") or raw.get("mid"),
                                    qty_src=raw.get("qty") or raw.get("volume"),
                                    side=str(tick.get("side") or ""),
                                    is_buyer_maker=tick.get("is_buyer_maker"),
                                )
                                tick["tick_uid"] = uid
                            if uid and runtime.is_duplicate_tick_uid(uid):
                                if tick_dedup_drop_total:
                                    tick_dedup_drop_total.labels(symbol=symbol).inc()
                                processed_ok = True
                                continue
                        except Exception:
                            pass

                        # Unknown-side policy: prevent implicit BUY/SELL bias at ingestion-time.
                        try:
                            if unknown_side:
                                try:
                                    ticks_unknown_side_policy_total.labels(symbol=str(symbol), policy=str(self._unknown_side_policy)).inc()
                                except Exception:
                                    pass

                                pol = str(self._unknown_side_policy or 'ignore_delta')
                                if pol in ('drop', 'quarantine'):
                                    try:
                                        if ticks_dropped_total:
                                            ticks_dropped_total.labels(symbol=symbol, reason=f'unknown_side_{pol}').inc()
                                    except Exception:
                                        pass

                                    if pol == 'quarantine':
                                        await self._quarantine_unknown_side_tick(
                                            symbol=str(symbol),
                                            msg_id=str(msg_id),
                                            tick=tick,
                                            raw_fields=raw,
                                            reason='unknown_side',
                                        )
                                    processed_ok = True
                                    continue

                                # For ignore_delta policy: zero-out signed qty downstream (deterministic)
                                if pol == 'ignore_delta':
                                    try:
                                        tick['qty_signed'] = 0.0
                                    except Exception:
                                        pass
                        except Exception:
                            pass

                        ticks_processed_total.labels(symbol=symbol).inc()

                        # Feed HealthMetrics for downstream periodic_reporter
                        try:
                            if hasattr(self, "health_metrics") and self.health_metrics:
                                book = runtime.get_book_snapshot()
                                if book and book.timestamp_ms:
                                    t_l2_age = max(0, event_ts_ms - book.timestamp_ms)
                                    self.health_metrics.on_tick(
                                        symbol=str(symbol),
                                        l2_age_ms=float(t_l2_age),
                                        l2_age_ms_tick=float(t_l2_age),
                                        l2_is_stale=(t_l2_age > 1500),
                                        l2_is_stale_now=(max(0, now_ms - book.timestamp_ms) > 1500)
                                    )
                        except Exception:
                            pass

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

                        try:
                            if signal and hasattr(self, "health_metrics") and self.health_metrics:
                                self.health_metrics.on_signal_emit(symbol=str(symbol))
                        except Exception:
                            pass

                        # Burst Processing (IMMEDIATE CHECK for Low Latency)
                        if not signal:
                            tick_ts = int(event_ts_ms)
                            burst_signal = await self._process_burst_flush(
                                runtime, "tick", tick_ts, do_publish=False
                            )
                            if burst_signal:
                                signal = burst_signal

                        if signal:
                            try:
                                preprocess_signal_for_publish(
                                    signal,
                                    symbol=str(getattr(runtime, "symbol", "") or symbol),
                                    source="CryptoOrderFlow",
                                    logger=logger,
                                )
                            except Exception:
                                pass
                            if self.strategy and await self._pre_publish_allows_signal(runtime, signal):
                                await self.strategy.publish_signal(runtime, signal)
                                signals_published_total.labels(symbol=symbol).inc()
                        processed_ok = True

                    except Exception as exc:  # noqa: BLE001
                        logger.error("❌ (%s) Crash processing tick %s: %s", symbol, msg_id, exc)

                        # Poison pill => quarantine + ACK (fail-open)
                        try:
                            await self.ticks.xadd(self.quarantine_stream, {
                                "symbol": symbol,
                                "msg_id": msg_id,
                                "error": str(exc)[:200],
                                "payload": json.dumps(fields, default=str)[:1000]
                            }, maxlen=5000)
                            logger.warning("☣️ (%s) Message %s quarantined", symbol, msg_id)
                            processed_ok = True
                        except Exception as q_err:
                            logger.error("Critical: Failed to quarantine: %s", q_err)
                            processed_ok = False

                    finally:
                        # ✅ Step 17: sampled ingest latency histograms
                        try:
                            if processed_ok and tick:
                                sample = getattr(self, "_tick_ingest_latency_sample", None)
                                if sample is None:
                                    sample = float(os.getenv("TICK_INGEST_LATENCY_SAMPLE", "0.02"))
                                    self._tick_ingest_latency_sample = sample
                                # deterministic sample key: prefer event_ts_ms else stream ms
                                key_ms = 0
                                try:
                                    key_ms = _safe_int(tick.get("event_ts_ms") or tick.get("ts_ms") or 0)
                                except Exception:
                                    key_ms = 0
                                if not key_ms:
                                    try:
                                        key_ms = int(str(msg_id).split("-")[0])
                                    except Exception:
                                        key_ms = int(time.time() * 1000)
                                if deterministic_sample(key_ms, float(sample)):
                                    sym_lbl = str(symbol)
                                    if _symbol_label:
                                        try:
                                            sym_lbl = _symbol_label(symbol)
                                        except Exception:
                                            sym_lbl = str(symbol)
                                    dt_ms = (_time.perf_counter() - _t0) * 1000.0
                                    tick_ingest_process_ms.labels(symbol=sym_lbl).observe(float(dt_ms))
                                    # e2e delay uses coerced event_ts_ms if present
                                    try:
                                        ev = _safe_int(tick.get("event_ts_ms") or tick.get("ts_ms") or 0)
                                    except Exception:
                                        ev = 0
                                    if ev:
                                        e2e = _safe_int(tick.get("ingest_ts_ms") or 0) - _safe_int(ev)
                                        if e2e < 0:
                                            e2e = 0
                                        tick_ingest_e2e_delay_ms.labels(symbol=sym_lbl).observe(float(e2e))
                        except Exception:
                            pass
                        if processed_ok:
                            ack_ids.append(msg_id)

                if sampled_ticks_dropped > 0 and runtime.throttle_log_sampler.should_log("tick_sampling"):
                    logger.info("🔪 (%s) Sampled and dropped %d ticks (rate=%.2f)", symbol, sampled_ticks_dropped, tick_sample_rate)

                # Batch ACK for throughput (XACK via pipeline)
                if ack_ids:
                    await self._xack_pipeline(stream=stream_name, group=group, ids=ack_ids, symbol=symbol, op="ack_ticks")
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
        while not self._shutdown:
            runtime = self.symbol_contexts.get(symbol)
            if runtime is None:
                await asyncio.sleep(1)
                continue

            # Initialize/Cache helper (Expert P4)
            stream = runtime.book_stream
            group = runtime.book_group
            helper = self.book_helpers.get(symbol)
            if helper is None:
                helper = AsyncRedisStreamHelper(self.ticks, group, self.consumer_id_books)
                self.book_helpers[symbol] = helper
                try:
                    await helper.ensure_group(stream)
                    sampled_info(logger, "BOOK_HELPER_INIT", "✅ (%s) book-helper initialized: stream=%s group=%s", symbol, stream, group)
                except RedisError as exc:
                    delay = backoff.get_delay()
                    logger.error("❌ (%s) ошибка создания book-группы %s: %s (backoff=%.2fs)", symbol, group, exc, delay)
                    await asyncio.sleep(delay)
                    continue

            try:
                messages = await helper.read(
                    {stream: ">"},
                    count=runtime.config.get("read_count", 200),
                    block=runtime.config.get("read_block_ms", 1000),
                )
            except (ConnectionError, TimeoutError) as exc:
                # ✅ FIX: Handle connection pool exhaustion and timeouts
                error_str = str(exc)
                is_pool_exhausted = "Too many connections" in error_str or "Name or service not known" in error_str
                is_timeout = isinstance(exc, TimeoutError) or "Timeout" in error_str
                
                if is_pool_exhausted:
                    if redis_errors_total:
                        redis_errors_total.labels(op="read_books_pool_exhausted", symbol=symbol).inc()
                    # More aggressive backoff for pool exhaustion (min 3s, max 20s)
                    delay = max(3.0, min(backoff.get_delay() * 2.5, 20.0))
                    active_count = len(self.active_symbols)
                    estimated_connections = active_count * 2  # ticks + books per symbol
                    # Calculate pool usage
                    pool_usage_pct = (estimated_connections / self.ticks_max * 100) if self.ticks_max > 0 else 0
                    logger.error(
                        "❌ (%s) Redis connection pool exhausted reading books (main_max=%d, ticks_max=%d, "
                        "active_symbols=%d, estimated_connections=%d, pool_usage=%.1f%%). Error: %s. "
                        "Retrying in %.2fs. Consider increasing REDIS_TICKS_MAX_CONNECTIONS or reducing symbol count.",
                        symbol, self.main_max, self.ticks_max, active_count, estimated_connections, pool_usage_pct, error_str, delay
                    )
                    # Longer delay to allow connections to be released
                    await asyncio.sleep(delay)
                    # Reset backoff to prevent exponential growth
                    backoff.reset()
                    continue
                elif is_timeout:
                    if redis_errors_total:
                        redis_errors_total.labels(op="read_books_timeout", symbol=symbol).inc()
                    delay = backoff.get_delay()
                    logger.warning("⚠️ (%s) Redis timeout reading books: %s (backoff=%.2fs)", symbol, error_str, delay)
                    await asyncio.sleep(delay)
                    continue
                else:
                    if redis_errors_total:
                        redis_errors_total.labels(op="read_books_connection", symbol=symbol).inc()
                    if is_transient_redis_error(exc):
                        delay = backoff.get_delay()
                        logger.warning("⚠️ (%s) Transient connection error reading books: %s (backoff=%.2fs)", symbol, exc, delay)
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error("❌ (%s) Connection error reading books: %s", symbol, exc)
                        delay = backoff.get_delay()
                        await asyncio.sleep(delay)
                        continue
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
                    delay = backoff.get_delay()
                    logger.warning("⚠️ (%s) Transient ошибка чтения книги: %s (backoff=%.2fs)", symbol, exc, delay)
                else:
                    logger.error("❌ (%s) Ошибка чтения книги: %s", symbol, exc)
                    await asyncio.sleep(1)
                    continue
            except RedisError as exc:
                if is_transient_redis_error(exc):
                    delay = backoff.get_delay()
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
                ack_ids: List[str] = []
                for msg_id, payload in entries:
                    try:
                        # Extract timestamp from msg_id for ingest_ts_ms
                        ingest_ts_ms = 0
                        try:
                            ingest_ts_ms = int(msg_id.split("-")[0])
                        except Exception:
                            ingest_ts_ms = int(time.time() * 1000)

                        await self.strategy.process_book(runtime, payload, ingest_ts_ms)
                    except Exception as exc:  # noqa: BLE001
                        logger.exception("❌ (%s) Ошибка обработки книги %s: %s", symbol, msg_id, exc)
                    finally:
                        ack_ids.append(msg_id)
                # Batch ACK for throughput (XACK via pipeline)
                if ack_ids:
                    await self._xack_pipeline(stream=stream_name, group=group, ids=ack_ids, symbol=symbol, op="ack_books")
            backoff.reset()

    # ── Обработка тиков и генерация сигналов ──────────────────────────────────

    def _should_sample_unknown_side(self, key_ms: int) -> bool:
        try:
            return deterministic_sample(int(key_ms), float(self._unknown_side_quarantine_sample))
        except Exception:
            return False

    async def _quarantine_unknown_side_tick(self, *, symbol: str, msg_id: str, tick: dict, raw_fields: dict, reason: str = 'unknown_side') -> None:
        """Publish a sampled unknown-side tick snapshot to a dedicated quarantine stream."""
        try:
            if not self.ticks:
                return
            key_ms = _safe_int(tick.get('event_ts_ms') or tick.get('ts_ms') or 0)
            if not self._should_sample_unknown_side(key_ms):
                return

            payload = {
                'symbol': str(symbol),
                'reason': str(reason),
                'policy': str(self._unknown_side_policy),
                'msg_id': str(msg_id),
                'tick_uid': str(tick.get('tick_uid') or ''),
                'event_ts_ms': str(_safe_int(tick.get('event_ts_ms') or 0)),
                'ts_source': str(tick.get('ts_source') or ''),
                'side': str(tick.get('side') or ''),
                'side_conf': str(tick.get('side_conf') or ''),
                'side_raw': str(tick.get('side_raw') or ''),
                'is_buyer_maker': str(tick.get('is_buyer_maker') if tick.get('is_buyer_maker') is not None else ''),
                'trade_id': str(tick.get('trade_id') or ''),
                'price': str(tick.get('price') or ''),
                'qty': str(tick.get('qty') or tick.get('volume') or ''),
            }
            try:
                raw_keys = sorted(list(raw_fields.keys()))
                payload['raw_keys'] = ','.join(raw_keys[:32])
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
            return

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


    @staticmethod
    def _msgid_ms(msg_id: str) -> int:
        # Redis stream id format: <ms>-<seq>
        try:
            return int(str(msg_id).split("-", 1)[0])
        except Exception:
            return 0

    def _coerce_event_ts_ms(self, *, msg_id: str, payload_ts_ms: int, now_ms: int) -> Tuple[int, str]:
        """Choose deterministic event time:
        1) tick.ts_ms (if sane)
        2) Redis msg_id ms
        3) wall clock (last resort)
        """
        ts = _safe_int(payload_ts_ms or 0)
        if ts > 0 and abs(int(now_ms) - ts) <= int(self._max_ts_skew_ms):
            return ts, "payload"
        mid = self._msgid_ms(msg_id)
        if mid > 0:
            return mid, "stream_id"
        return int(now_ms), "now"

    async def _xack_pipeline(self, *, stream: str, group: str, ids: List[str], symbol: str, op: str) -> None:
        """Ack many ids using pipeline, chunked."""
        if not ids:
            return
        batch = int(self._ack_batch or 0)
        if batch <= 0:
            batch = 200
        try:
            for i in range(0, len(ids), batch):
                chunk = ids[i:i + batch]
                pipe = self.ticks.pipeline(transaction=False)
                for mid in chunk:
                    pipe.xack(stream, group, mid)
                await pipe.execute()
        except Exception as exc:
            if redis_errors_total:
                redis_errors_total.labels(op=op, symbol=symbol).inc()
            logger.warning("⚠️ (%s) XACK pipeline failed (stream=%s group=%s n=%d): %s",
                           symbol, stream, group, len(ids), exc)

    async def _pel_sweeper_loop(self) -> None:
        """Optionally drains PEL to avoid 'black-hole' pending messages.
        Default behavior: XAUTOCLAIM + ACK (optionally quarantine), without reprocessing.
        """
        interval = float(os.getenv("CRYPTO_OF_PEL_SWEEP_INTERVAL_SEC", "10"))
        min_idle_ms = int(os.getenv("CRYPTO_OF_PEL_MIN_IDLE_MS", "60000"))
        count = int(os.getenv("CRYPTO_OF_PEL_COUNT", "200"))
        quarantine = self._env_bool("CRYPTO_OF_PEL_QUARANTINE", "false")

        while not self._shutdown:
            try:
                await asyncio.sleep(interval)
                runtimes = list(self.symbol_contexts.values())
                now_ms = int(time.time() * 1000)

                for rt in runtimes:
                    if self._shutdown:
                        break
                    sym = str(getattr(rt, "symbol", "") or "").upper()
                    if not sym:
                        continue

                    for kind in ("ticks", "books"):
                        if kind == "ticks":
                            stream = getattr(rt, "tick_stream", None)
                            group = getattr(rt, "tick_group", None)
                            consumer = self.consumer_id_ticks
                        else:
                            stream = getattr(rt, "book_stream", None)
                            group = getattr(rt, "book_group", None)
                            consumer = self.consumer_id_books

                        if not stream or not group:
                            continue

                        key = (sym, kind)
                        start_id = self._pel_cursor.get(key, "0-0")

                        try:
                            res = await self.ticks.xautoclaim(
                                stream, group, consumer,
                                min_idle_ms, start_id, count=count
                            )
                        except Exception:
                            continue

                        if not res:
                            continue

                        # redis-py: (next_start_id, messages, deleted_ids)
                        next_id = res[0] if isinstance(res, (list, tuple)) and len(res) > 0 else start_id
                        msgs = res[1] if isinstance(res, (list, tuple)) and len(res) > 1 else []
                        self._pel_cursor[key] = next_id or start_id

                        if not msgs:
                            continue

                        if pel_autoclaim_total:
                            pel_autoclaim_total.labels(symbol=sym, kind=kind).inc(len(msgs))

                        ack_ids: List[str] = []
                        for msg_id, fields in msgs:
                            ack_ids.append(str(msg_id))
                            if quarantine:
                                try:
                                    await self.ticks.xadd(self.quarantine_stream, {
                                        "symbol": sym,
                                        "msg_id": str(msg_id),
                                        "reason": f"pel_autoclaim:{kind}",
                                        "payload": json.dumps(fields, default=str)[:1000],
                                        "ts_ms": str(self._msgid_ms(msg_id) or now_ms),
                                    }, maxlen=5000)
                                except Exception:
                                    pass

                        await self._xack_pipeline(stream=stream, group=group, ids=ack_ids, symbol=sym, op=f"ack_pel_{kind}")

            except asyncio.CancelledError:
                break
            except Exception as exc:
                log_silent_error(exc, 'pel_sweep_failure', 'global', '_pel_sweeper_loop')
                await asyncio.sleep(1)

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
    # Auto-initialize ML Confirm config if needed
    try:
        from tools.init_ml_confirm_on_startup import ensure_ml_confirm_config
        success = ensure_ml_confirm_config()
        if success:
            logger.info("✅ ML Confirm config initialized successfully")
        else:
            logger.warning("⚠️ ML Confirm config initialization failed or skipped (check logs for details)")
    except Exception as e:
        logger.warning(f"⚠️ ML Confirm auto-init error: {e}", exc_info=True)
    
    # NOTE: Prometheus metrics server is started inside run_forever() using PROMETHEUS_PORT env var.
    # Do NOT start it here — double start causes [Errno 98] Address already in use.

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

