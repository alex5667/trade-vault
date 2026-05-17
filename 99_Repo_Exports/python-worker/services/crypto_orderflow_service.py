from __future__ import annotations

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

import asyncio
import json
import logging
import os
import random
import time
import traceback
from collections import deque
from typing import Any

from prometheus_client import start_http_server

from common.metrics2 import LagTracker
from handlers.crypto_orderflow.core.crypto_orderflow_calibration import (
    ConfidenceCalibratorCfg,
    RollingPercentileCalibrator,
)
from handlers.crypto_orderflow.utils.log_sampler import sampled_info, sampled_warning
from health_metrics import HealthMetrics
from services.orderflow.calibration_repo import CalibrationRepository
from services.orderflow.calibration_service import CalibrationService
from services.orderflow.configuration import DEFAULT_SYMBOLS, OrderFlowConfigLoader, _safe_int
from services.orderflow.metrics import (
    crypto_of_ml_gate_bootstrap_status,
    crypto_of_pel_cleanup_duration_ms,
    crypto_of_service_startup_duration_ms,
    crypto_of_shutdown_duration_ms,
    drain_forced_cancel_total,
    log_silent_error,
    pel_autoclaim_total,
    redis_errors_total,
    ticks_dropped_total,
)
from services.orderflow.pel_bootstrap_cleanup import cleanup_zombie_consumers, periodic_pel_cleanup_loop
from services.orderflow.runtime import SymbolRuntime
from services.orderflow.side_policy import deterministic_sample, normalize_unknown_side_policy
from services.orderflow.strategy import OrderFlowStrategy
from services.persistence_manager import get_persistence_manager
from utils.task_manager import safe_create_task
from utils.time_utils import get_ny_time_millis

try:
    # Step 16 (optional): collapse label cardinality
    from services.orderflow.metric_labels import symbol_label as _symbol_label
except Exception:
    _symbol_label = None


import redis.asyncio as aioredis
from redis.exceptions import ConnectionError, RedisError, ResponseError

from common.backoff import Backoff
from common.redis_errors import is_transient_error as is_transient_redis_error
from core.dq_policy import TickDQPolicy
from core.of_confirm_engine import OFConfirmEngine
from core.redis_keys import STREAM_RETENTION
from core.redis_keys import RedisStreams as RS
from core.redis_stream_consumer import AsyncRedisStreamHelper
from services.async_signal_publisher import AsyncSignalPublisher
from services.orderflow.burst_flusher import BurstFlusher
from services.orderflow.redis_pools import RedisPoolSet
from services.orderflow.service_config import ServiceConfig
from services.orderflow.signal_gate import SignalGate
from services.orderflow.components.tick_processor import TickProcessor
import contextlib

try:
    from core.redis_client import get_async_redis_client
except Exception:
    get_async_redis_client = None

try:
    # P3: deterministic Redis/data-quality veto — blocks publishing when infra is degraded
    from services.redis_dq_policy import RedisDQSnapshot, RedisDQThresholds, evaluate_redis_dq
except Exception:
    RedisDQSnapshot = RedisDQThresholds = evaluate_redis_dq = None  # type: ignore

try:
    # P4: unified risk policy engine — per-trade sizing + tier policy + exposure caps
    from services.risk.risk_policy_engine import (
        RISK_DENY_HARD,
        RISK_DENY_SOFT,
        RISK_FORCE_FLATTEN,
        PortfolioPosition,
        PortfolioRiskInput,
        PortfolioRiskLimits,
        evaluate_portfolio_risk,
        infer_symbol_tier,
    )
except Exception:
    PortfolioPosition = PortfolioRiskInput = PortfolioRiskLimits = evaluate_portfolio_risk = infer_symbol_tier = None  # type: ignore
    RISK_DENY_HARD = "DENY_HARD"
    RISK_DENY_SOFT = "DENY_SOFT"
    RISK_FORCE_FLATTEN = "FORCE_FLATTEN"

try:
    from services.risk.portfolio_risk_engine import (
        RISK_DENY_HARD,
        RISK_DENY_SOFT,
        RISK_FORCE_FLATTEN,
        PortfolioPosition,
        PortfolioRiskInput,
        PortfolioRiskLimits,
        evaluate_portfolio_risk,
    )
    infer_symbol_tier = None  # type: ignore
except Exception:
    pass

try:
    # P4.5: SQL audit sink for risk decisions (fail-open: publish path not blocked by DB outages)
    from services.risk.risk_audit_sql import RiskAuditSqlSink
except Exception:
    RiskAuditSqlSink = None  # type: ignore

try:
    from services.quarantine_denylist import check_signal_against_quarantine_cache
except Exception:
    check_signal_against_quarantine_cache = None  # type: ignore




# ──────────────────────────────────────────────────────────────────────────────
# Настройки по умолчанию /default_settings.py
# ──────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger("crypto_orderflow_service")


def ensure_audit_chain_fields(signal: dict[str, Any]) -> dict[str, Any]:
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
    signal_id = str(signal.get('signal_id') or signal.get('sid') or '').strip() or decision_id
    execution_plan_id = str(signal.get('execution_plan_id') or '').strip() or signal_id or decision_id
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
    return int(time.monotonic() * 1000)


def _safe_latency_delta_ms(start_mono_ms: int, end_mono_ms: int) -> int:
    try:
        return max(0, end_mono_ms - start_mono_ms)
    except Exception:
        return 0


# ──────────────────────────────────────────────────────────────────────────────
# Runtime для одного символа
# ──────────────────────────────────────────────────────────────────────────────


class CryptoOrderflowService:
    def __init__(self, redis_dsn: str, ticks_dsn: str | None = None) -> None:
        self.redis_dsn = redis_dsn
        self.logger = logger
        self.ticks_dsn = ticks_dsn or redis_dsn

        # ── Config + Redis pools ──────────────────────────────────────────────
        self._svc_cfg = ServiceConfig.from_env()
        self._pools = RedisPoolSet.build(self._svc_cfg.pools, redis_dsn, self.ticks_dsn)

        # Backward-compat aliases (methods still reference self.main / self.ticks etc.)
        self.main: aioredis.Redis = self._pools.main
        self.ticks: aioredis.Redis = self._pools.ticks
        self.notify_client: aioredis.Redis = self._pools.notify
        self.ml_gate_client: aioredis.Redis = self._pools.ml_gate
        self._config_redis_client: aioredis.Redis = self._pools.config
        self._health_redis_client: aioredis.Redis = self._pools.health_contract
        self.main_max: int = self._svc_cfg.pools.main_max
        self.ticks_max: int = self._svc_cfg.pools.ticks_max

        # Lifecycle
        self._stop_event: asyncio.Event | None = None
        self.tasks: list[asyncio.Task] = []
        self.active_symbols: set[str] = set()
        self.symbol_contexts: dict[str, SymbolRuntime] = {}
        self.consumer_id = f"worker-{os.getpid()}-{int(time.time())}"

        # Async publisher
        self.publisher = AsyncSignalPublisher(
            redis_client=None, # set in run_forever
            source="CryptoOrderFlow"  # ✅ FIX: Use canonical source name
        )

        # Engines (of_engine rebuilt later with ML gate)
        self.of_engine = OFConfirmEngine()
        self.strategy: OrderFlowStrategy | None = None
        # G0 (strategy.process_tick) is the single owner of monotonicity / backwards /
        # clamp / quarantine. DQ keeps bad_ts / stale / future_skew. Force-disable DQ's
        # out_of_order check regardless of TICK_DQ_MAX_OOO_MS env so G0 metrics light up.
        self.tick_dq_policy = TickDQPolicy(latency_lenient_mode=False)
        self.tick_dq_policy.max_out_of_order_ms = 0

        self.config_loader = OrderFlowConfigLoader(self._config_redis_client)

        # PersistenceManager (PG) injectable into SymbolRuntime for testability
        try:
            self.pm = get_persistence_manager()
        except Exception as exc:
            logger.error("Failed to init PersistenceManager: %s", exc, exc_info=True)
            self.pm = None  # type: ignore

        # Calibration Service (SRP Phase A)
        self.calib_repo = CalibrationRepository(redis_ticks=self.ticks, pm=self.pm)
        self.calib_svc = CalibrationService(repo=self.calib_repo)

        # ML Confidence Score Calibrator
        _c = self._svc_cfg.calib
        self.score_calibrator = RollingPercentileCalibrator(
            cfg=ConfidenceCalibratorCfg(
                window=_c.window,
                min_history=_c.min_history,
                fallback_k=_c.fallback_k,
            )
        )

        # Health metrics background loop
        self.health_metrics = HealthMetrics(
            redis_url=self.redis_dsn,
            window_sec=5,
            max_connections=self._svc_cfg.pools.health_max,
        )

        # Risk gate flags (kept as attrs: old methods _build_* still reference them)
        _r = self._svc_cfg.risk
        self.trade_dq_hard_veto_enable = _r.dq_hard_veto_enable
        self.trade_risk_engine_v2_enable = _r.risk_engine_v2_enable
        self.redis_dq_thresholds = RedisDQThresholds.from_env() if (RedisDQThresholds and _r.dq_hard_veto_enable) else None
        self.portfolio_risk_limits = PortfolioRiskLimits.from_env() if (PortfolioRiskLimits and _r.risk_engine_v2_enable) else None
        self.portfolio_risk_hard_veto = _r.risk_hard_veto
        self.trade_risk_sql_audit_enable = _r.risk_sql_audit_enable
        self.risk_audit_sql_sink = RiskAuditSqlSink.from_env() if (RiskAuditSqlSink and _r.risk_sql_audit_enable) else None
        self.exec_quarantine_denylist_enable = _r.quarantine_enable
        self.orders_quarantine_sids_key = _r.quarantine_sids_key
        self.quarantine_denylist_cache_ms = _r.quarantine_cache_ms
        self._quarantine_sid_cache: set[str] = set()
        self._quarantine_sid_cache_ts_ms: int = 0

        # Signal gate (DQ + risk + quarantine) — replaces _pre_publish_allows_signal
        self._gate = SignalGate.from_service(self)

        # Local caches for snapshot publisher
        self._rq_cache: dict[str, tuple[int, dict[str, Any]]] = {}
        self._adx_cache: dict[str, tuple[int, float]] = {}

        self.symbol_contexts: dict[str, SymbolRuntime] = {}
        self.symbol_tasks: dict[str, tuple[asyncio.Task, asyncio.Task]] = {}
        self.refresh_interval = self._svc_cfg.refresh_interval_sec

        self._lag_trackers: dict[str, LagTracker] = {}
        self._lag_export_counters: dict[str, int] = {}

        rnd = random.randint(1000, 9999)
        self.consumer_id_ticks = f"crypto-of-ticks-{os.getpid()}-{rnd}"
        self.consumer_id_books = f"crypto-of-books-{os.getpid()}-{rnd}"

        self.tick_helpers: dict[str, Any] = {}
        self.book_helpers: dict[str, Any] = {}
        self.poison_pill_counts: dict[str, int] = {}

        # Streams (resolved lazily from RedisStreams)
        _s = self._svc_cfg.resolved_streams()
        self.quarantine_stream = _s.quarantine_stream
        self.notify_stream = _s.notify_stream
        self.raw_signal_stream = _s.raw_signal_stream
        self.orders_queue_mt5 = _s.orders_queue_mt5
        self.orders_queue_binance = _s.orders_queue_binance
        self.cryptoorderflow_signal_stream_template = _s.signal_stream_template
        self.burst_audit_stream = _s.burst_audit_stream

        # Unknown-side tick policy
        _t = self._svc_cfg.tick
        self._unknown_side_policy = normalize_unknown_side_policy(_t.unknown_side_policy)
        self._unknown_side_quarantine_stream = _t.unknown_side_quarantine_stream
        self._unknown_side_quarantine_sample = _t.unknown_side_quarantine_sample
        self._unknown_side_quarantine_maxlen = _t.unknown_side_quarantine_maxlen

        # ML gate + OFConfirmEngine (P0: fail-open on import error)
        _ml_gate = None
        try:
            from services.ml_confirm import MLConfirmGate  # noqa: PLC0415
            _ml_gate = MLConfirmGate.from_env()
        except ImportError as _imp_err:
            if os.getenv("ML_CONFIRM_MODE", "OFF").upper() == "ENFORCE" and os.getenv("ML_CONFIRM_FAIL_POLICY", "OPEN").upper() == "CLOSED":
                crypto_of_ml_gate_bootstrap_status.labels(status="fail").set(1)
                raise RuntimeError("ml_gate_required_but_missing") from _imp_err
            crypto_of_ml_gate_bootstrap_status.labels(status="fail_open").set(1)
            logger.critical(
                "P0 CAPITAL-SAFETY: Не удалось импортировать MLConfirmGate: %s. "
                "ML-gate НЕ активен. Убедитесь что ML_CONFIRM_FAIL_POLICY=CLOSED "
                "или ML_CONFIRM_MODE=OFF в ENFORCE-окружении.",
                _imp_err,
            )
        except Exception as _gate_err:
            logger.critical(
                "P0 CAPITAL-SAFETY: Ошибка при инициализации MLConfirmGate: %s. "
                "ML-gate НЕ активен.",
                _gate_err,
                exc_info=True,
            )
        self.of_engine = OFConfirmEngine(
            version=self._svc_cfg.engine.of_confirm_version,
            ml_gate=_ml_gate,
        )

        self._refresh_task: asyncio.Task | None = None
        self._ml_gate_bg_task: asyncio.Task | None = None
        self._burst_task: asyncio.Task | None = None
        self._supervisor_task: asyncio.Task | None = None
        self._health_contract_task: asyncio.Task | None = None
        self._score_calib_persist_task: asyncio.Task | None = None
        self._task_restart_hist: dict[tuple[str, str], deque] = {}
        self._shutdown = False

        # Rolling calibrator persistence settings
        self._score_calib_redis_key = self._svc_cfg.calib.score_calib_redis_key
        self._score_calib_persist_interval = self._svc_cfg.calib.score_calib_persist_interval_sec
        self._score_calib_ttl = self._svc_cfg.calib.score_calib_ttl_sec

        self._bootstrap_sem = asyncio.Semaphore(self._svc_cfg.bootstrap_max_conc)

        self._ack_batch = _t.ack_batch
        self._xack_retries = self._svc_cfg.tick.xack_retries
        self._xack_backoff_ms = self._svc_cfg.tick.xack_backoff_ms
        self._max_lag_ms = _t.max_lag_ms
        self._drop_on_lag = _t.drop_on_lag
        self._max_ts_skew_ms = _t.max_ts_skew_ms

        self._pel_cursor: dict[tuple[str, str], str] = {}
        self._pel_sweeper_task: asyncio.Task | None = None
        self._pel_cleanup_task: asyncio.Task | None = None

        self.force_trail_after_tp1: bool | None = self._svc_cfg.engine.force_trail_after_tp1

        # Burst flusher (replaces _burst_flush_loop + _process_burst_flush)
        self._flusher = BurstFlusher(
            symbol_contexts_fn=lambda: self.symbol_contexts,
            strategy_fn=lambda: self.strategy,
            gate=self._gate,
            is_shutdown_fn=lambda: self._shutdown,
        )

        # Tick processor (replaces ~350-line inner loop of consume_ticks)
        self._tick_proc = TickProcessor.from_service(self)

        logger.info("✅ CryptoOrderflowService инициализирован")
        logger.info("   Main Redis:  %s", self.redis_dsn)
        logger.info("   Ticks Redis: %s", self.ticks_dsn)
        logger.info("   Telegram stream: %s", self.notify_stream)
        logger.info("   Signal min confidence: %s%%", os.getenv("CRYPTO_SIGNAL_MIN_CONF", "70"))

    def _env_bool(self, name: str, default: str | None = None) -> bool:
        val = os.getenv(name, default)
        if not val:
            return False
        return str(val).lower() in ("1", "true", "yes", "on")

    def _adaptive_tick_read_count(self, symbol: str, configured_count: int) -> int:
        """Reduce batch size when Redis-entry lag is high to limit HOL blocking."""
        try:
            count = max(1, configured_count)
            if not self._env_bool("CRYPTO_OF_ADAPTIVE_READ_COUNT", "1"):
                return count
            high_lag_ms = int(os.getenv("CRYPTO_OF_ADAPTIVE_LAG_HIGH_MS", "50"))
            burst_count = max(1, int(os.getenv("CRYPTO_OF_ADAPTIVE_READ_COUNT_BURST", "10")))
            tracker = self._lag_trackers.get(f"_redis_{symbol}")
            if tracker is None:
                return count
            snap = tracker.snapshot()
            if snap and float(getattr(snap, "p99", 0.0) or 0.0) > float(high_lag_ms):
                return min(count, burst_count)
            return count
        except Exception:
            return max(1, configured_count or 1)

    async def run_forever(self) -> None:
        """
        Основной цикл сервиса. Останавливается по сигналу отмены.
        """
        # Connect publisher and start retry worker (must be inside event loop)
        self._stop_event = asyncio.Event()  # lazy init inside running event loop
        self.publisher.r = self._pools.publish
        self.publisher.start()
        # NOTE: config_loader uses its own dedicated Redis client (_config_redis_client),
        # created in __init__. Do NOT reassign to self.main here.

        # Phase 4.5: full PostgreSQL-backed policy workflow rebuild
        loop = asyncio.get_running_loop()
        t0_start = time.time()
        try:
            if os.getenv("ATR_POLICY_FULL_RECOVERY_ENABLE", "0") == "1":
                from services.atr_policy_full_recovery_service import run_once as run_atr_policy_full_recovery_once
                await loop.run_in_executor(None, run_atr_policy_full_recovery_once)
        except Exception as exc:
            logger.warning("ATR policy full recovery failed: %s", exc)

        # --------------------------------------------------------------
        # Phase 4.2: bootstrap ATR policy control-plane from SQL snapshots
        # before loading dynamic symbols and background loops
        # --------------------------------------------------------------
        try:
            if os.getenv("ATR_POLICY_BOOTSTRAP_ENABLE", "1") == "1":
                from services.atr_policy_bootstrap_service import run_once as run_atr_policy_bootstrap_once
                await loop.run_in_executor(None, run_atr_policy_bootstrap_once)
        except Exception as exc:
            logger.warning("ATR policy bootstrap failed: %s", exc)

        try:
            if os.getenv("ATR_POLICY_DRIFT_CHECK_ON_START", "1") == "1":
                from services.atr_policy_state_consistency_checker import run_once as run_atr_policy_drift_check_once
                await loop.run_in_executor(None, run_atr_policy_drift_check_once)
        except Exception as exc:
            logger.warning("ATR policy drift check on start failed: %s", exc)

        # Init Strategy
        self.strategy = OrderFlowStrategy(
            redis=self.main,
            ticks=self.ticks,
            publisher=self.publisher,
            of_engine=self.of_engine,
            calib_svc=self.calib_svc,
            score_calibrator=self.score_calibrator,
            notify_client=self.notify_client,
            notify_stream=self.notify_stream,
            orders_queue_mt5=self.orders_queue_mt5,
            orders_queue_binance=self.orders_queue_binance,
        )

        # Start metrics server — respects PROMETHEUS_PORT env var (fallback: METRICS_PORT, then 8000)
        port = None
        try:
            port = int(os.getenv("PROMETHEUS_PORT") or os.getenv("METRICS_PORT") or "8000")
            start_http_server(port)
            logger.info("✅ Metrics server started on port %d", port)
        except Exception as e:
            logger.error("❌ Failed to start metrics server on port %s: %s", port, e)

        # Initial ML config/model load (async) before blocking fast-path
        # Must run BEFORE load_dynamic_symbols so that backlog ticks don't hit ERR_NO_CFG
        if self.of_engine and getattr(self.of_engine, "ml_gate", None) and hasattr(self.of_engine.ml_gate, "refresh_async"):
            # Retry loop for resilience against DB/Redis startup race conditions
            for _ in range(5):
                try:
                    await self.of_engine.ml_gate.refresh_async(self.main)  # type: ignore
                    if getattr(self.of_engine.ml_gate, "_cfg", None):  # type: ignore
                        logger.info("✅ ML_CONFIRM_GATE successfully loaded config on startup")
                        break
                    logger.warning("ML_CONFIRM_GATE config not found yet (raw_payload possibly None), retrying in 2s...")
                    await asyncio.sleep(2.0)
                except Exception as e:
                    logger.error(f"Failed to async refresh ml_confirm_gate on startup: {e}")
                    await asyncio.sleep(2.0)

        if hasattr(self, 'health_metrics') and self.health_metrics:
            self.health_metrics.start_background_loop()

        # ── PEL bootstrap cleanup: remove zombie consumers BEFORE processing ticks ──
        # This prevents WorkerLagP99High caused by XAUTOCLAIM reclaiming ancient PEL entries
        # after container restarts (each restart creates a new PID-based consumer).
        if self._svc_cfg.pel.cleanup_on_startup:
            try:
                idle_ms = self._svc_cfg.pel.cleanup_idle_threshold_ms
                # Resolve symbols early for cleanup (same logic as load_dynamic_symbols)
                symbols_override_env = os.getenv("CRYPTO_SYMBOLS_OVERRIDE", "")
                if symbols_override_env:
                    startup_symbols = [s.strip().upper() for s in symbols_override_env.split(",") if s.strip()]
                else:
                    startup_symbols = list(DEFAULT_SYMBOLS)
                    try:
                        symbols_key = os.getenv("CRYPTO_SYMBOLS_SET_KEY", "crypto:symbols")
                        redis_syms = await self.ticks.smembers(symbols_key)  # type: ignore
                        startup_symbols.extend(s.upper() for s in redis_syms)
                    except Exception:
                        pass
                startup_symbols = list(set(startup_symbols))

                if startup_symbols:
                    t0_cleanup = time.time()
                    result = await asyncio.wait_for(
                        cleanup_zombie_consumers(
                            self.ticks,
                            startup_symbols,
                            current_consumer_id=self.consumer_id_ticks,
                            idle_threshold_ms=idle_ms,
                            phase="startup",
                        ),
                        timeout=30.0,
                    )
                    dt_cleanup = (time.time() - t0_cleanup) * 1000
                    crypto_of_pel_cleanup_duration_ms.observe(dt_cleanup)
                    logger.info(
                        "✅ PEL startup cleanup: %d zombies removed, %d pending ACKed in %.0fms",
                        result.get("zombies_removed", 0),
                        result.get("pending_acked", 0),
                        dt_cleanup,
                    )
            except TimeoutError:
                logger.warning("⚠️ PEL startup cleanup timed out (30s)")
            except Exception as exc:
                logger.warning("⚠️ PEL startup cleanup failed (non-fatal): %s", exc)

        # Restore rolling calibrator from Redis before processing any ticks
        await self._restore_score_calibrator()

        await self.load_dynamic_symbols()
        self._refresh_task = safe_create_task(self._refresh_loop(), name="crypto-of-refresh")
        self._ml_gate_bg_task = safe_create_task(self._maintain_ml_gate_loop(), name="crypto-of-ml-gate-bg")
        self._score_calib_persist_task = safe_create_task(
            self._score_calibrator_persist_loop(), name="crypto-of-score-calib-persist"
        )
        self._flusher.start()
        self._burst_task = self._flusher._task
        if self._svc_cfg.lifecycle.supervisor_enable:
            self._supervisor_task = safe_create_task(self._supervisor_loop(), name="crypto-of-supervisor")

        if self._svc_cfg.pel.sweeper_enable:
            self._pel_sweeper_task = safe_create_task(self._pel_sweeper_loop(), name="crypto-of-pel-sweep")

        # Periodic PEL cleanup: removes zombie consumers every N seconds (default 300s)
        # This is a lightweight alternative to the PEL sweeper that focuses on zombie removal.
        if self._svc_cfg.pel.cleanup_periodic_enable:
            pel_interval = self._svc_cfg.pel.cleanup_periodic_sec
            pel_idle_ms = self._svc_cfg.pel.cleanup_idle_threshold_ms
            self._pel_cleanup_task = safe_create_task(
                periodic_pel_cleanup_loop(
                    redis_client_fn=lambda: self.ticks,
                    symbols_fn=lambda: list(self.symbol_contexts.keys()),
                    current_consumer_id=self.consumer_id_ticks,
                    is_shutdown_fn=lambda: self._shutdown,
                    interval_sec=pel_interval,
                    idle_threshold_ms=pel_idle_ms,
                ),
                name="crypto-of-pel-cleanup",
            )

        # Periodically flush exec_health_contract to prevent stale SLO alerts (P4.1)
        self._health_contract_task = safe_create_task(self._health_contract_flush_loop(), name="crypto-of-health-flush")

        crypto_of_ml_gate_bootstrap_status.labels(status="ok").set(1)
        crypto_of_service_startup_duration_ms.observe((time.time() - t0_start) * 1000)

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
        t0_shutdown = time.time()

        logger.info("🔻 Останавливаем CryptoOrderflowService...")

        if self._supervisor_task:
            self._supervisor_task.cancel()
            await asyncio.gather(self._supervisor_task, return_exceptions=True)

        if getattr(self, "_pel_sweeper_task", None):
            self._pel_sweeper_task.cancel()  # type: ignore
            await asyncio.gather(self._pel_sweeper_task, return_exceptions=True)  # type: ignore
  # type: ignore
        if getattr(self, "_pel_cleanup_task", None):
            self._pel_cleanup_task.cancel()  # type: ignore
            await asyncio.gather(self._pel_cleanup_task, return_exceptions=True)  # type: ignore
  # type: ignore
        if self._refresh_task:
            self._refresh_task.cancel()
            await asyncio.gather(self._refresh_task, return_exceptions=True)

        if getattr(self, "_ml_gate_bg_task", None):
            self._ml_gate_bg_task.cancel()  # type: ignore
            await asyncio.gather(self._ml_gate_bg_task, return_exceptions=True)  # type: ignore
  # type: ignore
        if hasattr(self, "_burst_task") and self._burst_task:
            self._burst_task.cancel()
            await asyncio.gather(self._burst_task, return_exceptions=True)

        if self._health_contract_task:
            self._health_contract_task.cancel()
            await asyncio.gather(self._health_contract_task, return_exceptions=True)

        if getattr(self, "_score_calib_persist_task", None):
            self._score_calib_persist_task.cancel()  # type: ignore
            await asyncio.gather(self._score_calib_persist_task, return_exceptions=True)  # type: ignore
  # type: ignore
        if hasattr(self, 'health_metrics') and self.health_metrics:
            self.health_metrics.stop()

        # Drain mode: let workers finish current iteration to reduce PEL growth
        drain_timeout = self._svc_cfg.lifecycle.drain_timeout_sec
        logger.info("🔻 Draining symbol workers (timeout=%.1fs)...", drain_timeout)

        # Track tasks with their metadata for reporting
        task_to_meta = {}
        for symbol, (t_tick, t_book) in list(self.symbol_tasks.items()):
            if t_tick and not t_tick.done():
                t_tick.cancel()
                task_to_meta[t_tick] = (symbol, "ticks")
            if t_book and not t_book.done():
                t_book.cancel()
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

        await self._pools.close_all()
        self.main = None  # type: ignore
        self.ticks = None  # type: ignore

        crypto_of_shutdown_duration_ms.observe((time.time() - t0_shutdown) * 1000)
        logger.info("✅ Завершено")


    # ── Динамическая загрузка символов ────────────────────────────────────────

    async def load_dynamic_symbols(self) -> None:
        """
        Загружает список символов и их конфиг из Redis, запускает новые задачи.
        """
        if self._shutdown:
            return

        symbols_override_env = os.getenv("CRYPTO_SYMBOLS_OVERRIDE", "")
        if symbols_override_env:
            symbols = set(sym.strip().upper() for sym in symbols_override_env.split(",") if sym.strip())
        else:
            use_default_symbols = self._env_bool("CRYPTO_DEFAULT_SYMBOLS_ENABLED", "true")
            symbols = set(sym.upper() for sym in DEFAULT_SYMBOLS) if use_default_symbols else set()

            symbols_key = os.getenv("CRYPTO_SYMBOLS_SET_KEY", "crypto:symbols")
            try:
                redis_symbols = await self.main.smembers(symbols_key)  # type: ignore
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

        # Группируем обработку символов для параллельного выполнения (Expert P5)
        t0_load = time.time()

        # Preload configs in a pipeline to avoid connection pool exhaustion and timeouts
        if hasattr(self.config_loader, "preload_configs"):
            try:
                await self.config_loader.preload_configs(list(symbols))
            except Exception as e:
                logger.error("Failed to preload configs: %s", e)

        async def _init_symbol(symbol: str):
            async with self._bootstrap_sem:
                try:
                    cfg = await self.config_loader.build_symbol_config(symbol)
                    tick_stream, book_stream = await self._resolve_streams(symbol)
                    return symbol, cfg, tick_stream, book_stream
                except Exception as ex:
                    logger.error("Failed to load config for %s: %s", symbol, ex)
                    return symbol, None, None, None

        # Limit concurrency to REDIS_CONFIG_MAX_CONNECTIONS (default 10) to avoid connection pool exhaustion
        max_concurrency = int(os.getenv("REDIS_CONFIG_MAX_CONNECTIONS", "10"))
        sem = asyncio.Semaphore(max(1, max_concurrency - 2)) # Leave 2 conns for other tasks

        async def bounded_init(sym: str):
            async with sem:
                return await _init_symbol(sym)

        load_tasks = [bounded_init(s) for s in sorted(symbols)]
        results = await asyncio.gather(*load_tasks)

        for symbol, cfg, tick_stream, book_stream in results:
            if cfg is None:
                continue

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

                # ✅ P1: EAGER BOOTSTRAP: Load calibrations BEFORE starting tasks
                try:
                    async def bootstrap_task():
                        async with self._bootstrap_sem:
                            try:
                                await asyncio.wait_for(self.calib_svc.ensure_loaded(runtime), timeout=2.0)
                            except Exception as exc:
                                log_silent_error(exc, 'bootstrap_timeout', symbol, 'load_dynamic_symbols:bootstrap')
                            runtime.ready = True  # type: ignore
                    safe_create_task(bootstrap_task())  # type: ignore
                except Exception:
                    runtime.ready = True # fail-open

                tick_task = safe_create_task(self.consume_ticks(symbol), name=f"crypto-of-ticks-{symbol}")
                book_task = safe_create_task(self.consume_books(symbol), name=f"crypto-of-book-{symbol}")
                self.symbol_tasks[symbol] = (tick_task, book_task)  # type: ignore
  # type: ignore
            # Конфиг должен применяться всегда, а не только при создании
            runtime.apply_config(cfg)
            runtime.tick_stream = tick_stream  # type: ignore
            runtime.book_stream = book_stream  # type: ignore
            runtime.tick_group = f"crypto-of:{symbol}"  # type: ignore
            runtime.book_group = f"crypto-of-book:{symbol}"

            # Throttled worker start log (every 10000th)
            global _symbols_added_counter
            if symbol not in current_symbols:
                _symbols_added_counter += 1
                if _symbols_added_counter % 100 == 0:
                     logger.info("🆕 Added symbol %s (total added: %d)", symbol, _symbols_added_counter)

            tasks = self.symbol_tasks.get(symbol)
            if tasks:
                t_tick, t_book = tasks
                if (t_tick and t_tick.done()) or (t_book and t_book.done()):
                    logger.error("❌ (%s) Detected dead tasks in load_dynamic_symbols; will restart via supervisor", symbol)

                sampled_info(
                    logger,
                    "WORKER_INIT",
                    "🚀 (%s) воркеры запущены: k=%s, delta_z_threshold=%.2f, min_conf=%.2f%%, every_n=%s",
                    symbol, runtime.book_stream,
                    float(cfg.get("delta_z_threshold") or 3.10),
                    float(os.getenv("CRYPTO_SIGNAL_MIN_CONF", "70")),
                    os.getenv("CRYPTO_NOTIFY_SIGNAL_EVERY_N", "30")
                )

        dt_load = time.time() - t0_load
        if dt_load > 1.0 or symbols_count > 10:
            logger.info("✅ load_dynamic_symbols completed: %d symbols in %.1fms", symbols_count, dt_load * 1000)

        # Выключаем символы, которые ушли из набора (кроме базовых)
        symbols_to_stop = current_symbols - symbols
        for symbol in symbols_to_stop:
            await self._stop_symbol(symbol)

    async def _restore_score_calibrator(self) -> None:
        """Restore RollingPercentileCalibrator history from Redis on startup (warm start)."""
        try:
            raw = await self.main.get(self._score_calib_redis_key)
            if not raw:
                logger.info("Score calibrator: no snapshot in Redis, starting cold (key=%s)", self._score_calib_redis_key)
                return
            snapshot = json.loads(raw)
            n = self.score_calibrator.restore(snapshot)
            logger.info("✅ Score calibrator restored %d samples from Redis (key=%s)", n, self._score_calib_redis_key)
        except Exception as exc:
            logger.warning("⚠️ Failed to restore score calibrator from Redis: %s", exc)

    async def _score_calibrator_persist_loop(self) -> None:
        """Periodically persist RollingPercentileCalibrator snapshot to Redis.

        Env:
          SCORE_CALIB_PERSIST_INTERVAL_SEC  default 300s
          SCORE_CALIB_REDIS_KEY             default cfg:rolling_calib:v1
          SCORE_CALIB_TTL_SEC               default 7 days
        """
        interval = self._score_calib_persist_interval
        key = self._score_calib_redis_key
        ttl = self._score_calib_ttl
        logger.info("📊 Score calibrator persist loop started (interval=%.0fs, key=%s)", interval, key)
        try:
            while not self._shutdown:
                await asyncio.sleep(interval)
                if self._shutdown:
                    break
                try:
                    snapshot = self.score_calibrator.snapshot()
                    if not snapshot:
                        continue
                    payload = json.dumps(snapshot)
                    await asyncio.wait_for(
                        self.main.set(key, payload, ex=ttl),
                        timeout=5.0,
                    )
                    logger.debug("Score calibrator snapshot saved: %d keys, %d bytes", len(snapshot), len(payload))
                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    logger.warning("⚠️ Score calibrator persist failed: %s", exc)
        except asyncio.CancelledError:
            # Final persist on graceful shutdown
            try:
                snapshot = self.score_calibrator.snapshot()
                if snapshot and self.main:
                    await asyncio.wait_for(
                        self.main.set(key, json.dumps(snapshot), ex=ttl),
                        timeout=3.0,
                    )
                    logger.info("Score calibrator: final snapshot saved on shutdown (%d keys)", len(snapshot))
            except Exception:
                pass

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

        # Stagger by 15s to avoid resource contention with load_dynamic_symbols which also runs every 30s
        await asyncio.sleep(15.0)

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
                    # Use ML-dedicated isolated client
                    await gate.refresh_async(self.ml_gate_client)
                    dt = time.time() - t0
                    if dt > 0.5:
                        # P1: More descriptive logging for async refresh latency (SRE hint)
                        # ml_confirm_gate.py internal results are stored in gate properties
                        cfg_key = getattr(gate, "_cfg_key_used", "N/A")
                        cfg_src = getattr(gate, "_cfg_source", "N/A")
                        logger.warning(
                            "⚠️ ML gate async refresh took %.1fms (cfg: %s, src: %s)",
                            dt * 1000, cfg_key, cfg_src
                        )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in ML gate refresh loop: %s", e)
                # Don't crash loop, just sleep and retry
                await asyncio.sleep(interval)

    async def _health_contract_flush_loop(self) -> None:
        """
        Periodically flush exec_health_slo_contract state for the 'pipeline' scope.
        This prevents 'stale instance' alerts when no signals are being generated (P4.1).
        """
        interval = float(os.getenv("EXEC_HEALTH_BG_FLUSH_INTERVAL_SEC", "60.0"))
        logger.info("🚀 Starting health contract flush loop (interval=%.1fs)", interval)

        try:
            from services.orderflow.exec_health_slo_contract import flush_exec_health_contract_state_async
        except ImportError:
            logger.error("❌ Could not import flush_exec_health_contract_state_async; background health flush disabled")
            return

        while not self._shutdown:
            try:
                # Use force=True to ensure updated_ts_ms is pushed to Redis
                # even if no new signals were recorded in this interval.
                # Use dedicated isolated client to avoid pool contention.
                await asyncio.wait_for(
                    flush_exec_health_contract_state_async(
                        redis_client=self._health_redis_client,
                        scope="pipeline",
                        force=True
                    ),
                    timeout=5.0
                )
            except asyncio.CancelledError:
                break
            except (TimeoutError, ConnectionError, RedisError) as e:
                # Handle connection/timeout errors gracefully to avoid spamming (P4.1)
                # These are usually transient pool acquisition delays or network blips.
                sampled_warning(
                    logger, "HEALTH_FLUSH_TIMEOUT",
                    "⚠️ Health contract flush loop timeout/connection error: %s (using isolated pool)",
                    e, sample_rate=5
                )
            except Exception as e:
                log_silent_error(e, 'health_contract_flush_failure', 'pipeline', '_health_contract_flush_loop')

            await asyncio.sleep(interval)

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

        exc: BaseException | None = None
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
            with contextlib.suppress(Exception):
                log_silent_error(exc, 'task_crash', symbol, f'supervisor:{kind}', sample_rate=1)  # type: ignore
            tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))  # type: ignore
            logger.error("❌ (%s) %s task died. Restarting. err=%r\n%s", symbol, kind, exc, tb)
        else:
            logger.error("❌ (%s) %s task ended/cancelled. Restarting.", symbol, kind)

        new_task = safe_create_task(coro_factory(), name=task_name)
        return new_task  # type: ignore
  # type: ignore

    async def _stop_symbol(self, symbol: str) -> None:
        """
        Останавливает обработку конкретного символа.
        """
        # Mark symbol as unloaded first to prevent supervisor restarts during cancellation
        self.symbol_contexts.pop(symbol, None)

        # Cleanup caches to prevent leaks for unloaded symbols
        sym = (symbol or "").upper()
        self.tick_helpers.pop(sym, None)
        self.book_helpers.pop(sym, None)
        self.poison_pill_counts.pop(sym, None)
        self._rq_cache.pop(sym, None)
        self._adx_cache.pop(sym, None)
        for kind in ("ticks", "books"):
            self._task_restart_hist.pop((sym, kind), None)
            self._pel_cursor.pop((sym, kind), None)

        if hasattr(self, 'processor') and hasattr(self.processor, 'cleanup_symbol'):  # type: ignore
            self.processor.cleanup_symbol(sym)  # type: ignore
        if hasattr(self, 'strategy') and hasattr(self.strategy, 'cleanup_symbol'):  # type: ignore
            self.strategy.cleanup_symbol(sym)  # type: ignore
  # type: ignore
        tasks = self.symbol_tasks.pop(symbol, None)
        if tasks:
            tick_task, book_task = tasks
            tick_task.cancel()
            book_task.cancel()
            await asyncio.gather(tick_task, book_task, return_exceptions=True)
            logger.info("🛑 Символ %s остановлен и выгружен", symbol)



    async def _process_burst_flush(
        self,
        runtime: SymbolRuntime,
        trigger_source: str,
        ts_ms: int,
        do_publish: bool = True,
    ) -> dict | None:
        """Delegates to BurstFlusher.process()."""
        return await self._flusher.process(runtime, trigger_source, ts_ms, do_publish)

    def _build_redis_dq_snapshot(self, runtime: Any, *, now_ms: int) -> RedisDQSnapshot | None:  # type: ignore
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
                outbox_backlog = pending.qsize()
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






    async def _get_adx_cached(self, *, symbol: str, now_ms: int) -> float:
        """
        Read ADX14 from Redis:
          key = adx:{SYMBOL}
        Cache in-memory for adx_cache_ms (default 300ms).
        Fail-open: returns 0.0.
        """
        sym = (symbol or "").upper()
        if not sym:
            return 0.0
        cache_ms = int(os.getenv("ADX_CACHE_MS", "300"))
        cur = self._adx_cache.get(sym)
        if cur is not None:
            ts0, v0 = cur
            if 0 <= now_ms - ts0 <= cache_ms:
                return v0 or 0.0
        try:
            raw = await self.main.get(f"adx:{sym}")
            v = float(raw) if raw is not None else 0.0
            if v < 0:
                v = 0.0
            self._adx_cache[sym] = (now_ms, v)
            return v
        except Exception as exc:
            log_silent_error(exc, 'redis_read_failure', sym, '_get_adx_cached')
            return self._adx_cache.get(sym, (0, 0.0))[1] or 0.0


    # ── Основные рабочие циклы ────────────────────────────────────────────────

    async def consume_ticks(self, symbol: str) -> None:
        """
        Читает тики для указанного символа, запускает детекторы и публикует сигналы.
        """
        sampled_info(logger, "LOOP_DIAGNOSTIC", "🔄 (%s) Запуск цикла чтения тиков", symbol)
        _tc = self._svc_cfg.tick
        backoff = Backoff(base_delay=_tc.backoff_base, multiplier=2.0, max_delay=_tc.backoff_cap, jitter=_tc.backoff_jitter)
        idle_sleep = _tc.idle_sleep_sec
        msg_counter = 0

        # P0 FIX: Cache loop-level ENV variables ONCE before loop.
        # Eliminates ~3 os.getenv syscalls per iteration (thousands/sec).
        _cached_tick_sample_rate = _tc.sample_rate
        _cached_block_ms_env = str(_tc.read_block_ms)
        _cached_lag_tracker_max_ms = _tc.lag_tracker_max_ms

        # Initialize stream, group, and helper once before the loop
        stream = None
        group = None
        helper = None
        nogroup_retries = 0

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

            tick_sample_rate = _cached_tick_sample_rate

            # --- Corrected consume_ticks logic (Expert Fix) ---
            # ENV override wins over runtime.config (default reduced 250→100ms to lower H2 lag)
            block_ms = int(_cached_block_ms_env) if _cached_block_ms_env.strip().isdigit() else int(runtime.config.get("read_block_ms", 100))
            count = self._adaptive_tick_read_count(symbol, int(runtime.config.get("read_count", 200)))
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
                    continue
                elif is_timeout:
                    if redis_errors_total:
                        redis_errors_total.labels(op="read_ticks_timeout", symbol=symbol).inc()
                    delay = backoff.get_delay()
                    sampled_warning(logger, "REDIS_READ_TRANSIENT", "⚠️ (%s) Redis timeout reading ticks: %s (backoff=%.2fs)", symbol, error_str, delay)
                    await asyncio.sleep(delay)
                    continue
                else:
                    if redis_errors_total:
                        redis_errors_total.labels(op="read_ticks_connection", symbol=symbol).inc()
                    if is_transient_redis_error(exc):
                        delay = backoff.get_delay()
                        sampled_warning(logger, "REDIS_READ_TRANSIENT", "⚠️ (%s) Transient connection error: %s (backoff=%.2fs)", symbol, exc, delay)
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
                    nogroup_retries += 1
                    try:
                        await helper.ensure_group(stream, recreate=True)
                        delay = min(1.0 * nogroup_retries, 10.0)
                        await asyncio.sleep(delay)
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
                        sampled_warning(logger, "REDIS_READ_TRANSIENT", "⚠️ (%s) Transient ошибка чтения стрима [%s]: %s (backoff=%.2fs)", symbol, stream, exc, delay)
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error("❌ (%s) Ошибка чтения стрима [%s]: %s", symbol, stream, exc)
                        delay = backoff.get_delay()
                        await asyncio.sleep(delay)
                        continue
            except Exception as exc:
                if redis_errors_total: redis_errors_total.labels(op="read_ticks", symbol=symbol).inc()
                if is_transient_redis_error(exc):
                    delay = backoff.get_delay()
                    sampled_warning(logger, "REDIS_READ_TRANSIENT", "⚠️ (%s) Transient ошибка чтения стрима [%s]: %s (backoff=%.2fs)", symbol, stream, str(exc), delay)
                    await asyncio.sleep(delay)
                    continue
                else:
                    logger.error("❌ (%s) Критическая ошибка чтения стрима [%s]: %s", symbol, stream, exc, exc_info=True)
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

            # Reset NOGROUP retry count on successful read
            nogroup_retries = 0

            # --- Обработка батча тиков ---
            if messages:
                sampled_info(logger, "LOOP_DIAGNOSTIC", "📥 (%s) Read %d messages from stream", symbol, sum(len(entries) for _, entries in messages))
                if runtime.throttle_log_sampler.should_log("stream_processing"):
                    logger.debug("🔍 (%s) Processing %d stream entries", symbol, len(messages))

            sampled_ticks_dropped = 0
            for stream_name, entries in messages:
                ack_ids: list[str] = []
                entry_idx = 0
                for msg_id, fields in entries:
                    # Yield every tick — не блокируем event loop.
                    # History: %5 → %2 → %1 (this change). At %2 redis_entry_lag p99
                    # was 244ms (10× the 25ms SLO) on bursty batches of up to
                    # read_count=200. Unconditional yield bounds HOL to ~1 tick
                    # (~5ms), at the cost of +30µs scheduler overhead per tick.
                    await asyncio.sleep(0)
                    entry_idx += 1

                    # Sampling: быстрый путь без инициализации TickProcessor
                    if tick_sample_rate < 1.0 and not deterministic_sample(entry_idx, tick_sample_rate):
                        ack_ids.append(msg_id)
                        sampled_ticks_dropped += 1
                        with contextlib.suppress(Exception):
                            ticks_dropped_total.labels(symbol=symbol, reason="sampled").inc()
                        continue

                    # Делегируем полную обработку тика в TickProcessor
                    if await self._tick_proc.process_tick(
                        runtime, msg_id, fields, symbol,
                        lag_tracker_max_ms=_cached_lag_tracker_max_ms,
                    ):
                        ack_ids.append(msg_id)

                if sampled_ticks_dropped > 0 and runtime.throttle_log_sampler.should_log("tick_sampling"):
                    logger.info("🔪 (%s) Sampled and dropped %d ticks (rate=%.2f)", symbol, sampled_ticks_dropped, tick_sample_rate)

                # Batch ACK for throughput (XACK via pipeline)
                if ack_ids:
                    try:
                        await self._xack_pipeline(stream=stream_name, group=group, ids=ack_ids, symbol=symbol, op="ack_ticks")
                    except Exception as xack_exc:
                        logger.error("❌ (%s) Unexpected error in XACK pipeline: %s", symbol, xack_exc)
            backoff.reset()

    async def consume_books(self, symbol: str) -> None:
        """
        Читает книги заявок и обновляет состояния детекторов OBI/Iceberg.
        """
        _tc = self._svc_cfg.tick
        backoff = Backoff(base_delay=_tc.backoff_base, multiplier=2.0, max_delay=_tc.backoff_cap, jitter=_tc.backoff_jitter)
        idle_sleep = _tc.idle_sleep_sec
        nogroup_retries = 0
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
                # FIX: was block=1000ms default → event loop starvation. Use service config (default 50ms).
                _book_block_ms = int(runtime.config.get("read_block_ms", self._svc_cfg.tick.read_block_ms or 50))
                messages = await helper.read(
                    {stream: ">"},
                    count=runtime.config.get("read_count", 200),
                    block=_book_block_ms,
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
                    sampled_warning(logger, "REDIS_READ_TRANSIENT", "⚠️ (%s) Redis timeout reading books: %s (backoff=%.2fs)", symbol, error_str, delay)
                    await asyncio.sleep(delay)
                    continue
                else:
                    if redis_errors_total:
                        redis_errors_total.labels(op="read_books_connection", symbol=symbol).inc()
                    if is_transient_redis_error(exc):
                        delay = backoff.get_delay()
                        sampled_warning(logger, "REDIS_READ_TRANSIENT", "⚠️ (%s) Transient connection error reading books: %s (backoff=%.2fs)", symbol, exc, delay)
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
                    nogroup_retries += 1
                    try:
                        await helper.ensure_group(stream, recreate=True)
                    except RedisError as err:
                        logger.error("❌ (%s) Ошибка повторного создания book-группы: %s", symbol, err)

                    delay = min(1.0 * nogroup_retries, 10.0)
                    await asyncio.sleep(delay)
                    continue
                if is_transient_redis_error(exc):
                    delay = backoff.get_delay()
                    sampled_warning(logger, "REDIS_READ_TRANSIENT", "⚠️ (%s) Transient ошибка чтения книги: %s (backoff=%.2fs)", symbol, exc, delay)
                    await asyncio.sleep(delay)  # ← FIX: was missing sleep+continue
                    continue
                else:
                    logger.error("❌ (%s) Ошибка чтения книги: %s", symbol, exc)
                    await asyncio.sleep(1)
                    continue
            except RedisError as exc:
                if is_transient_redis_error(exc):
                    delay = backoff.get_delay()
                    sampled_warning(logger, "REDIS_READ_TRANSIENT", "⚠️ (%s) Redis transient при чтении книги: %s (backoff=%.2fs)", symbol, exc, delay)
                    await asyncio.sleep(delay)
                    continue
                else:
                    # After redis_errors.py fix, "Buffer is closed" now reaches is_transient → sampled_warning.
                    # This branch handles truly non-transient RedisErrors only.
                    logger.error("❌ (%s) Redis ошибка при чтении книги: %s", symbol, exc)
                    await asyncio.sleep(1)
                    continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                # Catch-all: asyncio transport errors (InvalidStateError, OSError) that
                # redis-py 4.x can raise without wrapping in RedisError on sudden TCP disconnect.
                if redis_errors_total:
                    redis_errors_total.labels(op="read_books_unexpected", symbol=symbol).inc()
                delay = backoff.get_delay()
                sampled_warning(
                    logger, "REDIS_BOOKS_UNEXPECTED",
                    "⚠️ (%s) Unexpected error reading books (type=%s, backoff=%.2fs): %s",
                    symbol, type(exc).__name__, delay, exc,
                )
                await asyncio.sleep(delay)
                continue

            if not messages:
                # --- Burst selection (Phase D): background flush on idle/books ---
                # Removed redundant book/idle flush (dup of _burst_flush_loop)
                backoff.reset()
                if idle_sleep > 0:
                    await asyncio.sleep(idle_sleep)
                continue

            for stream_name, entries in messages:
                ack_ids: list[str] = []
                for msg_id, payload in entries:
                    try:
                        # Extract timestamp from msg_id for ingest_ts_ms
                        ingest_ts_ms = 0
                        try:
                            ingest_ts_ms = int(msg_id.split("-")[0])
                        except Exception:
                            ingest_ts_ms = get_ny_time_millis()

                        await self.strategy.process_book(runtime, payload, ingest_ts_ms)  # type: ignore
                    except Exception as exc:  # noqa: BLE001  # type: ignore
                        logger.exception("❌ (%s) Ошибка обработки книги %s: %s", symbol, msg_id, exc)
                    finally:
                        ack_ids.append(msg_id)
                # Batch ACK for throughput (XACK via pipeline)
                if ack_ids:
                    try:
                        await self._xack_pipeline(stream=stream_name, group=group, ids=ack_ids, symbol=symbol, op="ack_books")
                    except Exception as xack_exc:
                        logger.error("❌ (%s) Unexpected error in XACK books pipeline: %s", symbol, xack_exc)
            nogroup_retries = 0
            backoff.reset()

    # ── Обработка тиков и генерация сигналов ──────────────────────────────────



    async def _resolve_streams(self, symbol: str) -> tuple[str, str]:
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

    async def _first_existing_stream(self, candidates: list[str]) -> str | None:
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
            return int(msg_id.split("-", 1)[0])
        except Exception:
            return 0

    def _coerce_event_ts_ms(self, *, msg_id: str, payload_ts_ms: int, now_ms: int) -> tuple[int, str]:
        """Choose deterministic event time:
        1) tick.ts_ms (if sane)
        2) Redis msg_id ms
        3) wall clock (last resort)
        """
        ts = _safe_int(payload_ts_ms or 0)
        if ts > 0 and abs(now_ms - ts) <= self._max_ts_skew_ms:
            return ts, "payload"
        mid = self._msgid_ms(msg_id)
        if mid > 0:
            return mid, "stream_id"
        return now_ms, "now"

    async def _xack_pipeline(self, *, stream: str, group: str, ids: list[str], symbol: str, op: str) -> None:
        """Ack many ids using pipeline, chunked.

        P1-4: This method routes failed batches to `stream:signals:ack-dlq`.

        Transient connection errors ("Connection lost", TimeoutError, ECONNRESET,
        EOF, etc.) are retried per-chunk with exponential backoff + jitter
        before the batch is DLQ'd. Classifier is the project-wide
        `is_transient_redis_error` (common.redis_errors), so this loop picks up
        the same error taxonomy as the rest of the pipeline. Non-transient
        errors abort the retry loop immediately.

        Env (optional):
          CRYPTO_OF_XACK_RETRIES       default 3
          CRYPTO_OF_XACK_BACKOFF_MS    default 100 (first sleep; doubles each attempt)
        """
        if not ids:
            return
        batch = self._ack_batch or 0
        if batch <= 0:
            batch = 200

        max_retries = self._xack_retries
        base_backoff_ms = self._xack_backoff_ms

        for i in range(0, len(ids), batch):
            chunk = ids[i:i + batch]
            if not chunk:
                continue

            success = False
            last_exc: BaseException | None = None
            attempts_made = 0

            for attempt in range(max_retries + 1):
                attempts_made = attempt + 1
                try:
                    # P0: Hard timeout for XACK execution to prevent infinite wait in saturated pool
                    # Increased to 20.0s to accommodate REDIS_SOCKET_CONNECT_TIMEOUT (5s) retries during a storm
                    await asyncio.wait_for(self.ticks.xack(stream, group, *chunk), timeout=20.0)
                    success = True
                    break
                except Exception as exc:
                    last_exc = exc
                    if not is_transient_redis_error(exc) or attempt >= max_retries:
                        break
                    # Exponential backoff with ±25% jitter. Jitter de-synchronises
                    # retries across symbols when redis-ticks recovers, so the
                    # reconnect storm doesn't saturate the pool again.
                    delay_ms = base_backoff_ms * (2 ** attempt)
                    jitter = delay_ms * 0.25
                    delay_s = max(0.0, (delay_ms + random.uniform(-jitter, jitter)) / 1000.0)
                    if redis_errors_total:
                        with contextlib.suppress(Exception):
                            redis_errors_total.labels(op=f"{op}_retry", symbol=symbol).inc()
                    await asyncio.sleep(delay_s)

            if not success and last_exc is not None:
                exc = last_exc
                if redis_errors_total:
                    redis_errors_total.labels(op=op, symbol=symbol).inc()

                # Pool usage diagnostics (Best effort)
                pool_info = "unknown"
                try:
                    pool = getattr(self.ticks, "connection_pool", None)
                    if pool:
                        in_use = len(getattr(pool, "_in_use_connections", []))
                        max_c = getattr(pool, "max_connections", 0)
                        pool_info = f"{in_use}/{max_c}"
                except Exception:
                    pass

                # Downgrade from CRITICAL to ERROR to reduce PagerDuty spam for transient network blips.
                # The PEL sweeper eventually reclaims.
                logger.error(
                    "❌ (%s) XACK FAILURE (stream=%s group=%s n=%d pool=%s attempts=%d): %s | %s",
                    symbol, stream, group, len(chunk), pool_info, attempts_made, type(exc).__name__, repr(exc),
                )

                # DLQ routing (fail-safe and completely non-blocking)
                error_str = repr(exc)

                async def _write_xack_dlq(failed_chunk=chunk, fail_err=error_str):
                    try:
                        dlq_payload = {
                            "symbol": symbol,
                            "stream": stream,
                            "group": group,
                            "ids": ",".join(failed_chunk[:100]), # Limit size if massive
                            "error": fail_err,
                            "ts_ms": str(get_ny_time_millis())
                        }
                        await asyncio.wait_for(
                            self.main.xadd(RS.SIGNAL_ACK_DLQ, dlq_payload, maxlen=STREAM_RETENTION.get(RS.SIGNAL_ACK_DLQ, 10000), approximate=True),  # type: ignore
                            timeout=5.0
                        )
                    except asyncio.CancelledError:
                        pass
                    except Exception as dlq_exc:
                        logger.warning("⚠️ (%s) Failed to write XACK failure to DLQ: %s", symbol, dlq_exc)

                safe_create_task(_write_xack_dlq(), name=f"crypto-of-xack-dlq-{symbol}")

                # DO NOT raise RuntimeError here!
                # Messages that failed to ACK are left in PEL and will be recovered by the background PEL sweeper.

    async def _pel_sweeper_loop(self) -> None:
        """Optionally drains PEL to avoid 'black-hole' pending messages.
        Default behavior: XAUTOCLAIM + ACK (optionally quarantine), without reprocessing.
        """
        interval = float(os.getenv("CRYPTO_OF_PEL_SWEEP_INTERVAL_SEC", "10"))
        min_idle_ms = int(os.getenv("CRYPTO_OF_PEL_MIN_IDLE_MS", "60000"))
        count = int(os.getenv("CRYPTO_OF_PEL_COUNT", "200"))
        quarantine = self._env_bool("CRYPTO_OF_PEL_QUARANTINE", "false")

        while not self._shutdown:
            await asyncio.sleep(interval)
            if self._shutdown:
                break
            try:
                runtimes = list(self.symbol_contexts.values())

                async def _sweep_symbol(rt):
                    if self._shutdown:
                        return
                    sym = str(getattr(rt, "symbol", "") or "").upper()
                    if not sym:
                        return

                    async def _sweep_kind(kind: str):
                        if kind == "ticks":
                            stream = getattr(rt, "tick_stream", None)
                            group = getattr(rt, "tick_group", None)
                            consumer = self.consumer_id_ticks
                        else:
                            stream = getattr(rt, "book_stream", None)
                            group = getattr(rt, "book_group", None)
                            consumer = self.consumer_id_books

                        if not stream or not group:
                            return

                        async with self._bootstrap_sem:
                            key = (sym, kind)
                            start_id = self._pel_cursor.get(key, "0-0")
                            try:
                                res = await self.ticks.xautoclaim(
                                    stream, group, consumer,
                                    min_idle_ms, start_id, count=count
                                )
                                if not res:
                                    return

                                # redis-py: (next_start_id, messages, deleted_ids)
                                next_id = res[0] if isinstance(res, (list, tuple)) and len(res) > 0 else start_id
                                msgs = res[1] if isinstance(res, (list, tuple)) and len(res) > 1 else []
                                self._pel_cursor[key] = next_id or start_id

                                if not msgs:
                                    return

                                if pel_autoclaim_total:
                                    pel_autoclaim_total.labels(symbol=sym, kind=kind).inc(len(msgs))

                                ack_ids: list[str] = []
                                for msg_id, fields in msgs:
                                    ack_ids.append(msg_id)
                                    if quarantine:
                                        with contextlib.suppress(Exception):
                                            await self.ticks.xadd(self.quarantine_stream, {
                                                "symbol": sym,
                                                "msg_id": msg_id,
                                                "reason": f"pel_autoclaim:{kind}",
                                                "payload": json.dumps(fields, default=str)[:1000],
                                                "ts_ms": str(self._msgid_ms(msg_id) or get_ny_time_millis()),
                                            }, maxlen=50000)  # type: ignore

                                await self._xack_pipeline(stream=stream, group=group, ids=ack_ids, symbol=sym, op=f"ack_pel_{kind}")
                            except Exception as exc:
                                if random.random() < 0.05:
                                    logger.warning("⚠️ (%s:%s) PEL sweep failed: %s", sym, kind, exc)

                    await asyncio.gather(_sweep_kind("ticks"), _sweep_kind("books"))

                if runtimes and self.ticks is not None:
                    await asyncio.gather(*[_sweep_symbol(rt) for rt in runtimes])

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
                client.close()  # type: ignore
                await client.wait_closed()  # type: ignore
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
