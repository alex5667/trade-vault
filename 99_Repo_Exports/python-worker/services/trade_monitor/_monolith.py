# services/trade_monitor_service.py
from __future__ import annotations

import bisect
import collections
# ruff: noqa: E501, S110, E402, UP037, I001
import contextlib
import json
import os
import re
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from typing import Any

from prometheus_client import Counter, Gauge, Histogram

from domain.evidence_keys import MetaKeys
from domain.timebucket_snapshots import attach_timebucket_snapshots_to_closed
from utils.time_utils import get_ny_time_millis

try:
    from sortedcontainers import SortedList
    _SORTED_CONTAINERS_AVAILABLE = True
except ImportError:  # pragma: no cover
    _SORTED_CONTAINERS_AVAILABLE = False
    SortedList = list  # type: ignore

from common.log import setup_logger
from core.redis_client import get_redis
from core.redis_keys import RedisStreams as RS
from domain.models import PositionState, SignalNorm, TradeClosed, TradeEvent
from services.atr_horizon_trailing_canary import should_apply_trailing_surface
from services.atr_horizon_trailing_surface import build_trailing_surface
from services.atr_policy_rollout_router import build_rollout_sticky_key, should_apply_rollout
from services.atr_promotion_policy_resolver import get_active_policy
from services.horizon_contract import (
    apply_position_horizon_scalars_from_hash,
    build_horizon_event_scalars,
    hydrate_position_from_signal_payload,
    stamp_closed_trade_horizon_from_position,
    stamp_position_from_signal_payload,
)
from services.pnl_math import SymbolSpec, get_symbol_info, spec_from_symbol_info


# Define logging callback for futures
def _log_future_exception(fut):
    try:
        exc = fut.exception()
        if exc:
            import traceback
            tb_str = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            logger.error("Async DB task failed: %s\nTraceback:\n%s", exc, tb_str)
    except Exception:
        pass
from domain.handlers import apply_trailing_update, create_position, finalize_trade, maybe_arm_trailing_after_tp1, process_tick
from domain.normalizers import canon_source, canon_strategy, canon_symbol, canon_tf
from domain.position_fsm import PositionFSM, PositionStatus, fsm_from_position
from domain.tick_price import build_tick
from infra.order_schema import (
    extract_profile,
    extract_tp_fills,
    extract_tp_levels,
    normalize_side,
    parse_json_dict,
)

# ----------------- Prometheus Metrics (Module Level) -----------------
# We define metrics at the module level to avoid "Duplicated timeseries" error
# when TradeMonitorService is instantiated multiple times (e.g. in Actor Runtime shards).
TM_ORPHANS_FORCE_CLOSED = Counter(
    "orphans_force_closed_total",
    "Total number of positions force closed by orphan housekeep",
    ["symbol"],
)
TM_OPEN_POSITIONS = Gauge(
    "open_positions_count",
    "Number of currently open positions",
    ["symbol"],
)
TM_VIRTUAL_POSITIONS = Gauge(
    "virtual_positions_count",
    "Number of currently open virtual positions",
    ["symbol"],
)
TM_TICK_LATENCY_US = Histogram(
    "tick_processing_time_us",
    "Latency of on_tick processing in microseconds",
    ["symbol"],
    buckets=[100, 500, 1000, 5000, 10000, 50000]
)
TM_ORPHAN_CLEANUP_DURATION_MS = Gauge(
    "tm_orphan_cleanup_duration_ms",
    "Duration of the orphan housekeep sweep in milliseconds"
)

# [NEW] Backpressure metrics
TM_RG_PERSIST_PENDING = Gauge(
    "tm_regime_guard_persist_pending",
    "Number of RegimeGuard persist tasks currently in queue"
)
TM_RG_PERSIST_DROPPED = Counter(
    "tm_regime_guard_persist_dropped_total",
    "Total number of RegimeGuard persist tasks dropped due to backpressure",
    ["family", "venue"]
)
TM_RG_PERSIST_SUBMITTED = Counter(
    "tm_regime_guard_persist_submitted_total",
    "Total number of RegimeGuard persist tasks successfully submitted",
    ["family", "venue"]
)
TM_RG_PERSIST_FAILED = Counter(
    "tm_regime_guard_persist_failed_total",
    "Total number of RegimeGuard async persist tasks that failed",
    ["family", "venue"]
)
# Single-active-position guard — signals blocked in trade_monitor
TM_SIGNAL_BLOCKED_SINGLE_ACTIVE = Counter(
    "tm_signal_blocked_single_active_position_total",
    "Signals blocked by EXEC_SINGLE_ACTIVE_POSITION_PER_SYMBOL guard in trade_monitor",
    ["symbol"],
)
# Stale guard bypass — guard too old, signal allowed through
TM_SIGNAL_GUARD_STALE_BYPASS = Counter(
    "tm_signal_guard_stale_bypass_total",
    "Signals allowed through because the active guard exceeded stale timeout",
    ["symbol"],
)
TM_SIMULATED_SLIPPAGE_BPS = Histogram(
    "tm_simulated_slippage_bps",
    "Simulated slippage applied to paper trade entry prices (bps)",
    ["symbol"],
    buckets=[0, 1, 2, 4, 6, 8, 10, 15, 20],
)
EXEC_SLIPPAGE_BPS = Histogram(
    "trading_exec_slippage_bps",
    "Execution slippage in basis points",
    ["symbol"],
    buckets=[0, 1, 2, 4, 6, 8, 10, 15, 20, 30, 50]
)
# Возраст тика при обработке (задержка ingestion → Python)
TM_TICK_AGE_MS = Histogram(
    "tm_tick_age_ms",
    "Возраст тика при обработке (now - tick.ts_ms), мс",
    ["symbol"],
    buckets=[0, 5, 10, 50, 100, 500, 1000, 5000],
)
# [NEW] Versioning metrics
TM_SIGNAL_VERSION_MISMATCH = Counter(
    "tm_signal_version_mismatch_total",
    "Signals rejected due to DTO version mismatch (expected v: 1)",
    ["symbol"],
)
# [NEW] Duplicate signal metrics
TM_SIGNAL_DUPLICATE = Counter(
    "tm_signal_duplicate_total",
    "Signals ignored as duplicates in trade_monitor",
    ["symbol", "reason"]
)
TIME_BE_EXIT_DECISIONS_TOTAL = Counter(
    "time_be_exit_decisions_total",
    "Total number of TIME_BE_EXIT decisions",
    ["symbol", "reason", "mode"]
)
TIME_BE_EXIT_CLOSES_TOTAL = Counter(
    "time_be_exit_closes_total",
    "Total number of TIME_BE_EXIT actual closes",
    ["symbol", "reason"]
)
TIME_BE_EXIT_SHADOW_WOULD_CLOSE_TOTAL = Counter(
    "time_be_exit_shadow_would_close_total",
    "Total number of TIME_BE_EXIT would-closes in SHADOW mode",
    ["symbol", "reason"]
)

TM_JITTER_BUFFER_SIZE = Gauge(
    "trade_monitor_jitter_buffer_size", "Current number of signals in jitter buffer"
)

TM_JITTER_RELEASE_LATENCY_MS = Histogram(
    "trade_monitor_jitter_release_latency_ms",
    "Delay introduced by jitter buffer for signals (event_ts vs release_ts)",
    ["symbol"],
    buckets=(10, 20, 50, 100, 250, 500, 1000, 5000),
)

# ── Max-hold Timeout Close metrics ─────────────────────────────────────
TM_TIMEOUT_EVAL_TOTAL = Counter(
    "tm_timeout_eval_total",
    "Max-hold timeout evaluations",
    ["symbol", "decision", "reason"],
)
TM_TIMEOUT_CLOSE_REQUESTED_TOTAL = Counter(
    "tm_timeout_close_requested_total",
    "Real timeout close commands published to orders:queue",
    ["venue", "symbol", "reason"],
)
TM_TIMEOUT_CLOSE_DEDUP_TOTAL = Counter(
    "tm_timeout_close_dedup_total",
    "Duplicate timeout close commands suppressed by idempotency key",
    ["symbol", "reason"],
)
TM_ORPHAN_CLEANUP_TOTAL = Counter(
    "tm_orphan_cleanup_total",
    "Local orphan cleanup events (ORPHAN_CLEANUP_* codes)",
    ["symbol", "reason"],
)
TM_TIMEOUT_POSITION_AGE_MS = Histogram(
    "tm_timeout_position_age_ms",
    "Position age at max-hold timeout evaluation",
    ["symbol"],
    buckets=[60_000, 120_000, 300_000, 900_000, 3_600_000, 14_400_000],
)
# --------------------------------------------------------------------

from datetime import UTC

from infra.redis_repo import RedisTradeRepository
from services import analytics_db
from services.batch_trade_writer import get_batch_writer
from services.trade_events_logger import TradeEventsLogger

logger = setup_logger("TradeMonitorService")

try:
    # Fail-open: метрики не должны ломать trading loop.
    from common.metrics2 import NoopMetrics  # type: ignore
except Exception:  # pragma: no cover
    NoopMetrics = None  # type: ignore


@dataclass(frozen=True)
class _IOTask:
    """
    Отложенная I/O операция, выполняемая ВНЕ self._lock.
    Важно: fn не должна требовать удержания self._lock.
    """
    fn: Callable[[], None]
    desc: str


@dataclass
class _TickIOBatch:
    """
    Collected I/O actions to be executed OUTSIDE the global lock.
    Important:
      - must contain only primitives / immutable objects
      - must be safe if position is later mutated by other threads
    """
    # append-only trade events (already contain copies of fields)
    events: list[Any] = field(default_factory=list)
    # fast TP-hit persistence (primitives)
    tp_hits: list[dict[str, Any]] = field(default_factory=list)
    # trailing move/sync persistence (primitives)
    trailing_moves: list[dict[str, Any]] = field(default_factory=list)
    trailing_syncs: list[dict[str, Any]] = field(default_factory=list)
    # closed trade persistence
    closed: Any | None = None
    # final cleanup needs these
    close_pos_id: str | None = None
    close_sid: str | None = None
    close_source: str | None = None
    close_symbol: str | None = None
    # stats update uses snapshots (immutable dict copies)
    pos_snapshot: dict[str, Any] | None = None
    closed_snapshot: dict[str, Any] | None = None




def _canon_regime(v: Any) -> str:
    """
    Canonical regime label for persistence/segmentation.
    Unifies 'none', 'unknown', 'na' into 'na'.
    """
    from contexts import normalize_regime_label
    return normalize_regime_label(v)


def _extract_regime_from_signal(sig: Any) -> str:
    """
    Best-effort regime-at-signal extraction from normalized signal object.
    We do NOT assume exact field names across versions.
    """
    for k in ("entry_regime", "regime", "market_regime", "regime_label"):
        try:
            v = getattr(sig, k, None)
        except Exception:
            v = None
        if v:
            return _canon_regime(v)
    # sometimes nested: sig.meta / sig.ctx / sig.extras
    for k in ("meta", "ctx", "extras", "extra", "data"):
        try:
            obj = getattr(sig, k, None)
        except Exception:
            obj = None
        if isinstance(obj, dict):
            for kk in ("entry_regime", "regime", "market_regime", "regime_label"):
                if obj.get(kk):
                    return _canon_regime(obj.get(kk))
    return "na"



def _ev_open(pos: PositionState) -> TradeEvent:
    return TradeEvent(
        event_type="OPEN",
        order_id=pos.id,
        sid=pos.sid,
        strategy=pos.strategy,
        source=pos.source,
        symbol=pos.symbol,
        tf=pos.tf,
        direction=pos.direction,
        ts_ms=pos.entry_ts_ms,
        payload={
            "sl": pos.sl,
            "tp_levels": pos.tp_levels,
            "entry_price": pos.entry_price,
            "lot": pos.lot,
            "signal_payload": pos.signal_payload,
            "risk_horizon_bucket": getattr(pos, "risk_horizon_bucket", "unknown"),
            "atr_tf_ms": getattr(pos, "atr_tf_ms", 0),
        },
    )


def _ev_tp1_hit_external(
    pos: PositionState,
    fill_price: float,
    closed_qty: float,
    ts_ms: int,
    tp_level: int = 1,
) -> TradeEvent:
    return TradeEvent(
        event_type=f"TP{tp_level}_HIT",
        order_id=pos.id,
        sid=pos.sid,
        strategy=pos.strategy,
        source=pos.source,
        symbol=pos.symbol,
        tf=pos.tf,
        direction=pos.direction,
        ts_ms=ts_ms,
        payload={
            "tp_level": tp_level,
            "fill_price": fill_price,
            "closed_qty": closed_qty,
            "pnl_part_gross": 0.0,
            "external": True,
        },
    )


def _apply_entry_regime_to_position(pos: Any, regime: str) -> None:
    """
    Persist regime-at-entry on the position object.
    This is the single source of truth for EV stats segmentation.
    Fail-open: never break open flow.
    """
    if not regime or regime == "na":
        return
    try:
        pos.entry_regime = regime
        # alias (many parts of pipeline already look at pos.regime)
        if getattr(pos, "regime", None) in (None, "", "na"):
            pos.regime = regime
    except Exception:
        return


def _normalize_side(v: Any) -> str:
    """
    Normalize direction to the domain canonical runtime representation: 'LONG' / 'SHORT'.
    Accepts: 'LONG', 'SHORT', 'long', 'short', enum-like objects.
    """
    if v is None:
        return "LONG"
    s = str(v).strip()
    sl = s.lower()
    if sl in ("long", "buy"):
        return "LONG"
    if sl in ("short", "sell"):
        return "SHORT"
    # If it's already 'LONG'/'SHORT' or unknown, keep upper-case as a best-effort.
    su = s.upper()
    return su if su in ("LONG", "SHORT") else "LONG"

def parse_open_position_hash(
    h: dict[str, str],
    *,
    to_int_ms,
    logger=None,
) -> PositionState | None:
    """
    Pure parser for recovery. Extracted from TradeMonitorService._position_from_hash()
    to be unit-testable without constructing the full service.
    """
    try:
        if h.get("status") != "open":
            return None

        # --- TP levels: prefer JSON array, fallback to legacy tp1/tp2/tp3 ---
        tp_levels = []
        if h.get("tp_levels"):
            try:
                tp_levels = json.loads(h["tp_levels"])
            except Exception:
                tp_levels = []
        if not tp_levels:
            tp_levels = [float(h.get("tp1") or 0), float(h.get("tp2") or 0), float(h.get("tp3") or 0)]
        tp_levels = [float(x) for x in tp_levels if float(x) > 0][:3]

        pos = PositionState(  # type: ignore
            id=(h.get("id")),
            sid=(h.get("sid") or ""),
            strategy=(h.get("strategy") or "unknown"),
            source=(h.get("source") or "Unknown"),
            symbol=(h.get("symbol") or "UNKNOWN"),
            tf=(h.get("tf") or "tick"),
            # direction can come in multiple formats across components; normalize for stability.
            direction=_normalize_side(h.get("direction") or "LONG"),
            entry_price=float(h.get("entry_price") or 0.0),
            entry_ts_ms=to_int_ms(h.get("entry_ts_ms") or h.get("entry_time"), 0),
            lot=float(h.get("lot") or 0.0),
            remaining_qty=float(h.get("remaining_qty") or h.get("lot") or 0.0),
            sl=float(h.get("sl") or 0.0),
            tp_levels=tp_levels,
            tp_hits=int(float(h.get("tp_hits") or 0)),
            tp1_hit=(h.get("tp1_hit") or "0") == "1",
            tp2_hit=(h.get("tp2_hit") or "0") == "1",
            tp3_hit=(h.get("tp3_hit") or "0") == "1",
            trailing_started=(h.get("trailing_started") or "0") == "1",
            trailing_active=(h.get("trailing_active") or "0") == "1",
            trailing_moves_count=int(float(h.get("trailing_moves") or 0)),
            trailing_distance=float(h.get("trailing_distance") or 0.0),
            trailing_point=float(h.get("trailing_point") or 0.0),
            max_favorable_price=float(h.get("max_favorable_price") or 0.0),
            max_favorable_ts=to_int_ms(h.get("max_favorable_ts"), 0),
            atr=float(h.get("atr") or 0.0),
            is_virtual=(h.get("is_virtual") or "0") == "1",
            v_gate_status=(h.get("v_gate_status") or "na"),
            v_gate_reason=(h.get("v_gate_reason") or ""),
            # FIX 2026-05-14: restore one_r_money on recovery
            one_r_money=float(h.get("one_r_money") or 0.0),
        )

        # Optional fields (best-effort)
        try:
            pos.entry_tag = (h.get("entry_tag") or "")

            # p0_ metadata from hash (if saved by new worker)
            pos.p0_signal_id = h.get("p0_signal_id") or h.get("sid")
            pos.p0_regime = h.get("p0_regime")
            pos.p0_scenario = h.get("p0_scenario")
            pos.p0_session = h.get("p0_session")
            pos.p0_entry_reason = h.get("p0_entry_reason") or pos.entry_tag

            if h.get("p0_spread_bps"):
                pos.p0_spread_bps_at_entry = float(h["p0_spread_bps"])
            if h.get("p0_book_age_ms"):
                pos.p0_book_age_ms = int(h["p0_book_age_ms"])
            if h.get("p0_features_json"):
                with contextlib.suppress(Exception):
                    pos.p0_features_snapshot = json.loads(h["p0_features_json"])

            # Back-compat for excursion timestamps/prices if missing
            if not pos.max_favorable_price:
                pos.max_favorable_price = pos.entry_price
            if not pos.max_adverse_price:
                pos.max_adverse_price = pos.entry_price
            if getattr(pos, "max_favorable_ts_ms", 0) == 0:
                pos.max_favorable_ts_ms = pos.entry_ts_ms
            if getattr(pos, "max_adverse_ts_ms", 0) == 0:
                pos.max_adverse_ts_ms = pos.entry_ts_ms

            # Profile aliasing:
            # - old/open records: trail_profile
            # - some writers/readers: trailing_profile
            pos.trail_profile = (h.get("trail_profile") or h.get("trailing_profile") or "")

            pos.trailing_min_lock_r = float(h.get("trailing_min_lock_r") or 0.0)
            pos.min_lock_price = float(h.get("min_lock_price") or 0.0)
            pos.baseline_mode = (h.get("baseline_mode") or pos.baseline_mode)
            pos.baseline_horizon_ms = to_int_ms(h.get("baseline_horizon_ms"), pos.baseline_horizon_ms)
            pos.baseline_sl = float(h.get("baseline_sl") or pos.baseline_sl or pos.sl)
            pos.baseline_tp1 = float(h.get("baseline_tp1") or pos.baseline_tp1 or (pos.tp_levels[0] if pos.tp_levels else 0.0))
            # BUGFIX: baseline_tp2/tp3 must not fallback to baseline_tp1 (typo in old code).
            pos.baseline_tp2 = float(h.get("baseline_tp2") or pos.baseline_tp2 or (pos.tp_levels[1] if len(pos.tp_levels) > 1 else 0.0))
            pos.baseline_tp3 = float(h.get("baseline_tp3") or pos.baseline_tp3 or (pos.tp_levels[2] if len(pos.tp_levels) > 2 else 0.0))

            # P41 compliance (native meta)
            pos.meta_enforce_cov_bucket = (h.get(MetaKeys.ENFORCE_COV_BUCKET) or "")
            if h.get(MetaKeys.ENFORCE_APPLIED):
                try:
                    pos.meta_enforce_applied = int(float(h[MetaKeys.ENFORCE_APPLIED]))
                except (ValueError, TypeError):
                    pos.meta_enforce_applied = -1
        except Exception:
            pass

        # Phase 0.3: scalar-first recovery from hash fields (independent of signal_payload JSON).
        with contextlib.suppress(Exception):
            apply_position_horizon_scalars_from_hash(pos, h, source="pure_hash_recovery")

        # Phase 0.2/0.3: then hydrate from signal_payload if present (enriches nested contract).
        try:
            if h.get("signal_payload"):
                pos.signal_payload.update(parse_json_dict(h.get("signal_payload")))
            hydrate_position_from_signal_payload(pos, source="pure_hash_recovery")
        except Exception:
            pass

        return pos
    except Exception as e:
        if logger:
            logger.warning(f"Failed to recover position from hash: {e}")
        return None


class TradeMonitorService:
    # Class-level defaults for mock/spec compatibility
    _max_tick_ts_ms: int = 0
    redis: Any  # sync redis.Redis client (decode_responses=True)

    def __init__(
        self,
        redis_url: str | None = None,
        config: dict[str, Any] | None = None,
        regime_guard=None,
        health_metrics=None,
        *,
        redis_client=None,
        repo=None,
        metrics=None,
        atr_cache=None,
    ):
        import redis as redis_lib
        # ------------------------------------------------------------
        # Redis client injection for tests / local harness.
        # decode_responses=True is assumed across python-worker.
        # ------------------------------------------------------------
        if redis_client is not None:
            self.redis = redis_client
        else:
            self.redis = redis_lib.from_url(redis_url, decode_responses=True) if redis_url else get_redis()

        # unify logger access (code uses both logger and self.logger in places)
        self.logger = logger
        self.atr_cache = atr_cache

        # ------------------------------------------------------------------
        # Jitter Resilience (Phase 2): Ingestion & Simulation Sync
        # Using a Timestamp-Sorted buffer to ensure ticks are processed
        # BEFORE signals with the same or later timestamps.
        # ------------------------------------------------------------------
        self._signal_buffer: list[dict[str, Any]] = []
        # SIGNAL_JITTER_BUFFER_MS:
        #   - Live: 50-100ms (standard network jitter)
        #   - Simulation: 0 (deterministic playback order)
        self._jitter_ms = int(os.getenv("SIGNAL_JITTER_BUFFER_MS", "50"))
        self._is_sim = os.getenv("SIMULATION_MODE") == "1"
        if self._is_sim:
            self._jitter_ms = 0

        # Trade Events Logger for AB/Backtest
        try:
            # use redis_url from constructor OR from redis client if possible
            self.events_logger = TradeEventsLogger(redis_url)
        except Exception:
            self.events_logger = None

        # ---------------- Metrics (fail-open) ----------------
        if metrics is not None:
            self._metrics = metrics
        else:
            self._metrics = NoopMetrics() if NoopMetrics else None
        # Инъекция provider-а health snapshot:
        # repo.save_closed НЕ создаёт HealthMetrics и НЕ открыват коннекты.
        self.health_metrics = health_metrics
        # Repo injection for tests
        self.repo = repo if repo is not None else RedisTradeRepository(self.redis, health_provider=self._get_health_snapshot)
        self.regime_guard = regime_guard
        self.config = config or {}

        # Инициализируем критические атрибуты для совместимости
        # Thread safety - нужен всегда
        self._lock = threading.RLock()

        # ------------------------------------------------------------------
        # Per-symbol locks:
        #  - цель: не держать глобальный self._lock на время Redis/DB I/O
        #  - сериализация: только внутри одного symbol (tick-loop)
        #  - другие symbol могут обрабатываться параллельно
        # Rollback: TM_USE_SYMBOL_LOCKS=0 (вернет поведение ближе к прежнему, но I/O всё равно вне _lock)
        # ------------------------------------------------------------------
        self._use_symbol_locks = os.getenv("TM_USE_SYMBOL_LOCKS", "1") == "1"
        self._symbol_locks_guard = threading.Lock()
        self._symbol_locks: dict[str, threading.RLock] = {}

        # Executor for blocking DB IO (regime guard + reports) — analytics writes go through BatchTradeWriter now.
        # max_workers reduced: analytics writes are handled by BatchTradeWriter's single daemon thread.
        self._db_executor = ThreadPoolExecutor(
            max_workers=int(os.getenv("TM_DB_WORKERS", "2")),
            thread_name_prefix="TM_DB",
        )

        # BatchTradeWriter — заменяет одиночные INSERT через ThreadPoolExecutor.
        # Один daemon-поток накапливает сделки и делает batch execute_values каждую секунду.
        # Rollback: BATCH_WRITER_ENABLED=0 → синхронный INSERT (старое поведение).
        self._batch_writer = get_batch_writer()

        # RegimeGuard async persist (optional): avoid blocking tick/close path on PG.
        # Backpressure is mandatory to prevent unbounded executor queue growth when PG is slow/down.
        # Note: we use a dedicated executor (default 1 worker) to preserve ordering and isolate from analytics DB writes.
        self._rg_async_persist = os.getenv("TM_RG_ASYNC_PERSIST", "1") == "1"
        self._rg_max_pending = int(os.getenv("TM_RG_MAX_PENDING", "2000"))
        if self._rg_max_pending < 1:
            self._rg_max_pending = 1
        self._rg_db_max_workers = int(os.getenv("TM_RG_DB_MAX_WORKERS", "1"))
        if self._rg_db_max_workers < 1:
            self._rg_db_max_workers = 1
        self._rg_db_executor = ThreadPoolExecutor(
            max_workers=self._rg_db_max_workers,
            thread_name_prefix="TM_RG_DB",
        ) if self._rg_async_persist else None
        self._rg_persist_sem = threading.BoundedSemaphore(self._rg_max_pending) if self._rg_async_persist else None
        self._rg_pending_guard = threading.Lock()
        self._rg_pending = 0

        # Sharded storage (Symbol -> {PosID: PositionState})
        # This allows O(1) access to positions of a specific symbol without iterating all open positions.
        self.shards: dict[str, dict[str, PositionState]] = collections.defaultdict(dict)
        self.symbol_by_pos_id: dict[str, str] = {} # PosID -> Symbol mapping

        # ------------------------------------------------------------------
        # P1-9: Explicit FSM map.
        # Maps pos.id -> PositionFSM.  Guarded by same locks as open_positions.
        # ENV: FSM_ENABLED=1 (default) — disable with FSM_ENABLED=0 for rollback.
        # ------------------------------------------------------------------
        self._fsm_enabled = os.getenv("FSM_ENABLED", "1") == "1"
        self._fsm_map: dict[str, Any] = {}  # pos_id -> PositionFSM

        # SortedList price index для O(log N) pre-filter:
        # _sl_index[symbol] = SortedList[(sl_price, pos_id)]
        # _tp_index[symbol] = SortedList[(tp_price, pos_id)]
        # Включается через TM_PRICE_INDEX_ENABLED=1 (default: 0 until tested in prod)
        self._price_index_enabled = os.getenv("TM_PRICE_INDEX_ENABLED", "0") == "1" and _SORTED_CONTAINERS_AVAILABLE
        self._sl_index: dict[str, Any] = {}  # symbol -> SortedList[(sl, id)]
        self._tp_index: dict[str, Any] = {}  # symbol -> SortedList[(tp, id)]

        # Основные структуры данных (self.open_positions is kept as flat index for PosID -> Object)
        self.open_positions: dict[str, PositionState] = {}
        self.pos_by_sid: dict[str, str] = {}
        self.open_by_symbol: dict[str, set[str]] = {}
        self._last_price_by_symbol: dict[str, tuple[int, float]] = {}

        # [REMEDIATION P4.1] Cache hot-path environment variables
        self._trail_tp_activate_level = max(1, int(os.getenv("BINANCE_TRAIL_ACTIVATE_TP", "2")))
        self._trailing_local_fallback = os.getenv("TRAILING_LOCAL_FALLBACK", "1") == "1"
        self._simulated_slippage_bps = float(os.getenv("SIMULATED_SLIPPAGE_BPS", "0.0"))
        self._orphan_max_last_price_age_ms = int(os.getenv("TM_ORPHAN_MAX_PRICE_AGE_MS", "300000"))

        # Throttle metrics update (ms)
        self._metrics_update_interval_ms = int(os.getenv("TM_METRICS_UPDATE_INTERVAL_MS", "1000"))
        self._last_metrics_update_by_sym: dict[str, int] = {}

        # ✅ Dedup TTL для внешних событий (lossless-safe) - инициализируем по умолчанию
        self.external_event_dedup_ttl = 7 * 24 * 3600  # 7 дней в секундах

        # ✅ Namespace изоляция для дедупликации (решение race condition между сервисами)
        # Каждый сервис (scanner-trade-monitor, scanner-signal-tracker) должен иметь
        # уникальный TM_NAMESPACE для предотвращения конфликтов за SID claim
        self.namespace = os.getenv("TM_NAMESPACE", "default")
        if not self.namespace or self.namespace.strip() == "":
            self.namespace = "default"
        logger.info(f"🔖 TradeMonitorService namespace: {self.namespace}")

        # Orphan housekeep
        self._orphan_housekeep_interval_ms = int(os.getenv("TM_ORPHAN_HOUSEKEEP_INTERVAL_MS", "30000"))
        self._last_housekeep_ms: int = 0
        self._last_housekeep_by_symbol: dict[str, int] = {}

        # [FIX-1] Grace period after restart — do not housekeep for N ms to allow price cache warm-up
        # Prevents ORPHAN_TIMEOUT_NO_PRICE on positions that were open before restart.
        # TM_HOUSEKEEP_GRACE_AFTER_RESTART_MS: 0 = disabled (default: 90 000 ms = 90 sec)
        self._housekeep_grace_ms = int(os.getenv("TM_HOUSEKEEP_GRACE_AFTER_RESTART_MS", "90000"))
        self._housekeep_started_at_ms: int = 0  # set after warmup
        # Orphan TTL (ms). If your old code already had this attribute, it will be overwritten only if missing.
        if not hasattr(self, "_orphan_ttl_ms"):
            self._orphan_ttl_ms = int(os.getenv("TM_ORPHAN_TTL_MS", "120000"))

        # ── Orphan cleanup (A): housekeep stale monitor-state only ────────────
        _legacy_orphan = os.getenv("TM_ORPHAN_TIMEOUT_ENABLED")
        if _legacy_orphan is not None:
            self.orphan_cleanup_enabled: bool = _legacy_orphan == "1"
        else:
            self.orphan_cleanup_enabled = os.getenv("TM_ORPHAN_CLEANUP_ENABLED", "1") == "1"
        self.orphan_timeout_enabled = self.orphan_cleanup_enabled  # legacy alias

        # ── Max-hold timeout close (B): real time-based exit via executor ─────
        self.real_timeout_close_enabled: bool = os.getenv("TM_REAL_TIMEOUT_CLOSE_ENABLED", "0") == "1"
        self.timeout_close_mode: str = os.getenv("TM_TIMEOUT_CLOSE_MODE", "shadow").lower()
        self._max_hold_ms_default: int = max(0, int(os.getenv("TM_MAX_HOLD_MS_DEFAULT", "300000")))
        self._max_hold_bars_default: int = max(0, int(os.getenv("TM_MAX_HOLD_BARS_DEFAULT", "0")))
        self._max_hold_grace_ms: int = max(0, int(os.getenv("TM_MAX_HOLD_GRACE_MS", "15000")))
        self._timeout_skip_if_trailing: bool = os.getenv("TM_TIMEOUT_SKIP_IF_TRAILING_ACTIVE", "1") == "1"
        self._timeout_require_fresh_price: bool = os.getenv("TM_TIMEOUT_REQUIRE_FRESH_PRICE", "1") == "1"
        self._timeout_max_last_price_age_ms: int = max(0, int(os.getenv("TM_TIMEOUT_MAX_LAST_PRICE_AGE_MS", "5000")))
        self._timeout_idempotency_ttl_sec: int = int(os.getenv("TM_TIMEOUT_IDEMPOTENCY_TTL_SEC", "86400"))
        self._smart_timeout_enabled: bool = os.getenv("TM_SMART_TIMEOUT_ENABLED", "1") == "1"
        self._smart_timeout_min_profit_bps: float = float(os.getenv("TM_SMART_TIMEOUT_MIN_PROFIT_BPS", "4.0"))
        self._smart_timeout_adverse_atr: float = float(os.getenv("TM_SMART_TIMEOUT_ADVERSE_ATR", "1.0"))
        self._binance_orders_queue: str = os.getenv("BINANCE_ORDERS_QUEUE", "orders:queue")
        self._mt5_orders_queue: str = os.getenv("MT5_ORDERS_QUEUE", "orders:queue:mt5")

        # Trading parameters initialization
        self._attach_health_on_close = os.getenv("ATTACH_HEALTH_SNAPSHOT_ON_CLOSE", "1") == "1"

        # Trading parameters with fallbacks
        mon = self.config.get("monitor", {})
        self.default_lot = float(mon.get("default_lot", 1.0))

        # TP ratios configuration
        ratios_cfg = mon.get("tp_ratio", [0.50, 0.30, 0.20])
        try:
            r1_env = float(os.getenv("TP_RATIO1", "nan"))
        except Exception:
            r1_env = float("nan")
        try:
            r2_env = float(os.getenv("TP_RATIO2", "nan"))
        except Exception:
            r2_env = float("nan")
        try:
            r3_env = float(os.getenv("TP_RATIO3", "nan"))
        except Exception:
            r3_env = float("nan")

        if r1_env == r1_env:  # Check if not NaN
            ratios = [
                r1_env,
                r2_env if r2_env == r2_env else 0.35,
                r3_env if r3_env == r3_env else 0.35,
            ]
        else:
            ratios = list(ratios_cfg) if isinstance(ratios_cfg, (list, tuple)) and len(ratios_cfg) >= 3 else [0.30, 0.35, 0.35]

        # Normalize ratios
        s = sum(ratios)
        if s <= 1e-9:
            ratios = [1 / 3, 1 / 3, 1 / 3]
        else:
            ratios = [max(0.0, float(r)) for r in ratios]
            s = sum(ratios) or 1.0
            ratios = [r / s for r in ratios]

        self.tp_ratios = tuple(ratios)

        self.tp_ratios = tuple(ratios)

        # Metrics (Prometheus) - reference module-level globals
        self.tm_orphans_force_closed = TM_ORPHANS_FORCE_CLOSED
        self.tm_open_positions = TM_OPEN_POSITIONS
        self.tm_orphan_cleanup_duration_ms = TM_ORPHAN_CLEANUP_DURATION_MS
        self.tm_tick_latency_us = TM_TICK_LATENCY_US

        # [NEW] Backpressure metrics
        self.tm_rg_persist_pending = TM_RG_PERSIST_PENDING
        self.tm_rg_persist_dropped = TM_RG_PERSIST_DROPPED
        self.tm_rg_persist_submitted = TM_RG_PERSIST_SUBMITTED
        self.tm_rg_persist_failed = TM_RG_PERSIST_FAILED

        self.stop_atr_mult = float(mon.get("stop_atr_mult", 1.0))
        self.rr_levels = mon.get("rr_levels", [1.0, 2.0, 3.0])
        self.fill_policy = (mon.get("fill_policy", "level")).strip().lower()

        # Shadow Analytics Config
        # Global confidence threshold (single source of truth)
        self.shadow_conf_threshold = float(os.getenv("CRYPTO_SIGNAL_MIN_CONF", "70"))
        self._open_log_counter = 0

        # Simulation Time tracking (Expert Recommendation for neg_dur resolution)
        # We track the latest timestamp seen in the data feed to detect "future" signals
        # relative to simulation state (Time Sync).
        self._max_tick_ts_ms = 0

        # --------------------
        # Orphan housekeep (вошли, но не вышли)
        # --------------------
        # Позиция может зависнуть навсегда если:
        #  - потеряли событие закрытия
        #  - рестарт сервиса
        #  - лаг/пропуск со стороны MT5/bridge
        #
        # Решение: TTL после entry -> forced finalize (ORPHAN_TIMEOUT) + cleanup памяти.
        self._orphan_housekeep_interval_ms = int(os.getenv("TM_ORPHAN_HOUSEKEEP_INTERVAL_MS", "30000"))
        self._orphan_max_lifetime_ms_default = int(os.getenv("TM_ORPHAN_MAX_LIFETIME_MS", str(6 * 3600 * 1000)))  # 6h
        self._orphan_max_lifetime_bars_default = int(os.getenv("TM_ORPHAN_MAX_LIFETIME_BARS_AFTER_ENTRY", "0"))   # 0 = выключено

        # Максимально допустимая "старость" last price, чтобы использовать её для forced-close.
        self._orphan_max_last_price_age_ms = int(
            os.getenv("TM_ORPHAN_MAX_LAST_PRICE_AGE_MS", str(5 * 60 * 1000))  # 5m
        )

        self._last_housekeep_ms: int = 0

        # Последняя цена по символу (ts_ms, price) — чтобы forced-close был "по рынку"
        self._last_price_by_symbol: dict[str, tuple[int, float]] = {}

        # --------------------
        # Trailing config
        # --------------------
        self.trailing_tp1_offset_default = float(os.getenv("TRAILING_TP1_OFFSET_ATR", "0.6"))

        # TrailingProfilesRegistry — single source of truth for trail atr_mult,
        # shared with binance_executor. Fail-open: if unavailable, falls through
        # to ENV/SymbolSpec chain in _resolve_trailing_tp1_offset_atr.
        try:
            from services.trailing_profiles import TrailingProfilesRegistry as _TrailingProfilesRegistry
            self._trailing_profiles: Any = _TrailingProfilesRegistry()
            logger.info("✅ TrailingProfilesRegistry loaded in TM: %s", self._trailing_profiles.list_names())
        except Exception as _tp_err:
            self._trailing_profiles = None
            logger.warning("⚠️ TrailingProfilesRegistry unavailable in TM: %s — falling back to ENV", _tp_err)

        # ── Single-active-position guard (mirrors EXEC_SINGLE_ACTIVE_POSITION_PER_SYMBOL) ──
        # Reads the same Redis guard key that binance_executor writes.
        # When enabled, on_signal will block any new position (real or virtual)
        # while the guard for that symbol is held (i.e. another trade is active).
        # Fail-open: Redis errors never block signal processing.
        self.exec_single_active_position_per_symbol: bool = (
            os.getenv("EXEC_SINGLE_ACTIVE_POSITION_PER_SYMBOL", "0").strip() in ("1", "true", "yes")
        )
        self._active_symbol_key_prefix: str = (
            (os.getenv("ORDERS_ACTIVE_SYMBOL_KEY_PREFIX") or "orders:active_symbol_sid:").rstrip(":") + ":"
        )
        # Stale guard timeout: if guard is older than this, bypass it (fail-open)
        self._guard_stale_timeout_ms: int = int(
            os.getenv("EXEC_SINGLE_ACTIVE_POSITION_STALE_TIMEOUT_MS", "900000")
        )
        # Simulated slippage for paper trades (bps, applied adversely to entry_price)
        self._simulated_slippage_bps: float = float(
            os.getenv("TM_SIMULATED_SLIPPAGE_BPS", "0")
        )
        if self.exec_single_active_position_per_symbol:
            logger.info(
                "🔒 TM single_active_position_per_symbol=ON key_prefix=%s stale_ms=%d",
                self._active_symbol_key_prefix, self._guard_stale_timeout_ms,
            )
        if self._simulated_slippage_bps > 0:
            logger.info(
                "📊 TM simulated_slippage_bps=%.1f (paper trade entry price shift)",
                self._simulated_slippage_bps,
            )

        # ── Crypto / margin-FX symbol detection (ENV-configurable) ──
        # CRYPTO_SUFFIXES: comma-separated suffixes that mark a symbol as crypto
        #   e.g. "USDT,USDC,BUSD" → DOGEUSDT, ETHUSDC, etc.
        # CRYPTO_EXCLUDE_PREFIXES: comma-separated prefixes to exclude from crypto
        #   Default empty —  is already excluded by suffix check.
        #   Use only if you have a non-crypto symbol that ends with USDT/USDC/BUSD.
        # MARGIN_FX_SYMBOLS: comma-separated explicit symbols for margin-FX sizing
        #   e.g. ",XAGUSD"
        _suf_raw = os.getenv("CRYPTO_SUFFIXES", "USDT,USDC,BUSD")
        self._crypto_suffixes: tuple[str, ...] = tuple(
            s.strip().upper() for s in _suf_raw.split(",") if s.strip()
        )
        _excl_raw = os.getenv("CRYPTO_EXCLUDE_PREFIXES", "")
        self._crypto_exclude_prefixes: tuple[str, ...] = tuple(
            s.strip().upper() for s in _excl_raw.split(",") if s.strip()
        )
        _mfx_raw = os.getenv("MARGIN_FX_SYMBOLS", ",XAGUSD")
        self._margin_fx_symbols: frozenset[str] = frozenset(
            s.strip().upper() for s in _mfx_raw.split(",") if s.strip()
        )

        # Trailing audit stream — unified format for paper vs real trailing comparison
        self._trailing_audit_stream: str = (
            os.getenv("TM_TRAILING_AUDIT_STREAM", "").strip()
        )
        self._trailing_audit_maxlen: int = int(
            os.getenv("TM_TRAILING_AUDIT_MAXLEN", "50000")
        )
        if self._trailing_audit_stream:
            logger.info(
                "📝 TM trailing_audit_stream=%s maxlen=%d",
                self._trailing_audit_stream, self._trailing_audit_maxlen,
            )

        sources_raw = os.getenv("TRAILING_AFTER_TP1_SOURCES", "CryptoOrderFlow")
        self.trailing_after_tp1_sources = {
            canon_source(s.strip())
            for s in sources_raw.split(",")
            if s.strip()
        },

        # Health snapshot cache
        self._health_cache: dict[str, tuple[int, dict[str, str]]] = {}
        self._health_cache_ttl_ms = int(os.getenv("HEALTH_CACHE_TTL_MS", "30000"))

        # Paper vs Demo comparison report
        self._pvd_report_every_n: int = int(os.getenv("TM_PAPER_VS_DEMO_REPORT_EVERY_N", "10"))
        self._pvd_notify_stream: str = os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM)
        self._pvd_demo_stream: str = os.getenv("EXEC_STREAM", RS.ORDERS_EXEC)
        self._pvd_session_closed: int = 0
        self._pvd_recent_closed: list[dict[str, Any]] = []  # circular buffer of last N closed trades
        if "ACCOUNT_LEVERAGE" not in os.environ:
            logger.warning("⚠️ ACCOUNT_LEVERAGE not explicitly set in ENV! Defaulting to 100 for Paper vs Demo reports.")
        self._pvd_paper_leverage: int = int(os.getenv("ACCOUNT_LEVERAGE", "100"))
        self._pvd_demo_leverage: int = int(os.getenv("BINANCE_DEMO_DEFAULT_LEVERAGE", os.getenv("BINANCE_DEFAULT_LEVERAGE", "100")))
        if self._pvd_report_every_n > 0:
            logger.info(
                "📊 TM paper_vs_demo report every %d closed trades → %s (paper_lev=%dx demo_lev=%dx)",
                self._pvd_report_every_n, self._pvd_notify_stream,
                self._pvd_paper_leverage, self._pvd_demo_leverage,
            )

        self._recover_open_positions()

        # [MOVED to end of __init__] Price cache warmup now happens after DI resolution

        # Record the timestamp of service start (after warmup) for grace period tracking.
        self._housekeep_started_at_ms = get_ny_time_millis()
        logger.info(
            "⏳ TM housekeep grace period: %d ms (TM_HOUSEKEEP_GRACE_AFTER_RESTART_MS)",
            self._housekeep_grace_ms,
        )

        # Phase 3+4 DI block below — housekeep thread is started at the END of the DI block.

        # Phase 8.6 Protective Lifecycle Mirror
        try:
            from services.atr_protective_lifecycle_mirror import ProtectiveLifecycleMirror
            self._protective_mirror: Any = ProtectiveLifecycleMirror()
        except Exception as e:
            self._protective_mirror = None
            logger.warning("⚠️ ProtectiveLifecycleMirror unavailable: %s", e)

        # ------------------------------------------------------------------
        # Phase 3: wire extracted service components (DI)
        # All components are lazy wrappers — they delegate back to self.redis /
        # self.repo / self.regime_guard which are already assigned above.
        # ------------------------------------------------------------------
        from services.trade_monitor.position_loader import PositionLoader
        self._pos_loader = PositionLoader(
            self.redis,
            self.repo,
            add_pos_fn=self._register_pos,
            recover_fsm_fn=self._recover_fsm,
            get_open_symbols_fn=self._open_symbols_snapshot,
            set_price_fn=lambda sym, ts, px: self._last_price_by_symbol.__setitem__(sym, (ts, px)),
            log=logger,
        )

        from services.trade_monitor.pnl_calculator import PnlCalculator
        self._pnl_calc = PnlCalculator(
            redis=self.redis,
            regime_guard=self.regime_guard,
            log=logger,
        )

        from services.trade_monitor.trade_close_writer import TradeCloseWriter
        self._writer = TradeCloseWriter(
            redis=self.redis,
            repo=self.repo,
            db_executor=self._db_executor,
            batch_writer=self._batch_writer,
            analytics_db=analytics_db,
            pnl_calc=self._pnl_calc,
            submit_persist_task_fn=self._submit_regime_guard_persist_task,
            attach_health_on_close=self._attach_health_on_close,
            health_cache_ttl_ms=self._health_cache_ttl_ms,
            protective_mirror=self._protective_mirror,
            log=logger,
        )

        from services.trade_monitor.trade_event_emitter import TradeEventEmitter
        self._emitter = TradeEventEmitter(
            repo=self.repo,
            events_logger=self.events_logger,
            redis=self.redis,
            trailing_audit_stream=self._trailing_audit_stream,
            trailing_audit_maxlen=self._trailing_audit_maxlen,
            log=logger,
        )

        from services.trade_monitor.orphan_recovery_policy import OrphanRecoveryPolicy
        self._orphan_policy = OrphanRecoveryPolicy(
            get_shards_fn=lambda: dict(self.shards),
            pop_pos_fn=self._pop_pos,
            global_lock=self._lock,
            get_symbol_lock_fn=self._get_symbol_lock,
            fsm_transition_fn=self._fsm_transition,
            get_spec_fn=self._get_spec,
            get_price_fn=lambda sym: self._last_price_by_symbol.get(sym),
            commission_adj_exit_fn=self._calc_commission_adjusted_exit_price,
            finalize_trade_fn=lambda pos, spec, **kw: finalize_trade(pos, spec, **kw),
            persist_closed_fn=self._persist_closed_trade_io,
            emit_ab_closed_fn=lambda pos, closed, reason: self._log_ab_closed_event(pos, closed, reason),
            stamp_meta_fn=lambda pos, closed, reason: self._stamp_closed_trade_meta(pos, closed, reason),
            trigger_report_fn=self._safe_trigger_report,
            get_last_housekeep_by_symbol_fn=lambda sym: self._last_housekeep_by_symbol.get(sym, 0),
            set_last_housekeep_by_symbol_fn=lambda sym, v: self._last_housekeep_by_symbol.__setitem__(sym, v),
            get_last_housekeep_ms_fn=lambda: self._last_housekeep_ms,
            set_last_housekeep_ms_fn=lambda v: setattr(self, "_last_housekeep_ms", v),
            cleanup_stale_prices_fn=self._cleanup_stale_prices,
            is_grace_period_active_fn=self._is_grace_period_active,
            max_hold_scan_fn=self._run_max_hold_timeout_scan,
            tp_ratios=list(self.tp_ratios),
            housekeep_interval_ms=self._orphan_housekeep_interval_ms,
            orphan_max_price_age_ms=self._orphan_max_last_price_age_ms,
            smart_timeout_enabled=os.getenv("TM_SMART_TIMEOUT_ENABLED", "1") == "1",
            log=logger,
        )

        # Background Housekeep — OrphanRecoveryPolicy owns the daemon thread (Phase 4).
        # Must start AFTER _orphan_policy is fully constructed above.
        self._housekeep_thread_stop = threading.Event()
        self._orphan_policy.start()
        self._housekeep_thread = self._orphan_policy._thread  # alias for external compat

        # [FIX-1] Warm up _last_price_by_symbol from redis-ticks BEFORE first housekeep.
        # This prevents ORPHAN_TIMEOUT_NO_PRICE for positions that were open before restart.
        # Moved here to ensure _pos_loader is fully initialized (DI resolved).
        try:
            self._warmup_price_cache()
        except Exception as _wp_err:
            logger.warning("⚠️ price cache warmup failed (non-critical): %s", _wp_err)

    # ------------------------------------------------------------------
    # P1-9: FSM helpers — all fail-open; FSM_ENABLED=0 → no-op
    # ------------------------------------------------------------------

    def _attach_fsm(self, pos: PositionState) -> None:
        """Create and attach a PositionFSM for a newly-opened position."""
        if not self._fsm_enabled:
            return
        try:
            fsm = PositionFSM(pos, initial_status=PositionStatus.PENDING)
            fsm.transition(
                PositionStatus.OPEN,
                trigger="open_position",
                actor="trade_monitor",
                reason="position opened",
                ts_ms=int(getattr(pos, "entry_ts_ms", 0) or 0) or None,
            )
            self._fsm_map[pos.id] = fsm
        except Exception as exc:
            logger.warning("[FSM] _attach_fsm failed for %s: %s", getattr(pos, "id", "?"), exc)

    def _recover_fsm(self, pos: PositionState) -> None:
        """Reconstruct FSM from boolean flags (used after Redis reload / recovery)."""
        if not self._fsm_enabled:
            return
        try:
            self._fsm_map[pos.id] = fsm_from_position(pos)
        except Exception as exc:
            logger.warning("[FSM] _recover_fsm failed for %s: %s", getattr(pos, "id", "?"), exc)

    def _detach_fsm(self, pos_id: str) -> None:
        """Remove FSM from map on position close/pop (memory cleanup)."""
        self._fsm_map.pop(pos_id, None)

    def _fsm_transition(
        self,
        pos: PositionState,
        to: str,
        trigger: str,
        actor: str = "trade_monitor",
        reason: str = "",
        ts_ms: int | None = None,
        **meta: Any,
    ) -> None:
        """Attempt FSM transition by state name string.  Fail-open: never raises."""
        if not self._fsm_enabled:
            return
        try:
            fsm: PositionFSM | None = self._fsm_map.get(getattr(pos, "id", ""))
            if fsm is None:
                # Position recovered from Redis without FSM — create it now
                self._recover_fsm(pos)
                fsm = self._fsm_map.get(getattr(pos, "id", ""))
            if fsm is None:
                return
            target = PositionStatus(to)
            fsm.transition(target, trigger=trigger, actor=actor, reason=reason, ts_ms=ts_ms, **meta)
            # Non-blocking Redis publish (best-effort)
            self._fsm_publish_async(fsm)
        except Exception as exc:
            logger.warning("[FSM] _fsm_transition %s→%s failed: %s", getattr(pos, "id", "?"), to, exc)

    def _fsm_publish_async(self, fsm: Any) -> None:
        """Publish last FSM transition to Redis Stream (truly non-blocking, fail-open).
        
        P0 FIX: was synchronous self.redis.xadd() despite the name.
        Now offloaded to _db_executor to avoid blocking the hot path.
        """
        try:
            payload = fsm.to_redis_payload()
            stream = fsm.AUDIT_STREAM
            maxlen = fsm.AUDIT_MAXLEN

            def _do_publish():
                try:
                    self.redis.xadd(stream, payload, maxlen=maxlen, approximate=True)
                except Exception:
                    pass  # never break hot path for audit publishing

            self._db_executor.submit(_do_publish)
        except Exception:
            pass  # never break hot path for audit publishing

    def _safe_save_trade_to_db(self, closed: TradeClosed) -> None:

        """
        Ставит сделку в BatchTradeWriter (non-blocking, O(1)).
        Если BATCH_WRITER_ENABLED=0 — синхронный INSERT через analytics_db (старое поведение).
        """
        try:
            self._batch_writer.enqueue(closed)
        except Exception as e:
            # Fail-open: если batch writer недоступен — пробуем прямой INSERT
            logger.error("❌ BatchTradeWriter.enqueue failed, fallback to direct INSERT: %s", e)
            try:
                analytics_db.save_trade_closed(closed)
            except Exception as e2:
                logger.error("❌ Direct INSERT fallback also failed: %s", e2)


    def _safe_trigger_report(self, source: str, symbol: str, counter_type: str, order_id: str, demo_only: bool = False) -> None:
        """
        Helper explicitly for running in _db_executor (async trigger).
        PeriodicReporter uses SYNC Redis, so it MUST run in a thread to avoid blocking the loop.
        """
        try:
            from services.periodic_reporter import check_and_trigger_report
            #logger.debug(f"🔄 Async report trigger: {source}/{symbol} {order_id}")
            check_and_trigger_report(source, symbol, counter_type=counter_type, order_id=order_id, demo_only=demo_only)
        except Exception as e:
            logger.warning("Async report trigger failed: %s", e)

    # --------------------
    # Metrics helpers (fail-open)
    # --------------------
    def _m_inc(self, name: str, value: int = 1, tags: dict[str, Any] | None = None) -> None:
        m = getattr(self, "_metrics", None)
        if not m:
            return
        try:
            m.inc(name, int(value), tags)
        except Exception:
            return

    def _m_obs(self, name: str, value: float, tags: dict[str, Any] | None = None) -> None:
        m = getattr(self, "_metrics", None)
        if not m:
            return
        try:
            m.observe(name, float(value), tags)
        except Exception:
            return

    def _get_symbol_lock(self, symbol: str) -> threading.RLock:
        """
        Per-symbol lock used to serialize:
          - on_tick(symbol)
          - apply_external_* for positions of that symbol
          - orphan housekeep finalization affecting that symbol
        """
        s = (symbol or "").strip().upper() or "UNKNOWN"
        with self._symbol_locks_guard:
            lk = self._symbol_locks.get(s)
            if lk is None:
                lk = threading.RLock()
                self._symbol_locks[s] = lk
            return lk

    def _symbol_ctx(self, symbol: str):
        if not getattr(self, "_use_symbol_locks", False):
            return contextlib.nullcontext()
        return self._get_symbol_lock(symbol)

    def _pos_last_ts_ms(self, pos: Any) -> int:
        """
        Best-effort: какой ts считать "последней активностью".
        Адаптируйте под вашу модель PositionState, если поля другие.
        """
        for k in ("last_tick_ts_ms", "last_update_ts_ms", "last_ts_ms", "ts_ms", "entry_ts_ms"):
            try:
                v = int(getattr(pos, k))
                if v > 0:
                    return v
            except Exception as e:
                import logging
                logging.warning("Error fetching from Redis hash: %s", e)
                continue
        return 0

    def _is_orphan_expired(self, pos: Any, now_ms: int) -> bool:
        # Check explicit disable flag first
        if not getattr(self, "orphan_timeout_enabled", False):
            return False

        try:
            if getattr(pos, "closed", False):
                return False
        except Exception:
            pass
        if getattr(pos, "trailing_active", False):
             # User Request: "Disable TIMEOUT in modes where there is TP/trail"
             # If trailing is active, we trust the trailing logic to close it, not the janitor.
             return False

        # TTL counts from entry, not from last tick — ticks on liquid pairs would
        # otherwise reset the timer continuously, preventing timeout from ever firing.
        entry_ms = int(getattr(pos, "entry_ts_ms", 0) or 0)
        try:
            ttl = int(self._resolve_orphan_ttl_ms(pos))
        except Exception:
            ttl = int(getattr(self, "_orphan_max_lifetime_ms_default", 6 * 3600 * 1000))
        if ttl <= 0:
            return False
        return (entry_ms > 0) and ((now_ms - entry_ms) >= ttl)

    def _get_health_snapshot(self, symbol: str) -> dict[str, Any]:
        """
        Берем snapshot из Redis, который пишет HealthMetrics background loop:
          orderflow:{symbol}:health_snapshot (HASH)
        ВАЖНО:
          - Никаких новых redis клиентов
          - best-effort (ошибки глушим)
        """
        try:
            sym = (symbol or "").strip()
            if not sym:
                return {}
            key = f"orderflow:{sym}:health_snapshot"
            h = self.redis.hgetall(key) or {}
            # Значения уже str (decode_responses=True).
            # Возвращаем как есть: repo сам добавит префикс health_*
            return dict(h)
        except Exception:
            return {}


    # --------------------
    # Orphan housekeep helpers
    # --------------------

    @staticmethod
    def _is_plausible_epoch_ms(ts_ms: int) -> bool:
        """
        Защита от "не epoch" значений. Берём грубую отсечку: >= 2001-01-01.
        """
        # bool является подклассом int → исключаем, чтобы "True" не проходил как timestamp.
        if isinstance(ts_ms, bool):
            return False
        try:
            v = int(ts_ms)
        except Exception:
            return False
        return v >= 978307200000  # 2001-01-01 in ms

    @staticmethod
    def _tf_to_ms(tf: str) -> int:
        """
        Конвертация таймфрейма позиции в миллисекунды.
        Поддержка типичных форматов: "1m", "5m", "15m", "1h", "4h", "1d", "M1", "H1".
        Если не распознали — считаем 1m (fail-open).
        """
        if not tf:
            return 60_000
        s = tf.strip().lower()
        s = s.replace("m", "m").replace("h", "h").replace("d", "d")
        # mt5-style: M1/H1/D1
        if re.fullmatch(r"[mhd]\d+", s):
            unit = s[0]
            n = int(s[1:])
        else:
            m = re.fullmatch(r"(\d+)\s*([mhd])", s)
            if not m:
                return 60_000
            n = int(m.group(1))
            unit = m.group(2)
        mult = {"m": 60_000, "h": 3_600_000, "d": 86_400_000}.get(unit, 60_000)
        return max(1, n) * mult

    def _update_last_price(self, tick) -> None:
        """
        Обновляем last price по символу. Используем mid/last/price в порядке приоритета.
        Это нужно для forced-close orphan-позиций.
        """
        try:
            symbol = tick.symbol
            ts_ms = int(tick.ts_ms)
            if not self._is_plausible_epoch_ms(ts_ms):
                return
            price = float(
                getattr(tick, "mid", 0.0)
                or getattr(tick, "last", 0.0)
                or getattr(tick, "price", 0.0)
                or 0.0
            )
            if price > 0:
                # last_price_by_symbol шарится между потоками → держим общий lock
                with self._lock:
                    self._last_price_by_symbol[symbol] = (ts_ms, price)
        except Exception:
            # fail-open: отсутствие last_price не должно ломать обработку тиков
            pass

    # ------------------------------------------------------------------
    # Phase 3: DI helpers (used by PositionLoader callbacks above)
    # ------------------------------------------------------------------

    def _register_pos(self, pos: Any) -> None:
        """Thread-safe registration for PositionLoader callback."""
        with self._lock:
            self.open_positions[pos.id] = pos
            if pos.sid:
                self.pos_by_sid[pos.sid] = pos.id
            self._index_add(pos)

    def _open_symbols_snapshot(self) -> set[str]:
        """Return current set of open symbols (used by warmup)."""
        with self._lock:
            return {
                str(getattr(p, "symbol", "") or "").strip().upper()
                for p in self.open_positions.values()
                if getattr(p, "symbol", None)
            }

    def _warmup_price_cache(self) -> None:
        """
        [FIX-1] Warm up _last_price_by_symbol from redis-ticks after service restart.
        Phase 4: delegates to PositionLoader.warmup_price_cache().
        """
        if hasattr(self, "_pos_loader"):
            self._pos_loader.warmup_price_cache()
            return
        self._warmup_price_cache_legacy()

    def _warmup_price_cache_legacy(self) -> None:
        """Original body — fallback only, not called in normal operation."""
        ticks_url = os.getenv("REDIS_TICKS_URL", "redis://redis-ticks:6379/0")
        max_age_ms = int(os.getenv("TM_WARMUP_MAX_PRICE_AGE_MS", "600000"))  # 10 min
        now_ms = get_ny_time_millis()
        try:
            import redis as redis_lib
            r_ticks: Any = redis_lib.from_url(ticks_url, decode_responses=True, socket_timeout=2.0, socket_connect_timeout=2.0)
        except Exception as e:
            logger.warning("⚠️ [warmup] cannot connect to redis-ticks (%s): %s", ticks_url, e)
            return
        with self._lock:
            symbols = {str(pos.symbol or "").strip().upper() for pos in self.open_positions.values() if pos.symbol}
        warmed, skipped_old, skipped_err = 0, 0, 0
        for sym in symbols:
            if not sym:
                continue
            try:
                stream_key = f"stream:tick_{sym}"
                entries = r_ticks.xrevrange(stream_key, "+", "-", count=1)
                if not entries:
                    skipped_err += 1
                    continue
                entry_id, fields = entries[0]
                try:
                    ts_ms = int(str(entry_id).split("-")[0])
                except Exception:
                    ts_ms = now_ms
                age_ms = now_ms - ts_ms
                if age_ms > max_age_ms:
                    skipped_old += 1
                    continue
                price = float(fields.get("mid") or fields.get("price") or fields.get("last") or 0.0)
                if price > 0:
                    with self._lock:
                        existing = self._last_price_by_symbol.get(sym)
                        if existing is None or existing[0] < ts_ms:
                            self._last_price_by_symbol[sym] = (ts_ms, price)
                            warmed += 1
                else:
                    skipped_err += 1
            except Exception:
                skipped_err += 1
        with contextlib.suppress(Exception):
            r_ticks.close()
        logger.info(
            "🔥 [warmup] price cache: warmed=%d skipped_old=%d skipped_err=%d / total_symbols=%d",
            warmed, skipped_old, skipped_err, len(symbols),
        )

    def _resolve_orphan_ttl_ms(self, pos: PositionState) -> int:
        """
        Вычисляет TTL после entry для конкретной позиции.
        Приоритет:
          1) pos.signal_payload["orphan_ttl_ms"]
          2) pos.signal_payload["max_lifetime_bars_after_entry"] * tf_ms
          3) TM_ORPHAN_MAX_LIFETIME_BARS_AFTER_ENTRY * tf_ms (если >0)
          4) TM_ORPHAN_MAX_LIFETIME_MS
        """
        # 1) явный TTL в ms на уровне сигнала/позиции
        try:
            sp = getattr(pos, "signal_payload", None) or {}
            v = sp.get("orphan_ttl_ms")
            if v is not None:
                ttl = int(v)
                return max(0, ttl)
        except Exception:
            pass

        tf_ms = self._tf_to_ms(getattr(pos, "tf", "") or "1m")

        # 2) TTL в барах на уровне сигнала/позиции
        try:
            sp = getattr(pos, "signal_payload", None) or {}
            b = sp.get("max_lifetime_bars_after_entry")
            if b is not None:
                bars = int(b)
                if bars > 0:
                    return bars * tf_ms
        except Exception:
            pass

        # 3) глобальный TTL в барах (если включён)
        if self._orphan_max_lifetime_bars_default > 0:
            return self._orphan_max_lifetime_bars_default * tf_ms

        # 4) глобальный TTL в ms
        return max(0, int(self._orphan_max_lifetime_ms_default))

    # ──────────────────────────────────────────────────────────────────────
    # Max-hold timeout close helpers (Mechanism B)
    # ──────────────────────────────────────────────────────────────────────

    def _is_real_position(self, pos: PositionState) -> bool:
        venue = str(
            getattr(pos, "venue", "")
            or (getattr(pos, "signal_payload", None) or {}).get("venue", "")
        ).lower()
        source = str(getattr(pos, "source", "") or "").lower()
        if getattr(pos, "is_virtual", False):
            return False
        return venue in {"binance_futures", "mt5"} or source in {"binance", "mt5"}

    def _position_age_ms(self, pos: PositionState, now_ms: int) -> int:
        entry_ts_ms = int(getattr(pos, "entry_ts_ms", 0) or 0)
        if entry_ts_ms <= 0:
            return 0
        return max(0, int(now_ms) - entry_ts_ms)

    def _resolve_max_hold_ms(self, pos: PositionState) -> int:
        sp = getattr(pos, "signal_payload", None) or {}
        tf_ms = self._tf_to_ms(getattr(pos, "tf", "") or "1m")
        try:
            v = sp.get("max_hold_ms")
            if v is not None:
                return max(0, int(v))
        except Exception:
            pass
        try:
            bars = int(sp.get("max_hold_bars") or 0)
            if bars > 0:
                return bars * tf_ms
        except Exception:
            pass
        if self._max_hold_bars_default > 0:
            return self._max_hold_bars_default * tf_ms
        return max(0, self._max_hold_ms_default)

    def _is_max_hold_expired(self, pos: PositionState, now_ms: int) -> bool:
        if getattr(pos, "closed", False):
            return False
        mode = str(getattr(self, "timeout_close_mode", "shadow")).lower()
        if not getattr(self, "real_timeout_close_enabled", False) and mode not in {"shadow", "paper", "enforce"}:
            return False
        max_hold = self._resolve_max_hold_ms(pos)
        if max_hold <= 0:
            return False
        return self._position_age_ms(pos, now_ms) >= max_hold

    def _get_last_price_for_pos(self, pos: PositionState) -> tuple[int, float]:
        sym = str(getattr(pos, "symbol", "") or "").upper()
        lp = self._last_price_by_symbol.get(sym)
        if lp:
            return int(lp[0]), float(lp[1])
        return 0, 0.0

    def _emit_timeout_eval(self, pos: PositionState, decision: str, reason: str) -> None:
        sym = str(getattr(pos, "symbol", "") or "unknown")
        try:
            TM_TIMEOUT_EVAL_TOTAL.labels(symbol=sym, decision=decision, reason=reason).inc()
        except Exception:
            pass

    def _claim_timeout_close_once(self, sid: str, reason: str) -> bool:
        ttl = int(getattr(self, "_timeout_idempotency_ttl_sec", 86400))
        key = f"tm:timeout_close:claimed:{sid}:{reason}"
        try:
            return bool(self.redis.set(key, "1", nx=True, ex=ttl))
        except Exception:
            return False

    def _resolve_timeout_reason(
        self, pos: PositionState, *, last_price: float
    ) -> tuple[bool, str]:
        if self._timeout_skip_if_trailing and getattr(pos, "trailing_active", False):
            return False, "TIMEOUT_SKIP_TRAILING_ACTIVE"
        if not self._smart_timeout_enabled:
            return True, "TIMEOUT_MAX_HOLD"
        entry = float(getattr(pos, "entry_price", 0.0) or 0.0)
        if entry <= 0 or last_price <= 0:
            return False, "TIMEOUT_SKIP_NO_PRICE"
        direction = str(getattr(pos, "direction", "") or "").upper()
        if direction in {"LONG", "BUY"}:
            pnl_bps = (last_price - entry) / entry * 10_000
            adverse = entry - last_price
        else:
            pnl_bps = (entry - last_price) / entry * 10_000
            adverse = last_price - entry
        if pnl_bps >= self._smart_timeout_min_profit_bps:
            return True, "TIMEOUT_PROFITABLE"
        atr = float(getattr(pos, "atr", 0.0) or 0.0)
        if atr > 0 and adverse >= atr * self._smart_timeout_adverse_atr:
            return True, "TIMEOUT_ADVERSE_MOVE"
        return True, "TIMEOUT_MAX_HOLD"

    def _request_real_timeout_close(
        self, pos: PositionState, *, now_ms: int, max_hold_ms: int,
        age_ms: int, reason: str, last_price: float, last_price_ts_ms: int,
    ) -> None:
        import json as _json

        sid = str(getattr(pos, "sid", "") or getattr(pos, "id", "") or "")
        symbol = str(getattr(pos, "symbol", "") or "").upper()
        if not sid or not symbol:
            self._emit_timeout_eval(pos, "skip", "TIMEOUT_SKIP_MISSING_SID_SYMBOL")
            return

        if not self._claim_timeout_close_once(sid, reason):
            try:
                TM_TIMEOUT_CLOSE_DEDUP_TOTAL.labels(symbol=symbol, reason=reason).inc()
            except Exception:
                pass
            self._emit_timeout_eval(pos, "dedup", "TIMEOUT_DUPLICATE_SUPPRESSED")
            return

        mode = str(getattr(self, "timeout_close_mode", "shadow")).lower()
        if mode == "shadow" or not getattr(self, "real_timeout_close_enabled", False):
            self._emit_timeout_eval(pos, "shadow", reason)
            logger.info("⏳ [timeout_close shadow] %s sid=%s reason=%s age_ms=%d", symbol, sid, reason, age_ms)
            return

        venue = str(
            getattr(pos, "venue", "")
            or (getattr(pos, "signal_payload", None) or {}).get("venue", "")
            or "binance_futures"
        ).lower()
        cmd = {
            "v": 1, "action": "timeout_close", "sid": sid, "symbol": symbol,
            "venue": venue, "close_reason_raw": reason,
            "request_ts_ms": int(now_ms), "entry_ts_ms": int(getattr(pos, "entry_ts_ms", 0) or 0),
            "max_hold_ms": int(max_hold_ms), "age_ms": int(age_ms),
            "idempotency_key": f"timeout_close:{sid}:{reason}",
            "source": "trade_monitor",
            "expected_side": str(getattr(pos, "direction", "") or ""),
            "expected_qty": float(getattr(pos, "remaining_qty", 0.0) or getattr(pos, "lot", 0.0) or 0.0),
            "last_price": float(last_price or 0.0),
            "last_price_ts_ms": int(last_price_ts_ms or 0),
        }
        queue = self._mt5_orders_queue if venue == "mt5" else self._binance_orders_queue
        try:
            self.redis.xadd(queue, {"data": _json.dumps(cmd, ensure_ascii=False)}, maxlen=100000, approximate=True)
            TM_TIMEOUT_CLOSE_REQUESTED_TOTAL.labels(venue=venue, symbol=symbol, reason=reason).inc()
        except Exception as e:
            logger.error("❌ [timeout_close] failed to publish command: %s", e)
            return
        self._emit_timeout_eval(pos, "requested", reason)
        logger.info("📤 [timeout_close] queued sid=%s symbol=%s venue=%s reason=%s age_ms=%d", sid, symbol, venue, reason, age_ms)

    def _finalize_paper_timeout_close(
        self, pos: PositionState, *, exit_price: float, exit_ts_ms: int, close_reason_raw: str,
    ) -> None:
        sym = str(getattr(pos, "symbol", "") or "UNKNOWN").upper()
        spec = self._get_spec(sym)
        pos.closed = True
        pos.exit_ts_ms = int(exit_ts_ms)
        pos.exit_price = float(exit_price)
        with contextlib.suppress(Exception):
            self._fsm_transition(pos, "ORPHAN_CLOSED", trigger="max_hold_timeout_paper",
                                 reason=close_reason_raw, price=float(exit_price), ts_ms=int(exit_ts_ms))
        with contextlib.suppress(Exception):
            rq = float(getattr(pos, "remaining_qty", 0.0) or 0.0)
            if rq > 0 and not getattr(pos, "_pnl_finalized", False):
                pnl_rest = float(spec.pnl_money(pos.entry_price, float(exit_price), rq, pos.direction, symbol=sym))
                pos.realized_pnl_gross = float(getattr(pos, "realized_pnl_gross", 0.0) or 0.0) + pnl_rest
                pos.remaining_qty = 0.0
        closed = finalize_trade(pos, spec, exit_price=float(exit_price),
                                exit_ts_ms=int(exit_ts_ms), close_reason_raw=close_reason_raw,
                                tp_ratios=self.tp_ratios)
        with contextlib.suppress(Exception):
            object.__setattr__(closed, "is_orphan_cleanup", False)
            object.__setattr__(closed, "exclude_from_ml_labels", False)
            object.__setattr__(closed, "timeout_age_ms", self._position_age_ms(pos, exit_ts_ms))
            object.__setattr__(closed, "timeout_max_hold_ms", self._resolve_max_hold_ms(pos))
        with contextlib.suppress(Exception):
            self._log_ab_closed_event(pos, closed, close_reason_raw)
        with contextlib.suppress(Exception):
            hs = self._get_health_snapshot_for_trade(str(closed.symbol))
            self._io_save_closed(closed, health_snapshot=hs)
        with self._lock:
            self._pop_pos(pos.id)
        try:
            TM_TIMEOUT_EVAL_TOTAL.labels(symbol=sym, decision="finalized_paper", reason=close_reason_raw).inc()
        except Exception:
            pass

    def _on_max_hold_expired(self, pos: PositionState, now_ms: int) -> None:
        age_ms = self._position_age_ms(pos, now_ms)
        max_hold_ms = self._resolve_max_hold_ms(pos)
        if max_hold_ms <= 0 or age_ms < max_hold_ms:
            return
        sym = str(getattr(pos, "symbol", "") or "unknown")
        with contextlib.suppress(Exception):
            TM_TIMEOUT_POSITION_AGE_MS.labels(symbol=sym).observe(age_ms)
        last_ts, last_price = self._get_last_price_for_pos(pos)
        if self._timeout_require_fresh_price:
            if not last_ts or (now_ms - last_ts) > self._timeout_max_last_price_age_ms:
                self._emit_timeout_eval(pos, "skip", "TIMEOUT_SKIP_STALE_PRICE")
                return
        should_close, reason = self._resolve_timeout_reason(pos, last_price=last_price)
        if not should_close:
            self._emit_timeout_eval(pos, "skip", reason)
            return
        if self._is_real_position(pos):
            self._request_real_timeout_close(pos, now_ms=now_ms, max_hold_ms=max_hold_ms,
                                             age_ms=age_ms, reason=reason, last_price=last_price,
                                             last_price_ts_ms=last_ts)
        else:
            self._finalize_paper_timeout_close(
                pos,
                exit_price=last_price if last_price > 0 else float(getattr(pos, "entry_price", 0.0) or 0.0),
                exit_ts_ms=now_ms, close_reason_raw=reason,
            )

    def _run_max_hold_timeout_scan(self, now_ms: int) -> None:
        with self._lock:
            shards_snap = {s: dict(sh) for s, sh in self.shards.items()}
        for sym, shard in shards_snap.items():
            for pos_id, pos in shard.items():
                if getattr(pos, "closed", False):
                    continue
                if not self._is_max_hold_expired(pos, now_ms):
                    continue
                with contextlib.suppress(Exception):
                    self._on_max_hold_expired(pos, now_ms)

    # --------------------
    # Index helpers
    # --------------------
    def _index_add(self, pos: PositionState) -> None:
        """Добавляет позицию в индексы и шарды."""
        sym = str(pos.symbol or "UNKNOWN").strip().upper()

        # 1. Sharded storage
        self.shards[sym][pos.id] = pos

        # 2. Reverse lookup
        self.symbol_by_pos_id[pos.id] = sym

        # 3. Legacy open_by_symbol (backward compatibility)
        s = self.open_by_symbol.get(sym)
        if s is None:
            s = set()
            self.open_by_symbol[sym] = s
        s.add(pos.id)

        # 4. SortedList price index (O(log N) pre-filter)
        if self._price_index_enabled:
            try:
                sl_price = float(pos.sl or 0.0)
                if sl_price > 0:
                    if sym not in self._sl_index:
                        self._sl_index[sym] = SortedList(key=lambda x: x[0])  # type: ignore
                    self._sl_index[sym].add((sl_price, pos.id))

                tp_price = float(pos.tp_levels[0]) if pos.tp_levels else 0.0
                if tp_price > 0:
                    if sym not in self._tp_index:
                        self._tp_index[sym] = SortedList(key=lambda x: x[0])  # type: ignore
                    self._tp_index[sym].add((tp_price, pos.id))
            except Exception:
                pass  # fail-open: индекс вспомогательный, не критичный

    def _index_remove(self, pos: PositionState) -> None:
        """Удаляет позицию из индексов и шардов."""
        sym = str(pos.symbol or "UNKNOWN")

        # 1. Sharded storage cleanup
        if sym in self.shards:
            self.shards[sym].pop(pos.id, None)
            if not self.shards[sym]:
                self.shards.pop(sym, None)

        # 2. Reverse lookup cleanup
        self.symbol_by_pos_id.pop(pos.id, None)

        # 3. Legacy open_by_symbol cleanup
        s = self.open_by_symbol.get(sym)
        if not s:
            return
        s.discard(pos.id)
        if not s:
            self.open_by_symbol.pop(sym, None)

        # 4. SortedList price index cleanup
        if self._price_index_enabled:
            try:
                sl_price = float(pos.sl or 0.0)
                if sl_price > 0 and sym in self._sl_index:
                    with contextlib.suppress(AttributeError, ValueError):
                        self._sl_index[sym].discard((sl_price, pos.id))
                    if not self._sl_index[sym]:
                        self._sl_index.pop(sym, None)

                tp_price = float(pos.tp_levels[0]) if pos.tp_levels else 0.0
                if tp_price > 0 and sym in self._tp_index:
                    with contextlib.suppress(AttributeError, ValueError):
                        self._tp_index[sym].discard((tp_price, pos.id))
                    if not self._tp_index[sym]:
                        self._tp_index.pop(sym, None)
            except Exception:
                pass  # fail-open

    def _get_pos(self, pos_id: str, symbol: str | None = None) -> PositionState | None:
        """Возвращает позицию по ID (опционально по символу для скорости)."""
        if symbol:
            shards = getattr(self, "shards", None)
            if shards is not None:
                return shards.get(symbol, {}).get(pos_id)
        # fallback to global dict
        return self.open_positions.get(pos_id)

    def _price_index_update_sl(self, pos: PositionState, old_sl: float, new_sl: float) -> None:
        """
        Обновляет позицию в _sl_index при изменении SL (трейлинг).
        Вызывается ПОСЛЕ изменения pos.sl, передаём old_sl как аргумент.
        Fail-open: ошибки не бросаем.
        """
        if not self._price_index_enabled:
            return
        sym = str(pos.symbol or "UNKNOWN")
        try:
            sl_idx = self._sl_index.get(sym)
            if sl_idx is not None and old_sl > 0:
                with contextlib.suppress(AttributeError, ValueError):
                    sl_idx.discard((old_sl, pos.id))
            if new_sl > 0:
                if sym not in self._sl_index:
                    self._sl_index[sym] = SortedList(key=lambda x: x[0])  # type: ignore
                self._sl_index[sym].add((new_sl, pos.id))
        except Exception:
            pass

    def _collect_candidate_pos_ids(self, symbol: str, mid: float) -> set[str] | None:
        """
        Возвращает set pos_id у которых SL или TP может быть пересечён текущей ценой mid.
        Возвращает None если индекс отключён (caller должен использовать full list).

        Логика:
          LONG: SL ≤ mid (стоп-лосс снизу, срабатывает если цена падает ниже sl)
                TP ≥ mid (тейк-профит сверху, срабатывает если цена растёт выше tp)
          SHORT: SL ≥ mid (стоп-лосс сверху, срабатывает если цена растёт выше sl)
                 TP ≤ mid (тейк-профит снизу, срабатывает если цена падает ниже tp)

        Для индекса мы берём КОНСЕРВАТИВНЫЙ подход: возвращаем кандидатов с запасом
        (±1 bucket по обе стороны от mid), чтобы не пропустить позиции. Точная
        проверка выполняется в process_tick() как обычно — мы лишь уменьшаем N.
        """
        if not self._price_index_enabled:
            return None

        candidates: set[str] = set()
        try:
            # SL кандидаты: позиции у которых SL близко к текущей цене
            # LONG: sl <= mid (цена упала до стопа)
            # SHORT: sl >= mid (цена выросла до стопа)
            # Простое правило: все позиции чей SL в диапазоне [mid*0.99, mid*1.01]
            sl_idx = self._sl_index.get(symbol)
            if sl_idx:
                lo = mid * 0.98   # достаточно широкий буфер для трейлинга
                hi = mid * 1.02
                # irange возвращает элементы в [lo, hi] по ключу (sl_price)
                for _sl_price, pos_id in sl_idx.irange((lo,), (hi,), inclusive=(True, True)):
                    candidates.add(pos_id)

            # TP кандидаты: аналогично
            tp_idx = self._tp_index.get(symbol)
            if tp_idx:
                lo = mid * 0.98
                hi = mid * 1.02
                for _tp_price, pos_id in tp_idx.irange((lo,), (hi,), inclusive=(True, True)):
                    candidates.add(pos_id)

        except Exception:
            # На любой ошибке возвращаем None → caller использует full list (safe fallback)
            return None

        return candidates

    def _pop_pos(self, pos_id: str) -> PositionState | None:
        """Атомарно удаляет позицию из всех индексов и шардов. Вызывать под self._lock."""
        pos = self.open_positions.pop(pos_id, None)
        if pos:
            if getattr(pos, "sid", ""):
                self.pos_by_sid.pop(pos.sid, None)
            self._index_remove(pos)
            # P1-9: cleanup FSM entry when position is removed from memory
            self._detach_fsm(pos_id)
        return pos

    # --------------------
    # Dedup helpers (atomic SET NX EX)
    # --------------------
    def _dedup_key(self, kind: str, event_id: str) -> str:
        """
        Формирует ключ для dedup внешних событий.

        Использует namespace для изоляции между сервисами (scanner-trade-monitor,
        scanner-signal-tracker и т.д.), чтобы избежать race condition на общих ключах.
        """
        return f"dedup:trade_monitor:{self.namespace}:{kind}:{event_id}"

    # --------------------
    # Closed SID guard (idempotency across restarts)
    # --------------------
    def _closed_sid_key(self, sid: str) -> str:
        # Separate from existing order_id done_key in repo.save_closed().
        # This one is used when position is already absent from memory.
        return f"closed_sid_done:{sid}"

    def _is_sid_closed(self, sid: str) -> bool:
        """
        Idempotency helper for external events when the in-memory position is gone:
          - return True  -> already closed previously (do NOT emit duplicate events)
          - return False -> unknown to the system
        Fail-open: on Redis errors returns False (so upstream can retry).
        """
        if not sid:
            return False
        try:
            v = self.redis.get(self._closed_sid_key(sid))
            return bool(v)
        except Exception:
            return False

    def _mark_sid_closed(self, sid: str, ttl_days: int = 7) -> None:
        """
        Marks sid as closed (best-effort). Used to make apply_external_* idempotent
        even after cleanup/restart.
        """
        if not sid:
            return
        try:
            self.redis.set(self._closed_sid_key(sid), "1", ex=int(ttl_days) * 24 * 3600)
        except Exception:
            return

    def _is_sid_closed_repo_guard(self, sid: str) -> bool:
        """
        Checks repo-level sid close marker set by RedisTradeRepository.save_closed().
        Fail-open: on Redis errors returns False (so upstream can retry).
        """
        if not sid:
            return False
        try:
            return bool(self.redis.get(f"closed_sid_done:{sid}"))
        except Exception:
            return False

    def _dedup_acquire(self, kind: str, event_id: str | None) -> bool:
        """
        Атомарная проверка+установка dedup ключа (SET NX EX).

        Returns:
            True - если это первый раз (событие нужно обработать)
            False - если событие уже было обработано (дубликат)
        """
        if not event_id:
            return True  # нет event_id → обрабатываем как обычно
        try:
            key = self._dedup_key(kind, event_id)
            # SET NX EX - атомарная операция: устанавливает ключ только если его нет
            result = self.redis.set(key, "1", nx=True, ex=self.external_event_dedup_ttl)
            return result
        except Exception as e:
            # Если Redis недоступен, лучше обработать событие, чем молча пропустить
            logger.warning(f"⚠️ Dedup check failed (Redis error): {e}")
            return True

    def _sid_dedup_key(self, sid: str) -> str:
        """
        Формирует ключ для глобального sid-dedup (lossless-safe).

        КРИТИЧЕСКИ ВАЖНО: использует namespace для изоляции между сервисами.
        Без namespace возникает race condition: scanner-signal-tracker и
        scanner-trade-monitor соревнуются за один и тот же SID ключ в Redis,
        что приводит к пропуску сигналов (как в случае BTCUSDT 16:17 UTC).

        Примеры ключей:
        - scanner-trade-monitor: dedup:trade_monitor:trade-monitor:sid:{signal_id}
        - scanner-signal-tracker: dedup:trade_monitor:signal-tracker:sid:{signal_id}
        """
        return f"dedup:trade_monitor:{self.namespace}:sid:{sid}"

    def _sid_claim(self, sid: str, ttl_sec: int = 30) -> bool:
        """Ставит claim на sid на короткое время обработки."""
        if not sid:
            return True
        try:
            key = self._sid_dedup_key(sid)
            return bool(self.redis.set(key, "processing", nx=True, ex=ttl_sec))
        except Exception as e:
            logger.warning(f"⚠️ SID claim failed (Redis error): {e}")
            return True

    def _sid_finalize(self, sid: str, ttl_days: int = 7) -> None:
        """
        Фиксирует sid как завершённый после успешной записи сделки.

        ВАЖНО: xx=True означает "обновить только если ключ существует".
        Если processing ключ истек или отсутствует, "done" не запишется.
        Рекомендация: убрать xx=True для гарантированной записи "done".
        """
        if not sid:
            return
        try:
            key = self._sid_dedup_key(sid)
            # Убираем xx=True для гарантированной записи "done" даже если claim истек
            self.redis.set(key, "done", ex=ttl_days * 24 * 3600)
        except Exception as e:
            logger.warning(f"⚠️ SID finalize failed (Redis error): {e}")

    def _sid_release(self, sid: str) -> None:
        """Сбрасывает claim при ошибке."""
        if not sid:
            return
        try:
            self.redis.delete(self._sid_dedup_key(sid))
        except Exception as e:
            logger.warning(f"⚠️ SID release failed (Redis error): {e}")

    def _get_health_snapshot_with_timestamp(self, symbol: str, now_ms: int) -> dict[str, Any]:
        """
        Возвращает snapshot health_* для добавления в trades:closed stream.

        FIX #9:
          - repo не должен создавать HealthMetrics/подключения
          - snapshot собираем в сервисе (где уже есть self.redis)
          - используем кэш, чтобы не грузить Redis на каждом close

        Источник данных:
          - hash: orderflow:{symbol}:health_snapshot
          - keys: orderflow:{symbol}:signal_emit_rate, orderflow:{symbol}:dlq_rate

        Args:
            symbol: Символ для получения метрик
            now_ms: Текущее время в миллисекундах для кэширования

        Returns:
            Dict с health метриками (health_*), пустой dict если метрики недоступны
        """
        if not symbol:
            return {}
        cached = self._health_snapshot_cache.get(symbol)  # type: ignore
        if cached:
            ts_ms, snap = cached
            if now_ms - ts_ms <= self._health_snapshot_ttl_ms:  # type: ignore
                return snap

        try:
            # Используем тот же redis client (decode_responses=True).
            health_snapshot_key = f"orderflow:{symbol}:health_snapshot"
            pipe = self.redis.pipeline()
            pipe.hgetall(health_snapshot_key)
            pipe.get(f"orderflow:{symbol}:signal_emit_rate")
            pipe.get(f"orderflow:{symbol}:dlq_rate")
            h, signal_emit_rate, dlq_rate = pipe.execute()

            out: dict[str, Any] = {}
            if h:
                out["health_l2_stale_ratio_tick"] = h.get("l2_stale_ratio_tick", "0.0")
                out["health_l2_stale_ratio_now"] = h.get("l2_stale_ratio_now", "0.0")
                out["health_avg_l2_age_ms"] = h.get("avg_l2_age_ms", "0.0")
                out["health_avg_l2_age_tick_ms"] = h.get("avg_l2_age_tick_ms", "0.0")
            out["health_signal_emit_rate"] = signal_emit_rate or "0.0"
            out["health_dlq_rate"] = dlq_rate or "0.0"

            self._health_snapshot_cache[symbol] = (now_ms, out)  # type: ignore
            return out
        except Exception:
            return {}

    def _get_health_snapshot_for_trade(self, symbol: str) -> dict[str, str]:
        """
        Дешёвое чтение health snapshot (один HGETALL) через существующий redis client.
        Никаких новых коннектов/HealthMetrics внутри repo.

        Ключ создаёт HealthMetrics._flush_snapshot():
          orderflow:{symbol}:health_snapshot
        """
        try:
            sym = (symbol or "").strip()
            if not sym:
                return {}
            key = f"orderflow:{sym}:health_snapshot"
            h = self.redis.hgetall(key) or {}
            if not h:
                return {}
            # Префиксуем поля, чтобы не конфликтовать с торговыми полями.
            # Используем те же имена, что у вас уже были в stream (health_*).
            out = {
                "health_l2_stale_ratio_tick": (h.get("l2_stale_ratio_tick", "0.0")),
                "health_l2_stale_ratio_now": (h.get("l2_stale_ratio_now", "0.0")),
                "health_avg_l2_age_ms": (h.get("avg_l2_age_ms", "0.0")),
                "health_avg_l2_age_tick_ms": (h.get("avg_l2_age_tick_ms", "0.0")),
                "health_signal_emit_rate": (h.get("signal_emit_rate", "0.0")),
                "health_dlq_rate": (h.get("dlq_rate", "0.0")),
                "health_avg_book_lag_ms": (h.get("avg_book_lag_ms", "0.0")),
                "health_avg_ticks_lag_ms": (h.get("avg_ticks_lag_ms", "0.0")),
                "health_pending_len": (h.get("pending_len", "0")),
                "health_window_sec": (h.get("window_sec", "0")),
                "health_ts": (h.get("ts", "0")),
            }
            return out
        except Exception:
            return {}

    # --------------------
    # Recovery
    # --------------------
    def _recover_open_positions(self) -> None:
        """Delegate to PositionLoader (Phase 3 thin proxy)."""
        if hasattr(self, "_pos_loader"):
            self._pos_loader.recover_open_positions()
            return
        # fallback: before __init__ completes (should not happen in production)
        self._recover_open_positions_legacy()

    def _recover_open_positions_legacy(self) -> None:
        """Original recovery logic — kept as fallback, not called in normal operation."""
        try:
            rows = self.repo.load_open_positions(limit=5000)
            with self._lock:
                for h in rows:
                    oid = h.get("id") or ""
                    if not oid:
                        continue
                    pos = self._position_from_hash(h)
                    if not pos:
                        continue
                    self.open_positions[pos.id] = pos
                    if pos.sid:
                        self.pos_by_sid[pos.sid] = pos.id
                    self._index_add(pos)
            logger.info("♻️ recovered open positions: %s", len(self.open_positions))
        except Exception as e:
            logger.warning("⚠️ recovery failed: %s", e)

    @staticmethod
    def _to_int_ms(v, default=0) -> int:
        """
        Безопасная конвертация в int для epoch ms timestamp.
        КРИТИЧНО: никогда не прогоняем 13-значные ms через float (потеря точности).
        """
        try:
            if v is None:
                return default
            if isinstance(v, bool):
                return default
            if isinstance(v, int):
                return v
            s = str(v).strip()
            if not s:
                return default
            # допускаем "1700...".0 (отбрасываем дробную часть без float)
            if "." in s:
                s = s.split(".", 1)[0]
            return int(s)
        except Exception:
            return default

    def _position_from_hash(self, h: dict[str, str]) -> PositionState | None:
        try:
            if h.get("status") != "open":
                return None

            tp_levels = extract_tp_levels(h)

            pos = PositionState(
                id=(h.get("id")),  # type: ignore
                sid=(h.get("sid") or ""),
                strategy=(h.get("strategy") or "unknown"),
                source=(h.get("source") or "Unknown"),
                symbol=(h.get("symbol") or "UNKNOWN"),
                tf=(h.get("tf") or "tick"),
                direction=normalize_side(h.get("direction") or "LONG"),  # type: ignore
                entry_price=float(h.get("entry_price") or 0.0),
                # timestamps (ms)
                entry_ts_ms=self._to_int_ms(h.get("entry_ts_ms") or h.get("entry_time"), 0),
                lot=float(h.get("lot") or 0.0),
                qty=float(h.get("qty") or h.get("lot") or 0.0),
                quantity=float(h.get("quantity") or h.get("lot") or 0.0),
                remaining_qty=float(h.get("remaining_qty") or h.get("lot") or 0.0),
                sl=float(h.get("sl") or 0.0),
                tp_levels=tp_levels,
                tp_hits=int(float(h.get("tp_hits") or 0)),
                tp1_hit=(h.get("tp1_hit") or "0") == "1",
                tp2_hit=(h.get("tp2_hit") or "0") == "1",
                tp3_hit=(h.get("tp3_hit") or "0") == "1",
                trailing_started=(h.get("trailing_started") or "0") == "1",
                trailing_active=(h.get("trailing_active") or "0") == "1",
                trailing_moves_count=int(float(h.get("trailing_moves") or 0)),
                trailing_distance=float(h.get("trailing_distance") or 0.0),
                trailing_point=float(h.get("trailing_point") or 0.0),
                max_favorable_price=float(h.get("max_favorable_price") or 0.0),
                max_favorable_ts=self._to_int_ms(h.get("max_favorable_ts"), 0),
                atr=float(h.get("atr") or 0.0),
                is_virtual=(h.get("is_virtual") or "0") == "1",
                v_gate_status=(h.get("v_gate_status") or "na"),
                v_gate_reason=(h.get("v_gate_reason") or ""),
                # FIX 2026-05-14: restore one_r_money on recovery
                one_r_money=float(h.get("one_r_money") or 0.0),
            )
            try:
                pos.entry_tag = (h.get("entry_tag") or "")
                pos.trail_profile = extract_profile(h)
                pos.trailing_min_lock_r = float(h.get("trailing_min_lock_r") or 0.0)
                pos.min_lock_price = float(h.get("min_lock_price") or 0.0)
                pos.baseline_mode = str(h.get("baseline_mode") or pos.baseline_mode)
                pos.baseline_horizon_ms = self._to_int_ms(h.get("baseline_horizon_ms"), pos.baseline_horizon_ms)
                pos.baseline_sl = float(h.get("baseline_sl") or pos.baseline_sl or pos.sl)
                pos.baseline_tp1 = float(h.get("baseline_tp1") or pos.baseline_tp1 or (pos.tp_levels[0] if pos.tp_levels else 0.0))
                # FIX: baseline_tp2/tp3 must fallback to their own defaults, not tp1 (typo)
                pos.baseline_tp2 = float(h.get("baseline_tp2") or pos.baseline_tp2 or (pos.tp_levels[1] if len(pos.tp_levels) > 1 else 0.0))
                pos.baseline_tp3 = float(h.get("baseline_tp3") or pos.baseline_tp3 or (pos.tp_levels[2] if len(pos.tp_levels) > 2 else 0.0))

                # Restore TP fill dicts from persisted scalars
                prices, times = extract_tp_fills(h)
                if prices:
                    pos.tp_fill_prices.update(prices)
                if times:
                    pos.tp_fill_times.update(times)

                # Optional: restore signal payload if it was persisted
                if h.get("signal_payload"):
                    pos.signal_payload.update(parse_json_dict(h.get("signal_payload")))
            except Exception:
                pass

            # Phase 0.3:
            # 1) scalar-first recovery from hash fields (independent of signal_payload JSON presence)
            # 2) then nested contract hydration from signal_payload if present
            try:
                apply_position_horizon_scalars_from_hash(pos, h, source="service_recovery")
                hydrate_position_from_signal_payload(pos, source="service_recovery")
            except Exception:
                pass

            # P1-9: recover FSM from persisted flags
            self._recover_fsm(pos)
            return pos
        except Exception as e:
            self.logger.warning(f"Failed to recover position from hash: {e}")
            return None

    def _get_health_snapshot_cached(self, symbol: str) -> dict[str, str]:
        """
        Fetch orderflow:{symbol}:health_snapshot via existing redis client.
        Small TTL cache to avoid bursts when multiple closes happen back-to-back.
        """
        now_ms = get_ny_time_millis()
        sym = (symbol or "UNKNOWN")
        cached = self._health_cache.get(sym)
        if cached:
            ts_ms, snap = cached
            if (now_ms - ts_ms) <= self._health_cache_ttl_ms:
                return snap

        raw = self.redis.hgetall(f"orderflow:{sym}:health_snapshot") or {}
        if not raw:
            self._health_cache[sym] = (now_ms, {})
            return {}

        # Keep only the most useful fields on close to avoid bloating the event.
        snap = {
            "health_l2_stale_ratio_tick": (raw.get("l2_stale_ratio_tick", "0.0")),
            "health_l2_stale_ratio_now": (raw.get("l2_stale_ratio_now", "0.0")),
            "health_avg_l2_age_ms": (raw.get("avg_l2_age_ms", "0.0")),
            "health_avg_l2_age_tick_ms": (raw.get("avg_l2_age_tick_ms", "0.0")),
            "health_signal_emit_rate": (raw.get("signal_emit_rate", "0.0")),
            "health_dlq_rate": (raw.get("dlq_rate", "0.0")),
            "health_pending_len": (raw.get("pending_len", "0")),
            "health_snapshot_ts": (raw.get("ts", "0")),
            "health_window_sec": (raw.get("window_sec", "0")),
        }
        self._health_cache[sym] = (now_ms, snap)
        return snap

    def _attach_health_snapshot(self, closed: TradeClosed, symbol: str) -> None:  # type: ignore
        """
        Attach snapshot for repo.save_closed() without letting repo do extra connections.
        Repo merges closed._health_snapshot into stream payload if present.
        """
        try:
            snap = self._get_health_snapshot_cached(symbol)
            if snap:
                closed._health_snapshot = snap
        except Exception:
            pass

    def _get_health_snapshot_prefixed(self, symbol: str, now_ms: int) -> dict[str, str]:
        """
        Fetches last health snapshot from Redis and returns a FLAT dict with stable 'health_*' keys.
        Cached for a short TTL to avoid bursts.
        """
        sym = (symbol or "UNKNOWN")
        cached = self._health_cache.get(sym)
        if cached:
            ts_ms, snap = cached
            if (now_ms - ts_ms) <= self._health_cache_ttl_ms:
                return snap

        raw = self.redis.hgetall(f"orderflow:{sym}:health_snapshot") or {}
        if not raw:
            self._health_cache[sym] = (now_ms, {})
            return {}

        # Keep only the most useful fields on close to avoid bloating the event.
        snap = {
            "health_l2_stale_ratio_tick": (raw.get("l2_stale_ratio_tick", "0.0")),
            "health_l2_stale_ratio_now": (raw.get("l2_stale_ratio_now", "0.0")),
            "health_avg_l2_age_ms": (raw.get("avg_l2_age_ms", "0.0")),
            "health_avg_l2_age_tick_ms": (raw.get("avg_l2_age_tick_ms", "0.0")),
            "health_signal_emit_rate": (raw.get("signal_emit_rate", "0.0")),
            "health_dlq_rate": (raw.get("dlq_rate", "0.0")),
            "health_pending_len": (raw.get("pending_len", "0")),
            "health_snapshot_ts": (raw.get("ts", "0")),
            "health_window_sec": (raw.get("window_sec", "0")),
        }
        self._health_cache[sym] = (now_ms, snap)
        return snap

    def _attach_health_snapshot(self, closed: TradeClosed, symbol: str) -> None:
        """
        Attach snapshot for repo.save_closed() without letting repo do extra connections.
        Repo merges closed._health_snapshot into stream payload if present.
        """
        try:
            snap = self._get_health_snapshot_cached(symbol)
            if snap:
                closed._health_snapshot = snap
        except Exception:
            pass


    def _lock_is_owned(self) -> bool:
        """
        Test-helper: в CPython у RLock есть _is_owned(). В проде — fail-open.
        """
        try:
            f = getattr(self._lock, "_is_owned", None)
            return bool(f()) if callable(f) else False
        except Exception:
            return False

    def _symbol_lock_ctx(self, symbol: str):
        """
        Возвращает контекст symbol-lock, либо nullcontext если выключено.
        Держим symbol-lock, чтобы:
          - сериализовать tick-loop и external events для одного symbol
          - убрать гонки (SL_HIT/TP_HIT vs process_tick)
        """
        if not self._use_symbol_locks:
            return contextlib.nullcontext()
        return self._get_symbol_lock(symbol)

    def _peek_pos_and_symbol_by_sid(self, sid: str) -> tuple[str | None, str | None]:  # type: ignore
        """
        Быстрый peek под self._lock:
          - возвращает (pos_id, symbol) если позиция жива
          - (None, None) если позиции нет
        ВАЖНО: после выхода из self._lock symbol может устареть — ниже всегда делаем re-check
        уже под symbol-lock + self._lock.
        """
        if not sid:
            return None, None
        with self._lock:
            pos_id = self.pos_by_sid.get(sid)
            if not pos_id:
                return None, None
            pos = self.open_positions.get(pos_id)
            if not pos or getattr(pos, "closed", False):
                return pos_id, None
            return pos_id, str(getattr(pos, "symbol", "") or "")

    def _io_save_tp_hit(self, pos: PositionState, tp_level: int, fill_price: float, closed_qty: float, pnl_part: float, ts_ms: int) -> None:
        self.repo.save_tp_hit(pos, tp_level=tp_level, fill_price=fill_price, closed_qty=closed_qty, pnl_part=pnl_part, ts_ms=ts_ms)
        if getattr(self, "_protective_mirror", None) and tp_level == 1 and not getattr(pos, "tp1_mirrored", False):
            try:
                self._protective_mirror.on_tp1_reached(str(getattr(pos, "sid", "")), str(getattr(pos, "symbol", "")), float(fill_price), int(ts_ms))
                pos.tp1_mirrored = True  # type: ignore
            except Exception:
                pass

    def _io_save_trailing_sync(self, pos: PositionState, ts: int) -> None:
        self.repo.save_trailing_sync(pos, ts)
        if getattr(self, "_protective_mirror", None):
            try:
                m = self._protective_mirror
                sid = str(getattr(pos, "sid", ""))
                sym = str(getattr(pos, "symbol", ""))
                ts_int = int(ts)
                if getattr(pos, "trailing_active", False) and not getattr(pos, "be_mirrored", False):
                    m.on_break_even_activated(sid, sym, float(getattr(pos, "sl", 0.0) or 0.0), ts_int)
                    pos.be_mirrored = True  # type: ignore
                if getattr(pos, "trailing_active", False) and not getattr(pos, "trailing_mirrored", False):
                    m.on_trailing_activated(sid, sym, ts_int)
                    pos.trailing_mirrored = True  # type: ignore
            except Exception:
                pass

    def _io_save_trailing_move(self, pos: PositionState, previous_sl: float, new_sl: float, ts_ms: int) -> None:
        self.repo.save_trailing_move(pos, previous_sl, new_sl, ts_ms)
        if getattr(self, "_protective_mirror", None) and abs(float(previous_sl) - float(new_sl)) > 1e-9:
            with contextlib.suppress(Exception):
                self._protective_mirror.on_sl_moved(
                    str(getattr(pos, "sid", "")), str(getattr(pos, "symbol", "")), str(getattr(pos, "direction", "")),
                    float(previous_sl), float(new_sl), float(getattr(pos, "max_favorable_price", 0.0) or 0.0), int(ts_ms)
                )

    def _io_save_closed(self, closed: TradeClosed, health_snapshot: dict) -> None:
        self.repo.save_closed(closed, health_snapshot=health_snapshot)
        if getattr(self, "_protective_mirror", None):
            with contextlib.suppress(Exception):
                self._protective_mirror.on_position_closed(
                    signal_id=str(getattr(closed, "sid", "")),
                    symbol=str(getattr(closed, "symbol", "")),
                    exit_price=float(getattr(closed, "exit_price", 0.0) or 0.0),
                    pnl_bps=float(getattr(closed, "pnl_bps", 0.0) or 0.0),
                    close_reason=str(getattr(closed, "close_reason_raw", "") or getattr(closed, "close_reason", "")),
                    max_mae_pct=float(getattr(closed, "max_mae_pct", 0.0) or 0.0),
                    ts_ms=int(getattr(closed, "exit_ts_ms", getattr(closed, "closed_at_ms", 0)) or get_ny_time_millis())
                )

    def _run_io_tasks(self, tasks: list[_IOTask]) -> None:
        for t in tasks:
            try:
                t.fn()
            except Exception as e:
                logger.warning("⚠️ IO task failed: %s (%s)", t.desc, e)

    def _stamp_closed_trade_meta(self, pos: PositionState, closed: TradeClosed, close_reason_raw: str) -> None:
        """Delegate to TradeCloseWriter.stamp_closed_meta (Phase 3 thin proxy)."""
        if getattr(self, "_writer", None) is not None:
            self._writer.stamp_closed_meta(pos, closed, close_reason_raw)
            return
        self._stamp_closed_trade_meta_legacy(pos, closed, close_reason_raw)

    def _stamp_closed_trade_meta_legacy(self, pos: PositionState, closed: TradeClosed, close_reason_raw: str) -> None:
        """Original body — fallback only."""
        """
        Единая семантика для tick-close/external-close/orphan:
          - если трейлинг был активен -> close_reason_detail: TRAILING_PROFIT/TRAILING_STOP
          - иначе -> close_reason_detail = close_reason_raw (для аудита)
        """
        try:
            if getattr(pos, "trailing_started", False) or getattr(pos, "trailing_active", False):
                try:
                    closed.trailing_active = True
                    closed.trailing_started = True
                except Exception:
                    pass
                try:
                    closed.close_reason_detail = "TRAILING_PROFIT" if float(getattr(closed, "pnl_net", 0.0) or 0.0) > 1e-8 else "TRAILING_STOP"
                except Exception:
                    closed.close_reason_detail = "TRAILING_STOP"
            else:
                closed.close_reason_detail = str(close_reason_raw)
        except Exception:
            pass

        # -------------------------------------------------------------
        # Phase 5: policy provenance on closed trades
        # -------------------------------------------------------------
        try:
            sp = getattr(pos, "signal_payload", {}) or {}
            _cs_sm0 = (sp.get("config_snapshot") or {}) if isinstance(sp, dict) else {}
            meta = (sp.get("meta") or _cs_sm0.get("meta") or {}) if isinstance(sp, dict) else {}
            prov = meta.get("policy_provenance", {}) if isinstance(meta, dict) else {}

            # Primary: meta.policy_provenance; Fallback: top-level atr_policy_* from signal_preprocess
            def _prov_get(prov_key: str, sp_key: str | None = None, default: str = "") -> str:
                v = prov.get(prov_key)
                if v and str(v) not in ("", "None", "0"):
                    return str(v)
                if sp_key:
                    v2 = sp.get(sp_key)
                    if v2 and str(v2) not in ("", "None", "0"):
                        return str(v2)
                return default

            closed.atr_policy_ver = int(prov.get("policy_ver", 0) or sp.get("atr_policy_ver", 0) or 0)
            closed.atr_policy_tag = _prov_get("policy_tag", "atr_policy_tag")
            closed.atr_policy_source = _prov_get("policy_source")
            closed.atr_policy_scenario = _prov_get("scenario", "kind")
            closed.atr_policy_regime = _prov_get("regime")
            closed.atr_policy_bucket = _prov_get("risk_horizon_bucket")
            closed.atr_stop_ttl_mode = _prov_get("stop_ttl_mode")
            closed.atr_trailing_mode = _prov_get("trailing_mode")
            closed.atr_recovery_run_id = _prov_get("recovery_run_id", "atr_recovery_run_id")
            closed.atr_restore_cert_id = _prov_get("restore_cert_id")
            closed.atr_restore_cert_status = _prov_get("restore_cert_status", "atr_restore_cert_status")
            if prov:
                closed.atr_policy_snapshot_json = prov
            else:
                closed.atr_policy_snapshot_json = {
                    "policy_ver": int(sp.get("atr_policy_ver", 0) or 0),
                    "policy_tag": sp.get("atr_policy_tag", ""),
                    "policy_level": sp.get("atr_policy_level", ""),
                    "active_key": sp.get("atr_policy_key", ""),
                    "reason_code": sp.get("atr_policy_reason_code", ""),
                    "recovery_run_id": sp.get("atr_recovery_run_id", ""),
                    "restore_cert_status": sp.get("atr_restore_cert_status", ""),
                    "_fallback": True,
                }
        except Exception:
            pass


        # -------------------------------------------------------------
        # Phase 2.5: persist live-surface baseline vs selected snapshot
        # -------------------------------------------------------------
        try:
            sp = getattr(pos, "signal_payload", {}) or {}
            # meta lives at sp["meta"] (forwarded from signal stream) or sp["config_snapshot"]["meta"]
            _cs_sm = (sp.get("config_snapshot") or {}) if isinstance(sp, dict) else {}
            meta = (sp.get("meta") or _cs_sm.get("meta") or {}) if isinstance(sp, dict) else {}
            baseline = (meta.get("live_surface_baseline") or {}) if isinstance(meta, dict) else {}
            applied = (meta.get("live_surface_applied") or {}) if isinstance(meta, dict) else {}
            candidate = (meta.get("risk_surface_live_candidate") or {}) if isinstance(meta, dict) else {}

            closed.live_surface_applied = bool(applied.get("applied", False))
            closed.live_surface_reason_code = (applied.get("reason_code") or "")

            closed.baseline_sl_price = float(baseline.get("sl_price") or 0.0)
            closed.baseline_tp1_price = float(baseline.get("tp1_price") or 0.0)

            closed.selected_sl_price = float(candidate.get("selected_sl_price") or 0.0)
            closed.selected_tp1_price = float(candidate.get("selected_tp1_price") or 0.0)
            closed.live_surface_policy_level = (applied.get("policy_level", ""))

            # Fallback: if live surface was INCOMPLETE (atr_profile missing → prices = 0),
            # populate from the actual position levels used by trading logic.
            # This ensures analytics queries always have meaningful TP/SL values.
            if closed.selected_tp1_price == 0.0:
                tp_levels = getattr(pos, "tp_levels", None)
                if tp_levels and len(tp_levels) > 0 and float(tp_levels[0]) > 0:
                    closed.selected_tp1_price = float(tp_levels[0])
            if closed.selected_sl_price == 0.0:
                pos_sl = float(getattr(pos, "sl", 0.0) or 0.0)
                if pos_sl > 0:
                    closed.selected_sl_price = pos_sl
            # baseline = selected when live surface not applied (no override happened)
            if closed.baseline_tp1_price == 0.0:
                closed.baseline_tp1_price = closed.selected_tp1_price
            if closed.baseline_sl_price == 0.0:
                closed.baseline_sl_price = closed.selected_sl_price
        except Exception:
            pass

        # -------------------------------------------------------------
        # Phase 2.6: persist trailing-surface A/B snapshot
        # -------------------------------------------------------------
        try:
            sp = getattr(pos, "signal_payload", {}) or {}
            _cs_sm2 = (sp.get("config_snapshot") or {}) if isinstance(sp, dict) else {}
            meta = (sp.get("meta") or _cs_sm2.get("meta") or {}) if isinstance(sp, dict) else {}
            canary_decision = (meta.get("trailing_canary_decision") or {}) if isinstance(meta, dict) else {}
            surface_diag = (meta.get("trailing_surface_diagnostic") or {}) if isinstance(meta, dict) else {}

            closed.trailing_surface_applied = bool(canary_decision.get("should_apply", False))  # type: ignore
            closed.trailing_surface_reason_code = (canary_decision.get("reason_code") or "")  # type: ignore

            closed.baseline_trailing_offset_atr = float(surface_diag.get("baseline_offset_distance_px") or 0.0)  # type: ignore
            closed.selected_trailing_offset_atr = float(surface_diag.get("selected_offset_distance_px") or 0.0)  # type: ignore
            closed.trailing_policy_level = str(getattr(pos, "trailing_policy_level", ""))
        except Exception:
            pass

        # Phase 0.3: copy scalar horizon/ATR fields onto closed trade for analytics.
        with contextlib.suppress(Exception):
            stamp_closed_trade_horizon_from_position(pos, closed)

    def _update_stats_from_dicts(self, pos_dict: dict[str, Any], closed_dict: dict[str, Any]) -> None:
        """Delegate to PnlCalculator.update_stats (Phase 3 thin proxy)."""
        if getattr(self, "_pnl_calc", None) is not None:
            self._pnl_calc.update_stats(
                pos_dict, closed_dict,
                submit_persist_task_fn=self._submit_regime_guard_persist_task,
            )
            return
        # fallback: original body below (runs only before __init__ fully completes)

    def _update_stats_from_dicts_legacy(self, pos_dict: dict[str, Any], closed_dict: dict[str, Any]) -> None:
        """Original body — kept for reference, not called in normal operation."""
        try:
            # Exclude virtual trades from global stats aggregator
            is_virtual = bool(pos_dict.get("is_virtual") or closed_dict.get("is_virtual"))
            if is_virtual:
                return

            from services.stats_aggregator import StatsAggregator
            StatsAggregator.update_stats(self.redis, pos_dict, closed_dict)
        except Exception as e:
            logger.warning("stats update failed: %s", e)

        if self.regime_guard:
            try:
                signal_id = str(pos_dict.get("sid") or closed_dict.get("sid") or "")
                family = str(pos_dict.get("family") or closed_dict.get("family") or "unknown")
                venue = str(pos_dict.get("venue") or closed_dict.get("venue") or "unknown")
                symbol = str(pos_dict.get("symbol") or closed_dict.get("symbol") or "unknown")
                timeframe = str(pos_dict.get("tf") or pos_dict.get("timeframe") or closed_dict.get("tf") or closed_dict.get("timeframe") or "unknown")

                # recommendation 1: closure from guard
                # Using a dummy object that supports getattr for helpers
                class DummyPos:
                    pass
                dpos = DummyPos()
                dpos.__dict__.update(pos_dict)
                class DummyClosed:
                    pass
                dclosed = DummyClosed()
                dclosed.__dict__.update(closed_dict)

                r_value = self._calculate_r_value(dpos, dclosed)  # type: ignore
                closed_at = self._resolve_closed_at(dclosed)

                persist_task = self.regime_guard.on_signal_closed(
                    signal_id=signal_id,
                    family=family,
                    venue=venue,
                    symbol=symbol,
                    timeframe=timeframe,
                    r_value=r_value,
                    closed_at=closed_at,
                )

                if callable(persist_task):
                    self._submit_regime_guard_persist_task(
                        persist_task,  # type: ignore
                        tags={"family": family, "venue": venue}
                    )
            except Exception as e:
                self.logger.warning("regime guard update failed: %s", e)

    def _persist_closed_trade_io(self, closed: TradeClosed, pos_dict: dict[str, Any], closed_dict: dict[str, Any]) -> None:
        """
        Единая точка записи close (repo + analytics + stats).
        ВАЖНО: вызывать только вне self._lock.
        Phase 3: делегирует в TradeCloseWriter.persist_closed().
        """
        if getattr(self, "_writer", None) is not None:
            self._writer.persist_closed(closed, pos_dict, closed_dict)
            return
        # ── legacy path (before __init__ completes — should not happen) ──
        self._persist_closed_trade_io_legacy(closed, pos_dict, closed_dict)

    def _persist_closed_trade_io_legacy(self, closed: TradeClosed, pos_dict: dict[str, Any], closed_dict: dict[str, Any]) -> None:
        """Original body — fallback only, not called in normal operation."""
        hs: dict[str, str] = {}
        try:
            hs = self._get_health_snapshot_for_trade(str(getattr(closed, "symbol", "")))
        except Exception:
            hs = {}
        self._io_save_closed(closed, health_snapshot=hs)
        try:
            fut = self._db_executor.submit(analytics_db.save_trade_closed, closed)
            fut.add_done_callback(_log_future_exception)
        except Exception as e:
            logger.warning("Failed to submit trade to analytics DB: %s", e)
        try:
            from domain.signal_outcome import from_trade_closed as _build_outcome
            from services.signal_outcome_writer import get_signal_outcome_writer
            _outcome = _build_outcome(closed)
            if _outcome is not None:
                fut_o = self._db_executor.submit(get_signal_outcome_writer().emit, _outcome)
                fut_o.add_done_callback(_log_future_exception)
        except Exception as _so_err:
            logger.warning("⚠️ signal_outcome emit failed (legacy): %s", _so_err)
        self._update_stats_from_dicts(pos_dict, closed_dict)

    def _peek_pos_and_symbol_by_sid(self, sid: str) -> tuple[str | None, str | None]:
        """
        Быстрый peek под self._lock:
          - возвращает (pos_id, symbol) если позиция жива
          - (None, None) если позиции нет
        """
        if not sid:
            return None, None
        with self._lock:
            pid = self.pos_by_sid.get(sid)
            if not pid:
                return None, None
            p = self.open_positions.get(pid)
            if not p:
                return pid, None
            return pid, str(getattr(p, "symbol", "") or "")
    # Spec
    # --------------------
    def _get_spec(self, symbol: str) -> SymbolSpec:
        try:
            info = get_symbol_info(symbol, self.redis)
            return spec_from_symbol_info(info)
        except Exception:
            return SymbolSpec()

    # --------------------
    # Trailing helpers
    # --------------------
    def _is_trailing_after_tp1_enabled(self, pos: PositionState, spec: SymbolSpec) -> bool:
        """
        Решает, включать ли локальный трейлинг после TP1 для данной позиции.

        Приоритет (SymbolSpec имеет ВЫСШИЙ ПРИОРИТЕТ, даже если значение False):
        1) SymbolSpec.trailing_after_tp1_enabled / trailing_enabled (конфиг из Redis) - ВЫСШИЙ ПРИОРИТЕТ
           Если в SymbolSpec явно задано False, оно перекрывает все ENV переменные
        2) ENV: TRAILING_AFTER_TP1_<SYMBOL>   (используется только если SymbolSpec не задан)
        3) allowlist по source: TRAILING_AFTER_TP1_SOURCES (дефолт: CryptoOrderFlow)
        """
        symbol_up = (pos.symbol or "").upper()
        source_norm = canon_source(pos.source or "")

        # 1) Флаг в symbol spec (из Redis) - ВЫСШИЙ ПРИОРИТЕТ
        # Проверяем наличие атрибута (даже если значение False, оно имеет приоритет над ENV)
        for attr in ("trailing_after_tp1_enabled", "trailing_enabled"):
            if hasattr(spec, attr):
                val = getattr(spec, attr)
                # Если атрибут существует, используем его значение (даже если False)
                if isinstance(val, (bool, int, float)):
                    return val  # type: ignore
                elif isinstance(val, str) and val.strip():
                    # Пустая строка считается как "не задано"
                    return val.lower() in ("1", "true", "yes", "on")
                # Если значение None или пустое - продолжаем поиск по другим атрибутам

        # 2) Явный override по символу через ENV (только если SymbolSpec не задан)
        #    Например: TRAILING_AFTER_TP1_ETHUSDT=true
        env_sym = os.getenv(f"TRAILING_AFTER_TP1_{symbol_up}")
        if env_sym is not None:
            return env_sym.lower() in ("1", "true", "yes", "on")

        # 3) allowlist по source (fallback)
        if self.trailing_after_tp1_sources:
            return source_norm in self.trailing_after_tp1_sources

        return False

    def _resolve_trailing_tp1_offset_atr(self, pos: PositionState, spec: SymbolSpec) -> float:
        """
        Возвращает множитель ATR для сдвига SL после TP1.

        Приоритет (обновлено Phase 3):
        0) Active Promotion Policy (если trailing_mode == live) - АБСОЛЮТНЫЙ ПРИОРИТЕТ
        1) SymbolSpec.trailing_tp1_offset_atr (из Redis-конфига по символу)
        2) ENV: TRAILING_TP1_OFFSET_ATR_<SYMBOL> (используется только если SymbolSpec не задан)
        3) ENV: TRAILING_TP1_OFFSET_ATR_<SOURCE>
        4) Глобальный TRAILING_TP1_OFFSET_ATR
        """
        symbol_up = (pos.symbol or "").upper()
        source_norm = canon_source(pos.source or "")

        # 0) Active Promotion Policy override (now handled directly in execution loop via get_atr_policy_resolver)


        # 1) spec override (конфиг по символу — результат калибратора)
        try:
            v = getattr(spec, "trailing_tp1_offset_atr", None)
            if v is not None:
                v_float = float(v)
                if v_float > 0:
                    return v_float
        except Exception:
            pass

        # 1b) TrailingProfilesRegistry — shared single source of truth with binance_executor.
        # Profile is resolved identically to executor: pos.trail_profile → signal_payload → ENV.
        try:
            reg = getattr(self, "_trailing_profiles", None)
            if reg is not None:
                profile_name = str(
                    getattr(pos, "trail_profile", None)
                    or (getattr(pos, "signal_payload", None) or {}).get("trail_profile", "")
                    or os.getenv("BINANCE_TRAIL_PROFILE", "rocket_v1")
                ).strip() or "rocket_v1"
                profile = reg.get(profile_name)
                if profile is None:
                    profile = reg.get("rocket_v1")  # same fallback as binance_executor
                if profile is not None:
                    v_reg = float(profile.atr_mult)
                    if v_reg > 0:
                        return v_reg
        except Exception:
            pass  # fail-open: continue to ENV chain

        # 2) env по символу, напр. TRAILING_TP1_OFFSET_ATR_ETHUSDT=0.4
        env_sym = os.getenv(f"TRAILING_TP1_OFFSET_ATR_{symbol_up}")
        if env_sym:
            try:
                v = float(env_sym)
                if v > 0:
                    return v
            except Exception:
                pass

        # 3) env по source, напр. TRAILING_TP1_OFFSET_ATR_CRYPTOORDERFLOW=0.5
        env_src = os.getenv(f"TRAILING_TP1_OFFSET_ATR_{source_norm.upper()}")
        if env_src:
            try:
                v = float(env_src)
                if v > 0:
                    return v
            except Exception:
                pass

        # 4) глобальный дефолт из compose
        return self.trailing_tp1_offset_default

    # --------------------
    # Paper vs Demo comparison report (Telegram)
    # --------------------
    def _pvd_record_closed(self, pos: PositionState, closed: TradeClosed) -> None:
        """Record closed trade metadata for paper-vs-demo comparison.
        DISABLED per user request.
        """
        return

    def _maybe_paper_vs_demo_report(self) -> None:
        """Fire Telegram report every N closed trades.
        DISABLED per user request.
        """
        return

    def _send_paper_vs_demo_report(self) -> None:
        """Build comparison report and send to Telegram."""
        paper_by_sid = {}
        for t in self._pvd_recent_closed:
            if t.get("sid"):
                paper_by_sid[t["sid"]] = t

        if not paper_by_sid:
            return

        # Read demo closed events from orders:exec stream
        demo_by_sid: dict[str, dict[str, Any]] = {}
        try:
            demo_raw = self.redis.xrevrange(
                self._pvd_demo_stream, count=self._pvd_report_every_n * 10
            )
            for _mid, data in (demo_raw or []):
                raw = {}
                for k, v in data.items():
                    key = k.decode() if isinstance(k, bytes) else str(k)
                    val = v.decode() if isinstance(v, bytes) else str(v)
                    raw[key] = val
                event_type = raw.get("event_type", raw.get("type", raw.get("status", ""))).upper()
                if event_type not in ("CLOSE", "CLOSED", "FILLED", "SL_HIT", "TP_HIT",
                                      "TRAILING_STOP", "FORCE_CLOSE", "FINALIZED"):
                    continue
                sid = raw.get("sid", "")
                if sid and sid not in demo_by_sid:
                    demo_by_sid[sid] = {
                        "entry_price": float(raw.get("entry_price", raw.get("entry_px", 0)) or 0),
                        "exit_price": float(raw.get("exit_price", raw.get("exit_px", raw.get("close_price", 0))) or 0),
                        "pnl_net": float(raw.get("pnl_net", raw.get("pnl", raw.get("realized_pnl", 0))) or 0),
                        "qty": float(raw.get("qty", raw.get("lot", raw.get("quantity", 0))) or 0),
                        "close_reason": raw.get("close_reason", raw.get("reason", event_type)),
                    }
        except Exception as e:
            logger.warning("⚠️ Failed to read demo stream: %s", e)
            return

        # Match by SID
        matched = []
        for sid, paper in paper_by_sid.items():
            demo = demo_by_sid.get(sid)
            if not demo:
                continue
            ep_mid = (paper["entry_price"] + demo["entry_price"]) / 2.0
            xp_mid = (paper["exit_price"] + demo["exit_price"]) / 2.0
            matched.append({
                "symbol": paper["symbol"],
                "dir": paper["direction"][:1],
                "entry_Δ_bps": (paper["entry_price"] - demo["entry_price"]) / ep_mid * 10000 if ep_mid > 0 else 0,
                "exit_Δ_bps": (paper["exit_price"] - demo["exit_price"]) / xp_mid * 10000 if xp_mid > 0 else 0,
                "paper_pnl": paper["pnl_net"],
                "demo_pnl": demo["pnl_net"],
                "pnl_Δ": paper["pnl_net"] - demo["pnl_net"],
                "paper_qty": paper.get("qty", 0.0),
                "demo_qty": demo.get("qty", 0.0),
                "paper_notional": paper.get("qty", 0.0) * paper["entry_price"],
            })

        if not matched:
            return  # no overlapping trades yet

        # Build report
        n = len(matched)
        total_paper = sum(m["paper_pnl"] for m in matched)
        total_demo = sum(m["demo_pnl"] for m in matched)
        avg_entry_d = sum(abs(m["entry_Δ_bps"]) for m in matched) / n
        avg_exit_d = sum(abs(m["exit_Δ_bps"]) for m in matched) / n

        if avg_entry_d < 5 and avg_exit_d < 10:
            quality = "✅ EXCELLENT"
        elif avg_entry_d < 10 and avg_exit_d < 20:
            quality = "🟡 GOOD"
        else:
            quality = "🔴 DIVERGENT"

        # Read actual demo leverage from Redis (written by binance_executor)
        demo_lev_map: dict[str, int] = {}
        try:
            raw_lev = self.redis.hgetall("exec:leverage:actual")
            for k, v in (raw_lev or {}).items():
                sym = k.decode() if isinstance(k, bytes) else str(k)
                val = v.decode() if isinstance(v, bytes) else str(v)
                with contextlib.suppress(Exception):
                    demo_lev_map[sym.upper()] = int(float(val))
        except Exception:
            pass

        # Determine actual demo leverage for matched symbols
        matched_demo_levs = set()
        for m in matched:
            sym = m["symbol"].upper()
            lev = demo_lev_map.get(sym, self._pvd_demo_leverage)
            m["demo_lev"] = lev
            matched_demo_levs.add(lev)

        if len(matched_demo_levs) == 1:
            demo_lev_str = f"{matched_demo_levs.pop()}x"
        elif matched_demo_levs:
            demo_lev_str = f"{min(matched_demo_levs)}-{max(matched_demo_levs)}x"
        else:
            demo_lev_str = f"{self._pvd_demo_leverage}x"

        lines = [
            f"📊 *Paper vs Demo Report* (#{self._pvd_session_closed})",
            f"Matched: {n} trades | Paper-only: {len(paper_by_sid) - n}",
            f"⚖️ Leverage: paper={self._pvd_paper_leverage}x | demo={demo_lev_str} (actual)",
            "",
        ]
        # Top 5 trades
        for m in matched[:5]:
            def _qfmt(q):
                return f"{q:.4f}" if q < 1 else f"{q:.2f}"
            lines.append(
                f"`{m['symbol']:>8} {m['dir']}` "
                f"qty={_qfmt(m['paper_qty'])}|{_qfmt(m['demo_qty'])} "
                f"Δentry={m['entry_Δ_bps']:+.1f}bps "
                f"Δexit={m['exit_Δ_bps']:+.1f}bps "
                f"PnL: ${m['paper_pnl']:+.1f} vs ${m['demo_pnl']:+.1f}"
            )
        if n > 5:
            lines.append(f"... +{n - 5} more")

        total_notional = sum(m["paper_notional"] for m in matched)

        lines.extend([
            "",
            f"Σ PnL paper: ${total_paper:+.2f}",
            f"Σ PnL demo:  ${total_demo:+.2f}",
            f"Δ divergence: ${total_paper - total_demo:+.2f}",
            f"💰 Total notional: ${total_notional:,.0f}",
            f"Avg Δentry: {avg_entry_d:.1f}bps | Avg Δexit: {avg_exit_d:.1f}bps",
            f"Quality: {quality}",
        ])

        text = "\n".join(lines)

        try:
            self.redis.xadd(
                self._pvd_notify_stream,
                {"text": text, "source": "paper_vs_demo", "parse_mode": "Markdown"},
                maxlen=200000,
            )
            logger.info("📊 Paper vs Demo report #%d sent (%d matched)",
                        self._pvd_session_closed, n)
        except Exception as e:
            logger.warning("⚠️ Failed to send paper_vs_demo report: %s", e)

    # --------------------
    # Trailing audit stream (Point 4)
    # --------------------
    def _emit_trailing_audit(
        self,
        event_type: str,
        pos: PositionState,
        new_sl: float,
        prev_sl: float,
        ts_ms: int,
    ) -> None:
        """Emit trailing event to unified audit stream for paper-vs-real comparison."""
        if not self._trailing_audit_stream:
            return
        with contextlib.suppress(Exception):
            self.redis.xadd(
                self._trailing_audit_stream,
                {
                    "source": "trade_monitor",
                    "event_type": event_type,
                    "sid": str(getattr(pos, "sid", "") or ""),
                    "symbol": str(getattr(pos, "symbol", "") or ""),
                    "direction": str(getattr(pos, "direction", "") or ""),
                    "prev_sl": str(prev_sl),
                    "new_sl": str(new_sl),
                    "entry_price": str(getattr(pos, "entry_price", 0.0)),
                    "trailing_distance": str(getattr(pos, "trailing_distance", 0.0)),
                    "ts_ms": str(ts_ms),
                },
                maxlen=self._trailing_audit_maxlen,
            )

    # --------------------
    # Single-active-position guard (read-only check for trade_monitor)
    # --------------------
    def _tm_check_single_active_guard(self, sig: SignalNorm) -> bool:
        """
        Return True if the signal should be BLOCKED by the single-active-position guard.

        Reads the same Redis guard key that binance_executor writes
        (key prefix = ORDERS_ACTIVE_SYMBOL_KEY_PREFIX, default: orders:active_symbol_sid:).

        Design constraints:
          - No exchange-truth API call (paper trades don't have Binance positions).
          - No release logic — guard is owned exclusively by binance_executor.
          - Fail-open: any exception returns False (allow), never blocks on error.
        """
        if not self.exec_single_active_position_per_symbol:
            return False
        try:
            symbol = str(getattr(sig, "symbol", "") or "").strip().upper()
            if not symbol:
                return False
            key = f"{self._active_symbol_key_prefix}{symbol}"
            raw = self.redis.get(key)
            if not raw:
                return False
            doc = json.loads(raw)
            if not isinstance(doc, dict):
                return False
            # Skip released / tombstoned guards
            guard_status = (doc.get("guard_status") or "active").lower()
            if guard_status in ("released", "tombstone"):
                return False
            blocked_sid = (doc.get("sid") or "").strip()
            if not blocked_sid:
                return False
            # Don't double-block the same sid (idempotent reprocessing)
            sig_sid = str(getattr(sig, "sid", "") or "")
            if sig_sid and blocked_sid == sig_sid:
                return False
            # ── Stale guard watchdog: bypass if guard is too old ──
            if self._guard_stale_timeout_ms > 0:
                updated_ms = int(
                    doc.get("updated_at_ms")
                    or doc.get("ts_state_commit_ms")
                    or doc.get("ts_event_ms")
                    or 0
                )
                if updated_ms > 0:
                    age_ms = get_ny_time_millis() - updated_ms
                    if age_ms > self._guard_stale_timeout_ms:
                        logger.warning(
                            "⚠️ [GUARD] Stale guard for %s (age=%ds > stale=%ds) — bypassing",
                            symbol, age_ms // 1000, self._guard_stale_timeout_ms // 1000,
                        )
                        with contextlib.suppress(Exception):
                            TM_SIGNAL_GUARD_STALE_BYPASS.labels(symbol=symbol).inc()
                        return False  # stale → pass-through
            return True
        except Exception:
            return False  # fail-open: never block on Redis error

    # --------------------
    # Signal → open position
    # --------------------
    def on_signal(self, raw_signal: dict[str, Any]) -> str | None:
        """
        [PHASE 2: JITTER BUFFER]
        Нормализует сигнал и помещает в буфер переупорядочивания.
        Реальная обработка (открытие позиции) происходит в _flush_signal_buffer().
        """
        sig = self._normalize_signal(raw_signal)
        if not sig:
            return None

        # --- Strict DTO Versioning (schema_version: 1) ---
        # Any signal without correct schema_version is considered legacy or malformed and must be rejected.
        if sig.schema_version != 1:
            symbol_up = (sig.symbol or "UNKNOWN").upper()
            logger.warning("🚫 Signal REJECTED: version mismatch (expected schema_version: 1, got %d) symbol=%s sid=%s",
                           sig.schema_version, symbol_up, sig.sid)
            with contextlib.suppress(Exception):
                TM_SIGNAL_VERSION_MISMATCH.labels(symbol=symbol_up).inc()
            return None

        # [JITTER BUFFER] Enqueue for processing
        with self._lock:
            # P0 FIX: bisect.insort O(log n) replaces full sort O(n log n) — critical at backlog
            # Python 3.10+ supports key= in bisect.insort
            bisect.insort(self._signal_buffer, sig, key=lambda s: s.entry_ts_ms)  # type: ignore

            # Simple telemetry (metrics added later)
            if len(self._signal_buffer) > 100:
                logger.warning(f"⚠️ [JITTER_BUFFER] Backlog is high: {len(self._signal_buffer)} signals")

            with contextlib.suppress(Exception):
                TM_JITTER_BUFFER_SIZE.set(len(self._signal_buffer))

        # Evaluate flush on every signal so that wall-clock fallback can release stuck signals
        self._flush_signal_buffer()

        return "buffered"

    def _process_signal_norm(self, sig: SignalNorm) -> str | None:
        """
        Выполняет реальное открытие позиции на основе нормализованного сигнала.
        Вызывается из _flush_signal_buffer().
        """
        raw_signal = sig.payload  # Recover raw payload for legacy paths

        # --- Strict DTO Versioning (schema_version: 1) ---
        # Any signal without correct schema_version is considered legacy or malformed and must be rejected.
        # [FIXED] schema_version is parsed cleanly in _normalize_signal
        if sig.schema_version != 1:
            symbol_up = (sig.symbol or "UNKNOWN").upper()
            logger.warning("🚫 Signal REJECTED: version mismatch (expected schema_version: 1, got %d) symbol=%s sid=%s",
                           sig.schema_version, symbol_up, sig.sid)
            with contextlib.suppress(Exception):
                TM_SIGNAL_VERSION_MISMATCH.labels(symbol=symbol_up).inc()
            return None

        # Check if it's a real entry from policy vs a raw signal
        # ((sig.source or "").lower() == "smt_entry_policy")
        sig_conf = float(sig.payload.get("confidence") or sig.payload.get("conf") or 0.0)

        # Use global confidence threshold (single source of truth)
        conf_threshold = self.shadow_conf_threshold

        # Enforce confidence threshold for ALL signals (even policy entries)
        if sig_conf < (conf_threshold / 100.0):
            # Ignore signals below threshold
            logger.warning("⏭️ Signal filtered: confidence %.1f%% < threshold %.1f%% for %s",
                        sig_conf * 100.0, conf_threshold, sig.symbol)
            return None

        # ── Single-active-position guard (global — applies to paper trades too) ──
        if self._tm_check_single_active_guard(sig):
            symbol_up = (sig.symbol or "").upper()
            logger.warning(
                "⏭️ [GUARD] Signal blocked by single_active_position_per_symbol: "
                "symbol=%s sid=%s is_virtual=%s",
                symbol_up, sig.sid, sig.payload.get("is_virtual", 0),
            )
            with contextlib.suppress(Exception):
                TM_SIGNAL_BLOCKED_SINGLE_ACTIVE.labels(symbol=symbol_up).inc()
            return None

        # ── Simulated slippage for paper trades (Point 6) ──
        # Shifts entry_price adversely to simulate real-world fill slippage.
        # LONG → entry moves UP (worse fill); SHORT → entry moves DOWN (worse fill).
        # ⚠️ FIX (2026-04-25): SL and TP MUST shift by the same delta to maintain
        # constant distance from entry. Without this, TP can end up below entry
        # for LONG positions (e.g., TP1=77490 < entry=77501).
        if self._simulated_slippage_bps > 0 and hasattr(sig, "entry_price"):
            try:
                ep = float(sig.entry_price or 0.0)
                if ep > 0:
                    slip_frac = self._simulated_slippage_bps / 10_000.0
                    direction = str(getattr(sig, "direction", "") or "").upper()
                    if direction in ("LONG", "BUY"):
                        sig.entry_price = ep * (1.0 + slip_frac)
                    else:
                        sig.entry_price = ep * (1.0 - slip_frac)
                    # Shift SL/TP by the same absolute delta to preserve distance
                    delta = sig.entry_price - ep
                    if hasattr(sig, "sl") and float(getattr(sig, "sl", 0) or 0) > 0:
                        sig.sl = float(sig.sl) + delta
                    if hasattr(sig, "tp_levels") and getattr(sig, "tp_levels", None):
                        sig.tp_levels = [tp + delta for tp in sig.tp_levels]
                    with contextlib.suppress(Exception):
                        TM_SIMULATED_SLIPPAGE_BPS.labels(
                            symbol=(sig.symbol or "").upper()
                        ).observe(self._simulated_slippage_bps)
            except Exception:
                pass

        # Prepare state (in-memory)
        with self._lock:
            # фантом-дедуп: если sid уже mapped в открытых позициях → не открываем второй раз
            if sig.sid and sig.sid in self.pos_by_sid:
                pos_id = self.pos_by_sid[sig.sid]
                pos = self.open_positions.get(pos_id)
                # No upgrade logic - everything stays virtual
                logger.debug("⏭️ Duplicate signal ignored (sid=%s already open)", sig.sid)
                with contextlib.suppress(Exception):
                    TM_SIGNAL_DUPLICATE.labels(symbol=(sig.symbol or "").upper(), reason="already_open").inc()
                return pos_id

            # ✅ Глобальный sid-dedup для lossless reprocessing
            if sig.sid and not self._sid_claim(sig.sid, ttl_sec=30):
                logger.debug("⏭️ Duplicate signal ignored (sid=%s already processed globally)", sig.sid)
                with contextlib.suppress(Exception):
                    TM_SIGNAL_DUPLICATE.labels(symbol=(sig.symbol or "").upper(), reason="processed_globally").inc()
                return None

            # ✅ Per-symbol guard: 1 symbol = 1 open position (in-memory)
            # When EXEC_SINGLE_ACTIVE_POSITION_PER_SYMBOL=1, block if symbol
            # already has open position(s) in the in-memory index.
            if self.exec_single_active_position_per_symbol:
                sym_up = (sig.symbol or "").upper()
                existing = self.open_by_symbol.get(sym_up)
                if existing:
                    existing_pid = next(iter(existing), "?")
                    logger.debug(
                        "⏭️ Signal blocked by per-symbol guard "
                        "(symbol=%s, sid=%s, existing_pos=%s)",
                        sym_up, sig.sid, existing_pid,
                    )
                    with contextlib.suppress(Exception):
                        TM_SIGNAL_BLOCKED_SINGLE_ACTIVE.labels(symbol=sym_up).inc()
                    # Release sid claim so signal can be retried when guard clears
                    self._sid_release(sig.sid)
                    return None

            spec = self._get_spec(sig.symbol)
            pos = create_position(sig, spec)

            # Phase 0.2: attach horizon contract to PositionState from signal payload
            stamp_position_from_signal_payload(pos, sig.payload, source="signal_open")

            # Inherit is_virtual from payload or determine via shadow mode
            is_v = int(sig.payload.get("is_virtual", 0) or 0) > 0
            v_status = str(sig.payload.get("validation_status") or "").lower()
            g_mode = str(sig.payload.get("of_gate_mode") or sig.payload.get("gate_mode") or "").upper()

            if g_mode == "SHADOW" and v_status == "failed":
                is_v = True

            pos.is_virtual = is_v
            pos.v_gate_status = v_status if is_v else "na"

            # ---------------------------------------------------------------------
            # NEW: stamp entry_regime onto the position (used by empirical calibration).
            # Source of truth:
            #   - if normalized signal already has regime -> use it
            #   - else keep empty and downstream will fallback to "regime"/"na"
            # ---------------------------------------------------------------------
            try:
                # Regime sources, in priority order:
                #   1. sig.entry_regime / sig.regime  (normalized DTO fields)
                #   2. raw_signal.entry_regime / raw_signal.regime  (top-level legacy)
                #   3. raw_signal.indicators.regime  ← upstream-fix-aware path:
                #      `_publish_of_inputs` writes regime into indicators dict;
                #      top-level may stay None when regime resolved late.
                rg = (
                    getattr(sig, "entry_regime", None)
                    or getattr(sig, "regime", None)
                    or (raw_signal.get("entry_regime") if isinstance(raw_signal, dict) else None)
                    or (raw_signal.get("regime") if isinstance(raw_signal, dict) else None)
                )
                if not rg and isinstance(raw_signal, dict):
                    _ind = raw_signal.get("indicators")
                    if isinstance(_ind, dict):
                        _ind_rg = _ind.get("regime")
                        if _ind_rg and str(_ind_rg).lower() not in ("", "na", "none", "null", "unknown"):
                            rg = _ind_rg
                if rg is not None and not getattr(pos, "entry_regime", None):
                    pos.entry_regime = str(rg).lower()
            except Exception:
                pass

            # ---------------------------------------------------------------------
            # NEW: Conditional trailing decision propagated from normalized payload -> position.
            # Fail-open:
            #   - if payload has nothing -> default True (legacy behavior).
            # ---------------------------------------------------------------------
            try:
                payload = sig.payload if isinstance(getattr(sig, "payload", None), dict) else {}

                # --- NEW: Persist AB/context into PositionState.signal_payload (no schema migration) ---
                sp = getattr(pos, "signal_payload", None)
                if not isinstance(sp, dict):
                    sp = {}
                    pos.signal_payload = sp

                # AB attribution (prefer flat payload fields)
                sp["ab_arm"] = (payload.get("ab_arm", sp.get("ab_arm", "A")) or "A").upper()
                sp["ab_group"] = (payload.get("ab_group", sp.get("ab_group", "default")) or "default")
                sp["ab_key"] = (payload.get("ab_key", sp.get("ab_key", "")) or "")
                sp["arm_ver"] = int(payload.get("arm_ver", sp.get("arm_ver", 0)) or 0)

                # Context for winner slicing
                ctx = payload.get("ctx") if isinstance(payload.get("ctx"), dict) else {}
                # Also try top-level regime/zone_id from payload if not in ctx
                sp["regime"] = (ctx.get("regime", getattr(sig, "regime", None)) or "na").lower()  # type: ignore
                sp["zone_id"] = (ctx.get("zone_id", getattr(sig, "zone_id", None)) or "")  # type: ignore

                # --- Calibration / shadow trade fields ---
                # These get persisted per-position so they survive POSITION_CLOSED → trades:closed join,
                # giving calibrators (cont_ctx_window, adverse_gate, etc.) a real outcome feedback loop.
                try:
                    from services.shadow_calib_meta import extract_calib_fields, stamp_virtual_if_calib
                    calib_extracted = extract_calib_fields(payload)
                    sp.update(calib_extracted)
                    stamp_virtual_if_calib(sp)
                except Exception:
                    pass  # fail-open: never break position open on calib import

                # Conditional trailing logic
                v = payload.get("trail_after_tp1", True)
                pos.trail_after_tp1 = (v != 0)
                rr = payload.get("trail_after_tp1_reason", "")
                if rr:
                    pos.trail_after_tp1_reason = str(rr)[:256]
            except Exception:
                pass

            self.open_positions[pos.id] = pos
            if pos.sid:
                self.pos_by_sid[pos.sid] = pos.id
            self._index_add(pos)
            # P1-9: attach FSM (PENDING → OPEN)
            self._attach_fsm(pos)

        # ✅ PERSIST/OPEN OUTSIDE LOCK (split-lock architecture)
        # Includes rollback if critical I/O fails to avoid zombies.
        try:
            # P0 LATENCY FIX: pipeline persist — 1 RTT instead of 5
            # Feature flag: TM_PIPELINE_PERSIST=1 (default ON)
            # Rollback: TM_PIPELINE_PERSIST=0 → sequential Redis calls
            if os.getenv("TM_PIPELINE_PERSIST", "1") == "1":
                pipe = self.redis.pipeline(transaction=False)
                self.repo.persist_signal_pipe(sig, pipe)
                self.repo.save_open_pipe(pos, pipe)
                # sid_finalize into pipeline
                if sig.sid:
                    _dedup_key = self._sid_dedup_key(sig.sid)
                    pipe.set(_dedup_key, "done", ex=7 * 24 * 3600)
                # event OPEN into pipeline
                self.repo.append_event_pipe(ev=_ev_open(pos), pipe=pipe)
                pipe.execute()
            else:
                # Legacy sequential path (rollback)
                self.repo.persist_signal(sig)
                self.repo.save_open(pos)
                self._sid_finalize(sig.sid, ttl_days=7)
                self.repo.append_event(ev=_ev_open(pos))

            if getattr(self, "_protective_mirror", None):
                try:
                    self._protective_mirror.on_position_opened(
                        signal_id=str(getattr(sig, "sid", "") or ""),
                        symbol=str(getattr(pos, "symbol", "") or ""),
                        side=str(getattr(pos, "direction", "") or ""),
                        entry_price=float(getattr(pos, "entry_price", 0.0) or 0.0),
                        sl=float(getattr(pos, "sl", 0.0) or 0.0),
                        tp1=float(pos.tp_levels[0]) if getattr(pos, "tp_levels", None) else 0.0,
                        ts_ms=int(getattr(pos, "entry_ts_ms", 0) or get_ny_time_millis())
                    )
                except Exception as e:
                    logger.debug("Mirror on_position_opened err: %s", e)
        except Exception as e:
            # Rollback in-memory state to prevent zombie positions
            logger.error(f"❌ CRITICAL: Failed to persist position {pos.id} for sid {sig.sid}: {e}", exc_info=True)
            with self._lock:
                self._pop_pos(pos.id)
            self._sid_release(sig.sid)
            raise

        with self._lock:
            self._open_log_counter += 1
            log_it = (self._open_log_counter % 100 == 0) or (self._open_log_counter == 1)

        if log_it:
            logger.info("OPEN %s %s %s @ %.5f [#%d]%s", pos.id, pos.direction, pos.symbol, pos.entry_price, self._open_log_counter, " [VIRTUAL]" if pos.is_virtual else "")
        return pos.id

    def on_audit(self, audit_data: dict[str, Any]) -> None:
        """
        Processes gate audit events to update v_gate_status on positions.
        """
        try:
            # Audit payload can be flat or have 'data' as JSON
            data = audit_data
            if "data" in audit_data and isinstance(audit_data["data"], str):
                try:
                    data = json.loads(audit_data["data"])
                except Exception:
                    data = audit_data

            if not isinstance(data, dict):
                return

            sid = data.get("entry_id") or data.get("sid")
            if not sid:
                return

            with self._lock:
                pos_id = self.pos_by_sid.get(sid)
                if not pos_id:
                    return

                pos = self.open_positions.get(pos_id)
                if not pos:
                    return

                # Update gate status
                ok = data.get("ok")
                pos.v_gate_status = "passed" if ok else "failed"
                pos.v_gate_reason = str(data.get("reason_code") or data.get("notes") or "")

                # Persist change if it's an open position
                self.repo.save_open(pos)
        except Exception as e:
            logger.warning(f"Error in on_audit: {e}")

    def _flush_signal_buffer(self) -> None:
        """
        [PHASE 2: JITTER BUFFER]
        Освобождает ("релизит") сигналы из буфера, если рыночное время достаточно продвинулось.
        Рыночное время определяется по self._max_tick_ts_ms.
        """
        with getattr(self, "_lock", contextlib.nullcontext()):
            signal_buffer = getattr(self, "_signal_buffer", None)
            if not signal_buffer:
                return

            mature_signals = []
            now_ms = get_ny_time_millis()

            jitter_ms = getattr(self, "_jitter_ms", 50)
            is_sim = getattr(self, "_is_sim", False)
            fallback_margin_ms = max(2000, jitter_ms * 4)
            max_tick_ts_ms = getattr(self, "_max_tick_ts_ms", 0)

            # Buffer is already sorted by ts_ms in on_signal
            while signal_buffer:
                sig = signal_buffer[0]
                # Условие "зрелости" сигнала:
                # 1. Мы увидели тики со временем >= sig.ts + jitter (по рыночному времени)
                # 2. В режиме симуляции (jitter=0) достаточно увидеть тик со временем >= sig.ts
                # 3. Wall-clock fallback: если тиков нет, продвигаем по системному времени (только для Live)
                is_mature_by_tick = sig.entry_ts_ms <= (max_tick_ts_ms - jitter_ms)
                is_mature_by_clock = not is_sim and ((now_ms - sig.entry_ts_ms) > (jitter_ms + fallback_margin_ms))

                if is_mature_by_tick or is_mature_by_clock:
                    mature_signals.append(signal_buffer.pop(0))
                else:
                    # Остальные сигналы еще "молодые"
                    break

            with contextlib.suppress(Exception):
                TM_JITTER_BUFFER_SIZE.set(len(signal_buffer))

        # Обработка "зрелых" сигналов (ВНЕ LOCK если возможно, но _process_signal_norm сам управляет локом)
        for sig in mature_signals:
            try:
                # Record jitter latency metric
                try:
                    rel_lat = self._max_tick_ts_ms - sig.entry_ts_ms
                    TM_JITTER_RELEASE_LATENCY_MS.labels(symbol=(sig.symbol or "").upper()).observe(rel_lat)
                except Exception:
                    pass

                # [RELEASE]
                self._process_signal_norm(sig)
            except Exception as e:
                logger.error(f"❌ Error processing released signal {sig.sid}: {e}", exc_info=True)

    def process_signal(self, raw: dict[str, Any]) -> str | None:
        """
        Алиас для on_signal() для обратной совместимости.
        Обрабатывает сигнал и открывает позицию.
        """
        return self.on_signal(raw)

    def _normalize_signal(self, raw: dict[str, Any]) -> SignalNorm | None:
        try:
            data = raw
            if "data" in raw and isinstance(raw["data"], str):
                try:
                    data = json.loads(raw["data"])
                except Exception:
                    data = raw

            sid = str(data.get("sid") or data.get("signal_id") or "")
            symbol = canon_symbol(data.get("symbol") or "")

            # ✅ Получаем spec один раз для применения дефолтов
            spec = self._get_spec(symbol)

            # Accept both:
            #   - "tf"        (internal canonical name)
            #   - "timeframe" (emitter/outbox payload uses this in several handlers)
            # This keeps stream payload stable while preserving TradeMonitor behavior.
            tf = canon_tf(data.get("tf") or data.get("timeframe") or "tick")
            source = canon_source(data.get("source") or data.get("strategy_source") or "Unknown")
            strategy = canon_strategy(data.get("strategy") or data.get("strategy_name") or "")
            if strategy == "unknown":
                # если strategy нет — берём из source
                from domain.normalizers import strategy_from_source
                strategy = strategy_from_source(source)

            direction = str(data.get("side") or data.get("direction") or "").upper()
            # Normalize Binance-style BUY/SELL to internal LONG/SHORT
            _SIDE_MAP = {"BUY": "LONG", "SELL": "SHORT", "LONG": "LONG", "SHORT": "SHORT"}
            direction = _SIDE_MAP.get(direction, direction)
            if direction not in ("LONG", "SHORT"):
                return None

            entry_price = float(data.get("entry") or data.get("price") or 0.0)
            if entry_price <= 0:
                return None

            ts_raw = data.get("ts") or data.get("timestamp") or get_ny_time_millis()
            # STRICT anti-regression:
            #   - Reject non-epoch clocks (minutes-of-day, counters) by forcing 0.
            #   - If 0 => downstream behavior stays fail-open (fallback to now happens above).
            try:
                from domain.time_utils import normalize_ts_ms_hard
                now_ms = get_ny_time_millis()
                entry_ts_ms = normalize_ts_ms_hard(int(float(ts_raw)) if ts_raw else 0, now_ms=now_ms)
            except Exception:
                from domain.time_utils import normalize_ts_ms
                entry_ts_ms = normalize_ts_ms(int(float(ts_raw)) if ts_raw else 0)
            # HARDER: if ts provided but invalid, correct to now and mark for audit.
            # This prevents "entry_ts_ms=0" silently leaking into downstream (duration, session, gates).
            if entry_ts_ms <= 0:
                now_ms = get_ny_time_millis()
                entry_ts_ms = now_ms
                # Preserve original raw for debugging, but make behavior explicit.
                try:
                    if "ts_invalid" not in data:
                        data["ts_invalid"] = 1
                    if "ts_raw" not in data:
                        data["ts_raw"] = ts_raw
                    data["ts_corrected"] = 1
                    data["ts_corrected_to"] = "now"
                    data["ts"] = int(now_ms)
                    data["ts_ms"] = int(now_ms)
                except Exception:
                    pass

            # ✅ FINAL CLAMP: Never allow future timestamps to leak into trades (breaks reporting windows)
            # Time Sync: in replays, "future" is relative to the latest seen tick, not wall clock.
            # Fix: if we have tracking market time (_max_tick_ts_ms > 0) AND it is significantly
            # older than wall clock (> 24 hours), we treat it as replay mode.
            now_ms_current = get_ny_time_millis()
            is_replay = self._max_tick_ts_ms > 0 and abs(now_ms_current - self._max_tick_ts_ms) > 86400 * 1000

            # P41 Constants for causality handling
            SIGNAL_MARKET_GRACE_MS = 100  # Signals slightly ahead of ticks are allowed (ingestion jitter)
            CLOCK_SKEW_TOLERANCE_MS = 1000  # Genuine skew limit vs wall clock
            LAG_WARNING_THRESHOLD_MS = 5000 # Warning threshold for market lag

            if is_replay:
                effective_now_ms = self._max_tick_ts_ms
                if entry_ts_ms > effective_now_ms:
                    skew = entry_ts_ms - effective_now_ms
                    if skew > 5 and self.logger:
                        self.logger.warning(f"⚠️ Future entry timestamp detected: {entry_ts_ms} > {effective_now_ms} (skew={skew}ms, ctx=r_drift)")
                    entry_ts_ms = effective_now_ms
            else:
                # ── Live Mode Causality ──
                # 1. Genuine Future check (Wall Clock + Tolerance)
                if entry_ts_ms > now_ms_current + CLOCK_SKEW_TOLERANCE_MS:
                    skew = entry_ts_ms - now_ms_current
                    if self.logger:
                        self.logger.warning(f"⚠️ Clock skew detected (future signal): {entry_ts_ms} > {now_ms_current} (skew={skew}ms, ctx=live). Clamping to wall-clock.")
                    entry_ts_ms = now_ms_current

                # 2. Market Time check (Wait for ticks)
                elif self._max_tick_ts_ms > 0 and entry_ts_ms > self._max_tick_ts_ms:
                    # If signal is within grace period, we allow it to pass with original TS
                    # even if it's technically ahead of the last tick (avoids duration distortion).
                    market_skew = entry_ts_ms - self._max_tick_ts_ms
                    market_lag = now_ms_current - self._max_tick_ts_ms

                    if market_skew > SIGNAL_MARKET_GRACE_MS:
                        # Beyond grace period: we still allow it if it's within wall-clock time,
                        # but we log a market data lag warning.
                        if market_lag > LAG_WARNING_THRESHOLD_MS:
                            if self.logger:
                                self.logger.warning(f"🐢 Market data lag detected: signal {entry_ts_ms} is {market_skew}ms ahead of market time ({self._max_tick_ts_ms}), total lag {market_lag}ms. Allowing original TS.")
                        else:
                            # Moderate lag, log at info/debug
                            if self.logger:
                                self.logger.info(f"ℹ️ Ingestion jitter: signal {market_skew}ms ahead of market time ({market_skew}ms). Allowing original TS.")
                    # If within grace period, we allow silently to keep logs clean.


            _lot_raw = data.get("lot")
            signal_lot = float(_lot_raw) if _lot_raw is not None else self.default_lot

            # Отбраковка (Hard Veto): если сигнал с нулевым лотом (отсечен profitability floor), сбрасываем его
            if signal_lot <= 0.0:
                if self.logger:
                    self.logger.info(f"🚫 [GATE] signal {symbol} signal_lot <= 0.0 -> REJECTED (Veto in normalize)")
                return None

            atr = float(data.get("atr") or 0.0)

            sl = float(data.get("sl") or 0.0)
            tp_levels = _parse_tp_levels(data)

            # fallback SL/TP если нет
            # ⚠️ FIX (2026-04-25): Changed from `len(tp_levels) < 3` to `len(tp_levels) < 1`.
            # Range override in signal_pipeline intentionally produces 2 TP levels.
            # The old condition was overwriting valid 2-TP setups with default RR [1.0, 2.0, 3.0],
            # which destroyed range-aware TP levels and produced incorrect R:R ratios.
            if sl <= 0 or len(tp_levels) < 1:
                # ✅ Получаем stop_atr_mult и rr_levels с учетом SymbolSpec (приоритет над глобальными настройками)
                stop_atr_mult = getattr(spec, "stop_atr_mult", self.stop_atr_mult)
                rr_levels = getattr(spec, "rr_levels", self.rr_levels)

                if atr > 0:
                    sl_dist = atr * stop_atr_mult
                    tp_dist = [atr * float(r) for r in rr_levels]
                else:
                    sl_dist = entry_price * 0.01
                    tp_dist = [entry_price * float(r) * 0.01 for r in rr_levels]

                if sl <= 0:
                    sl = entry_price - sl_dist if direction == "LONG" else entry_price + sl_dist

                # ⚠️ FIX: Only generate TPs when NONE exist. Do NOT overwrite valid TPs
                # from signal_pipeline (e.g., 2-TP range setups).
                if len(tp_levels) < 1:
                    if direction == "LONG":
                        tp_levels = [entry_price + d for d in tp_dist]
                    else:
                        tp_levels = [entry_price - d for d in tp_dist]

            tp_levels = [float(x) for x in tp_levels][:3]

            # ✅ Position sizing: prefer risk-based lot from signal_pipeline,
            #    fallback to margin-based sizing if signal has no pre-calculated lot.
            #
            # signal_pipeline.calculate_position_size() computes:
            #   lot = risk_usd / sl_distance  (risk-based, ensures one_r_money ≈ risk_usd)
            # This is the CORRECT lot for R-metric normalization.
            #
            # The old margin-based formula (notional = margin × leverage; lot = notional / price)
            # ignored SL distance, producing one_r_money ≪ intended risk → broken R-multiples.
            position_size_usd = float(data.get("position_size_usd") or 0.0)
            symbol_up = symbol.upper()
            is_crypto = (
                symbol_up.endswith(self._crypto_suffixes)
                and not symbol_up.startswith(self._crypto_exclude_prefixes)
            )
            is_margin_fx = symbol_up in self._margin_fx_symbols
            is_margin_based = is_crypto or is_margin_fx

            leverage_env = float(os.getenv("ACCOUNT_LEVERAGE", "100"))
            deposit_env = float(os.getenv("ACCOUNT_DEPOSIT_USD", "100"))
            risk_percent_env = float(os.getenv("RISK_PERCENT", "5.0"))
            if 0 < risk_percent_env < 0.5:
                risk_percent_env *= 100.0
            risk_usd_target = deposit_env * (risk_percent_env / 100.0)

            if is_margin_based:
                # ✅ FIX: Use risk-based lot from signal_pipeline if available.
                # signal_lot > 0 means calculate_position_size() already sized the position
                # based on SL distance, so one_r_money will correctly ≈ risk_usd.
                has_risk_based_lot = (
                    signal_lot > 0
                    and signal_lot != self.default_lot
                    and sl > 0
                    and abs(entry_price - sl) > 1e-12
                )
                if has_risk_based_lot:
                    lot = signal_lot
                    # Compute actual risk for logging
                    sl_dist_abs = abs(entry_price - sl)
                    actual_risk = sl_dist_abs * lot
                    logger.debug(
                        f"✅ Risk-based sizing (from signal): {symbol} "
                        f"lot={lot:.6f}, sl_dist={sl_dist_abs:.4f}, "
                        f"risk=${actual_risk:.2f} (target=${risk_usd_target:.2f}), "
                        f"entry=${entry_price:.2f}"
                    )
                else:
                    # Fallback: margin-based sizing (when signal has no risk-based lot)
                    spec = self._get_spec(symbol)
                    cs = float(getattr(spec, "contract_size", 1.0) or 1.0)
                    max_margin_percent = float(os.getenv("MAX_MARGIN_PERCENT", str(risk_percent_env)))
                    if 0 < max_margin_percent <= 1:
                        max_margin_percent *= 100.0
                    margin_cap = deposit_env * (max_margin_percent / 100.0)
                    if position_size_usd <= 0:
                        position_size_usd = margin_cap
                    position_size_usd = min(position_size_usd, margin_cap)
                    notional_usd = position_size_usd * leverage_env
                    notional_usd = min(notional_usd, margin_cap * leverage_env)
                    denom = (entry_price * cs)
                    lot = (notional_usd / denom) if denom > 0 else self.default_lot
                    logger.debug(
                        f"⚠️ Margin-based sizing (fallback): {symbol} "
                        f"margin=${position_size_usd:.2f}, lev={leverage_env:.0f}x, "
                        f"notional=${notional_usd:.2f}, entry=${entry_price:.2f}, lot={lot:.6f}"
                    )
            else:
                # Для остальных инструментов используем lot из сигнала
                lot = signal_lot

            # HARD CAP ON LOT SIZE as a universal safety measure
            max_qty_cap = float(os.getenv("RISK_MAX_QTY", "0.0"))
            if max_qty_cap > 0 and lot > max_qty_cap:
                logger.warning(f"🚨 [HARD_CAP] {symbol} lot {lot:.6f} exceeds RISK_MAX_QTY {max_qty_cap}. Clamping to {max_qty_cap}")
                lot = max_qty_cap

            # HARD CAP ON NOTIONAL as a universal safety measure against pipeline bugs
            max_notional_cap = deposit_env * (float(os.getenv("MAX_MARGIN_PERCENT", "5.0")) / 100.0) * leverage_env
            cs_notional = float(getattr(spec, "contract_size", 1.0) or 1.0)
            computed_notional = lot * entry_price * cs_notional
            # We use a 1.5x buffer to avoid false alarms due to slightly loose constraints, but strictly clamp anomalies
            if max_notional_cap > 0 and computed_notional > max_notional_cap * 1.5:
                safe_lot = max_notional_cap / (entry_price * cs_notional) if (entry_price * cs_notional) > 0 else self.default_lot
                logger.warning(f"🚨 [NOTIONAL_CAP] {symbol} (signal lot {lot:.6f}) -> notional ${computed_notional:.2f} exceeds 1.5x margin cap ${max_notional_cap:.2f}. Clamping lot to {safe_lot:.6f}")
                lot = safe_lot

            # ✅ Применяем дефолты из SymbolSpec для trailing параметров

            # 1) trailing_profile
            trail_profile = (data.get("trail_profile") or "")
            if not trail_profile:
                # Если в сигнале нет trail_profile, берем из spec
                default_profile = getattr(spec, "trailing_profile_default", "") or ""
                if default_profile:
                    trail_profile = default_profile
                    data["trail_profile"] = trail_profile

            # 1b) Regime-aware trail_profile override.
            # Если trail_profile не задан явно сигналом — подставляем по режиму.
            # unknown/range/mixed/thin → range_protective
            _regime_from_payload = str(
                data.get("regime") or data.get("entry_regime") or data.get("regime_bucket") or ""
            ).strip().lower()
            if _regime_from_payload in ("", "na", "none"):
                _regime_from_payload = "unknown"

            _original_trail = (data.get("trail_profile") or "").strip()
            _was_explicit = bool(data.get("_trail_profile_explicit") or False)
            if not _was_explicit and _regime_from_payload:
                import json as _json
                _regime_map_env = os.getenv("REGIME_TRAIL_PROFILE_MAP", "")
                if _regime_map_env:
                    try:
                        _regime_map: dict[str, str] = _json.loads(_regime_map_env)
                    except Exception:
                        _regime_map = {}
                else:
                    # Полный маппинг по спецификации TradeProfileRouter:
                    _regime_map = {
                        # ── Range / Chop → protective_only (range_absorption_v1: BE, no trail) ──
                        "range":            "protective_only",
                        "range_bullish":    "protective_only",
                        "range_bearish":    "protective_only",
                        "chop":             "protective_only",
                        "meanrev":          "protective_only",
                        "sideways":         "protective_only",
                        # ── Squeeze → range_protective (сжатие, может выстрелить в любую сторону) ──
                        "squeeze":          "range_protective",
                        "squeeze_bullish":  "range_protective",
                        "squeeze_bearish":  "range_protective",
                        # ── Thin / Illiquid → protective_only (thin_defensive_v1: no trail) ──
                        "thin":             "protective_only",
                        "news":             "protective_only",
                        "illiquid":         "protective_only",
                        # ── High Vol → expansion_v1 (trail after TP2, survives noise) ──
                        "high_vol":         "expansion_v1",
                        "volatile":         "expansion_v1",
                        "vol_expansion":    "expansion_v1",
                        # ── High Vol + Low Liq → protective_only (thin_defensive_v1) ──
                        "high_vol_low_liq": "protective_only",
                        "volatile_thin":    "protective_only",
                        # ── Expansion → expansion_v1 (wide trail after TP2) ──
                        "expansion":        "expansion_v1",
                        "expansion_bull":   "expansion_v1",
                        "expansion_bear":   "expansion_v1",
                        # ── Unknown / Mixed → range_protective (conservative, FIX 2026-05-11) ──
                        "unknown":          "range_protective",
                        "mixed":            "range_protective",
                        # trend / trending_bull / trending_bear / momentum:
                        # НЕ в маппинге → проваливаются на spec.trailing_profile_default = rocket_v1
                    }
                _mapped_profile = _regime_map.get(_regime_from_payload, "")
                if _mapped_profile and _mapped_profile != _original_trail:
                    trail_profile = _mapped_profile
                    data["trail_profile"] = trail_profile
                    logger.debug(
                        "🎯 [REGIME_TRAIL] %s: regime=%s → trail_profile=%s (was=%s)",
                        symbol, _regime_from_payload, _mapped_profile, _original_trail or "empty"
                    )

            # 2) trailing_min_lock_r
            if "trailing_min_lock_r" not in data:
                try:
                    mlr_spec = float(getattr(spec, "trailing_min_lock_r", 0.0) or 0.0)
                except Exception:
                    mlr_spec = 0.0
                if mlr_spec > 0:
                    data["trailing_min_lock_r"] = mlr_spec

            # 3) baseline_mode / baseline_horizon_ms (опционально)
            if "baseline_mode" not in data and hasattr(spec, "baseline_mode_default"):
                baseline_mode_default = getattr(spec, "baseline_mode_default", None)
                if baseline_mode_default:
                    data["baseline_mode"] = str(baseline_mode_default)
            if "baseline_horizon_ms" not in data and hasattr(spec, "baseline_horizon_ms_default"):
                baseline_horizon_ms_default = getattr(spec, "baseline_horizon_ms_default", None)
                if baseline_horizon_ms_default:
                    data["baseline_horizon_ms"] = int(float(baseline_horizon_ms_default))

            # 4) trailing_tp1_offset_atr (для использования в on_tick)
            if "trailing_tp1_offset_atr" not in data:
                try:
                    off_spec = float(getattr(spec, "trailing_tp1_offset_atr", 0.0) or 0.0)
                except Exception:
                    off_spec = 0.0
                if off_spec > 0:
                    data["trailing_tp1_offset_atr"] = off_spec

            entry_tag = str(
                data.get("entry_tag")
                or data.get("signal_flavor")
                or data.get("reason")
                or data.get("detector")
                or ""
            )

            # ------------------------------------------------------------------
            # NEW (Variant A): confidence post-calibration via reliability curves.
            #
            # Philosophy:
            #   - DO NOT change producer / signal generation.
            #   - Only enrich the envelope at the entry point (before create_position).
            #   - Adjustment is fail-open and never vetoes signals here.
            #
            # Default for most systems:
            #   RELIABILITY_TARGETS=tp2
            #   RELIABILITY_ADJUST_ENABLED=1
            #
            # "Maximum stability" option:
            #   RELIABILITY_ADJ_PROFILE=hardest
            # ------------------------------------------------------------------
            try:
                from services.reliability_adjuster import maybe_apply_confidence_adjustment
                redis_client = getattr(self, "redis", None) or getattr(self, "redis_client", None)
                if redis_client is not None and isinstance(data, dict):
                    maybe_apply_confidence_adjustment(
                        redis_client,
                        envelope=data,
                        strategy=strategy,
                        symbol=symbol,
                        tf=tf,
                        direction=direction,
                    )
            except Exception:
                pass

            # -----------------------------------------------------------------
            # NEW: normalize conditional trailing decision (trail_after_tp1)
            #
            # Why:
            #   - trailing policy is applied at TP1 moment inside process_tick().
            #   - decision must be persisted per trade (auditable), so we normalize
            #     it once here and keep it in SignalNorm.payload.
            #
            # Fail-open:
            #   - If missing -> default True (keeps legacy behavior).
            #   - Accept multiple spellings (snake/camel).
            # ------------------------------------------------------------------
            # NEW: normalize conditional trailing flags from signal payload.
            #
            # Protocol:
            #   - trail_after_tp1: 0/1 or bool (default True = legacy behavior)
            #   - trail_after_tp1_reason: short string for audit
            #
            # We keep them in payload so create_position can copy into PositionState.
            # Fail-open defaults preserve existing behavior.
            # ------------------------------------------------------------------
            try:
                v = data.get("trail_after_tp1", None)
                if v is None:
                    data["trail_after_tp1"] = 1
                else:
                    if isinstance(v, bool):
                        data["trail_after_tp1"] = 1 if v else 0
                    elif isinstance(v, (int, float)):
                        data["trail_after_tp1"] = 1 if int(v) != 0 else 0
                    elif isinstance(v, str):
                        s = v.strip().lower()
                        if s.isdigit():
                            data["trail_after_tp1"] = 1 if int(s) != 0 else 0
                        elif s in {"true","yes","on"}:
                            data["trail_after_tp1"] = 1
                        elif s in {"false","no","off"}:
                            data["trail_after_tp1"] = 0
                        else:
                            data["trail_after_tp1"] = 1
                    else:
                        data["trail_after_tp1"] = 1
            except Exception:
                data["trail_after_tp1"] = 1
            try:
                rr = data.get("trail_after_tp1_reason", "") or ""
                data["trail_after_tp1_reason"] = str(rr)[:256]
            except Exception:
                data["trail_after_tp1_reason"] = ""

            v_raw = str(data.get("schema_version") or data.get("v") or "0").lower().replace("v", "")
            try:
                schema_version = int(v_raw)
            except ValueError:
                schema_version = 0

            return SignalNorm(
                sid=sid,
                strategy=strategy,
                source=source,
                symbol=symbol,
                tf=tf,
                direction=direction,  # type: ignore
                entry_price=entry_price,
                entry_ts_ms=entry_ts_ms,
                lot=lot,
                qty=lot,        # fallback: assume qty=lot for SignalNorm
                quantity=lot,   # fallback: assume quantity=lot for SignalNorm
                sl=sl,
                tp_levels=tp_levels,
                trail_profile=trail_profile,
                payload=data if isinstance(data, dict) else {},
                entry_tag=entry_tag,
                schema_version=schema_version,
            )
        except Exception:  # noqa: S112
            logger.error("Error normalizing signal", exc_info=True)
            return None

    # --------------------
    # Orphan housekeeping (вошли, но не вышли)
    # --------------------

    def _is_grace_period_active(self, now_ms: int) -> bool:
        """[FIX-2] Returns True if service is still within the post-restart grace period."""
        grace = int(getattr(self, "_housekeep_grace_ms", 0))
        started = int(getattr(self, "_housekeep_started_at_ms", 0))
        if grace <= 0 or started <= 0:
            return False
        return (now_ms - started) < grace

    def _calc_commission_adjusted_exit_price(
        self,
        entry_price: float,
        direction: str,
        spec: Any,
    ) -> float:
        """
        [FIX-3] Commission-aware exit price for ORPHAN_TIMEOUT_NO_PRICE.

        When closing with no market price available, instead of returning entry_price
        (which gives gross PnL = 0 and net PnL = -fees), we adjust the exit_price
        to reflect the round-trip commission cost:

          LONG:  exit = entry * (1 - 2 * rate)  → gross PnL = -entry * 2 * rate * lot = -fees_rt
          SHORT: exit = entry * (1 + 2 * rate)  → same magnitude

        This ensures gross PnL is not silently zero, and analytics/reports see real cost.
        Falls back to entry_price if commission_rate is unavailable.
        """
        try:
            rate = getattr(spec, "commission_rate", None)
            if rate is None:
                rate = float(os.getenv("CRYPTO_COMMISSION_RATE", "0.0005"))
            rate = max(0.0, float(rate))
            if rate <= 0 or entry_price <= 0:
                return entry_price
            direction_up = str(direction).strip().upper()
            if direction_up == "LONG":
                return entry_price * (1.0 - 2.0 * rate)
            else:  # SHORT
                return entry_price * (1.0 + 2.0 * rate)
        except Exception:
            return entry_price

    def _collect_orphan_closures(self, now_ms: int) -> list[tuple[PositionState, float, int, str]]:
        """
        Собирает orphan-позиции для закрытия.
        Важно: внутри lock сразу удаляем позиции из памяти/индексов, чтобы исключить зависание/двойную обработку.

        Возвращает список кортежей:
          (pos, exit_price, exit_ts_ms, close_reason_raw)
        """
        closures: list[tuple[PositionState, float, int, str]] = []

        # [FIX-2] Grace period: skip housekeep during warm-up window after restart
        if self._is_grace_period_active(now_ms):
            return closures

        if not self._is_plausible_epoch_ms(int(now_ms)):
            return closures

        with self._lock:
            # Snapshot по ids (не по symbol), потому что orphan может быть по любому символу.
            for _pos_id, pos in list(self.open_positions.items()):
                try:
                    if not pos or getattr(pos, "closed", False):
                        continue
                    entry_ts_ms = int(getattr(pos, "entry_ts_ms", 0) or 0)
                    if not self._is_plausible_epoch_ms(entry_ts_ms):
                        continue
                    age_ms = int(now_ms) - entry_ts_ms
                    if age_ms < 0:
                        # защита от скачка времени
                        continue

                    ttl_ms = self._resolve_orphan_ttl_ms(pos)
                    if ttl_ms <= 0:
                        continue  # TTL выключен

                    if age_ms < ttl_ms:
                        continue

                    # ------------------------------------------------------------------
                    # 1. Trailing Check (already implemented via helper, but good to be explicit here if needed)
                    # If trailing is active, we NEVER timeout. (Handled by _handle_orphan logic? No, we are in _collect here)
                    # We need to skip HERE if trailing is active.
                    # ------------------------------------------------------------------
                    if getattr(pos, "trailing_active", False):
                        continue

                    # ------------------------------------------------------------------
                    # 2. Smart Timeout Logic (User Request)
                    # "TIMEOUT allowed only if pnl_net >= +X bps ... or if MAE exceeds threshold"
                    # ------------------------------------------------------------------
                    smart_timeout_enabled = os.getenv("TM_SMART_TIMEOUT_ENABLED", "1") == "1"
                    if smart_timeout_enabled:
                        sym = str(getattr(pos, "symbol", "") or "")
                        last = self._last_price_by_symbol.get(sym)

                        # We need price to check PnL. If no price, we can't be "smart", so we flow to STALE_PRICE logic.
                        if last and float(last[1]) > 0:
                            last_px = float(last[1])
                            entry_px = float(getattr(pos, "entry_price", 0.0) or 0.0)

                            if entry_px > 0:
                                # A. Calculate PnL (Gross BPS)
                                direction = getattr(pos, "direction", "LONG")
                                if direction == "LONG":
                                    pnl_raw = (last_px - entry_px) / entry_px
                                else:
                                    pnl_raw = (entry_px - last_px) / entry_px

                                pnl_bps = pnl_raw * 10000.0

                                # B. Calculate MAE (ATR-based if available)
                                # We don't track live MAE in memory efficiently here, but we can estimate "risk status".
                                # User said: "MAE exceeds threshold (risk-off)".
                                # If we are deep in red, we might WANT to close (risk control).
                                # But if we are around 0 or slightly negative, we HOLD.

                                # Configs
                                param_min_pnl = float(os.getenv("TM_SMART_TIMEOUT_PNL_BPS", "4.0")) # cover fees
                                param_max_mae_atr = float(os.getenv("TM_SMART_TIMEOUT_MAE_ATR", "1.0"))

                                # Check PnL Condition: "Only allow timeout if pnl_net >= X"
                                # We use gross pnl_bps >= X (where X covers fees)
                                is_profitable_exit = (pnl_bps >= param_min_pnl)

                                # Check MAE Condition: "Only allow timeout if MAE > threshold"
                                # Since we don't store full MAE history in RAM here easily,
                                # we check CURRENT adverse excursion.
                                # If current price is worse than Entry - 1.0*ATR, we consider it "Risky" -> Close allowed.
                                atr = float(getattr(pos, "atr", 0.0) or 0.0)
                                is_risky = False
                                # B1 FIX: if atr == 0, we have no ATR reference → cannot classify as "safe hold".
                                # Fall through to normal ORPHAN_TIMEOUT close to avoid zombie positions.
                                if atr > 0:
                                    # Calc current drawdown in ATR units
                                    adverse_dist = entry_px - last_px if direction == "LONG" else last_px - entry_px

                                    if adverse_dist > (atr * param_max_mae_atr):
                                        is_risky = True

                                    # Rule: TIMEOUT Allowed IF (Profitable OR Risky)
                                    # Invert: If (Not Profitable AND Not Risky) -> SKIP Timeout (Hold)
                                    if not is_profitable_exit and not is_risky:
                                        # "Wins are made by timer" hypothesis check: we HOLD instead of closing.
                                        continue
                                # atr == 0: no ATR data → cannot be "smart", flow to normal orphan close

                    # Вычисляем exit_price: по последней цене, иначе по entry_price (нулевой pnl).
                    sym = str(getattr(pos, "symbol", "") or "")
                    last = self._last_price_by_symbol.get(sym)
                    if last and float(last[1]) > 0:
                        last_ts, last_px = int(last[0]), float(last[1])

                        # Защита от использования устаревшей цены (фид умер, но цена осталась в dict)
                        max_age = int(self._orphan_max_last_price_age_ms)
                        if max_age > 0 and (int(now_ms) - last_ts) > max_age:
                            exit_price = float(getattr(pos, "entry_price", 0.0) or 0.0)
                            close_reason_raw = "ORPHAN_CLEANUP_STALE_PRICE"
                        else:
                            exit_price = last_px
                            close_reason_raw = "ORPHAN_CLEANUP_STALE_MONITOR_STATE"
                    else:
                        # [FIX-3] Use commission-adjusted price, not raw entry_price.
                        _entry_px = float(getattr(pos, "entry_price", 0.0) or 0.0)
                        _spec = self._get_spec(str(getattr(pos, "symbol", "") or ""))
                        exit_price = self._calc_commission_adjusted_exit_price(
                            _entry_px,
                            str(getattr(pos, "direction", "LONG") or "LONG"),
                            _spec,
                        )
                        close_reason_raw = "ORPHAN_CLEANUP_NO_PRICE"

                    # Сразу удаляем из памяти/индексов, чтобы позиция не "жила вечно"
                    with self._lock:
                        self._pop_pos(pos.id)

                    # помечаем как закрытую в рантайме (чисто защитно)
                    try:
                        pos.closed = True
                        # P1-9: FSM transition
                        self._fsm_transition(
                            pos, "CLOSED",
                            trigger="orphan_collect_close",
                            reason=close_reason_raw,
                        )
                    except Exception:
                        pass

                    closures.append((pos, exit_price, int(now_ms), close_reason_raw))
                except Exception:  # noqa: S112
                    continue

        return closures

    def _finalize_orphan_closures(self, closures: list[tuple[PositionState, float, int, str]]) -> None:
        """
        Делает forced finalize для orphan-позиций: сохраняет как CLOSED и обновляет статистику.
        Отделено от _collect_* чтобы не держать lock на I/O.
        """
        if not closures:
            return

        report_triggers: list[tuple[str, str]] = []

        for pos, exit_price, exit_ts_ms, close_reason_raw in closures:
            try:
                # forced-close: используем стандартный finalize_trade, чтобы:
                #  - посчитать fees
                #  - посчитать giveback/missed/one_r/r_multiple
                #  - закрыть baseline-ветку (если она есть)
                spec = self._get_spec(pos.symbol)
                try:
                    custom_ratios = (getattr(pos, "signal_payload", {}) or {}).get("tp_ratio")
                    if custom_ratios and isinstance(custom_ratios, list) and len(custom_ratios) > 0:
                        effective_tp_ratios = [float(x) for x in custom_ratios]
                    else:
                        effective_tp_ratios = self.tp_ratios
                except Exception:
                    effective_tp_ratios = self.tp_ratios

                closed = finalize_trade(
                    pos, spec,
                    exit_price=float(exit_price),
                    exit_ts_ms=int(exit_ts_ms),
                    close_reason_raw=str(close_reason_raw),
                    tp_ratios=effective_tp_ratios,
                )
                self._log_ab_closed_event(pos, closed, str(close_reason_raw))

                # Mark orphan cleanup: excluded from ML labels
                with contextlib.suppress(Exception):
                    object.__setattr__(closed, "is_orphan_cleanup", True)
                    object.__setattr__(closed, "exclude_from_ml_labels", True)
                try:
                    TM_ORPHAN_CLEANUP_TOTAL.labels(
                        symbol=str(pos.symbol or ""), reason=str(close_reason_raw),
                    ).inc()
                except Exception:
                    pass

                # ---------------------------------------------------------------------
                # NEW: persist time-bucket snapshots into TradeClosed event so that
                # StatsAggregator can push them into statsbuf:*:mfe_bps_t{bucket} lists.
                # Fail-open (never breaks closing).
                # ---------------------------------------------------------------------
                with contextlib.suppress(Exception):
                    attach_timebucket_snapshots_to_closed(pos, closed)
                with contextlib.suppress(Exception):
                    stamp_closed_trade_horizon_from_position(pos, closed)

                # Явно помечаем "почему" — чтобы фильтровать в репортах/аналитике
                with contextlib.suppress(Exception):
                    closed.close_reason_detail = str(close_reason_raw)

                # FIX(#9): health snapshot добавляем здесь (в сервисе), а не внутри RedisTradeRepository.save_closed().
                # Это позволяет:
                #  - использовать in-memory API HealthMetrics, если есть;
                #  - кэшировать Redis HGETALL и не бить Redis на каждую сделку;
                #  - полностью выключить добавление health-полей через ENV при необходимости.
                if self._attach_health_on_close:
                    try:
                        now_ms = get_ny_time_millis()
                        closed._health_snapshot = self._get_health_snapshot_prefixed(closed.symbol, now_ms)  # type: ignore
                    except Exception:
                        pass

                # NEW: передаём health snapshot без создания новых HealthMetrics/коннектов.
                hs = {}
                try:
                    hs = self._get_health_snapshot_for_trade(str(closed.symbol))
                except Exception:
                    hs = {}
                self._io_save_closed(closed, health_snapshot=hs)
                try:
                    analytics_db.save_trade_closed(closed)
                except Exception as e:
                    logger.warning("Failed to save orphan-closed trade to analytics DB: %s", e)

                # stats лучше обновлять под lock (если внутри есть общие структуры)
                try:
                    with self._lock:
                        self._update_stats(pos, closed)
                except Exception:
                    pass

                report_triggers.append((pos.source, pos.symbol, pos.id, getattr(pos, "is_virtual", False)))  # type: ignore
            except Exception as e:
                logger.warning("⚠️ Orphan forced-close failed: %s", e)

        # триггер отчётов вне lock
        for trigger in report_triggers:
            try:
                from services.periodic_reporter import check_and_trigger_report
                src, sym, oid = trigger[0], trigger[1], trigger[2]  # type: ignore
                check_and_trigger_report(src, sym, counter_type="trades", order_id=oid)
            except Exception as e:
                logger.warning("Error triggering report (orphan close): %s", e)


    def _cleanup_stale_prices(self, ttl_ms: int = 3600000) -> None:
        """
        Recommendation 3: Fix memory leak in self._last_price_by_symbol.
        Removes prices older than ttl_ms (default 1 hour).
        """
        now = get_ny_time_millis()
        with self._lock:
            to_delete = [
                sym for sym, (ts, _) in self._last_price_by_symbol.items()
                if now - ts > ttl_ms
            ]
            for sym in to_delete:
                del self._last_price_by_symbol[sym]

            if to_delete:
                self.logger.info(f"🧹 Cleaned up {len(to_delete)} stale prices from cache")

    def _housekeep_loop(self) -> None:
        """
        Background thread loop — Phase 4: delegates to OrphanRecoveryPolicy.
        Kept as a no-op stub so that any direct reference (_housekeep_thread target)
        resolves without AttributeError during rollback.
        """
        # In normal operation this method is never called:
        # __init__ uses self._orphan_policy.start() instead.
        # During rollback (FSM_ENABLED=0), fall back to original loop.
        if not hasattr(self, "_orphan_policy"):
            self._housekeep_loop_legacy()
            return
        # orphan_policy already running its own thread — this body is a stub
        self._housekeep_thread_stop.wait()  # block until shutdown signal

    def _housekeep_loop_legacy(self) -> None:
        """Original housekeep loop body — fallback only."""
        interval_sec = max(1.0, self._orphan_housekeep_interval_ms / 1000.0)
        logger.info("Started TMHousekeep thread (legacy), interval=%ss", interval_sec)
        while not getattr(self, "_housekeep_thread_stop", threading.Event()).is_set():
            start_ms = get_ny_time_millis()
            try:
                self._housekeep_expired_positions(start_ms)
            except Exception as e:
                logger.error("Housekeep loop error: %s", e)
            try:
                self._run_max_hold_timeout_scan(start_ms)
            except Exception as e:
                logger.error("Max-hold timeout scan error: %s", e)
            finally:
                duration_ms = get_ny_time_millis() - start_ms
                if hasattr(self, "tm_orphan_cleanup_duration_ms"):
                    self.tm_orphan_cleanup_duration_ms.set(duration_ms)
            self._housekeep_thread_stop.wait(interval_sec)

    def _housekeep_expired_positions(self, now_ms: int, current_symbol: str | None = None) -> None:
        """
        Оптимизированная версия:
        1. Если есть current_symbol -> проверяем только шард этого символа (O(1) lookup).
        2. Глобальная очистка -> проверяем все шарды (O(N) total), но с троттлингом.
        """
        # [FIX-2] Grace period: do not run housekeep while price cache is still warming up.
        if self._is_grace_period_active(now_ms):
            return

        by_sym: dict[str, list[str]] = {}

        if current_symbol:
            # 1. Sharded mode (O(1) lookup of symbol, O(N_symbol) iteration)
            with self._lock:
                last_sh = self._last_housekeep_by_symbol.get(current_symbol, 0)
                if (now_ms - last_sh) < self._orphan_housekeep_interval_ms:
                    return
                self._last_housekeep_by_symbol[current_symbol] = now_ms

            shard = self.shards.get(current_symbol, {})
            if not shard:
                return

            # Check expiration under symbol lock (already held if called from on_tick)
            candidates = [pid for pid, pos in shard.items() if self._is_orphan_expired(pos, now_ms)]
            if candidates:
                by_sym[current_symbol] = candidates
        else:
            # 2. Global mode (thorough scan, throttled)
            with self._lock:
                if (now_ms - int(self._last_housekeep_ms or 0)) < int(self._orphan_housekeep_interval_ms or 0):
                    return
                self._last_housekeep_ms = now_ms

                # group orphans by symbol using shards
                for sym, shard in self.shards.items():
                    for pid, pos in shard.items():
                        if self._is_orphan_expired(pos, now_ms):
                            by_sym.setdefault(sym, []).append(pid)

            # Recommendation 3: Periodic cleanup of stale prices
            self._cleanup_stale_prices()

        if not by_sym:
            return

        report_triggers: list[tuple[str, str]] = []

        # process each symbol independently (avoid multi-lock deadlocks)
        for sym in sorted(by_sym.keys()):
            # Logic:
            # 1. If sym == current_symbol: we already hold the lock (called from on_tick). Re-enter allowed (RLock).
            # 2. If sym != current_symbol: try to acquire lock non-blocking. If locked by another thread, SKIP.

            lk = self._get_symbol_lock(sym)
            can_proceed = False
            ctx = contextlib.nullcontext()

            if sym == current_symbol:
                can_proceed = True
                # Context manager will re-acquire (increment recursion), which is fine for RLock
                ctx = lk
            else:
                # Try non-blocking acquire
                acquired = lk.acquire(blocking=False)
                if acquired:
                    can_proceed = True
                    # We manually acquired the lock, create a context manager that releases it
                    @contextlib.contextmanager
                    def _manual_lock(lk=lk):
                        try:
                            yield
                        finally:
                            lk.release()
                    ctx = _manual_lock()
                else:
                    can_proceed = False
                    # logger.debug("Skipping orphan housekeep for %s (locked)", sym)

            if not can_proceed:
                continue

            # Use context manager to ensure proper lock handling
            with ctx:

                io_tasks: list[_IOTask] = []
                local_triggers: list[tuple[str, str]] = []

                with self._lock:

                    # get last price for forced exit
                    lp = self._last_price_by_symbol.get(sym)
                    raw = "ORPHAN_TIMEOUT"
                    is_stale_price = False

                    if lp:
                        exit_ts_ms, exit_price = int(lp[0]), float(lp[1])
                        # Проверяем на "протухание" цены (защита от forced-close по цене часовой давности)
                        if (now_ms - exit_ts_ms) > self._orphan_max_last_price_age_ms:
                            logger.info(f"⚠️ Stale price for {sym} ({now_ms - exit_ts_ms}ms old), using entry_price for orphan closure")
                            exit_price = 0.0  # trigger fallback below
                            is_stale_price = True
                    else:
                        exit_ts_ms, exit_price = now_ms, 0.0


                    for pos_id in by_sym.get(sym, []):
                        # lookup in shard instead of global dict
                        pos = self.shards.get(sym, {}).get(pos_id)
                        if not pos or getattr(pos, "closed", False):
                            continue
                        # re-check expiration under lock (race-safe)
                        if not self._is_orphan_expired(pos, now_ms):
                            continue

                        # Smart Timeout: не закрывать если позиция вблизи безубытка
                        # и нет значительного adverse excursion.
                        if os.getenv("TM_SMART_TIMEOUT_ENABLED", "1") == "1":
                            if not getattr(pos, "trailing_active", False):
                                _sym = str(getattr(pos, "symbol", "") or "")
                                _last = self._last_price_by_symbol.get(_sym)
                                if _last and float(_last[1]) > 0:
                                    _last_px = float(_last[1])
                                    _entry_px = float(getattr(pos, "entry_price", 0.0) or 0.0)
                                    if _entry_px > 0:
                                        _dir = getattr(pos, "direction", "LONG")
                                        _pnl_bps = ((_last_px - _entry_px) / _entry_px * 10000.0
                                                    if _dir == "LONG"
                                                    else (_entry_px - _last_px) / _entry_px * 10000.0)
                                        _min_pnl = float(os.getenv("TM_SMART_TIMEOUT_PNL_BPS", "10.0"))
                                        _atr = float(getattr(pos, "atr", 0.0) or 0.0)
                                        _mae_atr = float(os.getenv("TM_SMART_TIMEOUT_MAE_ATR", "1.0"))
                                        _is_profitable = _pnl_bps >= _min_pnl
                                        _is_risky = False
                                        if _atr > 0:
                                            _adverse = (_entry_px - _last_px if _dir == "LONG"
                                                        else _last_px - _entry_px)
                                            _is_risky = _adverse > (_atr * _mae_atr)
                                            if not _is_profitable and not _is_risky:
                                                continue  # держим позицию

                        from domain.models import TradeEvent

                        # Apply fallback price per-position since entry_price is position-specific
                        pos_exit_price = exit_price
                        pos_raw = raw
                        if pos_exit_price <= 0:
                            # [FIX-3] Commission-adjusted exit price: instead of entry_price
                            # (gross PnL = 0), use price adjusted for round-trip fees so that
                            # gross PnL = -fees_round_trip and analytics show realistic cost.
                            _entry_px = float(getattr(pos, "entry_price", 0.0) or 0.0)
                            _spec = self._get_spec(str(getattr(pos, "symbol", "") or ""))
                            pos_exit_price = self._calc_commission_adjusted_exit_price(
                                _entry_px,
                                str(getattr(pos, "direction", "LONG") or "LONG"),
                                _spec,
                            )
                            pos_raw = "ORPHAN_TIMEOUT_STALE_PRICE" if is_stale_price else "ORPHAN_TIMEOUT_NO_PRICE"

                        # mark closed in-memory
                        pos.closed = True
                        pos.exit_ts_ms = int(exit_ts_ms or now_ms)
                        pos.exit_price = float(pos_exit_price)
                        # P1-9: FSM transition
                        self._fsm_transition(
                            pos, "ORPHAN_CLOSED",
                            trigger="orphan_housekeep_close",
                            reason=str(pos_raw),
                            price=float(pos_exit_price),
                            ts_ms=int(exit_ts_ms or now_ms),
                        )

                        # Recommendation 4: Prometheus metric
                        self.tm_orphans_force_closed.labels(symbol=sym).inc()

                        try:
                            # close remaining qty at last price (best-effort)
                            rq = float(getattr(pos, "remaining_qty", 0.0) or 0.0)
                            if rq > 0 and not getattr(pos, "_pnl_finalized", False):
                                # Idempotent guard (bug 2026-05-14): _pnl_finalized prevents
                                # double-counting when this orphan handler races with another close path.
                                spec_for_pnl = self._get_spec(sym)
                                pnl_rest = float(spec_for_pnl.pnl_money(pos.entry_price, float(pos_exit_price), rq, pos.direction, symbol=pos.symbol))
                                pos.realized_pnl_gross = float(getattr(pos, "realized_pnl_gross", 0.0) or 0.0) + pnl_rest
                                pos.remaining_qty = 0.0
                        except Exception:
                            pass

                        spec = self._get_spec(sym)
                        closed = finalize_trade(
                            pos, spec,
                            exit_price=float(pos_exit_price),
                            exit_ts_ms=int(exit_ts_ms or now_ms),
                            close_reason_raw=str(pos_raw),
                            tp_ratios=self.tp_ratios,
                        )
                        self._log_ab_closed_event(pos, closed, str(pos_raw))
                        self._stamp_closed_trade_meta(pos, closed, str(pos_raw))

                        orphan_ev = TradeEvent(
                            event_type="ORPHAN_CLOSE",
                            order_id=pos.id,
                            sid=getattr(pos, "sid", ""),
                            strategy=getattr(pos, "strategy", ""),
                            source=getattr(pos, "source", ""),
                            symbol=getattr(pos, "symbol", ""),
                            tf=getattr(pos, "tf", ""),
                            direction=getattr(pos, "direction", ""),  # type: ignore
                            ts_ms=int(exit_ts_ms or now_ms),
                            payload={
                                "exit_price": float(exit_price),
                                "exit_ts_ms": int(exit_ts_ms or now_ms),
                                "reason_raw": str(raw),
                                "close_reason_detail": str(getattr(closed, "close_reason_detail", "") or ""),
                                "orphan_now_ms": int(now_ms),
                            },
                        )
                        close_ev = TradeEvent(
                            event_type="CLOSE",
                            order_id=pos.id,
                            sid=getattr(pos, "sid", ""),
                            strategy=getattr(pos, "strategy", ""),
                            source=getattr(pos, "source", ""),
                            symbol=getattr(pos, "symbol", ""),
                            tf=getattr(pos, "tf", ""),
                            direction=getattr(pos, "direction", ""),  # type: ignore
                            ts_ms=int(exit_ts_ms or now_ms),
                            payload={
                                "reason": str(getattr(closed, "close_reason", "") or ""),
                                "reason_raw": str(getattr(closed, "close_reason_raw", "") or str(raw)),
                                "close_reason_detail": str(getattr(closed, "close_reason_detail", "") or ""),
                            },
                        )

                        pos_dict = asdict(pos) if hasattr(pos, "__dataclass_fields__") else dict(getattr(pos, "__dict__", {}) or {})
                        closed_dict = asdict(closed) if hasattr(closed, "__dataclass_fields__") else dict(getattr(closed, "__dict__", {}) or {})

                        from domain.normalizers import source_from_strategy
                        mapped_src = source_from_strategy(getattr(pos, "strategy", ""), str(getattr(pos, "source", "")))
                        local_triggers.append((mapped_src, str(getattr(pos, "symbol", "")), str(pos.id), getattr(pos, "is_virtual", False)))  # type: ignore

                        # cleanup memory under lock
                        with self._lock:
                            self._pop_pos(pos.id)

                        io_tasks.append(_IOTask(lambda ev=orphan_ev: self.repo.append_event(ev), f"append_event:ORPHAN_CLOSE:{pos_id}"))
                        io_tasks.append(_IOTask(lambda ev=close_ev: self.repo.append_event(ev), f"append_event:CLOSE_ORPHAN:{pos_id}"))
                        io_tasks.append(_IOTask(
                            lambda closed=closed, pos_dict=pos_dict, closed_dict=closed_dict:
                                self._persist_closed_trade_io(closed, pos_dict, closed_dict),
                            f"persist_closed_orphan:{pos_id}",
                        ))

                    # I/O outside global lock
                    if io_tasks:
                        self._run_io_tasks(io_tasks)
                    report_triggers.extend(local_triggers)

        # reports outside locks
        for trigger in report_triggers:
            try:
                from services.periodic_reporter import check_and_trigger_report
                src, sym, oid = trigger[0], trigger[1], trigger[2]  # type: ignore
                check_and_trigger_report(src, sym, counter_type="trades", order_id=oid)
            except Exception as e:
                logger.warning("⚠️ Ошибка при триггере отчета: %s", e)

    # --------------------
    # Tick → updates / close
    # --------------------
    def on_tick(self, raw_tick: dict[str, Any]) -> None:
        """
        Обрабатывает тик для всех открытых позиций данного символа (thread-safe, optimized).
        """
        t_start = time.perf_counter()
        tick = build_tick(raw_tick)
        if not tick:
            return

        symbol = tick.symbol
        ts_ms = int(tick.ts_ms)

        # Update Simulation Time (Time Sync)
        if ts_ms > self._max_tick_ts_ms:
            self._max_tick_ts_ms = ts_ms

        # [PHASE 2: Jitter Sync]
        # P0 FIX: Throttle flush to avoid signal persist blocking tick processing.
        # Previously called on every tick — now only if 10ms+ elapsed since last flush
        # or buffer is large (>5 signals). This prevents head-of-line blocking where
        # signal persist (even pipelined) delays tick drain during high-frequency data.
        _flush_now = False
        _buf_len = len(getattr(self, "_signal_buffer", []))
        if _buf_len > 0:
            _last_flush = getattr(self, "_last_flush_ts_ms", 0)
            if _buf_len >= 5 or (ts_ms - _last_flush) >= 10:
                _flush_now = True
                self._last_flush_ts_ms = ts_ms  # type: ignore
        if _flush_now:
            self._flush_signal_buffer()

        # --- Метрика возраста тика (задержка ingestion → Python обработка) ---
        now_ms: int = 0
        try:
            now_ms = get_ny_time_millis()
            tick_age_ms = max(0, now_ms - ts_ms)
            TM_TICK_AGE_MS.labels(symbol=symbol).observe(tick_age_ms)
        except Exception:
            pass

        # ------------------------------------------------------------------
        # Locking order (deadlock-safe):
        #   symbol-lock (optional) -> self._lock
        # External events already follow this order; keep it consistent here.
        # ------------------------------------------------------------------
        sym_ctx = self._symbol_ctx(symbol) if getattr(self, "_use_symbol_locks", False) else contextlib.nullcontext()
        with sym_ctx:
            # 1) update last price (used by orphan forced-close)
            self._update_last_price(tick)

            # Note: _housekeep_expired_positions is now handled in the background TMHousekeep thread

            # 2) snapshot positions for this symbol from shards
            # We are ALREADY under sym_ctx (symbol lock), which is the authoritative lock for this symbol.
            # We can now avoid holding the global self._lock for the iteration entirely.
            shard = self.shards.get(symbol, {})
            v_count = sum(1 for p in shard.values() if getattr(p, "is_virtual", False))

            # Recommendation 4: Prometheus Gauge for open positions count (Throttled)
            last_upd = self._last_metrics_update_by_sym.get(symbol, 0)
            if (now_ms - last_upd) >= self._metrics_update_interval_ms:
                self.tm_open_positions.labels(symbol=symbol).set(len(shard))
                TM_VIRTUAL_POSITIONS.labels(symbol=symbol).set(v_count)
                self._last_metrics_update_by_sym[symbol] = now_ms

            pos_list = list(shard.values()) if shard else []

            # 3) Collect IO steps OUTSIDE global _lock to reduce contention.
            #    IO steps are executed inside symbol-lock (serialization per symbol),
            #    but strictly outside self._lock.
            io_steps: list[tuple[str, Any]] = []
            report_triggers: list[tuple[str, str]] = []

            spec = self._get_spec(symbol)

            for pos in pos_list:
                if pos.closed or pos.symbol != symbol:
                    continue

                # Prevent active positions from being incorrectly flagged as orphans
                pos.last_update_ts_ms = ts_ms
                pos.last_tick_ts_ms = ts_ms

                try:
                    custom_ratios = (getattr(pos, "signal_payload", {}) or {}).get("tp_ratio")
                    if custom_ratios and isinstance(custom_ratios, list) and len(custom_ratios) > 0:
                        effective_tp_ratios = [float(x) for x in custom_ratios]
                    else:
                        effective_tp_ratios = self.tp_ratios
                except Exception:
                    effective_tp_ratios = self.tp_ratios

                # process_tick is pure (no IO), safe under symbol-lock without _lock
                events, closed = process_tick(
                    pos, tick, spec,
                    tp_ratios=effective_tp_ratios,
                    fill_policy=self.fill_policy,
                )

                # ---- accumulate events + repo side-effects in order ----
                for ev in (events or []):
                    io_steps.append(("append_event", ev))

                    if ev.event_type == "TIME_BE_EXIT_SHADOW":
                        p = ev.payload or {}
                        reason = p.get("reason_raw", "unknown")
                        TIME_BE_EXIT_DECISIONS_TOTAL.labels(symbol=symbol, reason=reason, mode="SHADOW").inc()
                        TIME_BE_EXIT_SHADOW_WOULD_CLOSE_TOTAL.labels(symbol=symbol, reason=reason).inc()

                    if ev.event_type == "TIME_BE_EXIT":
                        p = ev.payload or {}
                        reason = p.get("reason_raw", "unknown")
                        TIME_BE_EXIT_DECISIONS_TOTAL.labels(symbol=symbol, reason=reason, mode="ENFORCE").inc()
                        TIME_BE_EXIT_CLOSES_TOTAL.labels(symbol=symbol, reason=reason).inc()

                    if ev.event_type == "TP_HIT":
                        p = ev.payload or {}
                        io_steps.append(("save_tp_hit", {
                            "pos": pos,
                            "tp_level": int(p.get("tp_level", 0)),
                            "fill_price": float(p.get("fill_price", 0.0)),
                            "closed_qty": float(p.get("closed_qty", 0.0)),
                            "pnl_part": float(p.get("pnl_part_gross", 0.0)),
                            "ts_ms": int(tick.ts_ms),
                        }))

                        # TP_HIT event for external trailing orchestrator (crypto)
                        if int(p.get("tp_level", 0)) == self._trail_tp_activate_level and getattr(pos, "sid", ""):
                            io_steps.append(("append_event", _ev_tp1_hit_external(
                                pos,
                                float(p.get("fill_price", 0.0)),
                                float(p.get("closed_qty", 0.0)),
                                int(tick.ts_ms),
                                tp_level=self._trail_tp_activate_level,
                            )))

                            # Phase 2.6: Unconditional Trailing Surface Diagnostics
                            try:
                                # [PHASE 4] Try dynamic ATR from cache first, fallback to signal static ATR
                                pos_atr = 0.0
                                if hasattr(self, "atr_cache") and self.atr_cache and hasattr(self.atr_cache, "get_with_meta"):
                                    # We use the TF selected at entry time (from signal meta)
                                    # meta.atr_profile was added in Phase 4 SignalPipeline
                                    sig_meta = (pos.signal_payload.get("meta") or {})
                                    entry_tf = sig_meta.get("atr_profile", {}).get("atr_tf")
                                    if entry_tf:
                                        cached_atr, _ = self.atr_cache.get_with_meta(symbol=pos.symbol, timeframe=entry_tf)
                                        if cached_atr:
                                            pos_atr = float(cached_atr)

                                if pos_atr <= 0:
                                    pos_atr = float((getattr(pos, "signal_payload", {}) or {}).get("atr", 0.0) or 0.0)
                                offset_mult = self._resolve_trailing_tp1_offset_atr(pos, spec)

                                try:
                                    s_norm = str(getattr(pos, "source", "unknown")).lower()
                                    s_up = str(getattr(pos, "symbol", "")).upper()

                                    sp = getattr(pos, "signal_payload", {}) or {}
                                    r_g = str(sp.get("regime") or sp.get("meta", {}).get("regime") or "")
                                    s_c = str(sp.get("scenario") or sp.get("kind") or "")
                                    bucket = str((sp.get("meta", {}) or {}).get("horizon", {}).get("risk_horizon_bucket") or "na")

                                    ac_pol = get_active_policy(s_norm, s_up, s_c, r_g, bucket)

                                    can_dec = should_apply_trailing_surface(symbol=pos.symbol, sid=pos.sid, regime=r_g, scenario=s_c)

                                    if ac_pol and ac_pol.get("trailing_mode") == "live":
                                        trailing_decision = {"should_apply": True, "reason_code": "TRAILING_POLICY_APPLY"}
                                    else:
                                        trailing_decision = can_dec

                                    if ac_pol and "rollout_stage_trailing" in ac_pol:
                                        rollout_stage = (ac_pol.get("rollout_stage_trailing", "shadow"))
                                        if rollout_stage == "shadow":
                                            trailing_decision = {"should_apply": False, "reason_code": "ATR_POLICY_ROLLOUT_SHADOW"}
                                        elif rollout_stage in {"frozen", "rolled_back"}:
                                            trailing_decision = {"should_apply": False, "reason_code": f"ATR_POLICY_ROLLOUT_{rollout_stage.upper()}"}
                                        else:
                                            sticky_key = build_rollout_sticky_key(getattr(pos, "signal_payload", {}) or {})
                                            if should_apply_rollout(sticky_key=sticky_key, rollout_stage=rollout_stage):
                                                if ac_pol.get("trailing_mode") == "live":
                                                    trailing_decision = {"should_apply": True, "reason_code": f"TRAILING_POLICY_ACTIVE_{rollout_stage.upper()}"}
                                                else:
                                                    trailing_decision = {"should_apply": can_dec.get("should_apply", False), "reason_code": str(can_dec.get("reason_code") or f"TRAILING_CANARY_APPLY_{rollout_stage.upper()}")}
                                            else:
                                                trailing_decision = {"should_apply": False, "reason_code": f"ATR_POLICY_ROLLOUT_{rollout_stage.upper()}_MISS"}
                                except Exception:
                                    trailing_decision = {"should_apply": False, "reason_code": "ERROR_FAIL_OPEN"}

                                trailing_surface = build_trailing_surface(
                                    signal_payload=getattr(pos, "signal_payload", {}) or {},
                                    pos_atr=pos_atr,
                                    offset_mult=offset_mult,
                                )

                                # Unconditionally store diagnostics for telemetry/A-B service
                                if getattr(pos, "signal_payload", None) is not None:
                                    pos.signal_payload.setdefault("meta", {})
                                    pos.signal_payload["meta"]["trailing_canary_decision"] = trailing_decision
                                    pos.signal_payload["meta"]["trailing_surface_diagnostic"] = trailing_surface

                                # Recommendation C: allow disabling local fallback if orchestrator is the only authority
                                if self._trailing_local_fallback and self._is_trailing_after_tp1_enabled(pos, spec) and pos_atr > 0:
                                        # Application logic
                                        offset = trailing_surface.get("baseline_offset_distance_px", 0.0)
                                        if trailing_decision.get("should_apply") and trailing_surface.get("selected_offset_distance_px"):
                                            offset = float(trailing_surface.get("selected_offset_distance_px", offset))

                                        # Round offset to Binance callbackRate 0.1% step
                                        entry_px = float(getattr(pos, "entry_price", 0.0) or 0.0)
                                        if entry_px > 0 and offset > 0:
                                            step = entry_px * 0.001  # 0.1% of entry
                                            if step > 0:
                                                offset = max(step, round(offset / step) * step)
                                        if offset > 0:
                                            trail_profile = str(getattr(pos, "trail_profile", "") or (getattr(pos, "signal_payload", {}) or {}).get("trail_profile", "")).lower()
                                            clear_tp = trail_profile == "rocket_v1"
                                            if pos.is_long():
                                                new_sl = float(pos.entry_price + offset)
                                                if new_sl > float(pos.sl):
                                                    ev_tr = apply_trailing_update(
                                                        pos,
                                                        new_sl=new_sl,
                                                        ts_ms=int(tick.ts_ms),
                                                        trailing_distance=float(offset),
                                                        point_size=0.0,
                                                        clear_future_tp_levels=bool(clear_tp),
                                                    )
                                                    if ev_tr:
                                                        io_steps.append(("append_event", ev_tr))
                                                        io_steps.append(("save_trailing_sync", {"pos": pos, "ts_ms": int(tick.ts_ms)}))
                                            else:
                                                new_sl = float(pos.entry_price - offset)
                                                if new_sl < float(pos.sl):
                                                    ev_tr = apply_trailing_update(
                                                        pos,
                                                        new_sl=new_sl,
                                                        ts_ms=int(tick.ts_ms),
                                                        trailing_distance=float(offset),
                                                        point_size=0.0,
                                                        clear_future_tp_levels=bool(clear_tp),
                                                    )
                                                    if ev_tr:
                                                        io_steps.append(("append_event", ev_tr))
                                                        io_steps.append(("save_trailing_sync", {"pos": pos, "ts_ms": int(tick.ts_ms)}))
                            except Exception as trailing_err:
                                logger.warning("⚠️ Local trailing fallback / telemetry failed: %s", trailing_err)

                    elif ev.event_type == "TRAILING_MOVE":
                        pp = ev.payload or {}
                        io_steps.append(("save_trailing_move", {
                            "pos": pos,
                            "previous_sl": float(pp.get("previous_sl", 0.0)),
                            "new_sl": float(pp.get("new_sl", 0.0)),
                            "ts_ms": int(tick.ts_ms),
                        }))

                    elif ev.event_type == "TRAILING_SYNC":
                        io_steps.append(("save_trailing_sync", {"pos": pos, "ts_ms": int(tick.ts_ms)}))

                if closed:
                    # ── Simulated exit slippage (paper trades) ──
                    # Shifts exit_price adversely: SL exits → worse fill;
                    # TP exits → slightly worse fill (matching real exchange behavior).
                    if self._simulated_slippage_bps > 0:
                        try:
                            ep = float(getattr(closed, "exit_price", 0.0) or 0.0)
                            if ep > 0:
                                slip = self._simulated_slippage_bps / 10_000.0
                                d = str(getattr(pos, "direction", "") or "").upper()
                                if d in ("LONG", "BUY"):
                                    closed.exit_price = ep * (1.0 - slip)  # worse for long (lower exit)
                                else:
                                    closed.exit_price = ep * (1.0 + slip)  # worse for short (higher exit)
                        except Exception:
                            pass
                    # classify trailing outcome (pure)
                    if (getattr(pos, "trailing_started", False) or getattr(pos, "trailing_active", False)):
                        try:
                            closed.trailing_active = True
                            closed.trailing_started = True
                            closed.close_reason_detail = "TRAILING_PROFIT" if float(getattr(closed, "pnl_net", 0.0) or 0.0) > 1e-8 else "TRAILING_STOP"
                        except Exception:
                            pass

                    if self._attach_health_on_close:
                        try:
                            now_ms = get_ny_time_millis()
                            closed._health_snapshot = self._get_health_snapshot_prefixed(closed.symbol, now_ms)  # type: ignore
                        except Exception:
                            pass

                    with contextlib.suppress(Exception):
                        stamp_closed_trade_horizon_from_position(pos, closed)

                    # IO steps for close (repo + analytics + stats)
                    hs = {}
                    try:
                        # P0 FIX: use cached variant to avoid sync HGETALL per close
                        hs = self._get_health_snapshot_cached(str(closed.symbol))
                    except Exception:
                        hs = {}
                    io_steps.append(("save_closed", {"closed": closed, "health_snapshot": hs}))
                    io_steps.append(("analytics_closed", closed))
                    io_steps.append(("signal_outcome", closed))  # Signal → Outcome pipeline
                    io_steps.append(("update_stats", {"pos": pos, "closed": closed}))

                    from domain.normalizers import source_from_strategy
                    mapped_src = source_from_strategy(getattr(pos, "strategy", ""), str(getattr(pos, "source", "")))
                    report_triggers.append((mapped_src, pos.symbol, pos.id, getattr(pos, "is_virtual", False)))  # type: ignore

                    # cleanup shared maps under _lock
                    with self._lock:
                        self._pop_pos(pos.id)

            # 4) Flush IO steps strictly OUTSIDE _lock (but still inside symbol-lock)
            #    P0 FIX: Batch multiple append_event XADD calls into a single pipeline
            #    to reduce per-tick Redis RTTs from 3-5 down to 1.
            event_batch: list = []   # TradeEvent objects to batch
            for kind, payload in io_steps:
                try:
                    if kind == "append_event":
                        event_batch.append(payload)
                    elif kind == "save_tp_hit":
                        try:
                            _d = payload if isinstance(payload, dict) else {}
                            if not _d:
                                logger.error(f"save_tp_hit payload is not a dict: type={type(payload)} val={payload}")
                                continue
                            self._io_save_tp_hit(
                                _d.get("pos"),  # type: ignore
                                tp_level=int(_d.get("tp_level", 0)),
                                fill_price=float(_d.get("fill_price", 0.0)),
                                closed_qty=float(_d.get("closed_qty", 0.0)),
                                pnl_part=float(_d.get("pnl_part", 0.0)),
                                ts_ms=int(_d.get("ts_ms", 0)),
                            )
                        except Exception as e:
                            logger.error(f"Error in save_tp_hit: {e}", exc_info=True)
                    elif kind == "save_trailing_move":
                        try:
                            _d = payload if isinstance(payload, dict) else {}
                            if not _d:
                                logger.error(f"save_trailing_move payload is not a dict: type={type(payload)}")
                                continue
                            self._io_save_trailing_move(_d.get("pos"), float(_d.get("previous_sl", 0.0)), float(_d.get("new_sl", 0.0)), int(_d.get("ts_ms", 0)))  # type: ignore
                        except Exception as e:
                            logger.error(f"Error in save_trailing_move: {e}", exc_info=True)
                    elif kind == "save_trailing_sync":
                        try:
                            _d = payload if isinstance(payload, dict) else {}
                            if not _d:
                                logger.error(f"save_trailing_sync payload is not a dict: type={type(payload)}")
                                continue
                            self._io_save_trailing_sync(_d.get("pos"), int(_d.get("ts_ms", 0)))  # type: ignore
                        except Exception as e:
                            logger.error(f"Error in save_trailing_sync: {e}", exc_info=True)
                    elif kind == "save_closed":
                        try:
                            _d = payload if isinstance(payload, dict) else {}
                            if not _d:
                                logger.error(f"save_closed payload is not a dict: type={type(payload)}")
                                continue
                            self._io_save_closed(_d.get("closed"), health_snapshot=(_d.get("health_snapshot") or {}))  # type: ignore
                        except Exception as e:
                            logger.error(f"Error in save_closed: {e}", exc_info=True)
                    elif kind == "analytics_closed":
                        try:
                            # Offload blocking DB write to background thread
                            fut = self._db_executor.submit(analytics_db.save_trade_closed, payload)
                            fut.add_done_callback(_log_future_exception)
                        except Exception as e:
                            logger.warning("Failed to submit trade to analytics DB: %s", e)
                    elif kind == "signal_outcome":
                        # Signal → Outcome pipeline (fail-open)
                        try:
                            from domain.signal_outcome import from_trade_closed as _build_outcome
                            from services.signal_outcome_writer import get_signal_outcome_writer
                            _outcome = _build_outcome(payload)
                            if _outcome is not None:
                                _so_fut = self._db_executor.submit(get_signal_outcome_writer().emit, _outcome)
                                _so_fut.add_done_callback(_log_future_exception)
                        except Exception as _so_err:
                            logger.warning("⚠️ signal_outcome emit failed in on_tick (fail-open): %s", _so_err)
                    elif kind == "update_stats":
                        d = payload
                        self._update_stats(d["pos"], d["closed"])
                        # Paper vs demo: record + maybe fire report
                        self._pvd_record_closed(d["pos"], d["closed"])
                        self._maybe_paper_vs_demo_report()
                except Exception as e:
                    logger.warning("⚠️ on_tick IO step failed kind=%s err=%s", kind, e)

            # P0 FIX: Flush batched events in single pipeline (1 RTT for N events)
            if event_batch:
                try:
                    pipe = self.redis.pipeline(transaction=False)
                    for ev in event_batch:
                        self.repo.append_event_pipe(ev, pipe)
                    pipe.execute()
                except Exception as e:
                    # Fallback: sequential (ensures no silent data loss)
                    logger.warning("⚠️ Batched event pipeline failed, falling back to sequential: %s", e)
                    for ev in event_batch:
                        try:
                            self.repo.append_event(ev)
                        except Exception:
                            pass

            # ✅ Report triggers (outside all locks)
            for trigger in report_triggers:
                try:
                    from services.periodic_reporter import check_and_trigger_report
                    src_t, sym_t, oid_t = trigger[0], trigger[1], trigger[2]  # type: ignore
                    check_and_trigger_report(src_t, sym_t, counter_type="trades", order_id=oid_t)
                except Exception as _rte:
                    logger.warning("⚠️ on_tick report trigger failed: %s", _rte)

            # Recommendation 4: Prometheus Histogram for tick latency (microseconds)
            t_dur_us = (time.perf_counter() - t_start) * 1_000_000
            self.tm_tick_latency_us.labels(symbol=symbol).observe(t_dur_us)


    # --------------------
    # External trailing / SL sync
    # --------------------
    def update_trailing_sl(
        self,
        signal_id: str,
        new_sl: float,
        source: str | None = None,
        profile: str | None = None,
        event_id: str | None = None,
        clear_tp_levels: bool = False,
    ) -> bool:
        """
        Обновляет trailing SL для позиции (thread-safe, idempotent).
        Используется для синхронизации с внешними оркестраторами трейлинга.
        """
        # ✅ Idempotency: атомарная проверка+установка dedup ключа
        if not self._dedup_acquire("trailing_update", event_id):
            logger.debug("⏭️ TRAILING_UPDATE duplicate event_id=%s already applied", event_id)
            return True

        ts = get_ny_time_millis()

        # Peek symbol without holding _lock while waiting for symbol-lock (prevents deadlock)
        pos_id, sym = self._peek_pos_and_symbol_by_sid(signal_id)
        if not pos_id:
            return False
        if not sym:
            # позиция уже закрыта/не найдена в open_positions -> идемпотентно
            return False

        # Resolve symbol under global lock, then serialize with symbol lock
        with self._lock:
            pos_id = self.pos_by_sid.get(signal_id)
            if not pos_id:
                return False
            pos = self.open_positions.get(pos_id)
            if not pos or getattr(pos, "closed", False):
                return False
            sym = str(getattr(pos, "symbol", "") or "")

        with self._symbol_ctx(sym):
            io_tasks: list[_IOTask] = []
            with self._lock:
                # re-check (position may be closed between locks)
                pos_id = self.pos_by_sid.get(signal_id)
                if not pos_id:
                    return False
                pos = self.open_positions.get(pos_id)
                if not pos or getattr(pos, "closed", False):
                    return False

                ev = apply_trailing_update(
                    pos, new_sl=float(new_sl), ts_ms=ts,
                    trailing_distance=0.0,
                    point_size=0.0,
                    clear_future_tp_levels=clear_tp_levels,
                )
                if ev:
                    io_tasks.append(_IOTask(
                        fn=(lambda ev=ev: self.repo.append_event(ev)),
                        desc=f"append_event:TRAILING_UPDATE:{pos.id}",
                    ))
                    io_tasks.append(_IOTask(
                        fn=(lambda pos=pos, ts=ts: self._io_save_trailing_sync(pos, ts)),
                        desc=f"save_trailing_sync_update:{pos.id}",
                    ))

            if io_tasks:
                self._run_io_tasks(io_tasks)

            return True

    def apply_trailing_sl_sync(
        self,
        sid: str,
        new_sl: float,
        ts_ms: int | None = None,
        trailing_distance: float = 0.0,
        point_size: float = 0.0,
        clear_future_tp_levels: bool = False,
    ) -> bool:
        """
        Применяет обновление trailing SL из внешнего источника (thread-safe).
        """
        ts = int(ts_ms or get_ny_time_millis())

        pos_id, sym = self._peek_pos_and_symbol_by_sid(sid)
        if not pos_id or not sym:
            return False

        with self._symbol_lock_ctx(sym):
            io_tasks: list[_IOTask] = []
            with self._lock:
                pos_id2 = self.pos_by_sid.get(sid)
                if not pos_id2:
                    return False
                pos = self.open_positions.get(pos_id2)
                if not pos or getattr(pos, "closed", False):
                    return False

                ev = apply_trailing_update(
                    pos, new_sl=float(new_sl), ts_ms=ts,
                    trailing_distance=trailing_distance,
                    point_size=point_size,
                    clear_future_tp_levels=clear_future_tp_levels,
                )
                if ev:
                    io_tasks.append(_IOTask(
                        fn=(lambda ev=ev: self.repo.append_event(ev)),
                        desc=f"append_event:TRAILING_SYNC:{pos.id}",
                    ))
                    io_tasks.append(_IOTask(
                        fn=(lambda pos=pos, ts=ts: self._io_save_trailing_sync(pos, ts)),
                        desc=f"save_trailing_sync:{pos.id}",
                    ))
                    # Point 4: trailing audit stream
                    prev_sl = float(ev.payload.get("previous_sl", 0.0) if ev.payload else 0.0)
                    io_tasks.append(_IOTask(
                        fn=(lambda pos=pos, ns=float(pos.sl), ps=prev_sl, ts=ts: self._emit_trailing_audit("TRAILING_SYNC", pos, ns, ps, ts)),
                        desc=f"trailing_audit:TRAILING_SYNC:{pos.id}",
                    ))
            if io_tasks:
                self._run_io_tasks(io_tasks)
            return True

    def apply_external_trailing_move(
        self,
        sid: str,
        new_sl: float,
        ts_ms: int | None = None,
        event_id: str | None = None
    ) -> bool:
        """
        Обрабатывает внешнее событие TRAILING_MOVE (идемпотентно).
        """
        if not self._dedup_acquire("trailing_move", event_id or f"{sid}:{new_sl}:{ts_ms or 0}"):
            logger.debug("⏭️ TRAILING_MOVE duplicate event_id=%s", event_id)
            return True

        ts = int(ts_ms or get_ny_time_millis())

        pos_id, sym = self._peek_pos_and_symbol_by_sid(sid)
        if not pos_id or not sym:
            return False

        with self._symbol_lock_ctx(sym):
            io_tasks: list[_IOTask] = []
            with self._lock:
                pos_id2 = self.pos_by_sid.get(sid)
                if not pos_id2:
                    return False
                pos = self.open_positions.get(pos_id2)
                if not pos or getattr(pos, "closed", False):
                    return False

                ev = apply_trailing_update(
                    pos, new_sl=float(new_sl), ts_ms=ts,
                    trailing_distance=0.0,
                    point_size=0.0,
                    clear_future_tp_levels=False,
                )
                if ev:
                    io_tasks.append(_IOTask(
                        fn=(lambda ev=ev: self.repo.append_event(ev)),
                        desc=f"append_event:TRAILING_MOVE_EXT:{pos.id}",
                    ))
                    io_tasks.append(_IOTask(
                        fn=(lambda pos=pos, ts=ts: self._io_save_trailing_sync(pos, ts)),
                        desc=f"save_trailing_sync_ext:{pos.id}",
                    ))
                    # Point 4: trailing audit stream
                    prev_sl = float(ev.payload.get("previous_sl", 0.0) if ev.payload else 0.0)
                    io_tasks.append(_IOTask(
                        fn=(lambda pos=pos, ns=float(pos.sl), ps=prev_sl, ts=ts: self._emit_trailing_audit("TRAILING_MOVE", pos, ns, ps, ts)),
                        desc=f"trailing_audit:TRAILING_MOVE:{pos.id}",
                    ))
            if io_tasks:
                self._run_io_tasks(io_tasks)
            return True

    def apply_external_sl_hit(
        self,
        signal_id: str,
        price: float,
        timestamp: int | None = None,
        source: str | None = None,
        event_id: str | None = None
    ) -> bool:
        """
        Обрабатывает внешнее событие SL_HIT (thread-safe, idempotent).
        Закрывает позицию по указанной цене.

        Args:
            signal_id: ID сигнала (sid)
            price: Цена закрытия
            timestamp: Timestamp в миллисекундах (опционально)
            source: Источник события (опционально)
            event_id: ID события для идемпотентности (опционально)

        Returns:
            True если позиция закрыта или уже была закрыта (идемпотентно),
            False если позиция не найдена в системе
        """
        # ✅ Idempotency: атомарная проверка+установка dedup ключа
        if not self._dedup_acquire("sl_hit", event_id):
            logger.debug("⏭️ SL_HIT duplicate event_id=%s already applied", event_id)
            return True

        from domain.time_utils import normalize_ts_ms
        ts = normalize_ts_ms(int(timestamp or 0))

        report_trigger: tuple[str, str] | None = None

        # Peek symbol without holding _lock while waiting for symbol-lock
        pos_id, sym = self._peek_pos_and_symbol_by_sid(signal_id)
        if not pos_id:
            # If we already closed this sid in the past (restart/cleanup), be idempotent.
            if self._is_sid_closed_repo_guard(signal_id):
                return True
            logger.debug("⏭️ Позиция для sid=%s не найдена", signal_id)
            return False
        if not sym:
            # если pos_id был, но позиция уже закрыта/удалена -> идемпотентно True
            return True

        with self._symbol_lock_ctx(sym):
            # Re-check under _lock (position might disappear between peek and lock)
            with self._lock:
                pos_id2 = self.pos_by_sid.get(signal_id)
                if not pos_id2:
                    # already closed/removed while we waited for symbol lock
                    return True
                pos = self._get_pos(pos_id2, symbol=sym)
                if not pos or getattr(pos, "closed", False):
                    return True

            # --- Compute/finalize (no service dict mutations here; symbol-lock guarantees serialization) ---
            # Realize remaining PnL for the rest of qty (external close is authoritative)
            try:
                close_qty = float(getattr(pos, "remaining_qty", 0.0) or 0.0)
            except Exception:
                close_qty = 0.0
            if close_qty > 1e-9 and not getattr(pos, "_pnl_finalized", False):
                # Idempotent guard (bug 2026-05-14): _pnl_finalized blocks re-add when
                # process_tick's SL handler already realized this same qty.
                spec = self._get_spec(pos.symbol) # Need spec for pnl_money
                try:
                    pnl_rest = float(spec.pnl_money(pos.entry_price, float(price), close_qty, pos.direction, symbol=pos.symbol))
                    pos.realized_pnl_gross += pnl_rest
                except Exception:
                    pass

            raw = "TRAILING_STOP" if getattr(pos, "trailing_active", False) else "SL"
            try:
                if int(getattr(pos, "tp_hits", 0) or 0) > 0:
                    raw = f"SL_AFTER_TP{int(getattr(pos, 'tp_hits', 0) or 0)}"
            except Exception:
                pass

            # Stamp exit on position
            try:
                pos.closed = True
                pos.exit_ts_ms = int(ts)
                pos.exit_price = float(price)
                pos.remaining_qty = 0.0
                # P1-9: FSM transition
                self._fsm_transition(
                    pos, "CLOSED",
                    trigger="sl_hit",
                    reason=str(raw),
                    price=float(price),
                    ts_ms=int(ts),
                )
            except Exception:
                pass

            spec = self._get_spec(pos.symbol)
            closed = finalize_trade(
                pos, spec,
                exit_price=float(price),
                exit_ts_ms=int(ts),
                close_reason_raw=str(raw),
                tp_ratios=self.tp_ratios,
            )

            # If trailing is active: classify outcome by actual PnL (audit)
            try:
                if getattr(pos, "trailing_started", False) or getattr(pos, "trailing_active", False):
                    closed.trailing_active = True
                    closed.trailing_started = True
                    closed.close_reason_detail = "TRAILING_PROFIT" if float(getattr(closed, "pnl_net", 0.0) or 0.0) > 1e-8 else "TRAILING_STOP"
            except Exception:
                pass

            # Build events (in-memory)
            from domain.models import TradeEvent
            sl_event = TradeEvent(
                event_type="SL_HIT",
                order_id=pos.id,
                sid=pos.sid,
                strategy=pos.strategy,
                source=pos.source,
                symbol=pos.symbol,
                tf=pos.tf,
                direction=pos.direction,
                ts_ms=int(ts),
                payload={
                    "sl": float(getattr(pos, "sl", 0.0) or 0.0),
                    "exit_price": float(price),
                    "remaining_qty_closed": float(close_qty),
                    "reason_raw": str(raw),
                    "external_event_id": event_id,
                },
            )
            close_event = TradeEvent(
                event_type="CLOSE",
                order_id=pos.id,
                sid=pos.sid,
                strategy=pos.strategy,
                source=pos.source,
                symbol=pos.symbol,
                tf=pos.tf,
                direction=pos.direction,
                ts_ms=int(ts),
                payload={
                    "reason": getattr(closed, "close_reason", ""),
                    "reason_raw": getattr(closed, "close_reason_raw", str(raw)),
                    "external_event_id": event_id,
                },
            )

            # Prepare health snapshot (service-side, not in repo)
            if self._attach_health_on_close:
                try:
                    now_ms = get_ny_time_millis()
                    closed._health_snapshot = self._get_health_snapshot_prefixed(closed.symbol, now_ms)  # type: ignore
                except Exception:
                    pass
            try:
                hs = self._get_health_snapshot_for_trade(str(getattr(closed, "symbol", "") or pos.symbol))
            except Exception:
                hs = {}

            with contextlib.suppress(Exception):
                stamp_closed_trade_horizon_from_position(pos, closed)

            # --- Cleanup service in-memory indexes under _lock ---
            with self._lock:
                self._pop_pos(pos.id)

            # --- I/O (STRICTLY outside self._lock; still under symbol-lock) ---
            self.repo.append_event(sl_event)
            self.repo.append_event(close_event)
            self._io_save_closed(closed, health_snapshot=hs)
            # Async DB persist (non-blocking)
            self._db_executor.submit(self._safe_save_trade_to_db, closed)
            with contextlib.suppress(Exception):
                self._update_stats(pos, closed)

            # Mark sid closed for idempotency across restarts/cleanup
            with contextlib.suppress(Exception):
                self._mark_sid_closed(str(pos.sid or signal_id), ttl_days=7)

            report_trigger = (pos.source, pos.symbol, pos.id, getattr(pos, "is_virtual", False))  # type: ignore

        # ✅ Отчет вне lock (I/O/логика)
        if report_trigger:
            # Async trigger (PeriodicReporter uses SYNC redis, must be offloaded)
            self._db_executor.submit(
                self._safe_trigger_report,
                report_trigger[0],
                report_trigger[1],
                "trades",
                report_trigger[2],  # type: ignore
            )
            if getattr(pos, "is_virtual", False):
                self._db_executor.submit(
                    self._safe_trigger_report,
                    report_trigger[0],
                    report_trigger[1],
                    "trades",
                    report_trigger[2],  # type: ignore
                    True
                )
            # try:
            #     from services.periodic_reporter import check_and_trigger_report
            #     logger.debug(f"🔄 Триггер отчета для закрытой сделки (external SL): source={report_trigger[0]}, symbol={report_trigger[1]}")
            #     check_and_trigger_report(report_trigger[0], report_trigger[1], counter_type="trades", order_id=report_trigger[2])
            # except Exception as e:
            #     logger.warning(f"⚠️ Ошибка при триггере отчета: {e}")

        logger.info("🛑 External SL_HIT: закрыта позиция %s для %s @ %.5f", signal_id, report_trigger[1] if report_trigger else "?", price)
        return True

    def apply_external_tp_hit(self, *args, **kwargs) -> bool:
        """
        External TP fill event (from broker/exchange execution stream).

        Цели:
          - Сериализация по symbol (symbol-lock)
          - Под self._lock только in-memory мутации
          - Любой repo/DB I/O строго вне self._lock
          - Единая семантика CLOSE через _persist_closed_trade_io()

        ВАЖНО: сигнатура сохранена через *args/**kwargs (чтобы не ломать существующие call-sites).
        Ниже извлечение параметров best-effort по именам.
        """
        from domain.time_utils import normalize_ts_ms

        # -------- Extract params (best-effort) --------
        # allow both positional and keyword style from existing code
        def _pick(name: str, default=None):
            if name in kwargs:
                return kwargs.get(name)
            return default

        # common names
        signal_id = _pick("signal_id", _pick("sid", None))
        price = _pick("price", _pick("fill_price", _pick("tp_price", _pick("exit_price", None))))
        timestamp = _pick("timestamp", _pick("ts", _pick("ts_ms", None)))
        event_id = _pick("event_id", _pick("external_event_id", None))
        tp_level = _pick("tp_level", _pick("level", None))
        closed_qty_arg = _pick("closed_qty", _pick("qty", _pick("filled_qty", None)))

        # if positional args were used, map by existing signature (best-effort)
        if signal_id is None and len(args) >= 1:
            signal_id = args[0]
        if price is None and len(args) >= 2:
            price = args[1]
        if timestamp is None and len(args) >= 3:
            timestamp = args[2]

        if not signal_id:
            return False
        if price is None:
            return False

        # -------- Dedup --------
        if not self._dedup_acquire("tp_hit", (event_id or "")):
            logger.debug("⏭️ TP_HIT duplicate event_id=%s already applied", event_id)
            return True

        ts = normalize_ts_ms(int(timestamp or 0))
        if ts <= 0:
            ts = get_ny_time_millis()

        # tp_level default: external TP_HIT is considered final fill if not specified
        try:
            tp_level_i = int(tp_level) if tp_level is not None else 3
        except Exception:
            tp_level_i = 3
        tp_level_i = 1 if tp_level_i < 1 else (3 if tp_level_i > 3 else tp_level_i)

        try:
            closed_qty_f = float(closed_qty_arg) if closed_qty_arg is not None else 0.0
        except Exception:
            closed_qty_f = 0.0

        return self._apply_external_tp_hit_impl(
            signal_id=str(signal_id),
            tp_level=tp_level_i,
            price=float(price),
            ts_ms=int(ts),
            event_id=(event_id or ""),
            closed_qty=closed_qty_f,
        )

    def _apply_external_tp_hit_impl(
        self,
        *,
        signal_id: str,
        tp_level: int,
        price: float,
        ts_ms: int,
        event_id: str,
        closed_qty: float = 0.0,
    ) -> bool:
        """
        External TP fill event (authoritative).
        Semantics:
          - symbol-lock serialization
          - under self._lock: only in-memory index ops
          - repo/DB I/O outside self._lock
          - idempotent across restarts via closed_sid_done:{sid}

        Partial-close branch (opt-in via PARTIAL_CLOSE_TP1_MODE=ENFORCE):
          - When tp_level == 1 AND caller provided closed_qty AND
            closed_qty < remaining_qty, treat as partial TP1 fill:
            reduce remaining_qty, arm trailing + move SL→BE on remainder,
            keep position open. Default (mode=OFF) preserves full-close.
        """
        if not signal_id:
            return False

        # If position absent, check "sid closed" guard for idempotency
        pos_id, sym = self._peek_pos_and_symbol_by_sid(signal_id)
        if not pos_id:
            return bool(self._is_sid_closed_repo_guard(signal_id))
        if not sym:
            return True

        report_trigger: tuple[str, str] | None = None

        with self._symbol_lock_ctx(sym):
            # Re-check under _lock
            with self._lock:
                pos_id2 = self.pos_by_sid.get(signal_id)
                if not pos_id2:
                    return True
                pos = self._get_pos(pos_id2, symbol=sym)
                if not pos or getattr(pos, "closed", False):
                    return True

            spec = self._get_spec(pos.symbol)

            try:
                remaining_before = float(getattr(pos, "remaining_qty", 0.0) or 0.0)
            except Exception:
                remaining_before = 0.0

            partial_mode = (os.environ.get("PARTIAL_CLOSE_TP1_MODE", "OFF") or "OFF").upper().strip()
            try:
                cq = float(closed_qty)
            except Exception:
                cq = 0.0
            is_partial_tp1 = bool(
                tp_level == 1
                and partial_mode == "ENFORCE"
                and cq > 1e-9
                and cq < remaining_before - 1e-9
                and not getattr(pos, "_pnl_finalized", False)
            )

            if is_partial_tp1:
                pnl_part = 0.0
                try:
                    pnl_part = float(spec.pnl_money(pos.entry_price, float(price), cq, pos.direction, symbol=pos.symbol))
                    pos.realized_pnl_gross += pnl_part
                except Exception:
                    pnl_part = 0.0

                try:
                    pos.tp_hits = max(int(getattr(pos, "tp_hits", 0) or 0), 1)
                    pos.tp1_hit = True
                    pos.tp_before_sl = int(getattr(pos, "tp_hits", 0) or 1)  # type: ignore
                    pos.remaining_qty = remaining_before - cq
                    try:
                        pos.tp_fill_prices[1] = float(price)
                        pos.tp_fill_times[1] = int(ts_ms)
                    except Exception:
                        pass
                    fsm_now = getattr(self._fsm_map.get(getattr(pos, "id", "")), "status", None)
                    _fsm_sv = getattr(fsm_now, "value", "") if fsm_now else ""
                    if _fsm_sv not in ("TP1_HIT", "TRAILING_ARMED", "TRAILING_ACTIVE"):
                        self._fsm_transition(
                            pos, "TP1_HIT",
                            trigger="tp1_hit_partial",
                            reason="TP1 partial fill",
                            price=float(price),
                            ts_ms=int(ts_ms),
                        )
                except Exception:
                    pass

                ev_arm = None
                try:
                    ev_arm = maybe_arm_trailing_after_tp1(pos, spec=spec, ts_ms=int(ts_ms))
                except Exception:
                    ev_arm = None

                from domain.models import TradeEvent
                tp_event = TradeEvent(
                    event_type="TP_HIT",
                    order_id=pos.id,
                    sid=pos.sid,
                    strategy=pos.strategy,
                    source=pos.source,
                    symbol=pos.symbol,
                    tf=pos.tf,
                    direction=pos.direction,
                    ts_ms=int(ts_ms),
                    payload={
                        "tp_level": 1,
                        "tp_price": float(price),
                        "fill_price": float(price),
                        "closed_qty": float(cq),
                        "remaining_qty": float(pos.remaining_qty),
                        "pnl_part_gross": float(pnl_part),
                        "tp_hits": int(getattr(pos, "tp_hits", 1)),
                        "external_event_id": event_id,
                        "partial_close_tp1_mode": partial_mode,
                    },
                )

                self.repo.append_event(tp_event)
                if ev_arm is not None:
                    self.repo.append_event(ev_arm)
                    if ev_arm.event_type == "TRAILING_SYNC":
                        with contextlib.suppress(Exception):
                            self._io_save_trailing_move(
                                pos,
                                previous_sl=float(ev_arm.payload.get("previous_sl", 0.0)),
                                new_sl=float(ev_arm.payload.get("new_sl", 0.0)),
                                ts_ms=int(ts_ms),
                            )

                logger.info(
                    "✅ External TP1 PARTIAL: sid=%s closed_qty=%.6f remaining=%.6f BE-armed=%s",
                    signal_id, float(cq), float(pos.remaining_qty),
                    "yes" if ev_arm is not None and ev_arm.event_type == "TRAILING_SYNC" else "no",
                )
                return True

            # Close ALL remaining qty on external TP_HIT (final close)
            try:
                close_qty = float(getattr(pos, "remaining_qty", 0.0) or 0.0)
            except Exception:
                close_qty = 0.0

            pnl_part = 0.0
            if close_qty > 1e-9 and not getattr(pos, "_pnl_finalized", False):
                # Idempotent guard (bug 2026-05-14): _pnl_finalized blocks re-add when
                # process_tick's TP handler already realized this same qty.
                try:
                    pnl_part = float(spec.pnl_money(pos.entry_price, float(price), close_qty, pos.direction, symbol=pos.symbol))
                    pos.realized_pnl_gross += pnl_part
                except Exception:
                    pnl_part = 0.0

            # Update TP flags and close
            try:
                pos.tp_hits = max(int(getattr(pos, "tp_hits", 0) or 0), int(tp_level))
                pos.tp1_hit = (getattr(pos, "tp1_hit", False) or tp_level >= 1)
                pos.tp2_hit = (getattr(pos, "tp2_hit", False) or tp_level >= 2)
                pos.tp3_hit = (getattr(pos, "tp3_hit", False) or tp_level >= 3)
                pos.tp_before_sl = int(getattr(pos, "tp_hits", 0) or 0)  # type: ignore
                pos.closed = True
                pos.exit_ts_ms = int(ts_ms)
                pos.exit_price = float(price)
                pos.remaining_qty = 0.0
                # P1-9: FSM — step through intermediate TP states then CLOSED
                if tp_level >= 1:
                    fsm_from = getattr(self._fsm_map.get(getattr(pos, "id", "")), "status", None)
                    _fsm_st = getattr(fsm_from, "value", "") if fsm_from else ""
                    if _fsm_st not in ("TP1_HIT", "TP2_HIT", "TRAILING_ARMED", "TRAILING_ACTIVE"):
                        self._fsm_transition(
                            pos, "TP1_HIT",
                            trigger=f"tp{tp_level}_hit",
                            reason=f"TP{tp_level} close",
                            price=float(price),
                            ts_ms=int(ts_ms),
                        )
                if tp_level >= 2:
                    fsm_now = getattr(self._fsm_map.get(getattr(pos, "id", "")), "status", None)
                    _fsm_sv = getattr(fsm_now, "value", "") if fsm_now else ""
                    if _fsm_sv not in ("TP2_HIT", "TRAILING_ARMED", "TRAILING_ACTIVE"):
                        self._fsm_transition(
                            pos, "TP2_HIT",
                            trigger=f"tp{tp_level}_hit",
                            reason=f"TP{tp_level} close",
                            price=float(price),
                            ts_ms=int(ts_ms),
                        )
                self._fsm_transition(
                    pos, "CLOSED",
                    trigger=f"tp{tp_level}_final_close",
                    reason=f"TP{tp_level} final close",
                    price=float(price),
                    ts_ms=int(ts_ms),
                )
            except Exception:
                pass

            closed = finalize_trade(
                pos, spec,
                exit_price=float(price),
                exit_ts_ms=int(ts_ms),
                close_reason_raw=f"TP{int(tp_level)}",
                tp_ratios=self.tp_ratios,
            )
            self._log_ab_closed_event(pos, closed, f"TP{int(tp_level)}")

            from domain.models import TradeEvent
            tp_event = TradeEvent(
                event_type="TP_HIT",
                order_id=pos.id,
                sid=pos.sid,
                strategy=pos.strategy,
                source=pos.source,
                symbol=pos.symbol,
                tf=pos.tf,
                direction=pos.direction,
                ts_ms=int(ts_ms),
                payload={
                    "tp_level": int(tp_level),
                    "tp_price": float(price),
                    "fill_price": float(price),
                    "closed_qty": float(close_qty),
                    "remaining_qty": 0.0,
                    "pnl_part_gross": float(pnl_part),
                    "tp_hits": int(getattr(pos, "tp_hits", int(tp_level))),
                    "external_event_id": event_id,
                },
            )
            close_event = TradeEvent(
                event_type="CLOSE",
                order_id=pos.id,
                sid=pos.sid,
                strategy=pos.strategy,
                source=pos.source,
                symbol=pos.symbol,
                tf=pos.tf,
                direction=pos.direction,
                ts_ms=int(ts_ms),
                payload={
                    "reason": getattr(closed, "close_reason", ""),
                    "reason_raw": getattr(closed, "close_reason_raw", f"TP{int(tp_level)}"),
                    "external_event_id": event_id,
                },
            )

            if self._attach_health_on_close:
                try:
                    now_ms = get_ny_time_millis()
                    closed._health_snapshot = self._get_health_snapshot_prefixed(closed.symbol, now_ms)  # type: ignore
                except Exception:
                    pass
            try:
                hs = self._get_health_snapshot_for_trade(str(getattr(closed, "symbol", "") or pos.symbol))
            except Exception:
                hs = {}

            with contextlib.suppress(Exception):
                stamp_closed_trade_horizon_from_position(pos, closed)

            # Cleanup indexes under _lock
            with self._lock:
                self._pop_pos(pos.id)

            # I/O outside _lock
            self.repo.append_event(tp_event)
            self.repo.append_event(close_event)
            self._io_save_closed(closed, health_snapshot=hs)
            # Async DB persist (non-blocking)
            self._db_executor.submit(self._safe_save_trade_to_db, closed)
            with contextlib.suppress(Exception):
                self._update_stats(pos, closed)

            with contextlib.suppress(Exception):
                self._mark_sid_closed(str(pos.sid or signal_id), ttl_days=7)

            report_trigger = (pos.source, pos.symbol, pos.id, getattr(pos, "is_virtual", False))  # type: ignore

        if report_trigger:
            # Async trigger
            self._db_executor.submit(
                self._safe_trigger_report,
                report_trigger[0],
                report_trigger[1],
                "trades",
                report_trigger[2],  # type: ignore
            )
            if getattr(pos, "is_virtual", False):
                self._db_executor.submit(
                    self._safe_trigger_report,
                    report_trigger[0],
                    report_trigger[1],
                    "trades",
                    report_trigger[2],  # type: ignore
                    True
                )
            # try:
            #     from services.periodic_reporter import check_and_trigger_report
            #     logger.debug(f"🔄 Триггер отчета для закрытой сделки (external TP): source={report_trigger[0]}, symbol={report_trigger[1]}")
            #     check_and_trigger_report(report_trigger[0], report_trigger[1], counter_type="trades", order_id=report_trigger[2])
            # except Exception as e:
            #     logger.warning(f"⚠️ Ошибка при триггере отчета: {e}")

        logger.info("✅ External TP_HIT: закрыта позиция %s по TP%d @ %.5f", signal_id, int(tp_level), float(price))
        return True

    # --------------------
    # Stats
    # --------------------
    def get_position_count(self) -> int:
        """Возвращает количество открытых позиций (thread-safe)."""
        with self._lock:
            return len(self.open_positions)

    def peek_symbol_by_sid(self, sid: str) -> str | None:
        """
        Fast thread-safe lookup for routing:
          sid -> pos_id -> pos.symbol
        Returns None if sid not found / already closed.
        """
        if not sid:
            return None
        try:
            with self._lock:
                pid = self.pos_by_sid.get(str(sid))
                if not pid:
                    return None
                pos = self.open_positions.get(pid)
                if not pos:
                    return None
                sym = getattr(pos, "symbol", None)
                return str(sym) if sym else None
        except Exception:
            return None

    def _rg_pending_add(self, delta: int) -> None:
        """Безопасное обновление счетчика pending задач для метрик."""
        try:
            with self._rg_pending_guard:
                self._rg_pending = int(self._rg_pending) + int(delta)
                pending = int(self._rg_pending)
            # Update prometheus gauge
            self.tm_rg_persist_pending.set(float(pending))
        except Exception:
            pass

    def _submit_regime_guard_persist_task(self, task: Callable[[], None], tags: dict[str, Any] | None = None) -> None:
        """
        Отправляет задачу на сохранение в DB Executor с учетом Backpressure.
        Если очередь полна — сбрасывает задачу и пишет метрику (Drop).
        """
        family = (tags or {}).get("family", "unknown")
        venue = (tags or {}).get("venue", "unknown")

        # 1. Если выключено — запускаем синхронно (Legacy mode)
        if not getattr(self, "_rg_async_persist", False):
            try:
                task()
            except Exception as e:
                self.logger.warning("RegimeGuard persist failed (sync): %s", e)
            return

        sem = getattr(self, "_rg_persist_sem", None)
        if not sem:
            return

        # 2. Backpressure Check: Non-blocking acquire
        acquired = False
        try:
            acquired = sem.acquire(blocking=False)
        except Exception:
            acquired = True # Fail-open

        if not acquired:
            # Очередь переполнена — сбрасываем задачу (Shed Load)
            self.tm_rg_persist_dropped.labels(family=family, venue=venue).inc()
            if getattr(self, "_rg_pending", 0) % 100 == 0:
                self.logger.warning("⚠️ RegimeGuard persist DROPPED: pending limit reached (%s)", self._rg_max_pending)
            return

        # 3. Submit task
        self._rg_pending_add(1)
        try:
            exec_ = getattr(self, "_rg_db_executor", None) or self._db_executor
            fut = exec_.submit(task)
        except Exception as e:
            self._rg_pending_add(-1)
            sem.release()
            self.logger.error("Failed to submit RegimeGuard task: %s", e)
            return

        self.tm_rg_persist_submitted.labels(family=family, venue=venue).inc()

        # 4. Callback для очистки семафора и логирования ошибок
        def _done_cb(f):
            try:
                exc = f.exception()
                if exc:
                    self.tm_rg_persist_failed.labels(family=family, venue=venue).inc()
                    self.logger.error("❌ Async RegimeGuard persist failed: %s", exc)
            except Exception:
                pass
            finally:
                self._rg_pending_add(-1)
                with contextlib.suppress(RuntimeError):
                    sem.release()

        fut.add_done_callback(_done_cb)

    def _calculate_r_value(self, pos: PositionState, closed) -> float:
        """Расчет R-value (pnl_net / risk_amount)."""
        pnl = getattr(closed, 'pnl_net', 0.0) or 0.0
        risk = getattr(pos, 'risk_amount', 0.0) or 0.0
        return pnl / risk if risk > 0 else 0.0

    def _resolve_closed_at(self, closed) -> Any:
        """Разрешение времени закрытия в datetime с tz=utc."""
        from datetime import datetime
        closed_at = getattr(closed, 'exit_ts_ms', None) or getattr(closed, 'closed_at', None)
        if closed_at is None:
            return datetime.now(UTC)
        if isinstance(closed_at, (int, float)):
            ts_sec = float(closed_at)
            if ts_sec > 946684800000: # ms to sec
                ts_sec = ts_sec / 1000.0
            return datetime.fromtimestamp(ts_sec, tz=UTC)
        if not hasattr(closed_at, 'tzinfo'):
            return datetime.now(UTC)
        return closed_at

    def _update_stats(self, pos: PositionState, closed: Any) -> None:
        """Delegate to PnlCalculator.update_stats (Phase 3 thin proxy)."""
        try:
            if getattr(pos, "is_virtual", False) or getattr(closed, "is_virtual", False):
                return
            pos_dict = asdict(pos) if hasattr(pos, "__dataclass_fields__") else dict(getattr(pos, "__dict__", {}) or {})
            closed_dict = asdict(closed) if hasattr(closed, "__dataclass_fields__") else dict(getattr(closed, "__dict__", {}) or {})
            self._pnl_calc.update_stats(
                pos_dict, closed_dict,
                submit_persist_task_fn=self._submit_regime_guard_persist_task,
            )
        except Exception as e:
            logger.warning("_update_stats delegation failed: %s", e)




    def _log_ab_closed_event(self, pos: PositionState, closed: TradeClosed, close_reason: str) -> None:
        """Delegate to TradeEventEmitter.emit_ab_closed (Phase 3 thin proxy)."""
        if getattr(self, "_emitter", None) is not None:
            self._emitter.emit_ab_closed(
                pos, closed, close_reason,
                get_spec_fn=self._get_spec,
            )
            return
        # fallback: original body (before __init__ completes)
        self._log_ab_closed_event_legacy(pos, closed, close_reason)

    def _log_ab_closed_event_legacy(self, pos: PositionState, closed: TradeClosed, close_reason: str) -> None:
        """Original body — fallback only."""
        """
        Helper to extract AB metadata and PnL/Rice status and log via TradeEventsLogger.
        Fail-open.
        """
        if not getattr(self, "events_logger", None):
            return

        try:
            md = {}
            sp = getattr(pos, "signal_payload", None)
            if isinstance(sp, dict):
                # Copy AB fields (expanded list)
                for k in ("ab_arm", "ab_group", "ab_key", "ab_ver", "arm_ver", "regime", "zone_id", "zone_type", "bundle", "decision", "leader"):
                    if k in sp:
                        md[k] = sp.get(k)

            # Risk/PnL R calculation
            try:
                entry = float(getattr(pos, "entry_price", 0.0) or 0.0)
                # Use 'sl' (standard) or 'sl_price' fallback
                sl = float(getattr(pos, "sl", 0.0) or getattr(pos, "sl_price", 0.0) or 0.0)
                lot = float(getattr(pos, "lot", 0.0) or 0.0)

                # Simple risk calculation |entry - sl| * lot
                risk_usd = abs(entry - sl) * lot if (entry > 0 and sl > 0 and lot > 0) else 0.0

                # If explicit risk_usd was stored in signal_payload, prefer it?
                # (User request says "if PositionState does not have risk_usd, better add it at creation".
                # But here we compute it if missing).
                # Actually user patch says:
                # try:
                #     ru = getattr(pos, "risk_usd", None)
                #     if ru is not None...
                # except...

                # We check pos.risk_usd first
                # --- Enrich POSITION_CLOSED with AB + risk for evaluator/rollback ---
                risk_usd = 0.0
                # 1. Try existing pos.risk_usd
                try:
                    ru = getattr(pos, "risk_usd", None)
                    if ru is not None and float(ru) > 0:
                        risk_usd = float(ru)
                except Exception:
                    pass

                # 2. Try spec.risk_money
                if risk_usd <= 1e-9:
                    try:
                        spec = self._get_spec(pos.symbol)
                        if spec:
                            side = str(getattr(pos, "direction", "") or "").upper()
                            risk_usd = float(spec.risk_money(
                                float(pos.entry_price or 0.0),
                                float(pos.sl or 0.0),
                                float(pos.lot or 0.0),
                                side,
                                str(pos.symbol or ""),
                            ) or 0.0)
                    except Exception:
                        pass

                # 3. Fallback: abs(open - sl) * lot
                if risk_usd <= 1e-9:
                     with contextlib.suppress(Exception):
                        risk_usd = float(abs(float(pos.entry_price or 0.0) - float(pos.sl or 0.0)) * float(pos.lot or 0.0))

                ab_arm = ""
                ab_group = ""
                rg = ""
                try:
                    sp = getattr(pos, "signal_payload", None) or {}
                    if isinstance(sp, dict):
                        ab_arm = (sp.get("ab_arm") or "")
                        ab_group = (sp.get("ab_group") or "")
                        rg = (sp.get("regime") or "")
                except Exception:
                    pass

                extra = {
                    "risk_usd": float(risk_usd),
                    "ab_arm": (ab_arm or ""),
                    "ab_group": (ab_group or ""),
                    "regime": (rg or "na"),
                }

                # === AB attribution + entry context (flattened into event payload) ===
                try:
                    sp = getattr(pos, "signal_payload", {}) or {}
                    ab = sp.get("ab", {}) if isinstance(sp, dict) else {}
                    ctx = sp.get("ctx", {}) if isinstance(sp, dict) else {}
                    dec = sp.get("decision", sp.get("decision", "na")) if isinstance(sp, dict) else "na"

                    pnl_usd = float(getattr(closed, "pnl_net", 0.0) or 0.0)
                    r_usd = float(risk_usd or getattr(pos, "risk_usd", 0.0) or 0.0)

                    extra.update({
                        "ab_arm": (ab.get("arm", getattr(pos, "ab_arm", "A"))).upper(),
                        "ab_group": (ab.get("group", getattr(pos, "ab_group", "default"))).lower(),
                        "ab_key": (ab.get("key", getattr(pos, "ab_key", ""))),
                        "arm_ver": int(ab.get("arm_ver", getattr(pos, "arm_ver", 0))),
                        "ab_split_reason": (ab.get("split_reason","")),
                        "scenario": str(dec).lower(),  # continuation|reversal
                        "regime": (ctx.get("regime", getattr(pos, "regime", "na"))).lower(),
                        "entry_adx_q": float(ctx.get("adx_q", 0.5) or 0.5),
                        "entry_spread_z": float(ctx.get("spread_z", 0.0) or 0.0),
                        "entry_pressure_sps": float(ctx.get("pressure_sps", 0.0) or 0.0),
                        "entry_cooldown_sps": float(ctx.get("cooldown_sps", 0.0) or 0.0),
                        "entry_obi_age_ms": int(ctx.get("obi_age_ms", 0) or 0),
                        "entry_abs_th_unstable": int(ctx.get("abs_th_unstable", 0) or 0),
                        "entry_news_blocked": int(ctx.get("news_blocked", 0) or 0),
                        "risk_usd": r_usd,
                        # CRITICAL: Pass full signal_payload for PeriodicReporter metrics (of_confirm, ml, etc.)
                        "signal_payload": sp,
                    })

                    # --- NEW: Precise Policy Fields (Autopilot) ---
                    pol = sp.get("policy") or {}
                    if isinstance(pol, dict):
                        extra.update({
                            "abs_lvl_tier": int(pol.get("abs_lvl_tier", -1)),
                            "dn_tier": int(pol.get("dn_tier", -1)),
                            "book_health_ok": int(pol.get("book_health_ok", -1)),
                            "of_confirm_ok": int(pol.get("of_confirm_ok", 0)),
                            "of_confirm_score": float(pol.get("of_confirm_score", 0.0)),
                            "spread_bp": float(pol.get("spread_bp", 0.0)),
                            "book_age_ms": int(pol.get("book_age_ms", 0)),
                            "book_rate_hz": float(pol.get("book_rate_hz", 0.0)),
                        })

                    if r_usd > 1e-9:
                        r_val = float(pnl_usd / r_usd)
                        md["pnl_r"] = r_val
                        extra["r_mult"] = r_val
                        md["risk_usd"] = r_usd
                except Exception:
                    pass
            except Exception:
                extra = {}

            # Extract metadata for AB analysis
            ab_arm = ""
            ab_group = ""
            ab_key = ""
            arm_ver = 0
            regime = "na"
            regime_group = "na"
            scenario = ""
            scenario_v4 = ""
            risk_usd = 0.0
            r_mult = 0.0
            meta_veto = 0
            meta_enforce_key = ""
            meta_enforce_salt = "enf_v1"

            # New autopilot fields (best-effort; fail-open)
            abs_lvl_tier = -1
            dn_tier = -1
            book_health_ok = -1
            book_age_ms = -1
            spread_bp = -1.0
            of_confirm_ok = -1
            of_confirm_score = -1.0
            atr_bps_exec = -1.0
            atr_unified_th_bps = -1.0
            atr_floor_th_bps = -1.0
            atr_fees_th_bps = -1.0
            meta_enforce_applied = None

            try:
                sp = getattr(pos, "signal_payload", None)
                if isinstance(sp, dict):
                    ab_arm = (sp.get("ab_arm", "") or "")
                    ab_group = (sp.get("ab_group", "") or "")
                    ab_key = (sp.get("ab_key", "") or "")
                    arm_ver = int(sp.get("arm_ver", 0) or 0)
                    regime = str(sp.get("regime", (sp.get("ctx") or {}).get("regime", "na")) or "na")
                    # Extract regime_group for stratified analysis (prefer explicit, fallback to regime)
                    regime_group = str(
                        sp.get("regime_group")
                        or (sp.get("ctx") or {}).get("regime_group")
                        or (sp.get("indicators") or {}).get("regime_group")
                        or (sp.get("config_snapshot") or {}).get("indicators", {}).get("regime_group")
                        or regime
                        or "na"
                    )

                    # scenario taxonomy is strict: continuation|reversal
                    scenario = str(
                        sp.get("scenario")
                        or sp.get("decision")
                        or sp.get("strong_gate_scn")
                        or (sp.get("of") or {}).get("strong_gate_scn")
                        or ""
                    ).lower()
                    if scenario not in ("continuation", "reversal"):
                        # Attempt normalization if not strict
                        from core.autopilot_fields import normalize_scenario
                        scenario = normalize_scenario(scenario)

                    # Extract scenario_v4 for additional stratification (from of_confirm evidence)
                    scenario_v4 = ""
                    try:
                        of_dict = sp.get("of") or {}
                        if isinstance(of_dict, dict):
                            evidence = of_dict.get("evidence") or {}
                            if isinstance(evidence, dict):
                                scenario_v4 = (evidence.get("scenario_v4", "") or "")
                    except Exception:
                        scenario_v4 = ""

                    risk_usd = float(sp.get("risk_usd", 0.0) or 0.0)

                    # Pull indicators if present (best-effort)
                    ind = sp.get("indicators") or (sp.get("config_snapshot") or {}).get("indicators") or {}
                    if isinstance(ind, dict):
                        abs_lvl_tier = int(ind.get("abs_lvl_tier", ind.get("abs_lvl_tier_used", -1)) or -1)
                        dn_tier = int(ind.get("dn_tier", -1) or -1)
                        book_health_ok = int(ind.get("book_health_ok", -1) or -1)
                        book_age_ms = int(ind.get("book_age_ms", -1) or -1)
                        spread_bp = float(ind.get("spread_bp", ind.get("spread_bps", -1.0)) or -1.0)
                        of_confirm_ok = int(ind.get("of_confirm_ok", ind.get("strong_gate_ok", -1)) or -1)
                        of_confirm_score = float(ind.get("of_confirm_score", -1.0) or -1.0)
                        atr_bps_exec = float(ind.get("atr_bps_exec", -1.0) or -1.0)
                        atr_unified_th_bps = float(ind.get("atr_unified_th_bps", -1.0) or -1.0)
                        atr_floor_th_bps = float(ind.get("atr_floor_th_bps", -1.0) or -1.0)
                        atr_fees_th_bps = float(ind.get("atr_fees_th_bps", -1.0) or -1.0)

                    # Extract meta_enforce fields from of_confirm evidence (for ramp evaluation and Stage2 optimization)
                    # CRITICAL: meta_veto must be written ALWAYS (even when meta_enforce_applied=0) for correct counterfactual simulation
                    meta_enforce_applied = None
                    meta_veto = 0
                    meta_enforce_key = ""
                    meta_enforce_salt = "enf_v1"
                    try:
                        of_dict = sp.get("of") or {}
                        if isinstance(of_dict, dict):
                            evidence = of_dict.get("evidence") or {}
                            if isinstance(evidence, dict):
                                meta_enforce_applied = int(evidence.get(MetaKeys.ENFORCE_APPLIED, 0) or 0)
                                # meta_veto is computed always (even in SHADOW/bypass mode) - required for Stage2 optimization
                                meta_veto = int(evidence.get(MetaKeys.VETO, 0) or 0)
                                meta_enforce_key = (evidence.get(MetaKeys.ENFORCE_KEY, "") or "")
                                meta_enforce_salt = (evidence.get(MetaKeys.ENFORCE_SALT, "enf_v1") or "enf_v1")
                        # Fallback: try indicators directly
                        if meta_enforce_applied is None and isinstance(ind, dict):
                            meta_enforce_applied = int(ind.get(MetaKeys.ENFORCE_APPLIED, 0) or 0)
                            if meta_veto == 0:
                                meta_veto = int(ind.get(MetaKeys.VETO, 0) or 0)
                            if not meta_enforce_key:
                                meta_enforce_key = (ind.get(MetaKeys.ENFORCE_KEY, "") or "")
                            if meta_enforce_salt == "enf_v1":
                                meta_enforce_salt = (ind.get(MetaKeys.ENFORCE_SALT, "enf_v1") or "enf_v1")
                    except Exception:
                        meta_enforce_applied = None
                        # Keep defaults for meta_veto, meta_enforce_key, meta_enforce_salt

                if risk_usd <= 0:
                    risk_usd = float(getattr(pos, "risk_usd", 0.0) or 0.0)
            except Exception:
                pass

            r_mult = 0.0
            try:
                if risk_usd > 0:
                    pnl_net_val = float(getattr(closed, "pnl_net", 0.0) or 0.0)
                    r_mult = pnl_net_val / float(risk_usd)
            except Exception:
                r_mult = 0.0

            # Extract exit_ts_ms from closed or pos (required for Stage2 optimization time filtering)
            exit_ts_ms = 0
            try:
                exit_ts_ms = int(getattr(closed, "exit_ts_ms", None) or getattr(pos, "exit_ts_ms", None) or 0)
                if exit_ts_ms <= 0:
                    # Fallback: try to get from closed_at or use current time
                    from datetime import datetime
                    closed_at = getattr(closed, "closed_at", None)
                    if closed_at:
                        if isinstance(closed_at, (int, float)):
                            exit_ts_ms = int(closed_at)
                        elif isinstance(closed_at, datetime):
                            exit_ts_ms = int(closed_at.timestamp() * 1000)
                    if exit_ts_ms <= 0:
                        exit_ts_ms = get_ny_time_millis()
            except Exception:
                exit_ts_ms = get_ny_time_millis()

            # A3: enrich POSITION_CLOSED with join-critical execution fields.
            # We keep this fail-open: if any attribute is missing, we fall back to safe defaults.

            order_id = ""
            fee_bps = 0.0

            try:
                # Best-effort order_id: prefer exit_order_id (unique per fill), fallback to pos.id
                order_id = str(
                    getattr(closed, "order_id", None)
                    or getattr(closed, "exit_order_id", None)
                    or getattr(pos, "close_order_id", None)
                    or getattr(pos, "order_id", None)
                    or getattr(pos, "id", "")
                    or ""
                )
            except Exception:
                order_id = str(getattr(pos, "id", "") or "")

            try:
                fees_usd_val = float(getattr(closed, "fees", 0.0) or getattr(pos, "fees", 0.0) or 0.0)
                turnover_val = float(getattr(closed, "turnover_roundtrip", 0.0) or 0.0)
                if turnover_val > 0:
                    fee_bps = (fees_usd_val / turnover_val) * 10000.0
            except Exception:
                fee_bps = 0.0

            self.events_logger.log_position_closed(  # type: ignore
                sid=str(getattr(pos, "sid", "")),
                symbol=str(getattr(pos, "symbol", "")),
                # A3 time contract: use exchange close timestamp as primary
                ts_ms=int(exit_ts_ms or 0),
                exit_ts_ms=int(exit_ts_ms or 0),
                # A3 join-critical exec fields
                order_id=order_id,
                side=str(getattr(pos, "direction", "") or "").upper(),
                venue=str(getattr(pos, "source", "") or ""),
                qty=float(getattr(pos, "lot", 0.0) or 0.0),
                fee_bps=float(fee_bps),
                close_price=float(getattr(closed, "exit_price", 0.0) or 0.0),
                pnl=float(getattr(closed, "pnl_net", 0.0) or 0.0),
                position_id=str(getattr(pos, "pos_id", "") or getattr(pos, "id", "")),
                lot=float(getattr(pos, "lot", 0.0) or 0.0),
                source=str(getattr(pos, "source", "mt5")),
                close_reason=str(close_reason),
                metadata=md,
                payload={
                    "ab_arm": ab_arm,
                    "ab_group": ab_group,
                    "ab_key": ab_key,
                    "arm_ver": int(arm_ver),
                    "regime": regime,
                    "regime_group": regime_group,  # For stratified DiD analysis
                    "scenario": scenario,
                    "scenario_v4": scenario_v4,  # For additional stratification
                    "risk_usd": float(risk_usd),
                    "r_mult": float(r_mult),
                    "exit_ts_ms": int(exit_ts_ms or 0),
                    # A3: duplicate in payload for legacy consumers that read from here
                    "ts_fill_ms": int(exit_ts_ms or 0),
                    "order_id": str(order_id),
                    "qty": float(getattr(pos, "lot", 0.0) or 0.0),
                    "side": str(getattr(pos, "direction", "") or "").upper(),
                    "venue": str(getattr(pos, "source", "") or ""),
                    "fee_bps": float(fee_bps),
                    "abs_lvl_tier": int(abs_lvl_tier),
                    "dn_tier": int(dn_tier),
                    "book_health_ok": int(book_health_ok),
                    "book_age_ms": int(book_age_ms),
                    "spread_bp": float(spread_bp),
                    # Cost-aware evaluator inputs (entry-time)
                    # cost snapshot at entry (fallback to p0_features_snapshot if direct fields are empty)
#                     "p0_spread_bps_at_entry": float(
#                         (getattr(pos, "p0_spread_bps_at_entry", 0.0) or 0.0)
#                         or ((getattr(pos, "p0_features_snapshot", None) or {}).get("spread_bps") or 0.0)
#                         or ((getattr(pos, "p0_features_snapshot", None) or {}).get("p0_spread_bps_at_entry") or 0.0)
#                     )
#                     "p0_slippage_bps_est": float(
#                         (getattr(pos, "p0_slippage_bps_est", 0.0) or 0.0)
#                         or ((getattr(pos, "p0_features_snapshot", None) or {}).get("expected_slippage_bps") or 0.0)
#                         or ((getattr(pos, "p0_features_snapshot", None) or {}).get("p0_slippage_bps_est") or 0.0)
#                     )
                    "p0_book_age_ms": int(getattr(pos, "p0_book_age_ms", 0) or 0),
                    # Fees (avoid double-counting in evaluator by config)
                    "fees_usd": float(getattr(closed, "fees", 0.0) or getattr(pos, "fees", 0.0) or 0.0),
                    # Turnover (needed to convert slippage bps -> USD)
                    "turnover_roundtrip": float(getattr(closed, "turnover_roundtrip", 0.0) or 0.0),
                    "of_confirm_ok": int(of_confirm_ok),
                    "of_confirm_score": float(of_confirm_score),
                    "atr_bps_exec": float(atr_bps_exec),
                    "atr_unified_th_bps": float(atr_unified_th_bps),
                    "atr_floor_th_bps": float(atr_floor_th_bps),
                    "atr_fees_th_bps": float(atr_fees_th_bps),
                    "meta_enforce_applied": int(meta_enforce_applied) if meta_enforce_applied is not None else None,
                    # CRITICAL for Stage2 optimization: meta_veto must be written ALWAYS (even when meta_enforce_applied=0)
                    "meta_veto": int(meta_veto),
                    "meta_enforce_key": str(meta_enforce_key),
                    "meta_enforce_salt": str(meta_enforce_salt),
                    **build_horizon_event_scalars(pos),
                },
                extra_payload=extra,
            )
        except Exception:
            pass


def _parse_tp_levels(data: dict) -> list[float]:
    """
    Parse explicit TP levels from signal data.
    Looking for 'tp_levels' list or 'tp1'...'tp9' keys.
    """
    tps = []
    try:
        # 1. Try 'tp_levels' list
        raw_list = data.get("tp_levels")
        if raw_list and isinstance(raw_list, list):
            for x in raw_list:
                with contextlib.suppress(Exception):
                    tps.append(float(x))
            if tps:
                return tps

        # 2. Try tp1..tpN keys
        for i in range(1, 10):
            k = f"tp{i}"
            val = data.get(k)
            if val is not None:
                with contextlib.suppress(Exception):
                    tps.append(float(val))
    except Exception:
        pass
    return tps
