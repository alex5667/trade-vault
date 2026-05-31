# services/trade_monitor_service.py
from __future__ import annotations

import json
import os
import time
import re
import threading
import contextlib
import collections
from dataclasses import dataclass, field
from typing import Callable
from typing import Any, Dict, Optional, List, Set, Tuple, Callable
from concurrent.futures import ThreadPoolExecutor
from prometheus_client import Counter, Gauge, Histogram

try:
    from sortedcontainers import SortedList
    _SORTED_CONTAINERS_AVAILABLE = True
except ImportError:  # pragma: no cover
    _SORTED_CONTAINERS_AVAILABLE = False
    SortedList = list  # type: ignore

from core.redis_client import get_redis
from common.log import setup_logger
from core.redis_keys import RedisStreams as RS
from services.pnl_math import SymbolSpec, spec_from_symbol_info, get_symbol_info

from domain.models import SignalNorm, PositionState, TradeClosed, TradeEvent

# Define logging callback for futures
def _log_future_exception(fut):
    try:
        exc = fut.exception()
        if exc:
            logger.error("Async DB task failed: %s", exc)
    except Exception:
        pass
from domain.normalizers import canon_source, canon_symbol, canon_tf, canon_strategy
from infra.order_schema import (
    normalize_side,
    extract_tp_levels,
    extract_profile,
    extract_tp_fills,
    parse_json_dict,
)
from domain.handlers import create_position, process_tick, apply_trailing_update, finalize_trade
from domain.tick_price import build_tick

# ----------------- Prometheus Metrics (Module Level) -----------------
# We define metrics at the module level to avoid "Duplicated timeseries" error
# when TradeMonitorService is instantiated multiple times (e.g. in Actor Runtime shards).
TM_ORPHANS_FORCE_CLOSED = Counter(
    "orphans_force_closed_total", 
    "Total number of positions force closed by orphan housekeep",
    ["symbol"]
)
TM_OPEN_POSITIONS = Gauge(
    "open_positions_count",
    "Number of currently open positions",
    ["symbol"]
)
TM_TICK_LATENCY_US = Histogram(
    "tick_processing_time_us",
    "Latency of on_tick processing in microseconds",
    ["symbol"],
    buckets=[100, 500, 1000, 5000, 10000, 50000]
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
# Simulated slippage applied to paper entry prices
TM_SIMULATED_SLIPPAGE_BPS = Histogram(
    "tm_simulated_slippage_bps",
    "Simulated slippage applied to paper trade entry prices (bps)",
    ["symbol"],
    buckets=[0, 1, 2, 4, 6, 8, 10, 15, 20],
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
    ["symbol"]
)
# --------------------------------------------------------------------

from infra.redis_repo import RedisTradeRepository
from services import analytics_db
from services.trade_events_logger import TradeEventsLogger
from services.batch_trade_writer import get_batch_writer

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
    events: List[Any] = field(default_factory=list)
    # fast TP-hit persistence (primitives)
    tp_hits: List[Dict[str, Any]] = field(default_factory=list)
    # trailing move/sync persistence (primitives)
    trailing_moves: List[Dict[str, Any]] = field(default_factory=list)
    trailing_syncs: List[Dict[str, Any]] = field(default_factory=list)
    # closed trade persistence
    closed: Optional[Any] = None
    # final cleanup needs these
    close_pos_id: Optional[str] = None
    close_sid: Optional[str] = None
    close_source: Optional[str] = None
    close_symbol: Optional[str] = None
    # stats update uses snapshots (immutable dict copies)
    pos_snapshot: Optional[Dict[str, Any]] = None
    closed_snapshot: Optional[Dict[str, Any]] = None


def _canon_regime(v: Any) -> str:
    """
    Canonical regime label for persistence/segmentation.
    Keep it conservative: lowercased string; empty => "na".
    """
    try:
        s = str(v or "").strip().lower()
    except Exception:
        return "na"
    return s or "na"


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
        ts_ms=int(ts_ms),
        payload={
            "tp_level": int(tp_level),
            "fill_price": float(fill_price),
            "closed_qty": float(closed_qty),
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
        setattr(pos, "entry_regime", regime)
        # alias (many parts of pipeline already look at pos.regime)
        if getattr(pos, "regime", None) in (None, "", "na"):
            setattr(pos, "regime", regime)
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
    h: Dict[str, str],
    *,
    to_int_ms,
    logger=None,
) -> Optional[PositionState]:
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

        pos = PositionState(
            id=str(h.get("id")),
            sid=str(h.get("sid") or ""),
            strategy=str(h.get("strategy") or "unknown"),
            source=str(h.get("source") or "Unknown"),
            symbol=str(h.get("symbol") or "UNKNOWN"),
            tf=str(h.get("tf") or "tick"),
            # direction can come in multiple formats across components; normalize for stability.
            direction=_normalize_side(h.get("direction") or "LONG"),
            entry_price=float(h.get("entry_price") or 0.0),
            entry_ts_ms=to_int_ms(h.get("entry_time"), 0),
            lot=float(h.get("lot") or 0.0),
            remaining_qty=float(h.get("remaining_qty") or h.get("lot") or 0.0),
            sl=float(h.get("sl") or 0.0),
            tp_levels=tp_levels,
            tp_hits=int(float(h.get("tp_hits") or 0)),
            tp1_hit=str(h.get("tp1_hit") or "0") == "1",
            tp2_hit=str(h.get("tp2_hit") or "0") == "1",
            tp3_hit=str(h.get("tp3_hit") or "0") == "1",
            trailing_started=str(h.get("trailing_started") or "0") == "1",
            trailing_active=str(h.get("trailing_active") or "0") == "1",
            trailing_moves_count=int(float(h.get("trailing_moves") or 0)),
            trailing_distance=float(h.get("trailing_distance") or 0.0),
            trailing_point=float(h.get("trailing_point") or 0.0),
            max_favorable_price=float(h.get("max_favorable_price") or 0.0),
            max_favorable_ts=to_int_ms(h.get("max_favorable_ts"), 0),
            atr=float(h.get("atr") or 0.0),
            is_virtual=str(h.get("is_virtual") or "0") == "1",
            v_gate_status=str(h.get("v_gate_status") or "na"),
            v_gate_reason=str(h.get("v_gate_reason") or ""),
        )

        # Optional fields (best-effort)
        try:
            pos.entry_tag = str(h.get("entry_tag") or "")

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
                try:
                    pos.p0_features_snapshot = json.loads(h["p0_features_json"])
                except Exception:
                    pass

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
            pos.trail_profile = str(h.get("trail_profile") or h.get("trailing_profile") or "")

            pos.trailing_min_lock_r = float(h.get("trailing_min_lock_r") or 0.0)
            pos.min_lock_price = float(h.get("min_lock_price") or 0.0)
            pos.baseline_mode = str(h.get("baseline_mode") or pos.baseline_mode)
            pos.baseline_horizon_ms = to_int_ms(h.get("baseline_horizon_ms"), pos.baseline_horizon_ms)
            pos.baseline_sl = float(h.get("baseline_sl") or pos.baseline_sl or pos.sl)
            pos.baseline_tp1 = float(h.get("baseline_tp1") or pos.baseline_tp1 or (pos.tp_levels[0] if pos.tp_levels else 0.0))
            # BUGFIX: baseline_tp2/tp3 must not fallback to baseline_tp1 (typo in old code).
            pos.baseline_tp2 = float(h.get("baseline_tp2") or pos.baseline_tp2 or (pos.tp_levels[1] if len(pos.tp_levels) > 1 else 0.0))
            pos.baseline_tp3 = float(h.get("baseline_tp3") or pos.baseline_tp3 or (pos.tp_levels[2] if len(pos.tp_levels) > 2 else 0.0))

            # P41 compliance (native meta)
            pos.meta_enforce_cov_bucket = str(h.get("meta_enforce_cov_bucket") or "")
            if h.get("meta_enforce_applied"):
                try:
                    pos.meta_enforce_applied = int(float(h["meta_enforce_applied"]))
                except (ValueError, TypeError):
                    pos.meta_enforce_applied = -1
        except Exception:
            pass

        return pos
    except Exception as e:
        if logger:
            logger.warning(f"Failed to recover position from hash: {e}")
        return None


class TradeMonitorService:
    def __init__(
        self,
        redis_url: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        regime_guard=None,
        health_metrics=None,
        *,
        redis_client=None,
        repo=None,
        metrics=None,
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
        
        # Trade Events Logger for AB/Backtest
        try:
            self.events_logger = TradeEventsLogger(self.redis_url if redis_url else None)
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
        self._symbol_locks: Dict[str, threading.RLock] = {}

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
        self.shards: Dict[str, Dict[str, PositionState]] = collections.defaultdict(dict)
        self.symbol_by_pos_id: Dict[str, str] = {} # PosID -> Symbol mapping

        # SortedList price index для O(log N) pre-filter:
        # _sl_index[symbol] = SortedList[(sl_price, pos_id)]
        # _tp_index[symbol] = SortedList[(tp_price, pos_id)]
        # Включается через TM_PRICE_INDEX_ENABLED=1 (default: 0 until tested in prod)
        self._price_index_enabled = os.getenv("TM_PRICE_INDEX_ENABLED", "0") == "1" and _SORTED_CONTAINERS_AVAILABLE
        self._sl_index: Dict[str, Any] = {}  # symbol -> SortedList[(sl, id)]
        self._tp_index: Dict[str, Any] = {}  # symbol -> SortedList[(tp, id)]

        # Основные структуры данных (self.open_positions is kept as flat index for PosID -> Object)
        self.open_positions: Dict[str, PositionState] = {}
        self.pos_by_sid: Dict[str, str] = {}
        self.open_by_symbol: Dict[str, Set[str]] = {}
        self._last_price_by_symbol: Dict[str, Tuple[int, float]] = {}

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
        self._last_housekeep_by_symbol: Dict[str, int] = {}
        # Orphan TTL (ms). If your old code already had this attribute, it will be overwritten only if missing.
        if not hasattr(self, "_orphan_ttl_ms"):
            self._orphan_ttl_ms = int(os.getenv("TM_ORPHAN_TTL_MS", "120000"))

        # Optional Orphan Timeout (default: OFF to rely on TP/SL)
        self.orphan_timeout_enabled = os.getenv("TM_ORPHAN_TIMEOUT_ENABLED", "0") == "1"

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
        self.tm_tick_latency_us = TM_TICK_LATENCY_US
        
        # [NEW] Backpressure metrics
        self.tm_rg_persist_pending = TM_RG_PERSIST_PENDING
        self.tm_rg_persist_dropped = TM_RG_PERSIST_DROPPED
        self.tm_rg_persist_submitted = TM_RG_PERSIST_SUBMITTED
        self.tm_rg_persist_failed = TM_RG_PERSIST_FAILED
        
        self.stop_atr_mult = float(mon.get("stop_atr_mult", 1.0))
        self.rr_levels = mon.get("rr_levels", [1.0, 2.0, 3.0])
        self.fill_policy = str(mon.get("fill_policy", "level")).strip().lower()

        # Shadow Analytics Config
        # Global confidence threshold (single source of truth)
        self.shadow_conf_threshold = float(os.getenv("CRYPTO_SIGNAL_MIN_CONF", "70"))
        self._open_log_counter = 0

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
        self._last_price_by_symbol: Dict[str, Tuple[int, float]] = {}

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
        #   Default empty — XAUUSD is already excluded by suffix check.
        #   Use only if you have a non-crypto symbol that ends with USDT/USDC/BUSD.
        # MARGIN_FX_SYMBOLS: comma-separated explicit symbols for margin-FX sizing
        #   e.g. "XAUUSD,XAGUSD"
        _suf_raw = os.getenv("CRYPTO_SUFFIXES", "USDT,USDC,BUSD")
        self._crypto_suffixes: tuple[str, ...] = tuple(
            s.strip().upper() for s in _suf_raw.split(",") if s.strip()
        )
        _excl_raw = os.getenv("CRYPTO_EXCLUDE_PREFIXES", "")
        self._crypto_exclude_prefixes: tuple[str, ...] = tuple(
            s.strip().upper() for s in _excl_raw.split(",") if s.strip()
        )
        _mfx_raw = os.getenv("MARGIN_FX_SYMBOLS", "XAUUSD,XAGUSD")
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
        }

        # Health snapshot cache
        self._health_cache: Dict[str, Tuple[int, Dict[str, str]]] = {}
        self._health_cache_ttl_ms = int(os.getenv("HEALTH_CACHE_TTL_MS", "30000"))

        # Paper vs Demo comparison report
        self._pvd_report_every_n: int = int(os.getenv("TM_PAPER_VS_DEMO_REPORT_EVERY_N", "10"))
        self._pvd_notify_stream: str = os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM)
        self._pvd_demo_stream: str = os.getenv("EXEC_STREAM", RS.ORDERS_EXEC)
        self._pvd_session_closed: int = 0
        self._pvd_recent_closed: List[Dict[str, Any]] = []  # circular buffer of last N closed trades
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

        # Housekeep on start
        try:
            now_ms = int(time.time() * 1000)
            self._housekeep_expired_positions(now_ms)
        except Exception:
            pass

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
    def _m_inc(self, name: str, value: int = 1, tags: Optional[Dict[str, Any]] = None) -> None:
        m = getattr(self, "_metrics", None)
        if not m:
            return
        try:
            m.inc(name, int(value), tags)
        except Exception:
            return

    def _m_obs(self, name: str, value: float, tags: Optional[Dict[str, Any]] = None) -> None:
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
            except Exception:
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

        last_ms = self._pos_last_ts_ms(pos)
        ttl = int(getattr(self, "_orphan_ttl_ms", 120000))
        return (last_ms > 0) and ((now_ms - last_ms) >= ttl)

    def _get_health_snapshot(self, symbol: str) -> Dict[str, Any]:
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
        s = str(tf).strip().lower()
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
                        self._sl_index[sym] = SortedList(key=lambda x: x[0])
                    self._sl_index[sym].add((sl_price, pos.id))

                tp_price = float(pos.tp_levels[0]) if pos.tp_levels else 0.0
                if tp_price > 0:
                    if sym not in self._tp_index:
                        self._tp_index[sym] = SortedList(key=lambda x: x[0])
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
                    try:
                        self._sl_index[sym].discard((sl_price, pos.id))
                    except (AttributeError, ValueError):
                        # SortedList.discard не всегда есть; remove игнорируем
                        pass
                    if not self._sl_index[sym]:
                        self._sl_index.pop(sym, None)

                tp_price = float(pos.tp_levels[0]) if pos.tp_levels else 0.0
                if tp_price > 0 and sym in self._tp_index:
                    try:
                        self._tp_index[sym].discard((tp_price, pos.id))
                    except (AttributeError, ValueError):
                        pass
                    if not self._tp_index[sym]:
                        self._tp_index.pop(sym, None)
            except Exception:
                pass  # fail-open

    def _get_pos(self, pos_id: str, symbol: Optional[str] = None) -> Optional[PositionState]:
        """Возвращает позицию по ID (опционально по символу для скорости)."""
        if symbol:
            return self.shards.get(symbol, {}).get(pos_id)
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
                try:
                    sl_idx.discard((old_sl, pos.id))
                except (AttributeError, ValueError):
                    pass
            if new_sl > 0:
                if sym not in self._sl_index:
                    self._sl_index[sym] = SortedList(key=lambda x: x[0])
                self._sl_index[sym].add((new_sl, pos.id))
        except Exception:
            pass

    def _collect_candidate_pos_ids(self, symbol: str, mid: float) -> Optional[Set[str]]:
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

        candidates: Set[str] = set()
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
                for sl_price, pos_id in sl_idx.irange((lo,), (hi,), inclusive=(True, True)):
                    candidates.add(pos_id)

            # TP кандидаты: аналогично
            tp_idx = self._tp_index.get(symbol)
            if tp_idx:
                lo = mid * 0.98
                hi = mid * 1.02
                for tp_price, pos_id in tp_idx.irange((lo,), (hi,), inclusive=(True, True)):
                    candidates.add(pos_id)

        except Exception:
            # На любой ошибке возвращаем None → caller использует full list (safe fallback)
            return None

        return candidates

    def _pop_pos(self, pos_id: str) -> Optional[PositionState]:
        """Атомарно удаляет позицию из всех индексов и шардов. Вызывать под self._lock."""
        pos = self.open_positions.pop(pos_id, None)
        if pos:
            if getattr(pos, "sid", ""):
                self.pos_by_sid.pop(pos.sid, None)
            self._index_remove(pos)
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

    def _dedup_acquire(self, kind: str, event_id: Optional[str]) -> bool:
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
            return bool(result)
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

    def _get_health_snapshot_with_timestamp(self, symbol: str, now_ms: int) -> Dict[str, Any]:
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
        cached = self._health_snapshot_cache.get(symbol)
        if cached:
            ts_ms, snap = cached
            if now_ms - ts_ms <= self._health_snapshot_ttl_ms:
                return snap

        try:
            # Используем тот же redis client (decode_responses=True).
            health_snapshot_key = f"orderflow:{symbol}:health_snapshot"
            pipe = self.redis.pipeline()
            pipe.hgetall(health_snapshot_key)
            pipe.get(f"orderflow:{symbol}:signal_emit_rate")
            pipe.get(f"orderflow:{symbol}:dlq_rate")
            h, signal_emit_rate, dlq_rate = pipe.execute()

            out: Dict[str, Any] = {}
            if h:
                out["health_l2_stale_ratio_tick"] = h.get("l2_stale_ratio_tick", "0.0")
                out["health_l2_stale_ratio_now"] = h.get("l2_stale_ratio_now", "0.0")
                out["health_avg_l2_age_ms"] = h.get("avg_l2_age_ms", "0.0")
                out["health_avg_l2_age_tick_ms"] = h.get("avg_l2_age_tick_ms", "0.0")
            out["health_signal_emit_rate"] = signal_emit_rate or "0.0"
            out["health_dlq_rate"] = dlq_rate or "0.0"

            self._health_snapshot_cache[symbol] = (now_ms, out)
            return out
        except Exception:
            return {}

    def _get_health_snapshot_for_trade(self, symbol: str) -> Dict[str, str]:
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
                "health_l2_stale_ratio_tick": str(h.get("l2_stale_ratio_tick", "0.0")),
                "health_l2_stale_ratio_now": str(h.get("l2_stale_ratio_now", "0.0")),
                "health_avg_l2_age_ms": str(h.get("avg_l2_age_ms", "0.0")),
                "health_avg_l2_age_tick_ms": str(h.get("avg_l2_age_tick_ms", "0.0")),
                "health_signal_emit_rate": str(h.get("signal_emit_rate", "0.0")),
                "health_dlq_rate": str(h.get("dlq_rate", "0.0")),
                "health_avg_book_lag_ms": str(h.get("avg_book_lag_ms", "0.0")),
                "health_avg_ticks_lag_ms": str(h.get("avg_ticks_lag_ms", "0.0")),
                "health_pending_len": str(h.get("pending_len", "0")),
                "health_window_sec": str(h.get("window_sec", "0")),
                "health_ts": str(h.get("ts", "0")),
            }
            return out
        except Exception:
            return {}

    # --------------------
    # Recovery
    # --------------------
    def _recover_open_positions(self) -> None:
        """Восстанавливает открытые позиции из Redis с заполнением индекса."""
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

    def _position_from_hash(self, h: Dict[str, str]) -> Optional[PositionState]:
        try:
            if h.get("status") != "open":
                return None

            tp_levels = extract_tp_levels(h)

            pos = PositionState(
                id=str(h.get("id")),
                sid=str(h.get("sid") or ""),
                strategy=str(h.get("strategy") or "unknown"),
                source=str(h.get("source") or "Unknown"),
                symbol=str(h.get("symbol") or "UNKNOWN"),
                tf=str(h.get("tf") or "tick"),
                direction=normalize_side(h.get("direction") or "LONG"),
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
                tp1_hit=str(h.get("tp1_hit") or "0") == "1",
                tp2_hit=str(h.get("tp2_hit") or "0") == "1",
                tp3_hit=str(h.get("tp3_hit") or "0") == "1",
                trailing_started=str(h.get("trailing_started") or "0") == "1",
                trailing_active=str(h.get("trailing_active") or "0") == "1",
                trailing_moves_count=int(float(h.get("trailing_moves") or 0)),
                trailing_distance=float(h.get("trailing_distance") or 0.0),
                trailing_point=float(h.get("trailing_point") or 0.0),
                max_favorable_price=float(h.get("max_favorable_price") or 0.0),
                max_favorable_ts=self._to_int_ms(h.get("max_favorable_ts"), 0),
                atr=float(h.get("atr") or 0.0),
            )
            try:
                pos.entry_tag = str(h.get("entry_tag") or "")
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
            return pos
        except Exception as e:
            self.logger.warning(f"Failed to recover position from hash: {e}")
            return None

    def _get_health_snapshot_cached(self, symbol: str) -> Dict[str, str]:
        """
        Fetch orderflow:{symbol}:health_snapshot via existing redis client.
        Small TTL cache to avoid bursts when multiple closes happen back-to-back.
        """
        now_ms = int(time.time() * 1000)
        sym = str(symbol or "UNKNOWN")
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
            "health_l2_stale_ratio_tick": str(raw.get("l2_stale_ratio_tick", "0.0")),
            "health_l2_stale_ratio_now": str(raw.get("l2_stale_ratio_now", "0.0")),
            "health_avg_l2_age_ms": str(raw.get("avg_l2_age_ms", "0.0")),
            "health_avg_l2_age_tick_ms": str(raw.get("avg_l2_age_tick_ms", "0.0")),
            "health_signal_emit_rate": str(raw.get("signal_emit_rate", "0.0")),
            "health_dlq_rate": str(raw.get("dlq_rate", "0.0")),
            "health_pending_len": str(raw.get("pending_len", "0")),
            "health_snapshot_ts": str(raw.get("ts", "0")),
            "health_window_sec": str(raw.get("window_sec", "0")),
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
                setattr(closed, "_health_snapshot", snap)
        except Exception:
            pass

    def _get_health_snapshot_prefixed(self, symbol: str, now_ms: int) -> Dict[str, str]:
        """
        Fetches last health snapshot from Redis and returns a FLAT dict with stable 'health_*' keys.
        Cached for a short TTL to avoid bursts.
        """
        sym = str(symbol or "UNKNOWN")
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
            "health_l2_stale_ratio_tick": str(raw.get("l2_stale_ratio_tick", "0.0")),
            "health_l2_stale_ratio_now": str(raw.get("l2_stale_ratio_now", "0.0")),
            "health_avg_l2_age_ms": str(raw.get("avg_l2_age_ms", "0.0")),
            "health_avg_l2_age_tick_ms": str(raw.get("avg_l2_age_tick_ms", "0.0")),
            "health_signal_emit_rate": str(raw.get("signal_emit_rate", "0.0")),
            "health_dlq_rate": str(raw.get("dlq_rate", "0.0")),
            "health_pending_len": str(raw.get("pending_len", "0")),
            "health_snapshot_ts": str(raw.get("ts", "0")),
            "health_window_sec": str(raw.get("window_sec", "0")),
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
                setattr(closed, "_health_snapshot", snap)
        except Exception:
            pass


    def _get_symbol_lock(self, symbol: str) -> threading.Lock:
        sym = str(symbol or "").upper()
        with self._symbol_locks_guard:
            lk = self._symbol_locks.get(sym)
            if lk is None:
                lk = threading.Lock()
                self._symbol_locks[sym] = lk
            return lk

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

    def _peek_pos_and_symbol_by_sid(self, sid: str) -> tuple[Optional[str], Optional[str]]:
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

    def _run_io_tasks(self, tasks: List["_IOTask"]) -> None:
        for t in tasks:
            try:
                t.fn()
            except Exception as e:
                logger.warning("⚠️ IO task failed: %s (%s)", t.desc, e)

    def _stamp_closed_trade_meta(self, pos: PositionState, closed: TradeClosed, close_reason_raw: str) -> None:
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

    def _update_stats_from_dicts(self, pos_dict: Dict[str, Any], closed_dict: Dict[str, Any]) -> None:
        """
        Обновление stats без зависимости от живого pos объекта (который уже удалён из памяти).
        """
        try:
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
                class DummyPos: pass
                dpos = DummyPos(); dpos.__dict__.update(pos_dict)
                class DummyClosed: pass
                dclosed = DummyClosed(); dclosed.__dict__.update(closed_dict)
                
                r_value = self._calculate_r_value(dpos, dclosed)
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
                        persist_task, 
                        tags={"family": family, "venue": venue}
                    )
            except Exception as e:
                self.logger.warning("regime guard update failed: %s", e)

    def _persist_closed_trade_io(self, closed: TradeClosed, pos_dict: Dict[str, Any], closed_dict: Dict[str, Any]) -> None:
        """
        Единая точка записи close (repo + analytics + stats).
        ВАЖНО: вызывать только вне self._lock.
        """
        # health snapshot (opt-in)
        if getattr(self, "_attach_health_on_close", False):
            try:
                now_ms = int(time.time() * 1000)
                snap = self._get_health_snapshot_prefixed(str(getattr(closed, "symbol", "")), now_ms)
                if snap:
                    try:
                        setattr(closed, "_health_snapshot", snap)
                    except Exception:
                        pass
            except Exception:
                pass

        hs: Dict[str, str] = {}
        try:
            hs = self._get_health_snapshot_for_trade(str(getattr(closed, "symbol", "")))
        except Exception:
            hs = {}

        self.repo.save_closed(closed, health_snapshot=hs)
        try:
            # Offload blocking DB write to background thread
            fut = self._db_executor.submit(analytics_db.save_trade_closed, closed)
            fut.add_done_callback(_log_future_exception)
        except Exception as e:
            logger.warning("Failed to submit trade to analytics DB: %s", e)

        # Signal → Outcome pipeline (fail-open, background thread)
        try:
            from domain.signal_outcome import from_trade_closed as _build_outcome
            from services.signal_outcome_writer import get_signal_outcome_writer
            _outcome = _build_outcome(closed)
            if _outcome is not None:
                fut_o = self._db_executor.submit(get_signal_outcome_writer().emit, _outcome)
                fut_o.add_done_callback(_log_future_exception)
        except Exception as _so_err:
            logger.warning("⚠️ signal_outcome emit failed in _persist_closed_trade_io (fail-open): %s", _so_err)

        self._update_stats_from_dicts(pos_dict, closed_dict)

    def _peek_pos_and_symbol_by_sid(self, sid: str) -> tuple[Optional[str], Optional[str]]:
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
                if isinstance(val, bool):
                    return bool(val)
                elif isinstance(val, (int, float)):
                    return bool(val)
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

        Приоритет (SymbolSpec имеет ВЫСШИЙ ПРИОРИТЕТ):
        1) SymbolSpec.trailing_tp1_offset_atr (из Redis-конфига по символу) - ВЫСШИЙ ПРИОРИТЕТ
        2) ENV: TRAILING_TP1_OFFSET_ATR_<SYMBOL> (используется только если SymbolSpec не задан)
        3) ENV: TRAILING_TP1_OFFSET_ATR_<SOURCE>
        4) Глобальный TRAILING_TP1_OFFSET_ATR
        """
        symbol_up = (pos.symbol or "").upper()
        source_norm = canon_source(pos.source or "")

        # 1) spec override (конфиг по символу — результат калибратора, ВЫСШИЙ ПРИОРИТЕТ)
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
    def _pvd_record_closed(self, pos: "PositionState", closed: "TradeClosed") -> None:
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
        demo_by_sid: Dict[str, Dict[str, Any]] = {}
        try:
            demo_raw = self.redis.xrevrange(
                self._pvd_demo_stream, count=self._pvd_report_every_n * 10
            )
            for mid, data in (demo_raw or []):
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
        demo_lev_map: Dict[str, int] = {}
        try:
            raw_lev = self.redis.hgetall("exec:leverage:actual")
            for k, v in (raw_lev or {}).items():
                sym = k.decode() if isinstance(k, bytes) else str(k)
                val = v.decode() if isinstance(v, bytes) else str(v)
                try:
                    demo_lev_map[sym.upper()] = int(float(val))
                except Exception:
                    pass
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
        pos: "PositionState",
        new_sl: float,
        prev_sl: float,
        ts_ms: int,
    ) -> None:
        """Emit trailing event to unified audit stream for paper-vs-real comparison."""
        if not self._trailing_audit_stream:
            return
        try:
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
        except Exception:
            pass  # fail-open: never break trade execution

    # --------------------
    # Single-active-position guard (read-only check for trade_monitor)
    # --------------------
    def _tm_check_single_active_guard(self, sig: "SignalNorm") -> bool:
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
            guard_status = str(doc.get("guard_status") or "active").lower()
            if guard_status in ("released", "tombstone"):
                return False
            blocked_sid = str(doc.get("sid") or "").strip()
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
                    age_ms = int(time.time() * 1000) - updated_ms
                    if age_ms > self._guard_stale_timeout_ms:
                        logger.warning(
                            "⚠️ [GUARD] Stale guard for %s (age=%ds > stale=%ds) — bypassing",
                            symbol, age_ms // 1000, self._guard_stale_timeout_ms // 1000,
                        )
                        try:
                            TM_SIGNAL_GUARD_STALE_BYPASS.labels(symbol=symbol).inc()
                        except Exception:
                            pass
                        return False  # stale → pass-through
            return True
        except Exception:
            return False  # fail-open: never block on Redis error

    # --------------------
    # Signal → open position
    # --------------------
    def on_signal(self, raw_signal: Dict[str, Any]) -> Optional[str]:
        """
        Обрабатывает сигнал и открывает позицию (thread-safe, idempotent).
        """
        sig = self._normalize_signal(raw_signal)
        if not sig:
            return None

        # --- Strict DTO Versioning (v: 1) ---
        # Any signal without 'v: 1' is considered legacy or malformed and must be rejected.
        # [FIXED] v: 1 is inside the nested 'data' JSON, so we must read from sig.payload
        sig_v = int(sig.payload.get("v") or 0)
        if sig_v != 1:
            symbol_up = str(sig.symbol or "UNKNOWN").upper()
            logger.warning("🚫 Signal REJECTED: version mismatch (expected v: 1, got %d) symbol=%s sid=%s", 
                           sig_v, symbol_up, sig.sid)
            try:
                TM_SIGNAL_VERSION_MISMATCH.labels(symbol=symbol_up).inc()
            except Exception:
                pass
            return None

        # Check if it's a real entry from policy vs a raw signal
        is_policy_entry = (str(sig.source or "").lower() == "smt_entry_policy")
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
            symbol_up = str(sig.symbol or "").upper()
            logger.warning(
                "⏭️ [GUARD] Signal blocked by single_active_position_per_symbol: "
                "symbol=%s sid=%s is_virtual=%s",
                symbol_up, sig.sid, sig.payload.get("is_virtual", 0),
            )
            try:
                TM_SIGNAL_BLOCKED_SINGLE_ACTIVE.labels(symbol=symbol_up).inc()
            except Exception:
                pass
            return None

        # ── Simulated slippage for paper trades (Point 6) ──
        # Shifts entry_price adversely to simulate real-world fill slippage.
        # LONG → entry moves UP (worse fill); SHORT → entry moves DOWN (worse fill).
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
                    try:
                        TM_SIMULATED_SLIPPAGE_BPS.labels(
                            symbol=str(sig.symbol or "").upper()
                        ).observe(self._simulated_slippage_bps)
                    except Exception:
                        pass
            except Exception:
                pass

        # Prepare state (in-memory)
        with self._lock:
            # фантом-дедуп: если sid уже mapped в открытых позициях → не открываем второй раз
            if sig.sid and sig.sid in self.pos_by_sid:
                pos_id = self.pos_by_sid[sig.sid]
                pos = self.open_positions.get(pos_id)
                # No upgrade logic - everything stays virtual
                logger.warning("⏭️ Duplicate signal ignored (sid=%s already open)", sig.sid)
                return pos_id

            # ✅ Глобальный sid-dedup для lossless reprocessing
            if sig.sid and not self._sid_claim(sig.sid, ttl_sec=30):
                logger.warning("⏭️ Duplicate signal ignored (sid=%s already processed globally)", sig.sid)
                return None

            # ✅ Per-symbol guard: 1 symbol = 1 open position (in-memory)
            # When EXEC_SINGLE_ACTIVE_POSITION_PER_SYMBOL=1, block if symbol
            # already has open position(s) in the in-memory index.
            if self.exec_single_active_position_per_symbol:
                sym_up = str(sig.symbol or "").upper()
                existing = self.open_by_symbol.get(sym_up)
                if existing:
                    existing_pid = next(iter(existing), "?")
                    logger.debug(
                        "⏭️ Signal blocked by per-symbol guard "
                        "(symbol=%s, sid=%s, existing_pos=%s)",
                        sym_up, sig.sid, existing_pid,
                    )
                    try:
                        TM_SIGNAL_BLOCKED_SINGLE_ACTIVE.labels(symbol=sym_up).inc()
                    except Exception:
                        pass
                    # Release sid claim so signal can be retried when guard clears
                    self._sid_release(sig.sid)
                    return None

            spec = self._get_spec(sig.symbol)
            pos = create_position(sig, spec)

            # Inherit is_virtual from payload or determine via shadow mode
            is_v = bool(int(sig.payload.get("is_virtual", 0) or 0))
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
                rg = getattr(sig, "entry_regime", None) or getattr(sig, "regime", None) or (raw_signal.get("entry_regime") if isinstance(raw_signal, dict) else None) or (raw_signal.get("regime") if isinstance(raw_signal, dict) else None)
                if rg is not None and not getattr(pos, "entry_regime", None):
                    setattr(pos, "entry_regime", str(rg))
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
                sp["ab_arm"] = str(payload.get("ab_arm", sp.get("ab_arm", "A")) or "A").upper()
                sp["ab_group"] = str(payload.get("ab_group", sp.get("ab_group", "default")) or "default")
                sp["ab_key"] = str(payload.get("ab_key", sp.get("ab_key", "")) or "")
                sp["arm_ver"] = int(payload.get("arm_ver", sp.get("arm_ver", 0)) or 0)

                # Context for winner slicing
                ctx = payload.get("ctx") if isinstance(payload.get("ctx"), dict) else {}
                # Also try top-level regime/zone_id from payload if not in ctx
                sp["regime"] = str(ctx.get("regime", getattr(sig, "regime", None)) or "na").lower()
                sp["zone_id"] = str(ctx.get("zone_id", getattr(sig, "zone_id", None)) or "")

                # Conditional trailing logic
                v = payload.get("trail_after_tp1", True)
                pos.trail_after_tp1 = bool(v)
                rr = payload.get("trail_after_tp1_reason", "")
                if rr:
                    pos.trail_after_tp1_reason = str(rr)[:256]
            except Exception:
                pass

            self.open_positions[pos.id] = pos
            if pos.sid:
                self.pos_by_sid[pos.sid] = pos.id
            self._index_add(pos)

        # ✅ PERSIST/OPEN OUTSIDE LOCK (split-lock architecture)
        # Includes rollback if critical I/O fails to avoid zombies.
        try:
            self.repo.persist_signal(sig)
            self.repo.save_open(pos)
            self._sid_finalize(sig.sid, ttl_days=7)
            
            # event OPEN (also IO)
            self.repo.append_event(ev=_ev_open(pos))
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

    def on_audit(self, audit_data: Dict[str, Any]) -> None:
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

    def process_signal(self, raw: Dict[str, Any]) -> Optional[str]:
        """
        Алиас для on_signal() для обратной совместимости.
        Обрабатывает сигнал и открывает позицию.
        """
        return self.on_signal(raw)

    def _normalize_signal(self, raw: Dict[str, Any]) -> Optional[SignalNorm]:
        try:
            data = raw
            if "data" in raw and isinstance(raw["data"], str):
                try:
                    data = json.loads(raw["data"])
                except Exception:
                    data = raw

            sid = str(data.get("sid") or data.get("signal_id") or "")
            symbol = canon_symbol(data.get("symbol") or "XAUUSD")
            
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
            if direction not in ("LONG", "SHORT"):
                return None

            entry_price = float(data.get("entry") or data.get("price") or 0.0)
            if entry_price <= 0:
                return None

            ts_raw = data.get("ts") or data.get("timestamp") or int(time.time() * 1000)
            # STRICT anti-regression:
            #   - Reject non-epoch clocks (minutes-of-day, counters) by forcing 0.
            #   - If 0 => downstream behavior stays fail-open (fallback to now happens above).
            try:
                from domain.time_utils import normalize_ts_ms_hard
                now_ms = int(time.time() * 1000)
                entry_ts_ms = normalize_ts_ms_hard(int(float(ts_raw)) if ts_raw else 0, now_ms=now_ms)
            except Exception:
                from domain.time_utils import normalize_ts_ms
                entry_ts_ms = normalize_ts_ms(int(float(ts_raw)) if ts_raw else 0)
            # HARDER: if ts provided but invalid, correct to now and mark for audit.
            # This prevents "entry_ts_ms=0" silently leaking into downstream (duration, session, gates).
            if entry_ts_ms <= 0:
                now_ms = int(time.time() * 1000)
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
            if entry_ts_ms > now_ms:
                if self.logger:
                    self.logger.warning(f"⚠️ Future entry timestamp detected: {entry_ts_ms} > {now_ms} (skew={entry_ts_ms - now_ms}ms). Clamping to now.")
                entry_ts_ms = now_ms

            signal_lot = float(data.get("lot") or self.default_lot)

            atr = float(data.get("atr") or 0.0)

            sl = float(data.get("sl") or 0.0)
            tp_levels = _parse_tp_levels(data)

            # fallback SL/TP если нет
            if sl <= 0 or len(tp_levels) < 3:
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

                if len(tp_levels) < 3:
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
                    logger.info(
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
                    logger.info(
                        f"⚠️ Margin-based sizing (fallback): {symbol} "
                        f"margin=${position_size_usd:.2f}, lev={leverage_env:.0f}x, "
                        f"notional=${notional_usd:.2f}, entry=${entry_price:.2f}, lot={lot:.6f}"
                    )
            else:
                # Для остальных инструментов используем lot из сигнала
                lot = signal_lot

            # ✅ Применяем дефолты из SymbolSpec для trailing параметров
            source_norm = source  # canon_source уже применен выше
            
            # 1) trailing_profile
            trail_profile = str(data.get("trail_profile") or "")
            if not trail_profile:
                # Если в сигнале нет trail_profile, берем из spec
                default_profile = getattr(spec, "trailing_profile_default", "") or ""
                if default_profile:
                    trail_profile = default_profile
                    # Можно добавить проверку source_norm == "CryptoOrderFlow" если нужно
                    data["trail_profile"] = trail_profile
            
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
            )
        except Exception as e:
            logger.error(f"Error normalizing signal: {e}", exc_info=True)
            return None

    # --------------------
    # Orphan housekeeping (вошли, но не вышли)
    # --------------------
    
    def _collect_orphan_closures(self, now_ms: int) -> List[Tuple[PositionState, float, int, str]]:
        """
        Собирает orphan-позиции для закрытия.
        Важно: внутри lock сразу удаляем позиции из памяти/индексов, чтобы исключить зависание/двойную обработку.

        Возвращает список кортежей:
          (pos, exit_price, exit_ts_ms, close_reason_raw)
        """
        closures: List[Tuple[PositionState, float, int, str]] = []

        if not self._is_plausible_epoch_ms(int(now_ms)):
            return closures

        with self._lock:
            # Snapshot по ids (не по symbol), потому что orphan может быть по любому символу.
            for pos_id, pos in list(self.open_positions.items()):
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
                                if atr > 0:
                                    # Calc current drawdown in ATR units
                                    if direction == "LONG":
                                        adverse_dist = entry_px - last_px
                                    else:
                                        adverse_dist = last_px - entry_px
                                    
                                    if adverse_dist > (atr * param_max_mae_atr):
                                        is_risky = True
                                
                                # Rule: TIMEOUT Allowed IF (Profitable OR Risky)
                                # Invert: If (Not Profitable AND Not Risky) -> SKIP Timeout (Hold)
                                if not is_profitable_exit and not is_risky:
                                    # "Wins are made by timer" hypothesis check: we HOLD instead of closing.
                                    continue

                    # Вычисляем exit_price: по последней цене, иначе по entry_price (нулевой pnl).
                    sym = str(getattr(pos, "symbol", "") or "")
                    last = self._last_price_by_symbol.get(sym)
                    if last and float(last[1]) > 0:
                        last_ts, last_px = int(last[0]), float(last[1])

                        # Защита от использования устаревшей цены (фид умер, но цена осталась в dict)
                        max_age = int(self._orphan_max_last_price_age_ms)
                        if max_age > 0 and (int(now_ms) - last_ts) > max_age:
                            exit_price = float(getattr(pos, "entry_price", 0.0) or 0.0)
                            close_reason_raw = "ORPHAN_TIMEOUT_STALE_PRICE"
                        else:
                            exit_price = last_px
                            close_reason_raw = "ORPHAN_TIMEOUT"
                    else:
                        exit_price = float(getattr(pos, "entry_price", 0.0) or 0.0)
                        close_reason_raw = "ORPHAN_TIMEOUT_NO_PRICE"

                    # Сразу удаляем из памяти/индексов, чтобы позиция не "жила вечно"
                    with self._lock:
                        self._pop_pos(pos.id)

                    # помечаем как закрытую в рантайме (чисто защитно)
                    try:
                        pos.closed = True
                    except Exception:
                        pass

                    closures.append((pos, exit_price, int(now_ms), close_reason_raw))
                except Exception:
                    continue

        return closures
    
    def _finalize_orphan_closures(self, closures: List[Tuple[PositionState, float, int, str]]) -> None:
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
                    tp_ratios=effective_tp_ratios
                )
                self._log_ab_closed_event(pos, closed, str(close_reason_raw))

                # ---------------------------------------------------------------------
                # NEW: persist time-bucket snapshots into TradeClosed event so that
                # StatsAggregator can push them into statsbuf:*:mfe_bps_t{bucket} lists.
                # Fail-open (never breaks closing).
                # ---------------------------------------------------------------------
                try:
                    attach_timebucket_snapshots_to_closed(pos, closed)
                except Exception:
                    pass

                # Явно помечаем "почему" — чтобы фильтровать в репортах/аналитике
                try:
                    closed.close_reason_detail = str(close_reason_raw)
                except Exception:
                    pass

                # FIX(#9): health snapshot добавляем здесь (в сервисе), а не внутри RedisTradeRepository.save_closed().
                # Это позволяет:
                #  - использовать in-memory API HealthMetrics, если есть;
                #  - кэшировать Redis HGETALL и не бить Redis на каждую сделку;
                #  - полностью выключить добавление health-полей через ENV при необходимости.
                if self._attach_health_on_close:
                    try:
                        now_ms = int(time.time() * 1000)
                        setattr(closed, "_health_snapshot", self._get_health_snapshot_prefixed(closed.symbol, now_ms))
                    except Exception:
                        pass

                # NEW: передаём health snapshot без создания новых HealthMetrics/коннектов.
                hs = {}
                try:
                    hs = self._get_health_snapshot_for_trade(str(closed.symbol))
                except Exception:
                    hs = {}
                self.repo.save_closed(closed, health_snapshot=hs)
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

                report_triggers.append((pos.source, pos.symbol, pos.id, getattr(pos, "is_virtual", False)))
            except Exception as e:
                logger.warning("⚠️ Orphan forced-close failed: %s", e)

        # триггер отчётов вне lock
        for trigger in report_triggers:
            try:
                from services.periodic_reporter import check_and_trigger_report
                src, sym, oid = trigger[0], trigger[1], trigger[2]
                check_and_trigger_report(src, sym, counter_type="trades", order_id=oid)
            except Exception as e:
                logger.warning("Error triggering report (orphan close): %s", e)

    
    def _cleanup_stale_prices(self, ttl_ms: int = 3600000) -> None:
        """
        Recommendation 3: Fix memory leak in self._last_price_by_symbol.
        Removes prices older than ttl_ms (default 1 hour).
        """
        now = time.time() * 1000
        with self._lock:
            to_delete = [
                sym for sym, (ts, _) in self._last_price_by_symbol.items() 
                if now - ts > ttl_ms
            ]
            for sym in to_delete:
                del self._last_price_by_symbol[sym]
            
            if to_delete:
                self.logger.info(f"🧹 Cleaned up {len(to_delete)} stale prices from cache")

    def _housekeep_expired_positions(self, now_ms: int, current_symbol: Optional[str] = None) -> None:
        """
        Оптимизированная версия: 
        1. Если есть current_symbol -> проверяем только шард этого символа (O(1) lookup).
        2. Глобальная очистка -> проверяем все шарды (O(N) total), но с троттлингом.
        """
        by_sym: Dict[str, List[str]] = {}

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

        report_triggers: List[tuple[str, str]] = []

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
                    def _manual_lock():
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

                io_tasks: List[_IOTask] = []
                local_triggers: List[tuple[str, str]] = []

                with self._lock:

                    # get last price for forced exit
                    lp = self._last_price_by_symbol.get(sym)
                    raw = "ORPHAN_FORCED_CLOSE"
                    
                    if lp:
                        exit_ts_ms, exit_price = int(lp[0]), float(lp[1])
                        # Проверяем на "протухание" цены (защита от forced-close по цене часовой давности)
                        if (now_ms - exit_ts_ms) > self._orphan_max_last_price_age_ms:
                            logger.info(f"⚠️ Stale price for {sym} ({now_ms - exit_ts_ms}ms old), using entry_price for orphan closure")
                            exit_price = 0.0  # trigger fallback below
                    else:
                        exit_ts_ms, exit_price = now_ms, 0.0

                    # Logic: if no price or stale price -> use entry_price (zero PnL) to just clear the slot
                    using_fallback_price = False
                    if exit_price <= 0:
                        using_fallback_price = True
                        raw = "ORPHAN_TIMEOUT_NO_PRICE"

                    for pos_id in by_sym.get(sym, []):
                        # lookup in shard instead of global dict
                        pos = self.shards.get(sym, {}).get(pos_id)
                        if not pos or getattr(pos, "closed", False):
                            continue
                        # re-check expiration under lock (race-safe)
                        if not self._is_orphan_expired(pos, now_ms):
                            continue

                        from domain.models import TradeEvent

                        # mark closed in-memory
                        pos.closed = True
                        pos.exit_ts_ms = int(exit_ts_ms or now_ms)
                        pos.exit_price = float(exit_price)
                        
                        # Recommendation 4: Prometheus metric
                        self.tm_orphans_force_closed.labels(symbol=sym).inc()

                        raw = "ORPHAN_FORCED_CLOSE"
                        try:
                            # close remaining qty at last price (best-effort)
                            rq = float(getattr(pos, "remaining_qty", 0.0) or 0.0)
                            if rq > 0:
                                pnl_rest = float(spec.pnl_money(pos.entry_price, float(exit_price), rq, pos.direction, symbol=pos.symbol))
                                pos.realized_pnl_gross = float(getattr(pos, "realized_pnl_gross", 0.0) or 0.0) + pnl_rest
                                pos.remaining_qty = 0.0
                        except Exception:
                            pass

                        spec = self._get_spec(sym)
                        closed = finalize_trade(
                            pos, spec,
                            exit_price=float(exit_price),
                            exit_ts_ms=int(exit_ts_ms or now_ms),
                            close_reason_raw=str(raw),
                            tp_ratios=self.tp_ratios
                        )
                        self._log_ab_closed_event(pos, closed, str(raw))
                        self._stamp_closed_trade_meta(pos, closed, str(raw))

                        orphan_ev = TradeEvent(
                            event_type="ORPHAN_CLOSE",
                            order_id=pos.id,
                            sid=getattr(pos, "sid", ""),
                            strategy=getattr(pos, "strategy", ""),
                            source=getattr(pos, "source", ""),
                            symbol=getattr(pos, "symbol", ""),
                            tf=getattr(pos, "tf", ""),
                            direction=getattr(pos, "direction", ""),
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
                            direction=getattr(pos, "direction", ""),
                            ts_ms=int(exit_ts_ms or now_ms),
                            payload={
                                "reason": str(getattr(closed, "close_reason", "") or ""),
                                "reason_raw": str(getattr(closed, "close_reason_raw", "") or str(raw)),
                                "close_reason_detail": str(getattr(closed, "close_reason_detail", "") or ""),
                            },
                        )

                        pos_dict = dict(getattr(pos, "__dict__", {}) or {})
                        closed_dict = dict(getattr(closed, "__dict__", {}) or {})

                        from domain.normalizers import source_from_strategy
                        mapped_src = source_from_strategy(getattr(pos, "strategy", ""), str(getattr(pos, "source", "")))
                        local_triggers.append((mapped_src, str(getattr(pos, "symbol", "")), str(pos.id), getattr(pos, "is_virtual", False)))

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
                src, sym, oid = trigger[0], trigger[1], trigger[2]
                check_and_trigger_report(src, sym, counter_type="trades", order_id=oid)
            except Exception as e:
                logger.warning("⚠️ Ошибка при триггере отчета: %s", e)

    # --------------------
    # Tick → updates / close
    # --------------------
    def on_tick(self, raw_tick: Dict[str, Any]) -> None:
        """
        Обрабатывает тик для всех открытых позиций данного символа (thread-safe, optimized).
        """
        t_start = time.perf_counter()
        tick = build_tick(raw_tick)
        if not tick:
            return

        symbol = tick.symbol
        mid = float(getattr(tick, "mid", 0.0) or getattr(tick, "price", 0.0) or 0.0)
        ts_ms = int(tick.ts_ms)

        # --- Метрика возраста тика (задержка ingestion → Python обработка) ---
        try:
            now_ms = int(time.time() * 1000)
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
            # 1) last price (used by orphan forced-close) + orphan housekeep
            self._update_last_price(tick)
            self._housekeep_expired_positions(int(tick.ts_ms), current_symbol=symbol)

            # 2) snapshot positions for this symbol from shards
            # We are ALREADY under sym_ctx (symbol lock), which is the authoritative lock for this symbol.
            # We can now avoid holding the global self._lock for the iteration entirely.
            shard = self.shards.get(symbol, {})
            
            # Diagnostic log for virtual trades or unknown symbols
            if shard or symbol in {"NEARUSDT", "BTCUSDT", "XAUUSDT"}:
                v_count = sum(1 for p in shard.values() if getattr(p, "is_virtual", False))
                if v_count > 0:
                    logger.info(f"🔍 [TM] on_tick for {symbol}: found {len(shard)} positions ({v_count} virtual)")
            
            # Recommendation 4: Prometheus Gauge for open positions count
            self.tm_open_positions.labels(symbol=symbol).set(len(shard))

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
                        _trail_tp_level = max(1, int(os.getenv("BINANCE_TRAIL_ACTIVATE_TP", "2")))
                        if int(p.get("tp_level", 0)) == _trail_tp_level and getattr(pos, "sid", ""):
                            io_steps.append(("append_event", _ev_tp1_hit_external(
                                pos,
                                float(p.get("fill_price", 0.0)),
                                float(p.get("closed_qty", 0.0)),
                                int(tick.ts_ms),
                                tp_level=_trail_tp_level,
                            )))

                            # Local fallback trailing after TP1 (pure in-memory) -> IO as steps
                            try:
                                # Recommendation C: allow disabling local fallback if orchestrator is the only authority
                                if os.getenv("TRAILING_LOCAL_FALLBACK", "1") == "1" and self._is_trailing_after_tp1_enabled(pos, spec):
                                    atr = float((getattr(pos, "signal_payload", {}) or {}).get("atr", 0.0) or 0.0)
                                    if atr > 0:
                                        offset_mult = self._resolve_trailing_tp1_offset_atr(pos, spec)
                                        offset = max(0.0, atr * float(offset_mult))
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
                                logger.warning("⚠️ Local trailing fallback failed: %s", trailing_err)

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
                            now_ms = int(time.time() * 1000)
                            setattr(closed, "_health_snapshot", self._get_health_snapshot_prefixed(closed.symbol, now_ms))
                        except Exception:
                            pass

                    # IO steps for close (repo + analytics + stats)
                    hs = {}
                    try:
                        hs = self._get_health_snapshot_for_trade(str(closed.symbol))
                    except Exception:
                        hs = {}
                    io_steps.append(("save_closed", {"closed": closed, "health_snapshot": hs}))
                    io_steps.append(("analytics_closed", closed))
                    io_steps.append(("signal_outcome", closed))  # Signal → Outcome pipeline
                    io_steps.append(("update_stats", {"pos": pos, "closed": closed}))

                    from domain.normalizers import source_from_strategy
                    mapped_src = source_from_strategy(getattr(pos, "strategy", ""), str(getattr(pos, "source", "")))
                    report_triggers.append((mapped_src, pos.symbol, pos.id, getattr(pos, "is_virtual", False)))

                    # cleanup shared maps under _lock
                    with self._lock:
                        self._pop_pos(pos.id)

            # 4) Flush IO steps strictly OUTSIDE _lock (but still inside symbol-lock)
            for kind, payload in io_steps:
                try:
                    if kind == "append_event":
                        self.repo.append_event(payload)
                    elif kind == "save_tp_hit":
                        d = payload
                        self.repo.save_tp_hit(
                            d["pos"],
                            tp_level=int(d["tp_level"]),
                            fill_price=float(d["fill_price"]),
                            closed_qty=float(d["closed_qty"]),
                            pnl_part=float(d["pnl_part"]),
                            ts_ms=int(d["ts_ms"]),
                        )
                    elif kind == "save_trailing_move":
                        d = payload
                        self.repo.save_trailing_move(d["pos"], float(d["previous_sl"]), float(d["new_sl"]), int(d["ts_ms"]))
                    elif kind == "save_trailing_sync":
                        d = payload
                        self.repo.save_trailing_sync(d["pos"], int(d["ts_ms"]))
                    elif kind == "save_closed":
                        d = payload
                        self.repo.save_closed(d["closed"], health_snapshot=(d.get("health_snapshot") or {}))
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

            # ✅ Report triggers (outside all locks)
            for trigger in report_triggers:
                try:
                    from services.periodic_reporter import check_and_trigger_report
                    src_t, sym_t, oid_t = trigger[0], trigger[1], trigger[2]
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
        source: Optional[str] = None,
        profile: Optional[str] = None,
        event_id: Optional[str] = None,
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

        ts = int(time.time() * 1000)

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
            io_tasks: List[_IOTask] = []
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
                        fn=(lambda pos=pos, ts=ts: self.repo.save_trailing_sync(pos, ts)),
                        desc=f"save_trailing_sync_update:{pos.id}",
                    ))

            if io_tasks:
                self._run_io_tasks(io_tasks)

            return True

    def apply_trailing_sl_sync(
        self,
        sid: str,
        new_sl: float,
        ts_ms: Optional[int] = None,
        trailing_distance: float = 0.0,
        point_size: float = 0.0,
        clear_future_tp_levels: bool = False,
    ) -> bool:
        """
        Применяет обновление trailing SL из внешнего источника (thread-safe).
        """
        ts = int(ts_ms or time.time() * 1000)

        pos_id, sym = self._peek_pos_and_symbol_by_sid(sid)
        if not pos_id or not sym:
            return False

        with self._symbol_lock_ctx(sym):
            io_tasks: List[_IOTask] = []
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
                        fn=(lambda pos=pos, ts=ts: self.repo.save_trailing_sync(pos, ts)),
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
        ts_ms: Optional[int] = None,
        event_id: Optional[str] = None,
    ) -> bool:
        """
        Обрабатывает внешнее событие TRAILING_MOVE (идемпотентно).
        """
        if not self._dedup_acquire("trailing_move", event_id or f"{sid}:{new_sl}:{ts_ms or 0}"):
            logger.debug("⏭️ TRAILING_MOVE duplicate event_id=%s", event_id)
            return True

        ts = int(ts_ms or time.time() * 1000)

        pos_id, sym = self._peek_pos_and_symbol_by_sid(sid)
        if not pos_id or not sym:
            return False

        with self._symbol_lock_ctx(sym):
            io_tasks: List[_IOTask] = []
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
                        fn=(lambda pos=pos, ts=ts: self.repo.save_trailing_sync(pos, ts)),
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
        timestamp: Optional[int] = None,
        source: Optional[str] = None,
        event_id: Optional[str] = None
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

        report_trigger: Optional[tuple[str, str]] = None

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
            if close_qty > 1e-9:
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
            except Exception:
                pass

            spec = self._get_spec(pos.symbol)
            closed = finalize_trade(
                pos, spec,
                exit_price=float(price),
                exit_ts_ms=int(ts),
                close_reason_raw=str(raw),
                tp_ratios=self.tp_ratios
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
                    now_ms = int(time.time() * 1000)
                    setattr(closed, "_health_snapshot", self._get_health_snapshot_prefixed(closed.symbol, now_ms))
                except Exception:
                    pass
            try:
                hs = self._get_health_snapshot_for_trade(str(getattr(closed, "symbol", "") or pos.symbol))
            except Exception:
                hs = {}

            # --- Cleanup service in-memory indexes under _lock ---
            with self._lock:
                self._pop_pos(pos.id)

            # --- I/O (STRICTLY outside self._lock; still under symbol-lock) ---
            self.repo.append_event(sl_event)
            self.repo.append_event(close_event)
            self.repo.save_closed(closed, health_snapshot=hs)
            # Async DB persist (non-blocking)
            self._db_executor.submit(self._safe_save_trade_to_db, closed)
            try:
                self._update_stats(pos, closed)
            except Exception:
                pass

            # Mark sid closed for idempotency across restarts/cleanup
            try:
                self._mark_sid_closed(str(pos.sid or signal_id), ttl_days=7)
            except Exception:
                pass

            report_trigger = (pos.source, pos.symbol, pos.id, getattr(pos, "is_virtual", False))

        # ✅ Отчет вне lock (I/O/логика)
        if report_trigger:
            # Async trigger (PeriodicReporter uses SYNC redis, must be offloaded)
            self._db_executor.submit(
                self._safe_trigger_report,
                report_trigger[0],
                report_trigger[1],
                "trades",
                report_trigger[2]
            )
            if getattr(pos, "is_virtual", False):
                self._db_executor.submit(
                    self._safe_trigger_report,
                    report_trigger[0],
                    report_trigger[1],
                    "trades",
                    report_trigger[2],
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
        closed_qty = _pick("closed_qty", _pick("qty", _pick("filled_qty", None)))

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
        if not self._dedup_acquire("tp_hit", str(event_id or "")):
            logger.debug("⏭️ TP_HIT duplicate event_id=%s already applied", event_id)
            return True

        ts = normalize_ts_ms(int(timestamp or 0))
        if ts <= 0:
            ts = int(time.time() * 1000)

        # tp_level default: external TP_HIT is considered final fill if not specified
        try:
            tp_level_i = int(tp_level) if tp_level is not None else 3
        except Exception:
            tp_level_i = 3
        tp_level_i = 1 if tp_level_i < 1 else (3 if tp_level_i > 3 else tp_level_i)

        return self._apply_external_tp_hit_impl(
            signal_id=str(signal_id),
            tp_level=tp_level_i,
            price=float(price),
            ts_ms=int(ts),
            event_id=str(event_id or ""),
        )

    def _apply_external_tp_hit_impl(
        self,
        *,
        signal_id: str,
        tp_level: int,
        price: float,
        ts_ms: int,
        event_id: str,
    ) -> bool:
        """
        External TP fill event (authoritative).
        Semantics:
          - symbol-lock serialization
          - under self._lock: only in-memory index ops
          - repo/DB I/O outside self._lock
          - idempotent across restarts via closed_sid_done:{sid}
        """
        if not signal_id:
            return False

        # If position absent, check "sid closed" guard for idempotency
        pos_id, sym = self._peek_pos_and_symbol_by_sid(signal_id)
        if not pos_id:
            if self._is_sid_closed_repo_guard(signal_id):
                return True
            return False
        if not sym:
            return True

        report_trigger: Optional[tuple[str, str]] = None

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

            # Close ALL remaining qty on external TP_HIT (final close)
            try:
                close_qty = float(getattr(pos, "remaining_qty", 0.0) or 0.0)
            except Exception:
                close_qty = 0.0

            pnl_part = 0.0
            if close_qty > 1e-9:
                try:
                    pnl_part = float(spec.pnl_money(pos.entry_price, float(price), close_qty, pos.direction, symbol=pos.symbol))
                    pos.realized_pnl_gross += pnl_part
                except Exception:
                    pnl_part = 0.0

            # Update TP flags and close
            try:
                pos.tp_hits = max(int(getattr(pos, "tp_hits", 0) or 0), int(tp_level))
                pos.tp1_hit = bool(getattr(pos, "tp1_hit", False) or tp_level >= 1)
                pos.tp2_hit = bool(getattr(pos, "tp2_hit", False) or tp_level >= 2)
                pos.tp3_hit = bool(getattr(pos, "tp3_hit", False) or tp_level >= 3)
                pos.tp_before_sl = int(getattr(pos, "tp_hits", 0) or 0)
                pos.closed = True
                pos.exit_ts_ms = int(ts_ms)
                pos.exit_price = float(price)
                pos.remaining_qty = 0.0
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
                    now_ms = int(time.time() * 1000)
                    setattr(closed, "_health_snapshot", self._get_health_snapshot_prefixed(closed.symbol, now_ms))
                except Exception:
                    pass
            try:
                hs = self._get_health_snapshot_for_trade(str(getattr(closed, "symbol", "") or pos.symbol))
            except Exception:
                hs = {}

            # Cleanup indexes under _lock
            with self._lock:
                self._pop_pos(pos.id)

            # I/O outside _lock
            self.repo.append_event(tp_event)
            self.repo.append_event(close_event)
            self.repo.save_closed(closed, health_snapshot=hs)
            # Async DB persist (non-blocking)
            self._db_executor.submit(self._safe_save_trade_to_db, closed)
            try:
                self._update_stats(pos, closed)
            except Exception:
                pass

            try:
                self._mark_sid_closed(str(pos.sid or signal_id), ttl_days=7)
            except Exception:
                pass

            report_trigger = (pos.source, pos.symbol, pos.id, getattr(pos, "is_virtual", False))

        if report_trigger:
            # Async trigger
            self._db_executor.submit(
                self._safe_trigger_report,
                report_trigger[0],
                report_trigger[1],
                "trades",
                report_trigger[2]
            )
            if getattr(pos, "is_virtual", False):
                self._db_executor.submit(
                    self._safe_trigger_report,
                    report_trigger[0],
                    report_trigger[1],
                    "trades",
                    report_trigger[2],
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

    def peek_symbol_by_sid(self, sid: str) -> Optional[str]:
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

    def _submit_regime_guard_persist_task(self, task: Callable[[], None], tags: Optional[Dict[str, Any]] = None) -> None:
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
                try:
                    sem.release()
                except RuntimeError:
                    pass

        fut.add_done_callback(_done_cb)

    def _calculate_r_value(self, pos: PositionState, closed) -> float:
        """Расчет R-value (pnl_net / risk_amount)."""
        pnl = getattr(closed, 'pnl_net', 0.0) or 0.0
        risk = getattr(pos, 'risk_amount', 0.0) or 0.0
        return pnl / risk if risk > 0 else 0.0

    def _resolve_closed_at(self, closed) -> datetime:
        """Разрешение времени закрытия в datetime с tz=utc."""
        from datetime import datetime, timezone
        closed_at = getattr(closed, 'exit_ts_ms', None) or getattr(closed, 'closed_at', None)
        if closed_at is None:
            return datetime.now(timezone.utc)
        if isinstance(closed_at, (int, float)):
            ts_sec = float(closed_at)
            if ts_sec > 946684800000: # ms to sec
                ts_sec = ts_sec / 1000.0
            return datetime.fromtimestamp(ts_sec, tz=timezone.utc)
        if not hasattr(closed_at, 'tzinfo'):
            return datetime.now(timezone.utc)
        return closed_at

    def _update_stats(self, pos: PositionState, closed) -> None:
        try:
            from services.stats_aggregator import StatsAggregator
            StatsAggregator.update_stats(self.redis, pos.__dict__, closed.__dict__)  # final-close event

            # Интеграция с RegimeGuard для контроля качества сигналов
            if self.regime_guard:
                try:
                    # Получаем данные для regime guard
                    family = getattr(pos, 'family', 'unknown') or getattr(closed, 'family', 'unknown')
                    venue = getattr(pos, 'venue', 'unknown') or getattr(closed, 'venue', 'unknown')
                    
                    # [CHANGED] Сабмитим через наш безопасный метод
                    persist_task = self.regime_guard.on_signal_closed(
                        signal_id=getattr(pos, 'sid', '') or getattr(closed, 'sid', ''),
                        family=family,
                        venue=venue,
                        symbol=getattr(pos, 'symbol', 'unknown') or getattr(closed, 'symbol', 'unknown'),
                        timeframe=getattr(pos, 'timeframe', 'unknown') or getattr(closed, 'timeframe', 'unknown'),
                        r_value=self._calculate_r_value(pos, closed),
                        closed_at=self._resolve_closed_at(closed)
                    )

                    if callable(persist_task):
                        self._submit_regime_guard_persist_task(
                            persist_task, 
                            tags={"family": family, "venue": venue}
                        )
                except Exception as e:
                    self.logger.warning("regime guard update failed: %s", e)

        except Exception as e:
            logger.warning("stats update failed: %s", e)




    def _log_ab_closed_event(self, pos: PositionState, closed: TradeClosed, close_reason: str) -> None:
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
                     try:
                        risk_usd = float(abs(float(pos.entry_price or 0.0) - float(pos.sl or 0.0)) * float(pos.lot or 0.0))
                     except Exception:
                        pass

                ab_arm = ""
                ab_group = ""
                rg = ""
                try:
                    sp = getattr(pos, "signal_payload", None) or {}
                    if isinstance(sp, dict):
                        ab_arm = str(sp.get("ab_arm") or "")
                        ab_group = str(sp.get("ab_group") or "")
                        rg = str(sp.get("regime") or "")
                except Exception:
                    pass

                extra = {
                    "risk_usd": float(risk_usd),
                    "ab_arm": str(ab_arm or ""),
                    "ab_group": str(ab_group or ""),
                    "regime": str(rg or "na"),
                }

                # === AB attribution + entry context (flattened into event payload) ===
                try:
                    sp = getattr(pos, "signal_payload", {}) or {}
                    ab = sp.get("ab", {}) if isinstance(sp, dict) else {}
                    ctx = sp.get("ctx", {}) if isinstance(sp, dict) else {}
                    dec = sp.get("decision", sp.get("decision", "na")) if isinstance(sp, dict) else "na"
                    
                    pnl_usd = float(getattr(closed, "total_pnl", 0.0) or 0.0)
                    r_usd = float(risk_usd or getattr(pos, "risk_usd", 0.0) or 0.0)
                    
                    extra.update({
                        "ab_arm": str(ab.get("arm", getattr(pos, "ab_arm", "A"))).upper(),
                        "ab_group": str(ab.get("group", getattr(pos, "ab_group", "default"))).lower(),
                        "ab_key": str(ab.get("key", getattr(pos, "ab_key", ""))),
                        "arm_ver": int(ab.get("arm_ver", getattr(pos, "arm_ver", 0))),
                        "ab_split_reason": str(ab.get("split_reason","")),
                        "scenario": str(dec).lower(),  # continuation|reversal
                        "regime": str(ctx.get("regime", getattr(pos, "regime", "na"))).lower(),
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
            scenario = ""
            risk_usd = 0.0
            r_mult = 0.0

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
                    ab_arm = str(sp.get("ab_arm", "") or "")
                    ab_group = str(sp.get("ab_group", "") or "")
                    ab_key = str(sp.get("ab_key", "") or "")
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
                                scenario_v4 = str(evidence.get("scenario_v4", "") or "")
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
                                meta_enforce_applied = int(evidence.get("meta_enforce_applied", 0) or 0)
                                # meta_veto is computed always (even in SHADOW/bypass mode) - required for Stage2 optimization
                                meta_veto = int(evidence.get("meta_veto", 0) or 0)
                                meta_enforce_key = str(evidence.get("meta_enforce_key", "") or "")
                                meta_enforce_salt = str(evidence.get("meta_enforce_salt", "enf_v1") or "enf_v1")
                        # Fallback: try indicators directly
                        if meta_enforce_applied is None:
                            if isinstance(ind, dict):
                                meta_enforce_applied = int(ind.get("meta_enforce_applied", 0) or 0)
                                if meta_veto == 0:
                                    meta_veto = int(ind.get("meta_veto", 0) or 0)
                                if not meta_enforce_key:
                                    meta_enforce_key = str(ind.get("meta_enforce_key", "") or "")
                                if meta_enforce_salt == "enf_v1":
                                    meta_enforce_salt = str(ind.get("meta_enforce_salt", "enf_v1") or "enf_v1")
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
                        exit_ts_ms = int(time.time() * 1000)
            except Exception:
                exit_ts_ms = int(time.time() * 1000)

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

            self.events_logger.log_position_closed(
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
                pnl=float(getattr(closed, "total_pnl", 0.0) or 0.0),
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
                    "p0_spread_bps_at_entry": float(
                        (getattr(pos, "p0_spread_bps_at_entry", 0.0) or 0.0)
                        or ((getattr(pos, "p0_features_snapshot", None) or {}).get("spread_bps") or 0.0)
                        or ((getattr(pos, "p0_features_snapshot", None) or {}).get("p0_spread_bps_at_entry") or 0.0)
                    ),
                    "p0_slippage_bps_est": float(
                        (getattr(pos, "p0_slippage_bps_est", 0.0) or 0.0)
                        or ((getattr(pos, "p0_features_snapshot", None) or {}).get("expected_slippage_bps") or 0.0)
                        or ((getattr(pos, "p0_features_snapshot", None) or {}).get("p0_slippage_bps_est") or 0.0)
                    ),
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
                },
                extra_payload=extra,
            )
        except Exception:
            pass


def _parse_tp_levels(data: dict) -> List[float]:
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
                try:
                    tps.append(float(x))
                except Exception:
                    pass
            if tps:
                return tps
        
        # 2. Try tp1..tpN keys
        for i in range(1, 10):
            k = f"tp{i}"
            val = data.get(k)
            if val is not None:
                try:
                    tps.append(float(val))
                except Exception:
                    pass
    except Exception:
        pass
    return tps
