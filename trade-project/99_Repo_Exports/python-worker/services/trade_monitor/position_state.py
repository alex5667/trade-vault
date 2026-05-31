# services/trade_monitor/position_state.py
"""
PositionStateStore — sharded in-memory position index and FSM management.

Extracted from TradeMonitorService:
  open_positions dict, shards dict, pos_by_sid dict   (monolith __init__)
  _index_add / _index_remove / _pop_pos               (monolith 1279-1444)
  _attach_fsm / _recover_fsm / _detach_fsm            (902-930)
  _fsm_transition / _fsm_publish_async                (932-971)
  _get_symbol_lock / _symbol_ctx                      (1023-1041)
  _update_last_price                                  (1139-1161)
  _is_plausible_epoch_ms                              (1101-1113)

Design:
  - Not thread-safe internally — all callers must hold the appropriate locks.
  - Thread-safety of individual methods is documented per method.
  - FSM management is optional (disabled if fsm_enabled=False).
"""
from __future__ import annotations

import contextlib
import logging
import threading
from typing import Any

logger = logging.getLogger(__name__)

# Reasonable epoch-ms bounds
_MIN_PLAUSIBLE_MS = 978_307_200_000  # 2001-01-01


def is_plausible_epoch_ms(ts_ms: int) -> bool:
    """Return True if ts_ms looks like a real epoch-millisecond timestamp."""
    if isinstance(ts_ms, bool):
        return False
    try:
        return int(ts_ms) >= _MIN_PLAUSIBLE_MS
    except Exception:
        return False


class PositionStateStore:
    """
    Thread-safe sharded in-memory position index with FSM management.

    All mutation methods acquire self._lock internally unless documented
    otherwise (caller-must-hold-lock variants are prefixed with _unsafe_).

    Args:
        redis         — Redis client for FSM audit stream publishing.
        fsm_enabled   — if False, all FSM methods become no-ops.
        log           — optional logger.
    """

    def __init__(
        self,
        redis: Any = None,
        *,
        fsm_enabled: bool = True,
        use_symbol_locks: bool = True,
        log: logging.Logger | None = None,
    ) -> None:
        self._redis = redis
        self._fsm_enabled = fsm_enabled
        self._use_symbol_locks = use_symbol_locks
        self._logger = log or logger

        # Main position registry
        self.open_positions: dict[str, Any] = {}  # pos_id -> PositionState
        self.shards: dict[str, dict[str, Any]] = {}  # symbol -> {pos_id -> pos}
        self.pos_by_sid: dict[str, str] = {}  # sid -> pos_id

        # FSM registry
        self._fsm_map: dict[str, Any] = {}  # pos_id -> PositionFSM

        # Last known price per symbol (for orphan forced-close)
        self._last_price_by_symbol: dict[str, tuple[int, float]] = {}

        # Global RLock (used by TradeMonitorService for all critical sections)
        self._lock = threading.RLock()

        # Per-symbol RLocks (for serializing on_tick + external events per symbol)
        self._symbol_locks: dict[str, threading.RLock] = {}
        self._symbol_locks_guard = threading.Lock()

        # Housekeep throttle state
        self._last_housekeep_ms: int = 0
        self._last_housekeep_by_symbol: dict[str, int] = {}

    # ------------------------------------------------------------------
    # Symbol lock management
    # ------------------------------------------------------------------

    def get_symbol_lock(self, symbol: str) -> threading.RLock:
        """Return (creating if needed) the per-symbol RLock."""
        s = (symbol or "").strip().upper() or "UNKNOWN"
        with self._symbol_locks_guard:
            lk = self._symbol_locks.get(s)
            if lk is None:
                lk = threading.RLock()
                self._symbol_locks[s] = lk
            return lk

    def symbol_ctx(self, symbol: str):
        """Context manager: per-symbol lock or nullcontext if locks disabled."""
        if not self._use_symbol_locks:
            return contextlib.nullcontext()
        return self.get_symbol_lock(symbol)

    # ------------------------------------------------------------------
    # Index management (caller must hold self._lock)
    # ------------------------------------------------------------------

    def index_add(self, pos: Any) -> None:
        """
        Add position to all indexes (open_positions, shards, pos_by_sid).
        Caller must hold self._lock.
        """
        pos_id = getattr(pos, "id", None)
        if not pos_id:
            return
        symbol = str(getattr(pos, "symbol", "") or "").strip().upper() or "UNKNOWN"
        sid = str(getattr(pos, "sid", "") or "")

        self.open_positions[pos_id] = pos
        self.shards.setdefault(symbol, {})[pos_id] = pos
        if sid:
            self.pos_by_sid[sid] = pos_id

    def index_remove(self, pos_id: str, symbol: str = "", sid: str = "") -> None:
        """
        Remove position from all indexes.
        Caller must hold self._lock.
        """
        self.open_positions.pop(pos_id, None)
        sym = (symbol or "").strip().upper() or "UNKNOWN"
        shard = self.shards.get(sym)
        if shard is not None:
            shard.pop(pos_id, None)
        if sid:
            self.pos_by_sid.pop(sid, None)
        self._detach_fsm(pos_id)

    def pop_pos(self, pos_id: str) -> Any | None:
        """
        Remove and return a position from all indexes.
        Caller must hold self._lock.
        """
        pos = self.open_positions.pop(pos_id, None)
        if pos is not None:
            symbol = str(getattr(pos, "symbol", "") or "").strip().upper() or "UNKNOWN"
            sid = str(getattr(pos, "sid", "") or "")
            shard = self.shards.get(symbol)
            if shard is not None:
                shard.pop(pos_id, None)
            if sid:
                self.pos_by_sid.pop(sid, None)
            self._detach_fsm(pos_id)
        return pos

    def register(self, pos: Any) -> None:
        """Thread-safe version of index_add."""
        with self._lock:
            self.index_add(pos)

    def evict(self, pos_id: str) -> Any | None:
        """Thread-safe version of pop_pos."""
        with self._lock:
            return self.pop_pos(pos_id)

    def peek_by_sid(self, sid: str) -> tuple[str | None, str | None]:
        """
        Thread-safe peek: return (pos_id, symbol) or (None, None).

        WARNING: result may be stale immediately after returning — always
        re-verify under the symbol lock before mutating state.
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

    # ------------------------------------------------------------------
    # Last-price cache
    # ------------------------------------------------------------------

    def update_last_price(self, symbol: str, ts_ms: int, price: float) -> None:
        """Thread-safe update of last-known price for a symbol."""
        if not is_plausible_epoch_ms(ts_ms) or price <= 0:
            return
        with self._lock:
            existing = self._last_price_by_symbol.get(symbol)
            if existing is None or existing[0] < ts_ms:
                self._last_price_by_symbol[symbol] = (ts_ms, price)

    def get_last_price(self, symbol: str) -> tuple[int, float] | None:
        """Return (ts_ms, price) or None (thread-safe read under lock)."""
        with self._lock:
            return self._last_price_by_symbol.get(symbol)

    def cleanup_stale_prices(self, ttl_ms: int = 3_600_000) -> None:
        """Remove prices older than ttl_ms. Thread-safe."""
        now = 0
        try:
            from utils.time_utils import get_ny_time_millis
            now = get_ny_time_millis()
        except Exception:
            return
        with self._lock:
            to_delete = [
                sym
                for sym, (ts, _) in self._last_price_by_symbol.items()
                if now - ts > ttl_ms
            ]
            for sym in to_delete:
                del self._last_price_by_symbol[sym]
        if to_delete:
            self._logger.info("🧹 Cleaned up %d stale prices", len(to_delete))

    def update_last_price_from_tick(self, tick: Any) -> None:
        """Extract price from a TickData object and update the cache."""
        try:
            symbol = tick.symbol
            ts_ms = int(tick.ts_ms)
            price = float(
                getattr(tick, "mid", 0.0)
                or getattr(tick, "last", 0.0)
                or getattr(tick, "price", 0.0)
                or 0.0
            )
            self.update_last_price(symbol, ts_ms, price)
        except Exception:
            pass  # fail-open

    # ------------------------------------------------------------------
    # FSM management
    # ------------------------------------------------------------------

    def attach_fsm(self, pos: Any) -> None:
        """Create and attach a PositionFSM for a newly-opened position."""
        if not self._fsm_enabled:
            return
        try:
            from domain.position_fsm import PositionFSM, PositionStatus
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
            self._logger.warning("[FSM] attach_fsm failed for %s: %s", getattr(pos, "id", "?"), exc)

    def recover_fsm(self, pos: Any) -> None:
        """Reconstruct FSM from position flags (used after Redis recovery)."""
        if not self._fsm_enabled:
            return
        try:
            from domain.position_fsm import fsm_from_position
            self._fsm_map[pos.id] = fsm_from_position(pos)
        except Exception as exc:
            self._logger.warning("[FSM] recover_fsm failed for %s: %s", getattr(pos, "id", "?"), exc)

    def _detach_fsm(self, pos_id: str) -> None:
        """Remove FSM from map on position close. Caller must hold lock."""
        self._fsm_map.pop(pos_id, None)

    def fsm_transition(
        self,
        pos: Any,
        to: str,
        trigger: str,
        actor: str = "trade_monitor",
        reason: str = "",
        ts_ms: int | None = None,
        **meta: Any,
    ) -> None:
        """Attempt FSM transition by state name string. Fail-open."""
        if not self._fsm_enabled:
            return
        try:
            from domain.position_fsm import PositionFSM, PositionStatus
            fsm = self._fsm_map.get(getattr(pos, "id", ""))
            if fsm is None:
                self.recover_fsm(pos)
                fsm = self._fsm_map.get(getattr(pos, "id", ""))
            if fsm is None:
                return
            target = PositionStatus(to)
            fsm.transition(target, trigger=trigger, actor=actor, reason=reason, ts_ms=ts_ms, **meta)
            self._fsm_publish_async(fsm)
        except Exception as exc:
            self._logger.warning(
                "[FSM] _fsm_transition %s→%s failed: %s", getattr(pos, "id", "?"), to, exc
            )

    def _fsm_publish_async(self, fsm: Any) -> None:
        """Publish last FSM transition to Redis Stream. Fail-open."""
        if not self._redis:
            return
        try:
            payload = fsm.to_redis_payload()
            self._redis.xadd(
                fsm.AUDIT_STREAM,
                payload,
                maxlen=fsm.AUDIT_MAXLEN,
                approximate=True,
            )
        except Exception:
            pass  # never break hot path for audit publishing

    # ------------------------------------------------------------------
    # Housekeep throttle state (accessed by OrphanRecoveryPolicy)
    # ------------------------------------------------------------------

    def get_last_housekeep_ms(self) -> int:
        return self._last_housekeep_ms

    def set_last_housekeep_ms(self, v: int) -> None:
        self._last_housekeep_ms = v

    def get_last_housekeep_by_symbol(self, sym: str) -> int:
        return self._last_housekeep_by_symbol.get(sym, 0)

    def set_last_housekeep_by_symbol(self, sym: str, v: int) -> None:
        self._last_housekeep_by_symbol[sym] = v

    # ------------------------------------------------------------------
    # Convenience: counts
    # ------------------------------------------------------------------

    @property
    def open_count(self) -> int:
        return len(self.open_positions)

    def open_symbols(self) -> set[str]:
        """Return set of symbols with open positions (thread-safe snapshot)."""
        with self._lock:
            return {
                str(getattr(pos, "symbol", "") or "").strip().upper()
                for pos in self.open_positions.values()
                if getattr(pos, "symbol", None)
            }
