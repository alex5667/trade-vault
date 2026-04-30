"""
domain/position_fsm.py — P1-9: Explicit FSM for PositionState.

Goal:
  Replace implicit gate-chain state management (scattered boolean flags: pos.closed
  pos.trailing_active, pos.trail_armed, pos.tp1_hit, pos.tp2_hit …) with an explicit
  finite-state machine that:

    1. Defines a strict set of canonical states (PositionStatus).
    2. Enforces an allow-list of legal transitions (ALLOWED_TRANSITIONS).
    3. Maintains a full in-memory audit trail (list[TransitionRecord]).
    4. Keeps backward-compatible boolean flags on PositionState in sync automatically.
    5. Emits a Prometheus counter on every transition.
    6. Optionally publishes the audit event to a Redis Stream (trade:fsm:audit).

Usage:
    fsm = PositionFSM(pos)         # attach at open_position time
    fsm.transition(
        to=PositionStatus.TP1_HIT
        trigger="tp1_hit"
        actor="trade_monitor"
        reason="price crossed tp_levels[0]"
        price=42100.0
        ts_ms=tick_ts_ms
    )

Integration:
    TradeMonitor stores FSMs in self._fsm_map: dict[str, PositionFSM]
    All direct pos.closed = True / pos.tp1_hit = True assignments are replaced
    by the corresponding fsm.transition(...) call.

Backward compat:
    PositionState boolean flags are never removed — they are still serialised to Redis.
    FSM sets them internally so existing consumers (RedisTradeRepository, tests, …) work
    without modification.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, FrozenSet, Optional, Tuple

from prometheus_client import Counter

if TYPE_CHECKING:
    from domain.models import PositionState

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------

_TRANSITION_TOTAL = Counter(
    "position_fsm_transition_total"
    "Number of FSM state transitions"
    ["from_state", "to_state", "trigger"]
)

_INVALID_TRANSITION_TOTAL = Counter(
    "position_fsm_invalid_transition_total"
    "Number of rejected FSM transition attempts (illegal transitions)"
    ["from_state", "to_state", "trigger"]
)


# ---------------------------------------------------------------------------
# States
# ---------------------------------------------------------------------------


class PositionStatus(str, Enum):
    """Canonical lifecycle states for a trade position.

    Transitions are strictly controlled by ALLOWED_TRANSITIONS below.
    """

    PENDING = "PENDING"          # created in memory, not yet confirmed open
    OPEN = "OPEN"                # position active, monitoring ticks
    TP1_HIT = "TP1_HIT"          # TP1 level reached; partial close may have occurred
    TP2_HIT = "TP2_HIT"          # TP2 level reached
    TRAILING_ARMED = "TRAILING_ARMED"   # trailing stop has been armed (after TP1)
    TRAILING_ACTIVE = "TRAILING_ACTIVE"  # trailing stop is moving
    CLOSED = "CLOSED"            # position fully closed (SL / TP3 / external)
    ORPHAN_CLOSED = "ORPHAN_CLOSED"  # closed by orphan-timeout guard


# ---------------------------------------------------------------------------
# Transition table
# ---------------------------------------------------------------------------

# (from_state, to_state) — both as PositionStatus
ALLOWED_TRANSITIONS: FrozenSet[Tuple[PositionStatus, PositionStatus]] = frozenset(
    {
        (PositionStatus.PENDING, PositionStatus.OPEN)
        # From OPEN
        (PositionStatus.OPEN, PositionStatus.TP1_HIT)
        (PositionStatus.OPEN, PositionStatus.TRAILING_ARMED)
        (PositionStatus.OPEN, PositionStatus.TRAILING_ACTIVE)
        (PositionStatus.OPEN, PositionStatus.CLOSED)
        (PositionStatus.OPEN, PositionStatus.ORPHAN_CLOSED)
        # From TP1_HIT
        (PositionStatus.TP1_HIT, PositionStatus.TP2_HIT)
        (PositionStatus.TP1_HIT, PositionStatus.TRAILING_ARMED)
        (PositionStatus.TP1_HIT, PositionStatus.TRAILING_ACTIVE)
        (PositionStatus.TP1_HIT, PositionStatus.CLOSED)
        (PositionStatus.TP1_HIT, PositionStatus.ORPHAN_CLOSED)
        # From TP2_HIT
        (PositionStatus.TP2_HIT, PositionStatus.TRAILING_ARMED)
        (PositionStatus.TP2_HIT, PositionStatus.TRAILING_ACTIVE)
        (PositionStatus.TP2_HIT, PositionStatus.CLOSED)
        (PositionStatus.TP2_HIT, PositionStatus.ORPHAN_CLOSED)
        # From TRAILING_ARMED
        (PositionStatus.TRAILING_ARMED, PositionStatus.TRAILING_ACTIVE)
        (PositionStatus.TRAILING_ARMED, PositionStatus.CLOSED)
        (PositionStatus.TRAILING_ARMED, PositionStatus.ORPHAN_CLOSED)
        # From TRAILING_ACTIVE
        (PositionStatus.TRAILING_ACTIVE, PositionStatus.CLOSED)
        (PositionStatus.TRAILING_ACTIVE, PositionStatus.ORPHAN_CLOSED)
        # Self-transitions allowed for idempotency (e.g. multiple trailing moves)
        (PositionStatus.TRAILING_ACTIVE, PositionStatus.TRAILING_ACTIVE)
    }
)

# Terminal states — no further transitions are allowed.
TERMINAL_STATES: FrozenSet[PositionStatus] = frozenset(
    {PositionStatus.CLOSED, PositionStatus.ORPHAN_CLOSED}
)


# ---------------------------------------------------------------------------
# Audit record
# ---------------------------------------------------------------------------


@dataclass
class TransitionRecord:
    """Immutable record of a single FSM transition for audit purposes."""

    from_state: str
    to_state: str
    trigger: str       # machine-readable cause: "tp1_hit", "sl_hit", "orphan_expiry", ...
    ts_ms: int         # epoch-ms when the transition occurred
    actor: str         # component that initiated the transition
    reason: str        # human-readable description
    meta: Dict[str, Any] = field(default_factory=dict)  # price, pnl, ...

    def to_dict(self) -> Dict[str, Any]:
        return {
            "from": self.from_state
            "to": self.to_state
            "trigger": self.trigger
            "ts_ms": self.ts_ms
            "actor": self.actor
            "reason": self.reason
            **{f"meta_{k}": v for k, v in self.meta.items()}
        }


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class InvalidTransitionError(ValueError):
    """Raised when an attempt is made to perform an illegal FSM transition."""

    def __init__(
        self
        from_state: PositionStatus
        to_state: PositionStatus
        trigger: str
    ) -> None:
        self.from_state = from_state
        self.to_state = to_state
        self.trigger = trigger
        super().__init__(
            f"FSM invalid transition: {from_state.value} → {to_state.value} "
            f"(trigger={trigger!r})"
        )


# ---------------------------------------------------------------------------
# FSM
# ---------------------------------------------------------------------------


class PositionFSM:
    """Explicit finite-state machine bound to a single PositionState instance.

    Responsibilities:
      - Enforce the ALLOWED_TRANSITIONS table.
      - Keep PositionState boolean flags in sync (backward compat).
      - Maintain a bounded in-memory audit trail.
      - Emit Prometheus counters on every transition.

    Thread-safety:
      The FSM itself is not thread-safe — callers must hold the appropriate
      trade-monitor lock (symbol lock / global lock) before calling transition().
    """

    # Redis stream for FSM audit events, published non-blocking if redis is provided.
    AUDIT_STREAM = "trade:fsm:audit"
    AUDIT_MAXLEN = 10_000
    # Max audit trail kept in memory per position (oldest entries are dropped).
    MAX_TRAIL_LEN = 64

    def __init__(
        self
        pos: "PositionState"
        initial_status: PositionStatus = PositionStatus.PENDING
    ) -> None:
        self._pos = pos
        self._status: PositionStatus = initial_status
        self._trail: list[TransitionRecord] = []
        # Sync the string field on pos (used for Redis serialisation)
        self._sync_fsm_status()

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    @property
    def status(self) -> PositionStatus:
        return self._status

    def is_terminal(self) -> bool:
        """Return True if the position has reached a terminal state."""
        return self._status in TERMINAL_STATES

    def transition(
        self
        to: PositionStatus
        trigger: str
        actor: str = "trade_monitor"
        reason: str = ""
        ts_ms: Optional[int] = None
        **meta: Any
    ) -> TransitionRecord:
        """Attempt a state transition.

        Args:
            to:      Target state.
            trigger: Machine-readable cause label (e.g. "sl_hit", "tp1_hit").
            actor:   Component name initiating the transition.
            reason:  Human-readable description.
            ts_ms:   Epoch-ms timestamp; defaults to current wall-clock time.
            **meta:  Arbitrary key/value context (price, pnl, …).

        Returns:
            The TransitionRecord that was appended to the audit trail.

        Raises:
            InvalidTransitionError: If the transition is not in ALLOWED_TRANSITIONS.
        """
        from_status = self._status
        if ts_ms is None:
            ts_ms = int(time.time() * 1000)

        # --- Validate ---
        if not self._is_allowed(from_status, to):
            _INVALID_TRANSITION_TOTAL.labels(
                from_state=from_status.value
                to_state=to.value
                trigger=trigger
            ).inc()
            err = InvalidTransitionError(from_status, to, trigger)
            logger.error(
                "🚨 [FSM] %s | pos_id=%s | %s"
                err
                getattr(self._pos, "id", "?")
                meta
            )
            raise err

        # --- Apply ---
        self._status = to
        self._sync_flags(from_status, to)
        self._sync_fsm_status()

        # --- Record ---
        record = TransitionRecord(
            from_state=from_status.value
            to_state=to.value
            trigger=trigger
            ts_ms=ts_ms
            actor=actor
            reason=reason
            meta=dict(meta)
        )
        self._append_trail(record)

        # --- Metrics ---
        _TRANSITION_TOTAL.labels(
            from_state=from_status.value
            to_state=to.value
            trigger=trigger
        ).inc()

        logger.debug(
            "🔀 [FSM] %s → %s | trigger=%s | pos_id=%s | actor=%s | %s"
            from_status.value
            to.value
            trigger
            getattr(self._pos, "id", "?")
            actor
            reason
        )

        return record

    def force_transition(
        self
        to: PositionStatus
        trigger: str
        actor: str = "trade_monitor"
        reason: str = ""
        ts_ms: Optional[int] = None
        **meta: Any
    ) -> TransitionRecord:
        """Transition that skips the allow-list check (use sparingly for recovery).

        Should only be used during position-state reconstruction from Redis where
        the stored state may already be past an intermediate step.
        """
        old = self._status
        if ts_ms is None:
            ts_ms = int(time.time() * 1000)
        self._status = to
        self._sync_flags(old, to)
        self._sync_fsm_status()

        record = TransitionRecord(
            from_state=old.value
            to_state=to.value
            trigger=trigger
            ts_ms=ts_ms
            actor=actor
            reason=f"[FORCED] {reason}"
            meta=dict(meta)
        )
        self._append_trail(record)

        _TRANSITION_TOTAL.labels(
            from_state=old.value
            to_state=to.value
            trigger=f"FORCED:{trigger}"
        ).inc()

        if trigger == "recovery" and actor == "fsm_from_position":
            logger.debug(
                "⚠️ [FSM] FORCED %s → %s | trigger=%s | pos_id=%s | actor=%s"
                old.value, to.value, trigger
                getattr(self._pos, "id", "?"), actor
            )
        else:
            logger.warning(
                "⚠️ [FSM] FORCED %s → %s | trigger=%s | pos_id=%s | actor=%s"
                old.value, to.value, trigger
                getattr(self._pos, "id", "?"), actor
            )
        return record

    @property
    def trail(self) -> list[TransitionRecord]:
        """Return a copy of the audit trail."""
        return list(self._trail)

    def to_redis_payload(self) -> Dict[str, Any]:
        """Return a flat dict suitable for XADD to the FSM audit stream."""
        pos_id = getattr(self._pos, "id", "")
        symbol = getattr(self._pos, "symbol", "")
        last = self._trail[-1] if self._trail else None
        return {
            "pos_id": pos_id
            "symbol": symbol
            "status": self._status.value
            "trail_len": len(self._trail)
            **(last.to_dict() if last else {})
        }

    # ------------------------------------------------------------------ #
    # Internal                                                             #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _is_allowed(from_s: PositionStatus, to_s: PositionStatus) -> bool:
        return (from_s, to_s) in ALLOWED_TRANSITIONS

    def _append_trail(self, record: TransitionRecord) -> None:
        self._trail.append(record)
        if len(self._trail) > self.MAX_TRAIL_LEN:
            self._trail = self._trail[-self.MAX_TRAIL_LEN :]

    def _sync_fsm_status(self) -> None:
        """Write the canonical status string to PositionState (for Redis serialisation)."""
        try:
            object.__setattr__(self._pos, "fsm_status", self._status.value)
        except Exception:
            pass  # fail-open: pos may use slots or be readonly in tests

    def _sync_flags(
        self, from_s: PositionStatus, to_s: PositionStatus  # noqa: ARG002
    ) -> None:
        """Keep backward-compatible boolean flags on PositionState in sync.

        Called *after* self._status has been updated.
        Order matters — set flags matching the NEW state.
        """
        pos = self._pos
        try:
            if to_s is PositionStatus.OPEN:
                _safe_set(pos, "closed", False)

            elif to_s is PositionStatus.TP1_HIT:
                _safe_set(pos, "tp1_hit", True)
                _safe_set(pos, "tp1_touched", True)
                _safe_set(pos, "tp_hits", max(int(getattr(pos, "tp_hits", 0) or 0), 1))

            elif to_s is PositionStatus.TP2_HIT:
                _safe_set(pos, "tp2_hit", True)
                _safe_set(pos, "tp2_touched", True)
                _safe_set(pos, "tp_hits", max(int(getattr(pos, "tp_hits", 0) or 0), 2))

            elif to_s is PositionStatus.TRAILING_ARMED:
                _safe_set(pos, "trail_armed", True)
                _safe_set(pos, "trailing_started", True)

            elif to_s is PositionStatus.TRAILING_ACTIVE:
                _safe_set(pos, "trailing_active", True)
                _safe_set(pos, "trailing_started", True)
                _safe_set(pos, "trail_armed", True)

            elif to_s in (PositionStatus.CLOSED, PositionStatus.ORPHAN_CLOSED):
                _safe_set(pos, "closed", True)

        except Exception as exc:  # pragma: no cover
            logger.warning("[FSM] _sync_flags error: %s", exc)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _safe_set(obj: Any, attr: str, value: Any) -> None:
    """Set attribute on a dataclass (slots-safe)."""
    try:
        object.__setattr__(obj, attr, value)
    except (AttributeError, TypeError):
        setattr(obj, attr, value)


# ---------------------------------------------------------------------------
# Recovery helper
# ---------------------------------------------------------------------------


def fsm_from_position(pos: "PositionState") -> "PositionFSM":
    """Reconstruct an FSM from an existing PositionState (e.g. after Redis reload).

    Derives the status from the boolean flags already stored on the position
    using force_transition to skip the allow-list.

    IMPORTANT: read stored fsm_status BEFORE constructing PositionFSM, because
    PositionFSM.__init__ calls _sync_fsm_status() which writes "PENDING" onto pos.
    """
    # --- Read flags and stored status FIRST, before construction overwrites pos ---
    stored = str(getattr(pos, "fsm_status", "") or "")
    closed = bool(getattr(pos, "closed", False))
    trailing_active = bool(getattr(pos, "trailing_active", False))
    trailing_started = bool(getattr(pos, "trailing_started", False))
    trail_armed = bool(getattr(pos, "trail_armed", False))
    tp2_hit = bool(getattr(pos, "tp2_hit", False))
    tp1_hit = bool(getattr(pos, "tp1_hit", False))

    # Construction writes "PENDING" to pos.fsm_status — that's fine, we'll overwrite below.
    fsm = PositionFSM(pos, initial_status=PositionStatus.PENDING)

    # Restore stored fsm_status first (most reliable if present and not "PENDING")
    if stored and stored != "PENDING":
        try:
            target = PositionStatus(stored)
            fsm.force_transition(
                target
                trigger="recovery"
                actor="fsm_from_position"
                reason=f"recovered from stored fsm_status={stored!r}"
            )
            return fsm
        except Exception:
            pass  # fall through to flag inference

    # Flag-based inference.
    # A recovered position (loaded from Redis) has always been opened, so
    # the minimum inferred state is OPEN, never PENDING.
    if closed:
        target = PositionStatus.CLOSED
    elif trailing_active:
        target = PositionStatus.TRAILING_ACTIVE
    elif trailing_started or trail_armed:
        target = PositionStatus.TRAILING_ARMED
    elif tp2_hit:
        target = PositionStatus.TP2_HIT
    elif tp1_hit:
        target = PositionStatus.TP1_HIT
    else:
        target = PositionStatus.OPEN  # default for any recovered position

    fsm.force_transition(
        target
        trigger="recovery"
        actor="fsm_from_position"
        reason=f"inferred from boolean flags (closed={closed}, "
               f"trailing_active={trailing_active}, tp1={tp1_hit}, tp2={tp2_hit})"
    )
    return fsm
