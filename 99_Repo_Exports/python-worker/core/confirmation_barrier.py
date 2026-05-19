"""ConfirmationBarrier — next-bar / N-second follow-through filter.

Background (audit 2026-05-18): 74% of trades (1007/1356) closed by TIMEOUT
with average MFE ≈ +1.22 / MAE ≈ −1.43 ATR-equivalent — classic "chop"
where price oscillated but never decisively progressed. This is the signature
of entries placed on the trigger bar without waiting for follow-through.

This module implements a **post-emit deferred publish** barrier:

    1. ``submit(...)`` stores a pending signal with a deadline.
    2. ``observe(...)`` is fed price ticks (or any monotonic price source)
       for the symbol.
    3. ``poll(now_ms)`` returns the list of pending signals whose state is
       resolved (deadline reached or early-veto fired). Each result is
       ``(signal_id, decision, reason)`` with decision ∈
       {``"ALLOW"``, ``"DROP"``, ``"SHADOW_ALLOW"``, ``"SHADOW_DROP"``}.

Rules at resolution (all configurable per-symbol via ``BarrierConfig``):

    * **progress**: price moved in the side direction by ≥ ``min_progress_bps``
      relative to the trigger price.
    * **no-flip**: at no observation did adverse move exceed
      ``max_adverse_bps`` (catches early flip → instant DROP).
    * **min-observations**: at least ``min_observations`` observations were
      collected (prevents stale-tick early-allow).

Mode handling (top-level ``mode`` parameter):

    * ``"off"``  — barrier disabled, every submit() is a no-op and decision
      is always ``"ALLOW"`` immediately.
    * ``"shadow"`` — barrier evaluates but always returns ``SHADOW_ALLOW`` /
      ``SHADOW_DROP`` for telemetry; the caller must NOT block on the
      decision.
    * ``"enforce"`` — barrier returns ``ALLOW`` / ``DROP``; the caller must
      respect the decision.

The module is pure-Python, has no I/O, and is fully synchronous for
deterministic testing. Wiring into the async pipeline is done outside.
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Iterable, Literal

log = logging.getLogger(__name__)

Decision = Literal["ALLOW", "DROP", "SHADOW_ALLOW", "SHADOW_DROP"]
Side = Literal["LONG", "SHORT"]


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class BarrierConfig:
    """Resolution rules. All thresholds are basis points (1 bp = 0.01%)."""

    timeout_ms: int = 15_000
    """Maximum wait between submit() and forced resolution."""

    min_progress_bps: float = 1.0
    """Required favorable move from trigger price by deadline."""

    max_adverse_bps: float = 8.0
    """Adverse move that triggers an instant DROP (early flip)."""

    min_observations: int = 1
    """Minimum number of observe() calls before deadline can resolve."""

    @classmethod
    def from_env(cls, prefix: str = "CONFIRMATION_BARRIER_") -> "BarrierConfig":
        def _f(name: str, default: float) -> float:
            try:
                return float(os.getenv(prefix + name, str(default)))
            except (TypeError, ValueError):
                return default

        def _i(name: str, default: int) -> int:
            try:
                return int(float(os.getenv(prefix + name, str(default))))
            except (TypeError, ValueError):
                return default

        return cls(
            timeout_ms=_i("TIMEOUT_MS", 15_000),
            min_progress_bps=_f("MIN_PROGRESS_BPS", 1.0),
            max_adverse_bps=_f("MAX_ADVERSE_BPS", 8.0),
            min_observations=_i("MIN_OBSERVATIONS", 1),
        )


# ---------------------------------------------------------------------------
# Internal pending record
# ---------------------------------------------------------------------------

@dataclass
class _Pending:
    signal_id: str
    symbol: str
    side: Side
    trigger_price: float
    trigger_ts_ms: int
    deadline_ms: int
    payload: object | None
    observations: int = 0
    last_price: float = 0.0
    best_favorable_bps: float = 0.0
    worst_adverse_bps: float = 0.0
    early_decision: tuple[Decision, str] | None = None
    submitted_at_monotonic: float = field(default_factory=time.monotonic)


# ---------------------------------------------------------------------------
# Barrier
# ---------------------------------------------------------------------------

def _signed_bps(side: Side, ref: float, cur: float) -> float:
    """Return favorable move in bps for ``side`` relative to ``ref``.

    Positive = favorable for the position, negative = adverse.
    Returns 0.0 on bad ref.
    """
    if ref <= 0:
        return 0.0
    delta = (cur - ref) / ref * 10_000.0
    return delta if side == "LONG" else -delta


def _resolve_mode(value: str | None) -> str:
    m = (value or "off").strip().lower()
    if m not in {"off", "shadow", "enforce"}:
        return "off"
    return m


class ConfirmationBarrier:
    """In-memory pending registry; one instance per worker / symbol-group."""

    def __init__(
        self,
        *,
        config: BarrierConfig | None = None,
        mode: str | None = None,
    ) -> None:
        self._cfg = config or BarrierConfig.from_env()
        self._mode = _resolve_mode(mode if mode is not None else os.getenv("CONFIRMATION_BARRIER_MODE"))
        # signal_id → pending
        self._pending: dict[str, _Pending] = {}
        # symbol → set[signal_id] for fast lookup on observe()
        self._by_symbol: dict[str, set[str]] = {}

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def config(self) -> BarrierConfig:
        return self._cfg

    def pending_ids(self) -> list[str]:
        return list(self._pending.keys())

    def __len__(self) -> int:
        return len(self._pending)

    # ------------------------------------------------------------------
    # Submit
    # ------------------------------------------------------------------

    def submit(
        self,
        *,
        signal_id: str,
        symbol: str,
        side: str,
        trigger_price: float,
        trigger_ts_ms: int,
        payload: object | None = None,
        timeout_ms: int | None = None,
    ) -> Decision | None:
        """Register a pending signal.

        Returns immediate decision when mode == "off" (``"ALLOW"``); otherwise
        returns ``None`` to indicate "decision pending — caller must call
        :meth:`poll` later".
        """
        if self._mode == "off":
            return "ALLOW"
        side_u = str(side or "").strip().upper()
        if side_u not in ("LONG", "SHORT"):
            # Unknown side — fail-open: do not gate.
            return "ALLOW"
        if trigger_price <= 0:
            return "ALLOW"
        if not signal_id:
            return "ALLOW"
        if signal_id in self._pending:
            # Duplicate submit — replace silently (keep latest trigger).
            self._drop(signal_id)
        tmo = int(timeout_ms if timeout_ms is not None else self._cfg.timeout_ms)
        deadline = int(trigger_ts_ms) + max(0, tmo)
        rec = _Pending(
            signal_id=signal_id,
            symbol=symbol,
            side=side_u,  # type: ignore[arg-type]
            trigger_price=float(trigger_price),
            trigger_ts_ms=int(trigger_ts_ms),
            deadline_ms=deadline,
            payload=payload,
            last_price=float(trigger_price),
        )
        self._pending[signal_id] = rec
        self._by_symbol.setdefault(symbol, set()).add(signal_id)
        return None

    # ------------------------------------------------------------------
    # Observe
    # ------------------------------------------------------------------

    def observe(self, *, symbol: str, ts_ms: int, price: float) -> None:
        """Feed a new observation (tick or bar close)."""
        if self._mode == "off":
            return
        ids = self._by_symbol.get(symbol)
        if not ids:
            return
        try:
            p = float(price)
        except (TypeError, ValueError):
            return
        if not (p > 0):
            return
        for sid in list(ids):
            rec = self._pending.get(sid)
            if rec is None:
                ids.discard(sid)
                continue
            if ts_ms < rec.trigger_ts_ms:
                continue  # ignore observations from before the trigger
            rec.last_price = p
            rec.observations += 1
            move = _signed_bps(rec.side, rec.trigger_price, p)
            if move > rec.best_favorable_bps:
                rec.best_favorable_bps = move
            if move < rec.worst_adverse_bps:
                rec.worst_adverse_bps = move
            # Early veto: adverse move beyond threshold → instant DROP.
            if (-move) >= self._cfg.max_adverse_bps and rec.early_decision is None:
                rec.early_decision = (
                    self._decision_drop(),
                    f"early_flip_{(-move):.1f}bps",
                )

    # ------------------------------------------------------------------
    # Poll
    # ------------------------------------------------------------------

    def poll(self, now_ms: int) -> list[tuple[str, Decision, str, object | None]]:
        """Resolve all pending signals whose deadline has passed OR which have
        an ``early_decision`` set.

        Returns a list of ``(signal_id, decision, reason, payload)`` tuples
        and removes them from the pending registry.
        """
        if self._mode == "off" or not self._pending:
            return []
        out: list[tuple[str, Decision, str, object | None]] = []
        for sid in list(self._pending.keys()):
            rec = self._pending[sid]
            if rec.early_decision is not None:
                dec, reason = rec.early_decision
                out.append((sid, dec, reason, rec.payload))
                self._drop(sid)
                continue
            if now_ms < rec.deadline_ms:
                continue
            dec, reason = self._evaluate_at_deadline(rec)
            out.append((sid, dec, reason, rec.payload))
            self._drop(sid)
        return out

    def cancel(self, signal_id: str) -> bool:
        """Remove a pending signal without producing a decision."""
        return self._drop(signal_id)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _drop(self, signal_id: str) -> bool:
        rec = self._pending.pop(signal_id, None)
        if rec is None:
            return False
        ids = self._by_symbol.get(rec.symbol)
        if ids is not None:
            ids.discard(signal_id)
            if not ids:
                self._by_symbol.pop(rec.symbol, None)
        return True

    def _decision_allow(self) -> Decision:
        return "ALLOW" if self._mode == "enforce" else "SHADOW_ALLOW"

    def _decision_drop(self) -> Decision:
        return "DROP" if self._mode == "enforce" else "SHADOW_DROP"

    def _evaluate_at_deadline(self, rec: _Pending) -> tuple[Decision, str]:
        if rec.observations < self._cfg.min_observations:
            return self._decision_drop(), f"insufficient_obs={rec.observations}"
        if rec.best_favorable_bps < self._cfg.min_progress_bps:
            return (
                self._decision_drop(),
                f"no_progress={rec.best_favorable_bps:.1f}bps<{self._cfg.min_progress_bps:.1f}",
            )
        return (
            self._decision_allow(),
            f"confirmed_progress={rec.best_favorable_bps:.1f}bps",
        )

    # ------------------------------------------------------------------
    # Bulk helpers
    # ------------------------------------------------------------------

    def expire_symbol(self, symbol: str, reason: str = "shutdown") -> Iterable[tuple[str, Decision, str, object | None]]:
        """Forcefully resolve all pending signals for a symbol (e.g. on
        symbol restart). Each resolution returns DROP."""
        if symbol not in self._by_symbol:
            return []
        out: list[tuple[str, Decision, str, object | None]] = []
        for sid in list(self._by_symbol.get(symbol, set())):
            rec = self._pending.get(sid)
            if rec is None:
                continue
            out.append((sid, self._decision_drop(), f"forced_expire:{reason}", rec.payload))
            self._drop(sid)
        return out
