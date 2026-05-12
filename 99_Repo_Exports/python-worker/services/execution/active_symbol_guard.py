"""active_symbol_guard.py — Single-active-position guard for Binance executor.

Extracted from binance_executor.py (god-class decomposition).

Responsibilities:
- Guard store wrapper (ActiveSymbolGuardStore)
- Exchange-truth based guard release (P5)
- User-stream staleness checks
- Manual symbol hold enforcement
- Guard CAS metrics recording
- State terminalish detection
"""
from __future__ import annotations

import contextlib
import math
import time
from typing import TYPE_CHECKING, Any

from services.execution.binance_order_mapper import TERMINAL_FSM_STATES, _f, _i

if TYPE_CHECKING:
    from services.binance_futures_client import BinanceFuturesClient

try:
    from services.active_symbol_guard_store import ActiveSymbolGuardStore
except Exception:
    from active_symbol_guard_store import ActiveSymbolGuardStore  # type: ignore[no-redef]

try:
    from services.execution_metrics import (
        EXECUTION_ACTIVE_SYMBOL_GUARD_CAS_TOTAL,
        EXECUTION_ACTIVE_SYMBOL_GUARD_EXCHANGE_CHECK_TOTAL,
        EXECUTION_ACTIVE_SYMBOL_GUARD_RELEASE_TOTAL,
        EXECUTION_ACTIVE_SYMBOL_GUARD_STUCK_TOTAL,
        EXECUTION_OPEN_BLOCKED_ACTIVE_SYMBOL_TOTAL,  # type: ignore
        EXECUTION_DUPLICATE_PREVENTED_TOTAL,  # type: ignore
    )
except Exception:
    EXECUTION_ACTIVE_SYMBOL_GUARD_CAS_TOTAL = EXECUTION_ACTIVE_SYMBOL_GUARD_EXCHANGE_CHECK_TOTAL = None  # type: ignore
    EXECUTION_ACTIVE_SYMBOL_GUARD_RELEASE_TOTAL = EXECUTION_ACTIVE_SYMBOL_GUARD_STUCK_TOTAL = None  # type: ignore
    EXECUTION_OPEN_BLOCKED_ACTIVE_SYMBOL_TOTAL = EXECUTION_DUPLICATE_PREVENTED_TOTAL = None  # type: ignore


def _ms_now() -> int:
    try:
        from utils.time_utils import get_ny_time_millis
        return get_ny_time_millis()
    except Exception:
        return int(time.time() * 1000)


class OpenBlockedByActiveSymbolError(RuntimeError):
    """Raised when handle_open is blocked because the symbol already has an active execution."""
    def __init__(self, details: dict[str, Any]) -> None:
        super().__init__(str((details or {}).get("reason") or "single_active_position_per_symbol"))
        self.details = dict(details or {})


class ActiveSymbolGuard:
    """Manages per-symbol single-active-position invariant.

    Wraps ActiveSymbolGuardStore with executor-specific logic:
    - exchange-truth release (P5): requires Binance to confirm flat before releasing
    - user-stream staleness: if user stream is stale, guard decisions are degraded
    - manual hold enforcement: operator can block a symbol via Redis key
    - CAS metrics recording for observability

    DI-friendly: receives redis client + all config as constructor args.
    """

    def __init__(
        self,
        *,
        r: Any,
        active_symbol_key_prefix: str = "orders:active_symbol_sid:",
        tombstone_ttl_sec: int = 120,
        state_ttl: int = 86400,
        user_stream_status_key: str = "orders:user_stream:status",
        exec_active_symbol_user_stream_stale_ms: int = 30_000,
        exec_single_active_position_per_symbol: bool = False,
        exec_single_active_position_exchange_truth_release: bool = True,
        exec_single_active_position_release_on_terminal: bool = True,
        exec_single_active_position_require_flat_no_orders: bool = True,
        exec_single_active_position_stale_timeout_ms: int = 900_000,
        exec_single_active_position_guard_repair_enable: bool = True,
        write_event_fn: Any = None,
    ) -> None:
        self.r = r
        self.active_symbol_key_prefix = active_symbol_key_prefix.rstrip(":") + ":"
        self.tombstone_ttl_sec = tombstone_ttl_sec
        self.state_ttl = state_ttl
        self.user_stream_status_key = user_stream_status_key
        self.exec_active_symbol_user_stream_stale_ms = exec_active_symbol_user_stream_stale_ms
        self.exec_single_active_position_per_symbol = exec_single_active_position_per_symbol
        self.exec_single_active_position_exchange_truth_release = exec_single_active_position_exchange_truth_release
        self.exec_single_active_position_release_on_terminal = exec_single_active_position_release_on_terminal
        self.exec_single_active_position_require_flat_no_orders = exec_single_active_position_require_flat_no_orders
        self.exec_single_active_position_stale_timeout_ms = exec_single_active_position_stale_timeout_ms
        self.exec_single_active_position_guard_repair_enable = exec_single_active_position_guard_repair_enable
        self._write_event_fn = write_event_fn
        self._store: ActiveSymbolGuardStore | None = None

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _store_instance(self) -> ActiveSymbolGuardStore:
        if self._store is None:
            self._store = ActiveSymbolGuardStore(
                self.r,
                key_prefix=self.active_symbol_key_prefix,
                active_ttl_sec=self.state_ttl,
                tombstone_ttl_sec=self.tombstone_ttl_sec,
            )
        return self._store

    def _record_cas(self, symbol: str, outcome: str, reason: str) -> None:
        with contextlib.suppress(Exception):
            if EXECUTION_ACTIVE_SYMBOL_GUARD_CAS_TOTAL is not None:
                EXECUTION_ACTIVE_SYMBOL_GUARD_CAS_TOTAL.labels(
                    symbol=(symbol or "").strip().upper(),
                    writer="executor",
                    outcome=outcome,
                    reason=reason,
                ).inc()

    def _state_is_terminalish(self, state: dict[str, Any] | None) -> bool:
        doc = dict(state or {})
        if (doc.get("fsm_state") or "").strip().upper() in TERMINAL_FSM_STATES:
            return True
        status = (doc.get("status") or "").strip().lower()
        if status in {"closed", "cancelled", "canceled", "failed", "exited", "exit_filled", "emergency_flattened"}:
            return True
        return bool(doc.get("closed"))

    # ------------------------------------------------------------------
    # Load / clear
    # ------------------------------------------------------------------

    def load(self, symbol: str) -> dict[str, Any]:
        return self._store_instance().load_active(symbol)

    def acquire_or_refresh(self, symbol: str, sid: str, payload_patch: dict[str, Any]) -> dict[str, Any]:
        return self._store_instance().acquire_or_refresh(
            symbol=symbol, sid=sid, payload_patch=payload_patch, writer="executor"
        )

    def mark_released(self, symbol: str, *, expected_sid: str = "") -> None:
        try:
            res = self._store_instance().mark_released(
                symbol=symbol,
                expected_sid=expected_sid,
                release_reason="executor_terminal_clear",
                writer="executor",
            )
            self._record_cas(symbol, "success" if res.get("applied") else "rejected", res.get("reason") or "unknown")
        except Exception:
            self._record_cas(symbol, "error", "exception")

    # ------------------------------------------------------------------
    # User-stream staleness
    # ------------------------------------------------------------------

    def load_user_stream_status(self) -> dict[str, Any]:
        import json
        try:
            raw = self.r.get(self.user_stream_status_key)
            doc = json.loads(raw) if raw else {}
            return doc if isinstance(doc, dict) else {}
        except Exception:
            return {}

    def user_stream_is_stale(self) -> bool:
        threshold_ms = self.exec_active_symbol_user_stream_stale_ms
        if threshold_ms <= 0:
            return False
        doc = self.load_user_stream_status()
        if not doc or not bool(doc.get("connected", False)):
            return True
        last_ms = _i(doc.get("last_event_ms") or doc.get("last_ingest_ms") or doc.get("updated_at_ms"), 0)
        if last_ms <= 0:
            return True
        return max(0, _ms_now() - int(last_ms)) > threshold_ms

    # ------------------------------------------------------------------
    # Exchange-truth query
    # ------------------------------------------------------------------

    def read_exchange_truth(
        self, *, symbol: str, client: "BinanceFuturesClient | None"
    ) -> dict[str, Any]:
        """Query Binance for real position and open-order state (P5)."""
        sym = (symbol or "").strip().upper()
        truth: dict[str, Any] = {
            "symbol": sym,
            "checked_at_ms": _ms_now(),
            "position_amt": 0.0,
            "has_live_position": False,
            "open_plain_orders": 0,
            "open_algo_orders": 0,
            "has_open_orders": False,
            "is_flat": False,
            "is_reliable": False,
            "errors": [],
        }
        if client is None:
            truth["errors"] = ["client_missing"]
            return truth
        errors: list[str] = []
        with contextlib.suppress(Exception):
            for pos in (client.get_position_risk() or []):
                if str((pos or {}).get("symbol") or "").upper() != sym:
                    continue
                amt = _f((pos or {}).get("positionAmt"), 0.0)
                truth["position_amt"] = amt
                truth["has_live_position"] = not math.isclose(float(amt), 0.0, abs_tol=1e-12)
                break
        try:
            plain = client.get_open_orders(sym) or []
            truth["open_plain_orders"] = len(list(plain))
        except Exception as exc:
            errors.append(f"open_orders:{exc.__class__.__name__}")
        try:
            algo = client.get_open_algo_orders(sym) or []
            truth["open_algo_orders"] = len(list(algo))
        except Exception as exc:
            errors.append(f"open_algo_orders:{exc.__class__.__name__}")
        truth["has_open_orders"] = int(truth["open_plain_orders"]) > 0 or int(truth["open_algo_orders"]) > 0
        truth["errors"] = errors
        truth["is_reliable"] = not errors
        truth["is_flat"] = (
            not truth["has_live_position"]
            and (not truth["has_open_orders"] if self.exec_single_active_position_require_flat_no_orders else True)
            and truth["is_reliable"]
        )
        with contextlib.suppress(Exception):
            if EXECUTION_ACTIVE_SYMBOL_GUARD_EXCHANGE_CHECK_TOTAL is not None:
                result = "flat" if truth["is_flat"] else ("active" if truth["is_reliable"] else "error")
                EXECUTION_ACTIVE_SYMBOL_GUARD_EXCHANGE_CHECK_TOTAL.labels(symbol=sym, result=result).inc()
        return truth

    def refresh_from_exchange(
        self,
        *,
        symbol: str,
        blocked_by_sid: str,
        guard: dict[str, Any],
        blocked_state_doc: dict[str, Any],
        exchange_truth: dict[str, Any],
        reason: str,
    ) -> None:
        """Persist exchange-truth snapshot back into the guard key (P5)."""
        if not self.exec_single_active_position_guard_repair_enable:
            return
        try:
            state_terminalish = self._state_is_terminalish(blocked_state_doc)
            updated = dict(guard or {})
            updated.update({
                "symbol": (symbol or "").strip().upper(),
                "sid": blocked_by_sid,
                "fsm_state": str((blocked_state_doc or {}).get("fsm_state") or updated.get("fsm_state") or ""),
                "state": str((blocked_state_doc or {}).get("fsm_state") or updated.get("state") or ""),
                "updated_at_ms": _ms_now(),
                "exchange_truth_checked_at_ms": exchange_truth.get("checked_at_ms") or _ms_now(),
                "exchange_position_amt": exchange_truth.get("position_amt") or 0.0,
                "exchange_open_plain_orders": exchange_truth.get("open_plain_orders") or 0,
                "exchange_open_algo_orders": exchange_truth.get("open_algo_orders") or 0,
                "exchange_guard_reason": reason or "exchange_truth_active",
                "guard_release_policy": "exchange_truth" if self.exec_single_active_position_exchange_truth_release else "local_terminal",
                "guard_release_pending": bool(state_terminalish and self.exec_single_active_position_exchange_truth_release),
                "guard_release_reason": "await_exchange_flat_no_orders" if state_terminalish and self.exec_single_active_position_exchange_truth_release else "",
                "state_terminalish": bool(state_terminalish),
                "user_stream_stale": self.user_stream_is_stale(),
            })
            res = self.acquire_or_refresh(symbol, blocked_by_sid, updated)
            self._record_cas(symbol, "success" if res.get("applied") else "rejected", res.get("reason") or "unknown")
        except Exception:
            self._record_cas(symbol, "error", "exception")
        with contextlib.suppress(Exception):
            if EXECUTION_ACTIVE_SYMBOL_GUARD_STUCK_TOTAL is not None:
                EXECUTION_ACTIVE_SYMBOL_GUARD_STUCK_TOTAL.labels(
                    symbol=(symbol or "").strip().upper(),
                    reason=reason or "exchange_truth_active",
                ).inc()

    # ------------------------------------------------------------------
    # Manual hold enforcement
    # ------------------------------------------------------------------

    def load_manual_hold(self, symbol: str) -> dict[str, Any]:
        """Load the manual symbol hold document from Redis."""
        import json
        try:
            prefix = "orders:manual_hold:"
            key = f"{prefix}{(symbol or '').strip().upper()}"
            raw = self.r.get(key)
            if not raw:
                return {}
            doc = json.loads(raw)
            return doc if isinstance(doc, dict) else {}
        except Exception:
            return {}

    def guard_symbol_not_manually_held(self, *, symbol: str, action: str) -> None:
        """Raise if symbol is under manual hold. Writes event and raises RuntimeError."""
        hold_doc = self.load_manual_hold(symbol)
        if not hold_doc:
            return
        reason = (hold_doc.get("reason") or "manual_hold").strip() or "manual_hold"
        held_by = (hold_doc.get("held_by") or hold_doc.get("operator") or "unknown").strip()
        if self._write_event_fn:
            self._write_event_fn({
                "symbol": symbol,
                "action": action,
                "event_type": "MANUAL_HOLD_BLOCKED",
                "severity": "warning",
                "msg": f"action blocked: symbol under manual hold by {held_by}: {reason}",
                "hold_reason": reason,
                "held_by": held_by,
            })
        raise RuntimeError(f"symbol {symbol} under manual hold ({reason})")

    # ------------------------------------------------------------------
    # Open guard (single-active-position enforcement)
    # ------------------------------------------------------------------

    def guard_single_active_symbol_open(
        self,
        *,
        sid: str,
        symbol: str,
        payload: dict[str, Any],
        state_load_fn: Any,  # Callable[[str], dict]
        client: "BinanceFuturesClient | None" = None,
    ) -> None:
        """Block handle_open if this symbol already has an active execution.

        Raises OpenBlockedByActiveSymbolError if blocked.
        Raises RuntimeError for quarantined SIDs.

        Exchange-truth release (P5): if the blocking guard shows terminal state,
        we query Binance directly to confirm flat before releasing.
        """
        if not self.exec_single_active_position_per_symbol:
            return

        guard = self.load(symbol)
        if not guard:
            return

        blocked_by_sid = (guard.get("sid") or "").strip()
        if not blocked_by_sid:
            return
        if blocked_by_sid == sid:
            # Same SID resuming — allow
            return

        # Load state of blocking SID
        blocked_state_doc = state_load_fn(blocked_by_sid) if blocked_by_sid else {}
        is_terminal = self._state_is_terminalish(blocked_state_doc)

        # Stale guard: check if blocking SID has timed out
        stale_timeout_ms = self.exec_single_active_position_stale_timeout_ms
        guard_created_at = _i(guard.get("created_at_ms") or 0)
        if guard_created_at > 0 and stale_timeout_ms > 0:
            guard_age_ms = max(0, _ms_now() - guard_created_at)
            if guard_age_ms > stale_timeout_ms and is_terminal:
                # Stale terminal guard — release and allow
                self.mark_released(symbol, expected_sid=blocked_by_sid)
                return

        # Exchange-truth release
        if self.exec_single_active_position_exchange_truth_release and is_terminal:
            exchange_truth = self.read_exchange_truth(symbol=symbol, client=client)
            if exchange_truth.get("is_flat"):
                self.mark_released(symbol, expected_sid=blocked_by_sid)
                with contextlib.suppress(Exception):
                    if EXECUTION_ACTIVE_SYMBOL_GUARD_RELEASE_TOTAL is not None:
                        EXECUTION_ACTIVE_SYMBOL_GUARD_RELEASE_TOTAL.labels(
                            symbol=symbol.strip().upper(), reason="exchange_flat"
                        ).inc()
                return
            # Not flat — annotate guard with exchange truth, block
            self.refresh_from_exchange(
                symbol=symbol,
                blocked_by_sid=blocked_by_sid,
                guard=guard,
                blocked_state_doc=blocked_state_doc,
                exchange_truth=exchange_truth,
                reason="exchange_truth_not_flat",
            )

        # Block the open
        details = {
            "symbol": symbol,
            "blocked_by_sid": blocked_by_sid,
            "reason": "single_active_position_per_symbol",
            "blocked_state": (blocked_state_doc.get("fsm_state") or "unknown"),
            "guard_created_at_ms": guard_created_at,
        }
        with contextlib.suppress(Exception):
            if EXECUTION_OPEN_BLOCKED_ACTIVE_SYMBOL_TOTAL is not None:
                EXECUTION_OPEN_BLOCKED_ACTIVE_SYMBOL_TOTAL.labels(
                    symbol=(symbol or "").strip().upper(),
                    blocked_state=details["blocked_state"],
                ).inc()
        raise OpenBlockedByActiveSymbolError(details)
