from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""P5: Binance Active-Symbol Guard Repair Worker.

Background repair loop that scans all ``orders:active_symbol_sid:*`` keys and
releases stuck guards when Binance confirms the symbol is flat (positionAmt == 0
AND no open plain/algo orders).

This is the *second* release contour: the first is inline inside
BinanceExecutor._guard_single_active_symbol_open when a new open arrives.  This
worker clears stuck guards proactively, without waiting for a new signal.

ENV:
  ACTIVE_SYMBOL_GUARD_REPAIR_INTERVAL_SEC   – poll interval (default 5 s)
  ACTIVE_SYMBOL_GUARD_REPAIR_DRY_RUN        – 1 = log actions, do not delete keys
  EXEC_SINGLE_ACTIVE_POSITION_REQUIRE_FLAT_NO_ORDERS – 1 = require flat + no-orders
  ORDERS_ACTIVE_SYMBOL_KEY_PREFIX           – default orders:active_symbol_sid:
  ORDERS_STATE_KEY_PREFIX                   – default orders:state:
  USER_STREAM_STATUS_KEY                    – default orders:user_stream:status
  REDIS_URL                                 – default redis://redis-worker-1:6379/0
  ORDERS_STATE_TTL_SEC                      – guard key TTL after update (86400)
"""

import json
import math
import os
import time
from typing import Any, Dict, List, Optional

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

try:
    from services.binance_futures_client import BinanceFuturesClient
except Exception:  # pragma: no cover
    from binance_futures_client import BinanceFuturesClient  # type: ignore

try:
    from services.execution_metrics import (
        EXECUTION_ACTIVE_SYMBOL_GUARD_EXCHANGE_CHECK_TOTAL,
        EXECUTION_ACTIVE_SYMBOL_GUARD_CAS_TOTAL,
        EXECUTION_ACTIVE_SYMBOL_GUARD_RELEASE_TOTAL,
        EXECUTION_ACTIVE_SYMBOL_GUARD_RELEASED_TOMBSTONE_AGE_MS,
        EXECUTION_ACTIVE_SYMBOL_GUARD_STUCK_TOTAL,
    )
except Exception:  # pragma: no cover
    try:
        from execution_metrics import (  # type: ignore
            EXECUTION_ACTIVE_SYMBOL_GUARD_EXCHANGE_CHECK_TOTAL,
            EXECUTION_ACTIVE_SYMBOL_GUARD_CAS_TOTAL,
            EXECUTION_ACTIVE_SYMBOL_GUARD_RELEASE_TOTAL,
            EXECUTION_ACTIVE_SYMBOL_GUARD_STUCK_TOTAL,
        )
    except Exception:  # pragma: no cover
        EXECUTION_ACTIVE_SYMBOL_GUARD_EXCHANGE_CHECK_TOTAL = None  # type: ignore
        EXECUTION_ACTIVE_SYMBOL_GUARD_CAS_TOTAL = None  # type: ignore
        EXECUTION_ACTIVE_SYMBOL_GUARD_RELEASE_TOTAL = None  # type: ignore
        EXECUTION_ACTIVE_SYMBOL_GUARD_RELEASED_TOMBSTONE_AGE_MS = None  # type: ignore
        EXECUTION_ACTIVE_SYMBOL_GUARD_STUCK_TOTAL = None  # type: ignore

try:
    from services.active_symbol_guard_semantics import guard_view
    from services.active_symbol_guard_store import ActiveSymbolGuardStore
except Exception:
    from active_symbol_guard_semantics import guard_view
    from active_symbol_guard_store import ActiveSymbolGuardStore


def _ms_now() -> int:
    return get_ny_time_millis()


def _i(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return int(default)


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


class BinanceActiveSymbolGuardRepairWorker:
    """Scans active-symbol guard keys and releases those that Binance confirms are flat.

    Two-loop architecture:
      * run_once()     – one scan-and-repair cycle (testable)
      * run_forever()  – main loop with configurable sleep interval
    """

    def __init__(
        self,
        redis_client=None,
        client: Optional[BinanceFuturesClient] = None,
    ) -> None:
        if redis_client is None and redis is None:
            raise RuntimeError("redis-py is required")
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.r = (
            redis_client
            if redis_client is not None
            else redis.from_url(self.redis_url, decode_responses=True)
        )
        self.client = (
            client
            if client is not None
            else BinanceFuturesClient.from_env(
                prefix=(os.getenv("BINANCE_PREFIX") or "BINANCE_")
            )
        )
        self.active_symbol_key_prefix = (
            os.getenv("ORDERS_ACTIVE_SYMBOL_KEY_PREFIX") or "orders:active_symbol_sid:"
        ).rstrip(":") + ":"
        self.state_key_prefix = (
            os.getenv("ORDERS_STATE_KEY_PREFIX") or "orders:state:"
        ).rstrip(":") + ":"
        self.user_stream_status_key = os.getenv(
            "USER_STREAM_STATUS_KEY", "orders:user_stream:status"
        )
        # How often to poll (seconds)
        self.interval_sec = float(os.getenv("ACTIVE_SYMBOL_GUARD_REPAIR_INTERVAL_SEC", "5"))
        # Whether to require flat + no-orders (not just flat position) for release
        self.require_flat_no_orders = (
            str(os.getenv("EXEC_SINGLE_ACTIVE_POSITION_REQUIRE_FLAT_NO_ORDERS", "1"))
            .strip()
            .lower()
            not in {"0", "false", "no", "off"}
        )
        # Dry-run: log but do not delete guard keys
        self.dry_run = (
            str(os.getenv("ACTIVE_SYMBOL_GUARD_REPAIR_DRY_RUN", "0"))
            .strip()
            .lower()
            in {"1", "true", "yes", "on"}
        )
        self.state_ttl_sec = int(os.getenv("ORDERS_STATE_TTL_SEC", "86400"))
        self.active_symbol_guard_tombstone_ttl_sec = int(os.getenv("ACTIVE_SYMBOL_GUARD_TOMBSTONE_TTL_SEC", "120"))

    def _guard_store(self) -> ActiveSymbolGuardStore:
        if not hasattr(self, '_guard_store_instance'):
            self._guard_store_instance = ActiveSymbolGuardStore(
                self.r,
                key_prefix=self.active_symbol_key_prefix,
                active_ttl_sec=self.state_ttl_sec,
                tombstone_ttl_sec=self.active_symbol_guard_tombstone_ttl_sec,
            )
        return self._guard_store_instance

    def _record_guard_cas(self, symbol: str, outcome: str, reason: str) -> None:
        try:
            if EXECUTION_ACTIVE_SYMBOL_GUARD_CAS_TOTAL is not None:
                EXECUTION_ACTIVE_SYMBOL_GUARD_CAS_TOTAL.labels(
                    symbol=str(symbol or "").strip().upper(),
                    writer="guard_repair",
                    outcome=str(outcome or ""),
                    reason=str(reason or "")
                ).inc()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # FSM helpers
    # ------------------------------------------------------------------

    def _state_is_terminalish(self, state: Optional[Dict[str, Any]]) -> bool:
        """Return True if the order-state document represents a closed/terminal position."""
        doc = dict(state or {})
        fsm_state = str(doc.get("fsm_state") or "").strip().upper()
        if fsm_state in {"CANCELLED", "CANCELED", "FAILED", "EXIT_FILLED", "EMERGENCY_FLATTENED"}:
            return True
        status = str(doc.get("status") or "").strip().lower()
        if status in {
            "closed", "cancelled", "canceled", "failed",
            "exited", "exit_filled", "emergency_flattened",
        }:
            return True
        return bool(doc.get("closed"))

    # ------------------------------------------------------------------
    # Redis helpers
    # ------------------------------------------------------------------

    def _load_json(self, key: str) -> Dict[str, Any]:
        try:
            raw = self.r.get(key)
            doc = json.loads(raw) if raw else {}
            return doc if isinstance(doc, dict) else {}
        except Exception:
            return {}

    def _load_active_symbol_guard(self, symbol: str) -> Dict[str, Any]:
        return self._guard_store().load_active(symbol)

    # ------------------------------------------------------------------
    # Exchange truth check
    # ------------------------------------------------------------------


    def _set_tombstone_age_metric(self, symbol: str, age_ms: int) -> None:
        try:
            if EXECUTION_ACTIVE_SYMBOL_GUARD_RELEASED_TOMBSTONE_AGE_MS is not None:
                EXECUTION_ACTIVE_SYMBOL_GUARD_RELEASED_TOMBSTONE_AGE_MS.labels(symbol=str(symbol or '').strip().upper()).set(max(0, int(age_ms or 0)))
        except Exception:
            pass

    def _read_exchange_truth(self, symbol: str) -> Dict[str, Any]:
        """Query Binance for real position and open-order state.

        Returns a structured result with is_flat=True iff:
          positionAmt == 0  AND (no open orders if require_flat_no_orders)
          AND all three API calls succeeded (is_reliable=True).
        """
        symbol = str(symbol or "").strip().upper()
        out: Dict[str, Any] = {
            "symbol": symbol,
            "checked_at_ms": _ms_now(),
            "position_amt": 0.0,
            "has_live_position": False,
            "open_plain_orders": 0,
            "open_algo_orders": 0,
            "has_open_orders": False,
            "errors": [],
            "is_reliable": False,
            "is_flat": False,
        }
        errors: List[str] = []
        # positionRisk check
        try:
            for pos in self.client.get_position_risk() or []:
                if str((pos or {}).get("symbol") or "").upper() != symbol:
                    continue
                amt = _f((pos or {}).get("positionAmt"), 0.0)
                out["position_amt"] = amt
                out["has_live_position"] = not math.isclose(float(amt), 0.0, abs_tol=1e-12)
                break
        except Exception as exc:
            errors.append(f"position_risk:{exc.__class__.__name__}")
        # Open plain orders check
        try:
            out["open_plain_orders"] = len(list(self.client.get_open_orders(symbol) or []))
        except Exception as exc:
            errors.append(f"open_orders:{exc.__class__.__name__}")
        # Open algo orders check
        try:
            out["open_algo_orders"] = len(list(self.client.get_open_algo_orders(symbol) or []))
        except Exception as exc:
            errors.append(f"open_algo_orders:{exc.__class__.__name__}")
        out["has_open_orders"] = (
            int(out["open_plain_orders"]) > 0 or int(out["open_algo_orders"]) > 0
        )
        out["errors"] = errors
        out["is_reliable"] = not errors
        out["is_flat"] = (
            not out["has_live_position"]
            and (not out["has_open_orders"] if self.require_flat_no_orders else True)
            and out["is_reliable"]
        )
        # Emit Prometheus metric for exchange-check result
        try:
            if EXECUTION_ACTIVE_SYMBOL_GUARD_EXCHANGE_CHECK_TOTAL is not None:
                result = "flat" if out["is_flat"] else ("active" if out["is_reliable"] else "error")
                EXECUTION_ACTIVE_SYMBOL_GUARD_EXCHANGE_CHECK_TOTAL.labels(
                    symbol=symbol, result=result
                ).inc()
        except Exception:
            pass
        return out

    # ------------------------------------------------------------------
    # Guard key management
    # ------------------------------------------------------------------

    def _clear_guard(self, symbol: str, expected_sid: str = "") -> bool:
        """Release the guard key using a tombstone. Returns True if successfully released."""
        symbol = str(symbol or "").strip().upper()
        if self.dry_run:
            return False
        try:
            res = self._guard_store().mark_released(
                symbol=symbol,
                expected_sid=expected_sid,
                release_reason="repair_worker_flat_no_orders",
                writer="guard_repair"
            )
            self._record_guard_cas(
                symbol, outcome="success" if res.get('applied') else "rejected", reason=res.get('reason') or "unknown"
            )
            return bool(res.get('applied'))
        except Exception:
            self._record_guard_cas(symbol, outcome="error", reason="exception")
            return False

    # ------------------------------------------------------------------
    # Single-symbol repair
    # ------------------------------------------------------------------

    def repair_one(self, symbol: str) -> Dict[str, Any]:
        """Attempt to release the guard for a single symbol.

        Returns a status dict:
          status: 'released' | 'noop' | 'blocked'
          reason: release/block reason string
        """
        symbol = str(symbol or "").strip().upper()
        guard = self._guard_store().load_raw(symbol)
        view = guard_view(guard)
        if view.get('is_released'):
            self._set_tombstone_age_metric(symbol, int(view.get('tombstone_age_ms') or 0))
            return {"symbol": symbol, "sid": str(view.get('sid') or ''), "status": "released_tombstone", "reason": "already_released", "tombstone_age_ms": int(view.get('tombstone_age_ms') or 0)}
        self._set_tombstone_age_metric(symbol, 0)
        sid = str(guard.get("sid") or "").strip()
        # Load the order state to check for terminal FSM
        state = self._load_json(f"{self.state_key_prefix}{sid}") if sid else {}
        # Query Binance exchange truth
        truth = self._read_exchange_truth(symbol)

        if truth.get("is_flat"):
            # Exchange confirmed: clear the guard
            cleared = self._clear_guard(symbol, expected_sid=sid)
            reason = "exchange_flat_no_orders"
            try:
                if cleared and EXECUTION_ACTIVE_SYMBOL_GUARD_RELEASE_TOTAL is not None:
                    EXECUTION_ACTIVE_SYMBOL_GUARD_RELEASE_TOTAL.labels(
                        symbol=symbol, reason=reason
                    ).inc()
            except Exception:
                pass
            return {
                "symbol": symbol,
                "sid": sid,
                "status": "released" if cleared else "noop",
                "reason": reason,
                "exchange_truth": truth,
            }

        # Exchange still shows active exposure or open orders — keep guard, annotate it
        reason = "exchange_truth_active"
        if truth.get("errors"):
            reason = "exchange_check_error"
        elif truth.get("has_live_position"):
            reason = "exchange_open_position"
        elif truth.get("has_open_orders"):
            reason = "exchange_open_orders"
        elif self._state_is_terminalish(state):
            reason = "terminal_state_but_exchange_not_flat"

        # Annotate guard key with latest exchange snapshot for operator visibility
        updated = dict(guard or {})
        updated.update({
            "symbol": symbol,
            "sid": sid,
            "updated_at_ms": _ms_now(),
            "exchange_truth_checked_at_ms": int(truth.get("checked_at_ms") or _ms_now()),
            "exchange_position_amt": float(truth.get("position_amt") or 0.0),
            "exchange_open_plain_orders": int(truth.get("open_plain_orders") or 0),
            "exchange_open_algo_orders": int(truth.get("open_algo_orders") or 0),
            "exchange_guard_reason": reason,
            # P6 unified semantic fields: same contract as executor and projection worker
            "guard_release_policy": "exchange_truth",
            "guard_release_pending": bool(self._state_is_terminalish(state)),
            "guard_release_reason": "await_exchange_flat_no_orders" if self._state_is_terminalish(state) else "",
            "state_terminalish": bool(self._state_is_terminalish(state)),
        })
        try:
            if not self.dry_run:
                res = self._guard_store().acquire_or_refresh(
                    symbol=symbol,
                    sid=sid,
                    payload_patch=updated,
                    writer="guard_repair"
                )
                self._record_guard_cas(
                    symbol, outcome="success" if res.get('applied') else "rejected", reason=res.get('reason') or "unknown"
                )
            if EXECUTION_ACTIVE_SYMBOL_GUARD_STUCK_TOTAL is not None:
                EXECUTION_ACTIVE_SYMBOL_GUARD_STUCK_TOTAL.labels(
                    symbol=symbol, reason=reason
                ).inc()
        except Exception:
            pass
        return {
            "symbol": symbol,
            "sid": sid,
            "status": "blocked",
            "reason": reason,
            "exchange_truth": truth,
            "state_terminalish": self._state_is_terminalish(state),
        }

    # ------------------------------------------------------------------
    # Main loops
    # ------------------------------------------------------------------

    def run_once(self) -> List[Dict[str, Any]]:
        """Scan all guard keys and repair each one. Returns list of results."""
        out: List[Dict[str, Any]] = []
        released_seen = set()
        prefix = f"{self.active_symbol_key_prefix}*"
        for key in list(self.r.scan_iter(match=prefix)):
            symbol = str(key).replace(self.active_symbol_key_prefix, "", 1).strip().upper()
            if not symbol:
                continue
            result = self.repair_one(symbol)
            if str(result.get('status') or '') == 'released_tombstone':
                released_seen.add(symbol)
            out.append(result)
        for symbol in set(getattr(self, '_last_released_tombstone_symbols', set())) - released_seen:
            self._set_tombstone_age_metric(symbol, 0)
        self._last_released_tombstone_symbols = released_seen
        return out

    def run_forever(self) -> None:  # pragma: no cover
        """Run repair loop until process exits."""
        while True:
            try:
                self.run_once()
            except Exception:
                pass
            time.sleep(max(0.25, self.interval_sec))


if __name__ == "__main__":  # pragma: no cover
    BinanceActiveSymbolGuardRepairWorker().run_forever()
