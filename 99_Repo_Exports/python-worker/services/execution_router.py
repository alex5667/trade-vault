from __future__ import annotations

"""Execution Router — Variant B intent→queue pre-processor.

Reads from ``orders:intent:binance`` (BLPOP), applies routing logic:
- Passthrough when ``EXEC_ROUTER_ENABLE=0``
- For ``action=open``: check for existing same-side position and redirect to ``resize``
- All other actions: passthrough to ``orders:queue:binance``

Scale-in redirect conditions (all must pass when EXEC_ROUTER_SCALE_IN_ENABLE=1):
1. Existing active symbol guard for the symbol
2. Same logical side (LONG→LONG, SHORT→SHORT)
3. Owner stability (guard_status == "active", not pending release)
4. WCL budget check (worst_case_loss after add <= EXEC_ROUTER_RISK_BUDGET_USDT)
5. MAX_LEGS not exceeded
6. [P1-8] Owner FSM state must NOT be in a protection-arming or reconcile-pending state
7. [P1-8] If owner is in PROTECTION_ARMING, the arm must not have timed out

Emits routing events to ``orders:exec`` stream for auditability.

Rollback:
- EXEC_ROUTER_ENABLE=0  → pure passthrough (copy intent→queue unchanged)
- EXEC_ROUTER_SCALE_IN_ENABLE=0 → open actions pass through without redirect
- EXEC_ROUTER_RECONCILE_FIRST=0 → disables FSM state safety check

ENV (P1-8 additions):
  EXEC_ROUTER_RECONCILE_FIRST           bool, default True
      Block scale-in when owner is in PENDING_RECONCILE, PROTECTION_ARMING,
      or PROTECTION_REPLACING FSM states.
  EXEC_ROUTER_PROTECTION_ARM_TIMEOUT_MS int, default 5000
      If owner has been in PROTECTION_ARMING longer than this (ms), block
      scale-in regardless of reconcile_first flag — position may be unprotected.
"""
import json
import logging
import os
import signal
import time
from typing import Any

import redis as _redis_mod

from core.redis_keys import RedisStreams as RS
from utils.time_utils import get_ny_time_millis

logger = logging.getLogger("execution_router")

# ---------------------------------------------------------------------------
# P1-8: FSM states that are unsafe for scale-in (local constants, no executor import)
# ---------------------------------------------------------------------------
# Mirror of FSM_* constants from binance_executor.py — kept inline to avoid
# circular imports.  Update here if executor FSM state names change.

_FSM_PROTECTION_ARMING = "PROTECTION_ARMING"
_FSM_PROTECTION_REPLACING = "PROTECTION_REPLACING"
_FSM_PENDING_RECONCILE = "PENDING_RECONCILE"

# Any of these states indicates the owner position is in mid-flight and cannot
# safely absorb a scale-in resize at this moment.
_FSM_UNSAFE_FOR_SCALE_IN = frozenset({
    _FSM_PROTECTION_ARMING,
    _FSM_PROTECTION_REPLACING,
    _FSM_PENDING_RECONCILE,
})

# ---------------------------------------------------------------------------
# ENV config
# ---------------------------------------------------------------------------

def _env_bool(key: str, default: bool = False) -> bool:
    return os.getenv(key, str(default)).lower() in {"1", "true", "yes", "on"}


def _env_float(key: str, default: float = 0.0) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except (ValueError, TypeError):
        return default


def _env_int(key: str, default: int = 0) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except (ValueError, TypeError):
        return default


def _ms_now() -> int:
    return get_ny_time_millis()


# ---------------------------------------------------------------------------
# ExecutionRouter
# ---------------------------------------------------------------------------

class ExecutionRouter:
    """Pre-processor that sits between producers and BinanceExecutor.

    Producers write to ``intent_queue`` (default: orders:intent:binance).
    Router reads, applies logic, and writes to ``exec_queue`` (default: orders:queue:binance).
    """

    def __init__(self, redis_client: Any) -> None:
        self.r = redis_client

        # Queue names
        self.intent_queue = os.getenv("ORDERS_INTENT_BINANCE", RS.ORDERS_INTENT_BINANCE)
        self.exec_queue = os.getenv("ORDERS_QUEUE_BINANCE", RS.ORDERS_QUEUE_BINANCE)
        self.exec_stream = os.getenv("EXEC_STREAM", RS.ORDERS_EXEC)

        # Resolve position strategy from POSITION_STRATEGY enum + kill-switch
        try:
            from services.position_strategy import resolve_strategy, strategy_summary
            self._strategy = resolve_strategy()
            logger.info("📋 %s", strategy_summary(self._strategy))
        except ImportError:
            # Fallback if module not available
            self._strategy = None

        # Feature flags — resolved from POSITION_STRATEGY or direct ENV
        if self._strategy:
            self.enabled = self._strategy.router_enable
            self.scale_in_enabled = self._strategy.scale_in_enable
        else:
            self.enabled = _env_bool("EXEC_ROUTER_ENABLE", True)
            self.scale_in_enabled = _env_bool("EXEC_ROUTER_SCALE_IN_ENABLE", False)

        self.same_side_only = _env_bool("EXEC_ROUTER_SAME_SIDE_ONLY", True)
        self.require_owner_stable = _env_bool("EXEC_ROUTER_REQUIRE_OWNER_STABLE", True)
        self.require_wcl_budget = _env_bool("EXEC_ROUTER_REQUIRE_WCL_BUDGET", True)
        self.max_legs = _env_int("EXEC_ROUTER_MAX_LEGS", 3)

        # P1-8: reconcile-first and protection-arm-timeout guards
        # Block scale-in when owner FSM is in mid-flight state (PROTECTION_ARMING,
        # PROTECTION_REPLACING, PENDING_RECONCILE).  Default ON for safety.
        self.reconcile_first = _env_bool("EXEC_ROUTER_RECONCILE_FIRST", True)
        # Hard deadline for PROTECTION_ARMING: if the owner has been arming longer
        # than this many ms (even with reconcile_first=False), block scale-in.
        # Mirrors PROTECTION_ARM_TIMEOUT_MS in binance_executor.py (default 2500ms)
        # with an extra safety margin.  Set to 0 to disable the hard deadline.
        self.protection_arm_timeout_ms = _env_int("EXEC_ROUTER_PROTECTION_ARM_TIMEOUT_MS", 5000)

        # Dynamic WCL budget: per_leg = deposit * risk_pct / 100, total = per_leg * max_legs
        # Falls back to EXEC_ROUTER_RISK_BUDGET_USDT if set explicitly.
        self.deposit_usd = _env_float("ACCOUNT_DEPOSIT_USD", 0.0)
        self.risk_percent = _env_float("RISK_PERCENT", 5.0)
        explicit_budget = os.getenv("EXEC_ROUTER_RISK_BUDGET_USDT")
        if explicit_budget is not None:
            self.risk_budget_usdt = _env_float("EXEC_ROUTER_RISK_BUDGET_USDT", 50.0)
            self.risk_per_leg_usdt = self.risk_budget_usdt / max(self.max_legs, 1)
        elif self.deposit_usd > 0 and self.risk_percent > 0:
            self.risk_per_leg_usdt = self.deposit_usd * self.risk_percent / 100.0
            self.risk_budget_usdt = self.risk_per_leg_usdt * self.max_legs
        else:
            self.risk_per_leg_usdt = 50.0
            self.risk_budget_usdt = self.risk_per_leg_usdt * self.max_legs
        logger.info(
            "💰 WCL budget: per_leg=%.2f total=%.2f (deposit=%.0f risk=%.1f%% legs=%d)",
            self.risk_per_leg_usdt, self.risk_budget_usdt,
            self.deposit_usd, self.risk_percent, self.max_legs,
        )

        # Active symbol guard store
        self.active_symbol_prefix = os.getenv(
            "ORDERS_ACTIVE_SYMBOL_KEY_PREFIX", "orders:active_symbol_sid:"
        ).rstrip(":") + ":"
        self.state_prefix = os.getenv("ORDERS_STATE_KEY_PREFIX", "orders:state:")

        # Shutdown flag
        self._running = True

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _load_active_guard(self, symbol: str) -> dict[str, Any]:
        """Load active symbol guard doc from Redis."""
        key = f"{self.active_symbol_prefix}{symbol.upper()}"
        try:
            raw = self.r.get(key)
            if not raw:
                return {}
            doc = json.loads(raw)
            if not isinstance(doc, dict):
                return {}
            # Only return if guard is active (not released tombstone)
            status = (doc.get("guard_status") or "active").lower()
            if status == "released":
                return {}
            return doc
        except Exception:
            return {}

    def _load_state(self, sid: str) -> dict[str, Any]:
        """Load order state from Redis."""
        key = f"{self.state_prefix}{sid}"
        try:
            raw = self.r.get(key)
            if not raw:
                return {}
            doc = json.loads(raw)
            return doc if isinstance(doc, dict) else {}
        except Exception:
            return {}

    def _emit_route_event(self, payload: dict[str, Any]) -> None:
        """Emit routing event to orders:exec stream for auditability."""
        try:
            _maxlen = _env_int("EXEC_STREAM_MAXLEN", 50000)
            fields = {k: str(v) for k, v in payload.items() if v is not None}
            fields["event_type"] = "execution_route_event"
            fields["ts_event_ms"] = str(_ms_now())
            kwargs: dict[str, Any] = {}
            if _maxlen > 0:
                kwargs = {"maxlen": _maxlen, "approximate": True}
            self.r.xadd(self.exec_stream, fields, **kwargs)
        except Exception as e:
            logger.warning("Failed to emit route event: %s", e)

    def _passthrough(self, raw_msg: str) -> None:
        """Push message to exec queue unchanged."""
        self.r.rpush(self.exec_queue, raw_msg)

    # ------------------------------------------------------------------
    # Routing logic
    # ------------------------------------------------------------------

    def _check_scale_in_conditions(
        self, payload: dict[str, Any], guard: dict[str, Any], state: dict[str, Any]
    ) -> dict[str, Any]:
        """Check all scale-in conditions. Returns {"ok": bool, "reason": str, ...}."""

        # P1-8 [FIRST]: FSM state safety check — must run before any other conditions.
        # If the owner position is mid-flight (protection arming, reconcile pending,
        # protection replacing), scale-in redirect is unsafe regardless of other flags.
        owner_fsm = (state.get("fsm_state") or "").strip().upper()
        if owner_fsm:
            # 0a. Hard deadline: if arming has been running longer than
            #     protection_arm_timeout_ms (even with reconcile_first=False),
            #     the position may be unprotected — always block.
            if owner_fsm == _FSM_PROTECTION_ARMING and self.protection_arm_timeout_ms > 0:
                # fsm_ts_ms is written by _transition_state in the executor when
                # entering PROTECTION_ARMING.  Fall back to updated_at_ms / ts_ms.
                arm_started_ms = int(
                    state.get("fsm_ts_ms")
                    or state.get("updated_at_ms")
                    or state.get("ts_state_commit_ms")
                    or state.get("ts_ms")
                    or 0
                )
                if arm_started_ms > 0:
                    age_ms = _ms_now() - arm_started_ms
                    if age_ms > self.protection_arm_timeout_ms:
                        return {
                            "ok": False,
                            "reason": "protection_arm_timeout",
                            "owner_fsm": owner_fsm,
                            "arm_age_ms": age_ms,
                            "timeout_ms": self.protection_arm_timeout_ms,
                        }

            # 0b. Reconcile-first: block while owner is in any unsafe FSM state.
            if self.reconcile_first and owner_fsm in _FSM_UNSAFE_FOR_SCALE_IN:
                return {
                    "ok": False,
                    "reason": "owner_fsm_unsafe_for_scale_in",
                    "owner_fsm": owner_fsm,
                    "unsafe_states": sorted(_FSM_UNSAFE_FOR_SCALE_IN),
                }

        # 1. Same-side check
        if self.same_side_only:
            payload_side = (payload.get("side") or "").upper()
            guard_side = str(guard.get("side") or state.get("side") or "").upper()
            if payload_side and guard_side and payload_side != guard_side:
                return {"ok": False, "reason": "opposite_side", "payload_side": payload_side, "guard_side": guard_side}

        # 2. Owner stability check
        if self.require_owner_stable:
            guard_status = (guard.get("guard_status") or "").lower()
            release_pending = bool(guard.get("guard_release_pending"))
            if guard_status != "active" or release_pending:
                return {"ok": False, "reason": "owner_unstable", "guard_status": guard_status, "release_pending": release_pending}

        # 3. Max legs check
        current_legs = int(state.get("scale_in_seq") or 0) + 1  # original = 1 leg
        if current_legs >= self.max_legs:
            return {"ok": False, "reason": "max_legs_exceeded", "current_legs": current_legs, "max_legs": self.max_legs}

        # 4. WCL budget check
        if self.require_wcl_budget:
            try:
                from services.position_leg_policy import PositionLeg, max_add_qty_for_budget, worst_case_loss_usdt

                existing_entry = float(state.get("exec_price") or state.get("avg_price") or 0)
                existing_qty = float(state.get("qty") or state.get("filled_qty") or 0)
                existing_side = str(state.get("side") or guard.get("side") or "LONG").upper()
                sl = float(state.get("sl_requested") or state.get("sl") or 0)
                new_qty = float(payload.get("qty") or payload.get("lot") or 0)

                if existing_entry > 0 and existing_qty > 0 and sl > 0 and new_qty > 0:
                    legs = [PositionLeg(entry=existing_entry, qty=existing_qty, side=existing_side)]
                    max_add = max_add_qty_for_budget(legs, sl, self.risk_budget_usdt, new_entry=float(payload.get("entry") or 0))
                    if new_qty > max_add:
                        return {
                            "ok": False,
                            "reason": "wcl_budget_exceeded",
                            "new_qty": new_qty,
                            "max_add": max_add,
                            "budget": self.risk_budget_usdt,
                        }
            except ImportError:
                logger.warning("position_leg_policy not available, skipping WCL check")
            except Exception as e:
                logger.warning("WCL budget check error: %s", e)

        return {"ok": True, "reason": "all_checks_passed"}

    def _build_resize_payload(
        self, original_payload: dict[str, Any], guard: dict[str, Any], state: dict[str, Any]
    ) -> dict[str, Any]:
        """Transform open payload into resize payload for scale-in."""
        owner_sid = (guard.get("sid") or "").strip()
        new_qty = float(original_payload.get("qty") or original_payload.get("lot") or 0)
        current_seq = int(state.get("scale_in_seq") or 0)

        # Build TP schema for scale-in
        tp_qtys = None
        trail_activate = None
        try:
            from services.position_leg_policy import PositionLeg, build_scale_in_tp_schema

            existing_entry = float(state.get("exec_price") or state.get("avg_price") or 0)
            existing_qty = float(state.get("qty") or state.get("filled_qty") or 0)
            existing_side = (state.get("side") or "LONG").upper()
            tp_prices = original_payload.get("tp_levels") or state.get("tp_levels_requested") or []
            tp_prices = [float(x) for x in tp_prices if x not in (None, "")]

            if existing_entry > 0 and existing_qty > 0 and tp_prices:
                legs = [PositionLeg(entry=existing_entry, qty=existing_qty, side=existing_side)]
                _, tp_qtys, trail_activate = build_scale_in_tp_schema(legs, new_qty, tp_prices)
        except Exception as e:
            logger.warning("build_scale_in_tp_schema error: %s", e)

        resize_payload: dict[str, Any] = {
            "action": "resize",
            "sid": owner_sid,
            "symbol": (original_payload.get("symbol") or "").upper(),
            "resize_mode": "delta_qty",
            "delta_qty": new_qty,
            # Preserve SL/TP from original signal
            "sl": original_payload.get("sl") or state.get("sl_requested"),
            "tp_levels": original_payload.get("tp_levels") or state.get("tp_levels_requested"),
            "trail_after_tp1_requested": original_payload.get("trail_after_tp1") or state.get("trail_after_tp1_requested"),
            # Scale-in metadata
            "scale_in_seq": current_seq + 1,
            "source_signal_id": (original_payload.get("sid") or ""),
            "owner_sid": owner_sid,
            "ts_ms": int(original_payload.get("ts_ms") or _ms_now()),
            # Passthrough fields
            "is_virtual": original_payload.get("is_virtual"),
            "source": original_payload.get("source"),
            "strategy": original_payload.get("strategy"),
            "confidence": original_payload.get("confidence"),
        }

        if tp_qtys:
            resize_payload["tp_qtys_requested_json"] = json.dumps(tp_qtys)
        if trail_activate is not None:
            resize_payload["trail_activate_tp_level_requested"] = trail_activate

        # --- Calibration / shadow metadata passthrough ---
        # Scale-in must not lose attribution from the originating shadow signal.
        try:
            from services.shadow_calib_meta import extract_calib_fields
            calib = extract_calib_fields(original_payload)
            if calib:
                resize_payload.update(calib)
        except Exception:
            pass  # fail-open

        # Remove None values
        return {k: v for k, v in resize_payload.items() if v is not None}

    def route_one(self, raw_msg: str) -> dict[str, Any]:
        """Route one message. Returns routing result dict for observability."""
        try:
            payload = json.loads(raw_msg)
        except Exception:
            # Bad JSON — passthrough to DLQ handling in executor
            self._passthrough(raw_msg)
            return {"status": "passthrough", "reason": "bad_json"}

        action = (payload.get("action") or "").strip().lower()
        symbol = (payload.get("symbol") or "").strip().upper()
        sid = (payload.get("sid") or "").strip()

        # Non-open actions → passthrough
        if action != "open":
            self._passthrough(raw_msg)
            return {"status": "passthrough", "reason": f"action={action}"}

        # Router disabled → passthrough
        if not self.enabled or not self.scale_in_enabled:
            self._passthrough(raw_msg)
            return {"status": "passthrough", "reason": "router_disabled"}

        # Check for existing position via active symbol guard
        guard = self._load_active_guard(symbol)
        if not guard:
            # No existing position → passthrough as normal open
            self._passthrough(raw_msg)
            return {"status": "passthrough", "reason": "no_existing_position"}

        owner_sid = (guard.get("sid") or "").strip()
        if not owner_sid:
            self._passthrough(raw_msg)
            return {"status": "passthrough", "reason": "no_owner_sid"}

        # Load owner state for condition checks
        state = self._load_state(owner_sid)
        if not state:
            self._passthrough(raw_msg)
            self._emit_route_event({
                "sid": sid, "symbol": symbol, "action": "route_open",
                "route_decision": "passthrough", "reason": "owner_state_missing",
                "owner_sid": owner_sid,
            })
            return {"status": "passthrough", "reason": "owner_state_missing"}

        # Run condition checks
        check = self._check_scale_in_conditions(payload, guard, state)
        if not check.get("ok"):
            # Conditions not met → passthrough as normal open
            self._passthrough(raw_msg)
            self._emit_route_event({
                "sid": sid, "symbol": symbol, "action": "route_open",
                "route_decision": "passthrough", "reason": check.get("reason"),
                "owner_sid": owner_sid,
            })
            return {"status": "passthrough", "reason": check.get("reason"), "check": check}

        # All conditions passed → redirect open → resize
        resize_payload = self._build_resize_payload(payload, guard, state)
        resize_json = json.dumps(resize_payload, ensure_ascii=False)
        self.r.rpush(self.exec_queue, resize_json)

        self._emit_route_event({
            "sid": sid, "symbol": symbol, "action": "route_open_to_resize",
            "route_decision": "scale_in",
            "owner_sid": owner_sid,
            "new_sid": sid,
            "scale_in_seq": resize_payload.get("scale_in_seq"),
            "delta_qty": resize_payload.get("delta_qty"),
        })

        logger.info(
            "🔀 [ROUTER] (%s) open→resize: sid=%s → owner=%s seq=%s delta_qty=%s",
            symbol, sid, owner_sid,
            resize_payload.get("scale_in_seq"),
            resize_payload.get("delta_qty"),
        )
        return {"status": "scale_in", "owner_sid": owner_sid, "resize_payload": resize_payload}

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Main BLPOP loop — blocks until message arrives or shutdown."""
        strategy_name = self._strategy.name if self._strategy else "direct_env"
        logger.info(
            "🚀 ExecutionRouter started: intent=%s → exec=%s | strategy=%s enabled=%s scale_in=%s",
            self.intent_queue, self.exec_queue, strategy_name, self.enabled, self.scale_in_enabled,
        )

        while self._running:
            try:
                result = self.r.blpop(self.intent_queue, timeout=5)
                if result is None:
                    continue
                _, raw_msg = result
                if isinstance(raw_msg, bytes):
                    raw_msg = raw_msg.decode("utf-8", "replace")
                self.route_one(str(raw_msg))
            except KeyboardInterrupt:
                break
            except getattr(_redis_mod.exceptions, "BusyLoadingError", type("DummyError", (Exception,), {})):
                logger.warning("⏳ Redis is loading dataset in memory, waiting 5s...")
                time.sleep(5.0)
            except Exception as e:
                if "loading the dataset in memory" in str(e).lower():
                    logger.warning("⏳ Redis is loading dataset in memory, waiting 5s...")
                    time.sleep(5.0)
                else:
                    logger.error("Router loop error: %s", e, exc_info=True)
                    time.sleep(1.0)

        logger.info("ExecutionRouter shutdown complete")

    def stop(self) -> None:
        self._running = False


# ---------------------------------------------------------------------------
# __main__ entry point
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = _redis_mod.from_url(redis_url, decode_responses=False)
    router = ExecutionRouter(r)

    def _shutdown(signum, frame):
        logger.info("Received signal %s, shutting down...", signum)
        router.stop()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    router.run()


if __name__ == "__main__":
    main()
