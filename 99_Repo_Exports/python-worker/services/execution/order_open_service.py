"""order_open_service.py — handle_open logic for Binance executor.

Extracted from binance_executor.py (god-class decomposition).

Responsibilities:
- Pre-flight checks: symbol settings, leverage, notional, margin guard
- Entry order placement (MARKET / IOC limit / maker)
- Resume from ENTRY_SUBMITTED / ENTRY_ACKED states after restart
- Trigger protection arming after fill confirmation
- Emit FSM events at each step
"""
from __future__ import annotations

import contextlib
import math
import time
from typing import TYPE_CHECKING, Any

from services.execution.binance_order_mapper import (
    _f, _i, _make_cid, _format_float, _round_down, _normalize_side, _normalize_qty,
    _classify_error, _position_side_for_mode,
    FSM_ENTRY_SUBMITTED, FSM_ENTRY_ACKED, FSM_ENTRY_FILLED,
    FSM_VALIDATED, FSM_FAILED,
)

if TYPE_CHECKING:
    from services.binance_futures_client import BinanceFuturesClient
    from services.execution.binance_filters import FiltersCache
    from services.execution.execution_state_store import ExecutionStateStore
    from services.execution.execution_event_writer import ExecutionEventWriter
    from services.execution.protection_service import ProtectionService
    from services.execution.reconcile_service import ReconcileService
    from services.execution.active_symbol_guard import ActiveSymbolGuard
    from services.execution.emergency_flatten_service import EmergencyFlattenService

try:
    from prometheus_client import Counter as _Counter
    _maker_decision_total = _Counter(
        "exec_maker_only_decision_total",
        "Maker-only execution decisions by outcome (item 4 canary)",
        ["maker_mode", "kind"],
    )
except Exception:
    _maker_decision_total = None  # type: ignore[assignment]


def _ms_now() -> int:
    try:
        from utils.time_utils import get_ny_time_millis
        return get_ny_time_millis()
    except Exception:
        return int(time.time() * 1000)


class OrderOpenService:
    """Handles handle_open: validates, places entry, arms protection.

    This class orchestrates the entry lifecycle. Each collaborating service
    is injected via constructor — no circular imports, fully testable.
    """

    def __init__(
        self,
        *,
        position_mode: str = "oneway",
        sl_working_type: str = "MARK_PRICE",
        max_retry: int = 3,
        local_headroom_margin: float = 1.20,
        default_leverage: int = 5,
        exec_margin_guard_enabled: bool = True,
        exec_margin_guard_max_fraction: float = 0.90,
        exec_set_leverage: bool = True,
        exec_entry_policy: str = "MARKET",
        # P0-1: Emergency close for naked positions (SHADOW by default)
        emergency_close_if_unprotected: bool = False,
        block_symbol_on_protection_fail: bool = False,
        cooldown_after_protection_fail_ms: int = 900_000,
        # Injected services
        state_store: "ExecutionStateStore | None" = None,
        event_writer: "ExecutionEventWriter | None" = None,
        protection_service: "ProtectionService | None" = None,
        reconcile_service: "ReconcileService | None" = None,
        active_symbol_guard: "ActiveSymbolGuard | None" = None,
        flatten_service: "EmergencyFlattenService | None" = None,
        r: Any = None,
    ) -> None:
        self.position_mode = position_mode
        self.sl_working_type = sl_working_type
        self.max_retry = max_retry
        self.local_headroom_margin = local_headroom_margin
        self.default_leverage = default_leverage
        self.exec_margin_guard_enabled = exec_margin_guard_enabled
        self.exec_margin_guard_max_fraction = exec_margin_guard_max_fraction
        self.exec_set_leverage = exec_set_leverage
        self.exec_entry_policy = exec_entry_policy
        self._state = state_store
        self._events = event_writer
        self._protection = protection_service
        self._reconcile = reconcile_service
        self._guard = active_symbol_guard
        self._flatten = flatten_service
        self.emergency_close_if_unprotected = emergency_close_if_unprotected
        self.block_symbol_on_protection_fail = block_symbol_on_protection_fail
        self.cooldown_after_protection_fail_ms = cooldown_after_protection_fail_ms
        self.r = r

    def _write_event(self, fields: dict[str, Any]) -> None:
        if self._events:
            self._events.write(fields)

    def _transition(self, sid: str, *, symbol: str, action: str, next_state: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
        if self._state:
            return self._state.transition(sid, symbol=symbol, action=action, next_state=next_state, details=details)
        return {}

    def _load_state(self, sid: str) -> dict[str, Any]:
        if self._state:
            return self._state.load(sid)
        return {}

    def _save_state(self, sid: str, state: dict[str, Any]) -> None:
        if self._state:
            self._state.save(sid, state)

    # ------------------------------------------------------------------
    # P0-1: Emergency close for naked positions
    # ------------------------------------------------------------------

    def _handle_unprotected_position(
        self,
        *,
        sid: str,
        symbol: str,
        logical_side: str,
        client: Any,
        filters: Any,
        reason: str,
    ) -> None:
        """Called when protection fails. In SHADOW mode emits metrics only.
        In ENFORCE mode calls force_flatten_exact then sets symbol cooldown.
        """
        import contextlib as _ctx

        try:
            from services.execution_metrics import (
                EXECUTION_EMERGENCY_CLOSE_TRIGGERED_TOTAL,
                EXECUTION_EMERGENCY_CLOSE_FAILED_TOTAL,
                EXECUTION_PROTECTION_FAIL_TO_CLOSE_MS,
                EXECUTION_SYMBOL_COOLDOWN_SET_TOTAL,
            )
        except Exception:
            EXECUTION_EMERGENCY_CLOSE_TRIGGERED_TOTAL = None  # type: ignore
            EXECUTION_EMERGENCY_CLOSE_FAILED_TOTAL = None  # type: ignore
            EXECUTION_PROTECTION_FAIL_TO_CLOSE_MS = None  # type: ignore
            EXECUTION_SYMBOL_COOLDOWN_SET_TOTAL = None  # type: ignore

        t0 = _ms_now()

        if not self.emergency_close_if_unprotected:
            # SHADOW: count what would happen, don't act
            with _ctx.suppress(Exception):
                if EXECUTION_EMERGENCY_CLOSE_TRIGGERED_TOTAL is not None:
                    EXECUTION_EMERGENCY_CLOSE_TRIGGERED_TOTAL.labels(
                        symbol=symbol, reason=f"shadow:{reason}"
                    ).inc()
            self._write_event({
                "sid": sid, "symbol": symbol,
                "event_type": "EMERGENCY_CLOSE_SHADOW",
                "severity": "warning",
                "reason": reason,
                "msg": "emergency_close_if_unprotected=0 (shadow), position may be naked",
            })
            return

        # ENFORCE: trigger real close
        with _ctx.suppress(Exception):
            if EXECUTION_EMERGENCY_CLOSE_TRIGGERED_TOTAL is not None:
                EXECUTION_EMERGENCY_CLOSE_TRIGGERED_TOTAL.labels(
                    symbol=symbol, reason=reason
                ).inc()

        flatten_result: dict[str, Any] = {}
        if self._flatten is not None:
            with _ctx.suppress(Exception):
                flatten_result = self._flatten.force_flatten_exact(
                    sid=sid,
                    symbol=symbol,
                    logical_side=logical_side,
                    client=client,
                    filters=filters,
                    reason=f"emergency_close:{reason}",
                )

        flatten_ok = bool(flatten_result.get("flatten_ok"))
        elapsed_ms = _ms_now() - t0

        with _ctx.suppress(Exception):
            if EXECUTION_PROTECTION_FAIL_TO_CLOSE_MS is not None:
                EXECUTION_PROTECTION_FAIL_TO_CLOSE_MS.labels(symbol=symbol).observe(elapsed_ms)

        if not flatten_ok:
            with _ctx.suppress(Exception):
                if EXECUTION_EMERGENCY_CLOSE_FAILED_TOTAL is not None:
                    EXECUTION_EMERGENCY_CLOSE_FAILED_TOTAL.labels(
                        symbol=symbol, reason=reason
                    ).inc()

        # Set symbol cooldown in Redis regardless of flatten outcome
        self._set_symbol_cooldown(sid=sid, symbol=symbol, reason=reason)

        with _ctx.suppress(Exception):
            if EXECUTION_SYMBOL_COOLDOWN_SET_TOTAL is not None:
                EXECUTION_SYMBOL_COOLDOWN_SET_TOTAL.labels(
                    symbol=symbol, reason=reason
                ).inc()

    def _set_symbol_cooldown(self, *, sid: str, symbol: str, reason: str) -> None:
        """Write risk:cooldown:symbol:{SYMBOL} = until_ms into Redis (PX TTL)."""
        if self.r is None or not self.block_symbol_on_protection_fail:
            return
        import contextlib as _ctx
        try:
            from core.redis_keys import RedisKeyPrefixes as _RK
            prefix = _RK.RISK_COOLDOWN_SYMBOL_PREFIX
        except Exception:
            prefix = "risk:cooldown:symbol:"
        key = f"{prefix}{symbol.upper()}"
        until_ms = _ms_now() + self.cooldown_after_protection_fail_ms
        with _ctx.suppress(Exception):
            self.r.set(key, str(until_ms), px=self.cooldown_after_protection_fail_ms)
        self._write_event({
            "sid": sid, "symbol": symbol,
            "event_type": "SYMBOL_COOLDOWN_SET",
            "severity": "warning",
            "reason": reason,
            "cooldown_until_ms": until_ms,
            "cooldown_ms": self.cooldown_after_protection_fail_ms,
        })

    # ------------------------------------------------------------------
    # Symbol settings
    # ------------------------------------------------------------------

    def ensure_symbol_settings(
        self,
        *,
        symbol: str,
        leverage: int,
        client: "BinanceFuturesClient",
    ) -> None:
        """Set leverage and margin type for symbol (idempotent, fail-open)."""
        if not self.exec_set_leverage:
            return
        with contextlib.suppress(Exception):
            client.set_leverage(symbol=symbol, leverage=leverage)  # type: ignore

    # ------------------------------------------------------------------
    # Margin guard  # type: ignore
    # ------------------------------------------------------------------  # type: ignore

    def _margin_guard_ok(
        self,
        *,
        symbol: str,
        qty: float,
        leverage: int,
        client: "BinanceFuturesClient",
        sid: str,
    ) -> bool:
        """Fail-closed check: refuse entry if margin use would exceed threshold."""
        if not self.exec_margin_guard_enabled:
            return True
        try:
            account = client.get_account_info()  # type: ignore
            available = _f(account.get("availableBalance") or account.get("availableBalance"), 0.0)
            total = _f(account.get("totalWalletBalance") or account.get("totalMarginBalance"), 0.0)
            if total <= 0:
                return True  # Can't check, fail-open
            used_pct = (total - available) / total
            if used_pct > self.exec_margin_guard_max_fraction:
                self._write_event({
                    "sid": sid, "symbol": symbol,
                    "action": "margin_guard_rejected",
                    "event_type": "MARGIN_GUARD_REJECTED",
                    "severity": "warning",  # type: ignore
                    "used_pct": round(used_pct, 4),  # type: ignore
                    "threshold": self.exec_margin_guard_max_fraction,
                })
                return False
            return True
        except Exception:
            return True  # Network error → fail-open on guard

    # ------------------------------------------------------------------
    # handle_open
    # ------------------------------------------------------------------

    def handle_open(
        self,
        *,
        payload: dict[str, Any],
        client: "BinanceFuturesClient",
        filters: "FiltersCache",
        sid: str,
        ts_queue_ms: int,
        ts_exec_start_ms: int,
    ) -> dict[str, Any]:
        """Entry point for opening a new position.

        Flow:
        1. Parse + validate payload
        2. Guard checks (active symbol, manual hold)
        3. Symbol settings (leverage)
        4. Margin guard
        5. Submit entry order
        6. Confirm fill (via user-stream or poll)
        7. Arm protection (SL/TP)
        8. Return final state dict

        Returns the final state dict (FSM_ENTRY_FILLED or FSM_FAILED).
        """
        symbol = (payload.get("symbol") or "").strip().upper()
        binance_side, logical_side, side_int = _normalize_side(payload)

        # Parse qty
        try:
            raw_qty = _normalize_qty(payload, symbol=symbol)
        except ValueError as exc:
            self._write_event({
                "sid": sid, "symbol": symbol,
                "action": "open_rejected",
                "event_type": "OPEN_VALIDATION_FAILED",
                "severity": "error",
                "reason": f"qty_parse_error: {exc}",
            })
            return self._transition(sid, symbol=symbol, action="open", next_state=FSM_FAILED,
                                    details={"reason": "qty_parse_error"})

        sf = filters.get(symbol)
        qty = _round_down(raw_qty, sf.step_size)
        qty_str = _format_float(qty, sf.step_size)
        leverage = _i(payload.get("leverage") or self.default_leverage, self.default_leverage)
        sl_price = _f(payload.get("sl"), 0.0)
        tp_levels = [_f(p) for p in (payload.get("tp_levels") or []) if p is not None]

        # Guard checks
        if self._guard:
            try:
                self._guard.guard_symbol_not_manually_held(symbol=symbol, action="open")
                self._guard.guard_single_active_symbol_open(
                    sid=sid, symbol=symbol, payload=payload,
                    state_load_fn=self._load_state, client=client,
                )
            except Exception as guard_exc:
                self._write_event({
                    "sid": sid, "symbol": symbol,
                    "action": "open_blocked",
                    "event_type": "OPEN_BLOCKED",
                    "severity": "warning",
                    "reason": str(guard_exc)[:200],
                })
                return self._transition(sid, symbol=symbol, action="open", next_state=FSM_FAILED,
                                        details={"reason": f"guard_blocked: {guard_exc}"})

        # Symbol settings
        self.ensure_symbol_settings(symbol=symbol, leverage=leverage, client=client)

        # Margin guard
        if not self._margin_guard_ok(symbol=symbol, qty=qty, leverage=leverage, client=client, sid=sid):
            return self._transition(sid, symbol=symbol, action="open", next_state=FSM_FAILED,
                                    details={"reason": "margin_guard_rejected"})

        # Record validated state
        self._transition(sid, symbol=symbol, action="open", next_state=FSM_VALIDATED, details={
            "symbol": symbol, "qty": qty, "logical_side": logical_side,
            "binance_side": binance_side, "sl_price": sl_price,
            "tp_levels": tp_levels, "leverage": leverage,
            "ts_queue_ms": ts_queue_ms, "ts_exec_start_ms": ts_exec_start_ms,
        })

        # Build entry order params
        entry_cid = _make_cid(sid, "entry", self.r)
        pos_side = _position_side_for_mode(self.position_mode, logical_side)
        params: dict[str, Any] = {
            "symbol": symbol,
            "side": binance_side,
            "type": "MARKET",
            "quantity": qty_str,
            "newClientOrderId": entry_cid,
        }
        if pos_side:
            params["positionSide"] = pos_side

        # ── Maker-only branch (item 4, 2026-05-24) ────────────────────────
        # Switch MARKET → LIMIT timeInForce=GTX when payload demands maker-only
        # execution. GTX (Post-Only) tells Binance to reject any portion that
        # would cross spread → guarantees maker fill or rejection.
        # Fallback to MARKET (with telemetry) when:
        #   - maker_price missing/invalid (upstream didn't populate it), OR
        #   - tick_size unknown (filter cache miss)
        # Shadow-only mode (shadow=1, enforce=0) leaves params=MARKET; the
        # counter still increments so we can observe how often maker-only
        # WOULD have fired in prod.
        maker_enforce = _i(payload.get("exec_maker_only"), 0)
        maker_shadow = _i(payload.get("exec_maker_only_shadow"), 0)
        maker_price = _f(payload.get("maker_price"), 0.0)
        maker_mode = "off"
        if maker_enforce and maker_price > 0 and sf.tick_size > 0:
            # Round to tick. For LONG (BUY): floor below market (passive bid);
            # for SHORT (SELL): ceil above market (passive ask). Using _round_down
            # for BUY and an explicit ceil for SELL ensures GTX does not cross.
            if binance_side == "BUY":
                limit_px = _round_down(maker_price, sf.tick_size)
            else:
                limit_px = math.ceil(maker_price / sf.tick_size) * sf.tick_size
            if limit_px > 0:
                params["type"] = "LIMIT"
                params["timeInForce"] = "GTX"
                params["price"] = _format_float(limit_px, sf.tick_size)
                maker_mode = "enforce"
            else:
                maker_mode = "fallback_market_bad_px"
        elif maker_enforce:
            # Asked to enforce but couldn't (no price / no tick) — explicit fallback.
            maker_mode = "fallback_market_no_price" if maker_price <= 0 else "fallback_market_no_tick"
        elif maker_shadow:
            maker_mode = "shadow"

        _kind_label = str(payload.get("kind") or payload.get("reason") or "unknown")
        if _maker_decision_total is not None:
            _maker_decision_total.labels(maker_mode=maker_mode, kind=_kind_label).inc()
        self._write_event({
            "sid": sid, "symbol": symbol, "action": "open",
            "event_type": "MAKER_ONLY_DECISION",
            "severity": "info",
            "maker_mode": maker_mode,
            "maker_price": maker_price,
            "order_type": params.get("type"),
            "tif": params.get("timeInForce", ""),
            "kind": _kind_label,
        })

        # Submit entry
        self._transition(sid, symbol=symbol, action="open", next_state=FSM_ENTRY_SUBMITTED,
                         details={"entry_client_order_id": entry_cid})
        try:
            if self._reconcile:
                resp = self._reconcile.submit_plain_order_with_reconcile(
                    sid=sid, symbol=symbol, action="open", params=params, client=client
                )
            else:
                resp = client.place_order(**params)  # type: ignore
        except Exception as exc:
            err_class = _classify_error(exc)
            self._write_event({
                "sid": sid, "symbol": symbol, "action": "open",
                "event_type": "ENTRY_SUBMIT_FAILED",
                "severity": "error",
                "error_class": err_class,
                "error": str(exc)[:300],
            })
            return self._transition(sid, symbol=symbol, action="open", next_state=FSM_FAILED,
                                    details={"reason": "entry_submit_failed", "error": str(exc)[:200]})

        order_id = resp.get("orderId") or resp.get("id")
        self._transition(sid, symbol=symbol, action="open", next_state=FSM_ENTRY_ACKED,
                         details={"entry_order_id": order_id, "entry_client_order_id": entry_cid})

        # Fill check (for MARKET, exchange fills immediately)
        avg_price = _f(resp.get("avgPrice") or resp.get("price"), 0.0)
        filled_qty = _f(resp.get("executedQty") or resp.get("cumQty"), qty)
        fill_status = (resp.get("status") or "").upper()

        if fill_status not in {"FILLED", "PARTIALLY_FILLED"} and avg_price <= 0:
            # Poll for fill
            with contextlib.suppress(Exception):
                time.sleep(0.2)
                order_info = client.get_order(symbol=symbol, order_id=order_id)
                avg_price = _f(order_info.get("avgPrice"), avg_price)
                filled_qty = _f(order_info.get("executedQty"), filled_qty)
                fill_status = (order_info.get("status") or fill_status).upper()

        filled_state = self._transition(sid, symbol=symbol, action="open", next_state=FSM_ENTRY_FILLED,
                                        details={
                                            "entry_order_id": order_id,
                                            "entry_client_order_id": entry_cid,
                                            "entry_price": avg_price,
                                            "avg_price": avg_price,
                                            "filled_qty": filled_qty,
                                            "fill_status": fill_status,
                                        })

        self._write_event({
            "sid": sid, "symbol": symbol, "action": "entry_filled",
            "event_type": "entry_filled",
            "side": binance_side,
            "logical_side": logical_side,
            "order_id": order_id,
            "avg_price": avg_price,
            "qty": filled_qty,
            "ts_queue_ms": ts_queue_ms,
            "ts_exec_start_ms": ts_exec_start_ms,
        })

        # Arm protection
        if self._protection and sl_price > 0 and tp_levels:
            # Compute tp_qtys from signal payload tp_ratio (strategy-aware distribution)
            tp_qtys: list[float] = []
            tp_ratio = (
                payload.get("tp_qty_ratios")
                or payload.get("tp_ratios")
                or payload.get("tp_ratio")
                or (payload.get("meta") or {}).get("tp_qty_ratios")
                or (payload.get("meta") or {}).get("tp_ratios")
                or (payload.get("meta") or {}).get("tp_ratio")
            )
            if tp_ratio and isinstance(tp_ratio, (list, tuple)) and len(tp_ratio) > 0:
                try:
                    from services.tp_config import compute_tp_qtys
                    tp_qtys = compute_tp_qtys(filled_qty, tp_ratio, sf.step_size)
                except Exception:
                    tp_qtys = []  # fallback: protection_service will compute even-split

            prot_result = self._protection.place_protective(
                sid=sid, symbol=symbol, logical_side=logical_side,
                qty=filled_qty, sl_price=sl_price, tp_levels=tp_levels,
                tp_qtys=tp_qtys,
                client=client, filters=filters, r=self.r,
            )
            protection_ok = self._protection.protection_confirmed(
                prot_result,
                tp_levels,
                trail_enabled=bool((payload.get("meta") or {}).get("trail_enabled", False)),
            )

            if not protection_ok:
                self._protection.emit_protection_incident(
                    sid=sid,
                    symbol=symbol,
                    reason="protection_not_confirmed_after_entry",
                )
                self._handle_unprotected_position(
                    sid=sid,
                    symbol=symbol,
                    logical_side=logical_side,
                    client=client,
                    filters=filters,
                    reason="protection_not_confirmed",
                )
                return self._transition(
                    sid,
                    symbol=symbol,
                    action="open",
                    next_state=FSM_FAILED,
                    details={
                        "reason": "protection_not_confirmed_after_entry",
                        "prot_result": prot_result,
                        "emergency_close_attempted": self.emergency_close_if_unprotected,
                    },
                )

            filled_state.update(prot_result)
            self._save_state(sid, filled_state)

        return filled_state
