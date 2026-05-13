"""trailing_service.py — Trailing stop management for Binance executor.

Extracted from binance_executor.py (god-class decomposition).

Responsibilities:
- Native trailing: TRAILING_STOP_MARKET placement after TP1 confirmed
- Orchestrator trailing: profile-based continuous SL-move loop
- Profile SL computation (_compute_profile_sl)
- SL order replacement on exchange (_replace_sl_order_on_exchange)
- Activation price computation
- Trailing arm notifications
"""
from __future__ import annotations

import contextlib
import threading
import time
from typing import TYPE_CHECKING, Any

from services.execution.binance_order_mapper import (
    _f, _make_cid, _format_float, _position_side_for_mode, _round_down,
    _round_half_up,
    compute_trailing_callback_rate_pct,
    compute_trailing_activate_price,
)

if TYPE_CHECKING:
    from services.binance_futures_client import BinanceFuturesClient
    from services.execution.binance_filters import FiltersCache

try:
    from services.trailing_profiles import TrailingProfile, TrailingProfilesRegistry
    _HAS_TRAILING_PROFILES = True
except Exception:
    _HAS_TRAILING_PROFILES = False
    TrailingProfilesRegistry = None  # type: ignore[assignment]
    TrailingProfile = None  # type: ignore[assignment]

try:
    from services.trailing_condition import TrailingConditionConfig, TrailingConditionEvaluator
    _HAS_TRAILING_CONDITION = True
except Exception:
    _HAS_TRAILING_CONDITION = False
    TrailingConditionEvaluator = None  # type: ignore[assignment]
    TrailingConditionConfig = None  # type: ignore[assignment]


def _ms_now() -> int:
    try:
        from utils.time_utils import get_ny_time_millis
        return get_ny_time_millis()
    except Exception:
        return int(time.time() * 1000)


class TrailingService:
    """Manages trailing stop placement and orchestration.

    Supports two modes:
    - native: TRAILING_STOP_MARKET order placed on exchange (Binance-native)
    - orchestrator: profile-based continuous SL move loop (custom logic)

    All arming operations run in daemon threads to not block the main loop.
    """

    def __init__(
        self,
        *,
        trail_mode: str = "orchestrator",
        trail_profile_name: str = "rocket_v1",
        trail_cb_min: float = 0.1,
        trail_cb_max: float = 5.0,
        trail_cb_default: float = 0.3,
        trail_atr_mult_default: float = 1.0,
        trail_arm_poll_s: float = 1.0,
        trail_arm_timeout_s: float = 7200.0,
        trail_notify: bool = True,
        trail_activate_price_bps: float = 5.0,
        trail_activate_tp_level: int = 2,
        trail_sl_move_min_delta_pct: float = 0.05,
        trail_loop_poll_s: float = 2.0,
        trail_loop_timeout_s: float = 14400.0,
        trail_working_type: str = "MARK_PRICE",
        position_mode: str = "oneway",
        write_event_fn: Any = None,
        telegram_fn: Any = None,
        r: Any = None,
    ) -> None:
        self.trail_mode = trail_mode
        self.trail_profile_name = trail_profile_name
        self.trail_cb_min = trail_cb_min
        self.trail_cb_max = trail_cb_max
        self.trail_cb_default = trail_cb_default
        self.trail_atr_mult_default = trail_atr_mult_default
        self.trail_arm_poll_s = trail_arm_poll_s
        self.trail_arm_timeout_s = trail_arm_timeout_s
        self.trail_notify = trail_notify
        self.trail_activate_price_bps = trail_activate_price_bps
        self.trail_activate_tp_level = trail_activate_tp_level
        self.trail_sl_move_min_delta_pct = trail_sl_move_min_delta_pct
        self.trail_loop_poll_s = trail_loop_poll_s
        self.trail_loop_timeout_s = trail_loop_timeout_s
        self.trail_working_type = trail_working_type
        self.position_mode = position_mode
        self._write_event_fn = write_event_fn
        self._telegram_fn = telegram_fn
        self.r = r

        self._arm_counts: dict[str, int] = {}
        self._arm_lock = threading.Lock()

        # Trailing profiles (fail-open)
        self._profiles: Any = None
        self._condition: Any = None
        if trail_mode == "orchestrator" and _HAS_TRAILING_PROFILES:
            with contextlib.suppress(Exception):
                self._profiles = TrailingProfilesRegistry()  # type: ignore
        if trail_mode == "orchestrator" and _HAS_TRAILING_CONDITION and r is not None:
            with contextlib.suppress(Exception):
                self._condition = TrailingConditionEvaluator(redis_client=r)  # type: ignore

    def _write_event(self, fields: dict[str, Any]) -> None:
        if self._write_event_fn:
            self._write_event_fn(fields)

    def _notify(self, msg: str) -> None:
        if self.trail_notify and self._telegram_fn:
            with contextlib.suppress(Exception):
                self._telegram_fn(msg)

    # ------------------------------------------------------------------
    # Callback rate computation
    # ------------------------------------------------------------------

    def resolve_callback_rate(self, payload: dict[str, Any]) -> float:
        return compute_trailing_callback_rate_pct(
            payload,
            min_pct=self.trail_cb_min,
            max_pct=self.trail_cb_max,
            default_pct=self.trail_cb_default,
        )

    # ------------------------------------------------------------------
    # Profile SL computation
    # ------------------------------------------------------------------

    def compute_profile_sl(
        self,
        *,
        profile_name: str,
        logical_side: str,
        entry_price: float,
        mark_price: float,
        current_sl: float,
        atr: float | None = None,
    ) -> float | None:
        """Compute new SL from profile. Returns None if no move needed."""
        if self._profiles is None:
            return None
        try:
            profile = self._profiles.get(profile_name)
            if profile is None:
                return None
            new_sl = profile.compute_sl(
                logical_side=logical_side,
                entry_price=entry_price,
                mark_price=mark_price,
                current_sl=current_sl,
                atr=atr,
            )
            if new_sl is None:
                return None
            # Only move in the direction that improves the stop
            if logical_side == "LONG" and new_sl <= current_sl:
                return None
            if logical_side == "SHORT" and new_sl >= current_sl:
                return None
            min_delta = abs(current_sl) * self.trail_sl_move_min_delta_pct / 100.0
            if abs(new_sl - current_sl) < min_delta:
                return None
            return new_sl
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Replace SL on exchange
    # ------------------------------------------------------------------

    def replace_sl_order(
        self,
        *,
        sid: str,
        symbol: str,
        logical_side: str,
        new_sl_price: float,
        qty: float,
        old_sl_order_id: Any,
        client: "BinanceFuturesClient",
        filters: "FiltersCache",
        r: Any = None,
    ) -> dict[str, Any]:
        """Cancel old SL and place new SL order. Returns new protection dict."""
        sym = symbol.upper()
        sf = filters.get(sym)
        result: dict[str, Any] = {"sid": sid, "symbol": sym, "new_sl_price": new_sl_price}

        close_side = "SELL" if logical_side == "LONG" else "BUY"
        pos_side = _position_side_for_mode(self.position_mode, logical_side)
        reduce_params: dict[str, Any] = {}
        if pos_side:
            reduce_params["positionSide"] = pos_side
        else:
            reduce_params["reduceOnly"] = "true"

        # Cancel old SL
        if old_sl_order_id:
            with contextlib.suppress(Exception):
                client.cancel_order(sym, order_id=old_sl_order_id)  # type: ignore

        # Place new SL
        new_cid = _make_cid(sid, "sl-trail", r)
        sl_qty_str = _format_float(_round_down(qty, sf.step_size), sf.step_size)
        try:
            sl_resp = client.place_order(  # type: ignore
                symbol=sym,
                side=close_side,
                type="STOP_MARKET",
                stopPrice=_format_float(new_sl_price, sf.tick_size),
                quantity=sl_qty_str,
                workingType=self.trail_working_type,
                newClientOrderId=new_cid,
                **reduce_params,
            )
            result["sl_order_id"] = sl_resp.get("orderId") or sl_resp.get("id")
            result["sl_client_order_id"] = new_cid
            result["sl_replace_ok"] = True
        except Exception as exc:
            result["sl_replace_error"] = str(exc)
            result["sl_replace_ok"] = False

        return result

    # ------------------------------------------------------------------
    # Native trailing
    # ------------------------------------------------------------------

    def place_trailing_stop(
        self,
        *,
        sid: str,
        symbol: str,
        logical_side: str,
        qty: float,
        callback_rate: float,
        activate_price: float | None,
        client: "BinanceFuturesClient",
        filters: "FiltersCache",
        r: Any = None,
    ) -> dict[str, Any]:
        """Place TRAILING_STOP_MARKET order on Binance."""
        sym = symbol.upper()
        sf = filters.get(sym)
        close_side = "SELL" if logical_side == "LONG" else "BUY"
        pos_side = _position_side_for_mode(self.position_mode, logical_side)
        reduce_params: dict[str, Any] = {}
        if pos_side:
            reduce_params["positionSide"] = pos_side
        else:
            reduce_params["reduceOnly"] = "true"

        trail_cid = _make_cid(sid, "trail", r)
        qty_str = _format_float(_round_down(qty, sf.step_size), sf.step_size)
        params: dict[str, Any] = {
            "symbol": sym,
            "side": close_side,
            "type": "TRAILING_STOP_MARKET",
            "quantity": qty_str,
            "callbackRate": str(_round_half_up(callback_rate, 1)),
            "workingType": self.trail_working_type,
            "newClientOrderId": trail_cid,
            **reduce_params,
        }
        if activate_price is not None:
            params["activationPrice"] = _format_float(activate_price, sf.tick_size)

        try:
            resp = client.place_algo_order(**params)  # type: ignore
            return {
                "sid": sid, "symbol": sym,
                "trail_algo_order_id": resp.get("orderId") or resp.get("id"),
                "trail_client_order_id": trail_cid,
                "trail_callback_rate": callback_rate,
                "trail_activate_price": activate_price,
            }
        except Exception as exc:
            return {
                "sid": sid, "symbol": sym,
                "trail_error": str(exc),
                "trail_callback_rate": callback_rate,
            }

    # ------------------------------------------------------------------
    # Arm trailing after TP (background thread)
    # ------------------------------------------------------------------

    def maybe_start_trailing_after_tp(
        self,
        *,
        sid: str,
        symbol: str,
        logical_side: str,
        qty: float,
        payload: dict[str, Any],
        state: dict[str, Any],
        client: "BinanceFuturesClient",
        filters: "FiltersCache",
        r: Any = None,
    ) -> None:
        """Start trailing arm in a daemon thread (non-blocking for main loop)."""
        if not payload.get("trail_after_tp1") and not payload.get("trail_after_tp"):
            return

        callback_rate = self.resolve_callback_rate(payload)

        if self.trail_mode == "orchestrator" and self._profiles is not None:
            target = self._arm_trailing_orchestrator
        else:
            target = self._arm_trailing_native

        t = threading.Thread(
            target=target,
            kwargs={
                "sid": sid,
                "symbol": symbol,
                "logical_side": logical_side,
                "qty": qty,
                "payload": payload,
                "state": state,
                "client": client,
                "filters": filters,
                "callback_rate": callback_rate,
                "r": r,
            },
            daemon=True,
        )
        t.name = f"trail-{symbol[:6]}-{sid[:8]}"
        t.start()

    def _arm_trailing_native(
        self,
        *,
        sid: str,
        symbol: str,
        logical_side: str,
        qty: float,
        payload: dict[str, Any],
        state: dict[str, Any],
        client: "BinanceFuturesClient",
        filters: "FiltersCache",
        callback_rate: float,
        r: Any = None,
        **_: Any,
    ) -> None:
        """Poll until TP1 is confirmed, then place TRAILING_STOP_MARKET."""
        tp_levels = payload.get("tp_levels") or []
        tp1_price = float(tp_levels[0]) if tp_levels else None
        if tp1_price is None:
            return

        deadline = time.monotonic() + self.trail_arm_timeout_s
        while time.monotonic() < deadline:
            try:
                mark_resp = client.get_mark_price(symbol)
                mark = _f(mark_resp.get("markPrice") or mark_resp.get("price"), 0.0)  # type: ignore
                if mark <= 0:
                    time.sleep(self.trail_arm_poll_s)
                    continue

                tp_hit = mark >= tp1_price if logical_side == "LONG" else mark <= tp1_price
                if tp_hit:
                    sf = filters.get(symbol.upper())
                    try:
                        activate_price = compute_trailing_activate_price(
                            logical_side,
                            latest_price=mark,
                            tick_size=sf.tick_size,
                            buffer_bps=self.trail_activate_price_bps,
                        )
                    except Exception:
                        activate_price = None

                    result = self.place_trailing_stop(
                        sid=sid, symbol=symbol, logical_side=logical_side,
                        qty=qty, callback_rate=callback_rate,
                        activate_price=activate_price, client=client, filters=filters, r=r,
                    )
                    if result.get("trail_algo_order_id"):
                        self._write_event({
                            "sid": sid, "symbol": symbol, "action": "trail_armed",
                            "event_type": "TRAIL_ARMED",
                            **result,
                        })
                        self._notify(f"🎯 Trail armed {symbol} | sid={sid[:8]} | cb={callback_rate}%")
                    return

                time.sleep(self.trail_arm_poll_s)
            except Exception:
                time.sleep(self.trail_arm_poll_s)

        # Timeout
        self._write_event({
            "sid": sid, "symbol": symbol, "action": "trail_arm_timeout",
            "event_type": "TRAIL_ARM_TIMEOUT", "severity": "warning",
        })

    def _arm_trailing_orchestrator(
        self,
        *,
        sid: str,
        symbol: str,
        logical_side: str,
        qty: float,
        payload: dict[str, Any],
        state: dict[str, Any],
        client: "BinanceFuturesClient",
        filters: "FiltersCache",
        callback_rate: float,
        r: Any = None,
        **_: Any,
    ) -> None:
        """Profile-based SL move loop (orchestrator trailing mode)."""
        current_sl = _f(state.get("sl_price") or payload.get("sl"), 0.0)
        if current_sl <= 0:
            return

        sl_order_id = state.get("sl_order_id")
        deadline = time.monotonic() + self.trail_loop_timeout_s

        while time.monotonic() < deadline:
            try:
                mark_resp = client.get_mark_price(symbol)
                mark = _f(mark_resp.get("markPrice") or mark_resp.get("price"), 0.0)  # type: ignore
                if mark <= 0:
                    time.sleep(self.trail_loop_poll_s)
                    continue

                current_profile_name = str(
                    state.get("trail_profile")
                    or payload.get("trail_profile")
                    or self.trail_profile_name
                ).strip() or "rocket_v1"

                new_sl = self.compute_profile_sl(
                    profile_name=current_profile_name,
                    logical_side=logical_side,
                    entry_price=_f(state.get("entry_price") or payload.get("entry"), 0.0),
                    mark_price=mark,
                    current_sl=current_sl,
                )
                if new_sl is not None:
                    result = self.replace_sl_order(
                        sid=sid, symbol=symbol, logical_side=logical_side,
                        new_sl_price=new_sl, qty=qty,
                        old_sl_order_id=sl_order_id,
                        client=client, filters=filters, r=r,
                    )
                    if result.get("sl_replace_ok"):
                        current_sl = new_sl
                        sl_order_id = result.get("sl_order_id")
                        self._write_event({
                            "sid": sid, "symbol": symbol, "action": "trail_sl_moved",
                            "event_type": "TRAIL_SL_MOVED",
                            "new_sl_price": new_sl,
                            "mark_price": mark,
                        })

                time.sleep(self.trail_loop_poll_s)
            except Exception:
                time.sleep(self.trail_loop_poll_s)
