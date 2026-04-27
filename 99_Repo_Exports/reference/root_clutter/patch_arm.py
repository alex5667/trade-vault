import re

with open("python-worker/services/binance_executor.py", "r") as f:
    code = f.read()

# Replace _arm_trailing_after_tp1_thread
old_arm = """    def _arm_trailing_after_tp1_thread(
        self, *, sid: str, symbol: str, logical_side: str,
        tp1: float, callback_rate_pct: float, sl_order_id: Optional[int],
        client: "BinanceFuturesClient",
        filters: "FiltersCache",
    ) -> None:
        \"\"\"Daemon thread: poll mark price → when TP1 touched, replace SL with trailing stop.

        Rationale:
          - Keep hard SL active until TP1 is reached (protects downside before runners)
          - After TP1: cancel hard SL, arm trailing stop for remainder
          - This matches the upstream trail_after_tp1 strategy pattern
        \"\"\"
        try:
            deadline = time.time() + float(self.trail_arm_timeout_s)
            poll_s = max(0.2, float(self.trail_arm_poll_s))
            touched = False

            while time.time() < deadline:
                # Use mark price (less noisy than last trade price)
                mp = float(client.get_mark_price(symbol) or 0.0)
                if mp <= 0:
                    time.sleep(poll_s)
                    continue

                if logical_side == "LONG":
                    touched = mp >= float(tp1)
                else:
                    touched = mp <= float(tp1)

                if touched:
                    break
                time.sleep(poll_s)

            if not touched:
                # Timed out without TP1 touch — log and exit silently
                self._exec_event({
                    "sid": sid, "symbol": symbol, "action": "trail_arm",
                    "status": "timeout", "trail_tp1": tp1,
                    "trail_callback_rate_pct": callback_rate_pct,
                })
                return

            # Verify position is still open
            qty = self._get_position_qty(symbol, logical_side=logical_side, client=client)
            if qty <= 0:
                self._exec_event({
                    "sid": sid, "symbol": symbol,
                    "action": "trail_arm", "status": "no_position",
                })
                return

            # Cancel the hard SL (best-effort: might already be cancelled by partial TP)
            if sl_order_id:
                try:
                    client.delete_order(symbol, order_id=int(sl_order_id))
                except Exception:
                    pass

            trail = self._place_trailing_stop(
                sid=sid, symbol=symbol, logical_side=logical_side,
                qty=qty, callback_rate_pct=callback_rate_pct,
                client=client, filters=filters,
            )

            ev = {
                "sid": sid, "symbol": symbol, "action": "trail_arm",
                "status": "armed", "side": logical_side, "qty": qty,
                "trail_tp1": tp1, "trail_callback_rate_pct": callback_rate_pct,
                **trail,
            }
            self._exec_event(ev)

            # Update orders:state:{sid} with trail_order_id so lookup shows full picture
            self._save_order_state(sid, {
                "action": "trail_arm",
                "status": "armed",
                "symbol": symbol,
                "side": logical_side,
                "trail_order_id": trail.get("trail_order_id"),
                "trail_client_id": trail.get("trail_client_id"),
                "trail_tp1": tp1,
                "trail_callback_rate_pct": callback_rate_pct,
            })

            if self.tg is not None and self.trail_notify:
                self.tg.send_text(
                    f"🧷 BINANCE trailing armed\\n"
                    f"symbol={symbol} side={logical_side}\\n"
                    f"sid={sid[:24]}...\\n"
                    f"tp1={tp1} cb={callback_rate_pct:.1f}%"
                )
        except Exception as e:
            self._exec_event({
                "sid": sid, "symbol": symbol, "action": "trail_arm",
                "status": "error", "msg": str(e)[:900],
            })"""

new_arm = """    def _arm_trailing_after_tp1_thread(
        self, *, sid: str, symbol: str, logical_side: str,
        tp1: float, callback_rate_pct: float, sl_algo_id: Optional[int],
        initial_qty: float, tp1_working_type: str,
        client: "BinanceFuturesClient",
        filters: "FiltersCache",
    ) -> None:
        \"\"\"Wait for TP1 touch and confirmed reduction before arming trailing.\"\"\"
        try:
            deadline = time.time() + float(self.trail_arm_timeout_s)
            poll_s = max(0.2, float(self.trail_arm_poll_s))
            touched = False
            touch_ts_ms: Optional[int] = None
            tol = self._position_qty_tolerance(symbol, filters=filters)

            while time.time() < deadline:
                px = float(client.get_working_price(symbol, tp1_working_type) or 0.0)
                if px <= 0:
                    time.sleep(poll_s)
                    continue

                if not touched:
                    touched = (px >= float(tp1)) if logical_side == "LONG" else (px <= float(tp1))
                    if touched:
                        touch_ts_ms = _ms_now()
                        self._exec_event({
                            "sid": sid, "symbol": symbol, "action": "trail_gate",
                            "status": "tp1_touched", "tp1": tp1, "working_price": px,
                        })

                if touched:
                    current_qty = self._get_position_qty(symbol, logical_side=logical_side, client=client)
                    if current_qty < float(initial_qty) - tol:
                        self._try_arm_trailing_after_confirmed_tp(
                            sid=sid, symbol=symbol, logical_side=logical_side, sl_algo_id=sl_algo_id,
                            callback_rate_pct=callback_rate_pct, client=client, filters=filters,
                        )
                        return
                    if touch_ts_ms is not None and (_ms_now() - touch_ts_ms) >= int(self.tp_limit_watchdog_timeout_ms):
                        self._exec_event({
                            "sid": sid, "symbol": symbol, "action": "trail_gate",
                            "status": "tp1_touch_without_confirmed_reduction", "tp1": tp1,
                        })
                        return
                time.sleep(poll_s)

            self._exec_event({
                "sid": sid, "symbol": symbol, "action": "trail_gate", "status": "timeout", "tp1": tp1,
            })
        except Exception as e:
            self._exec_event({
                "sid": sid, "symbol": symbol, "action": "trail_gate", "status": "error", "msg": str(e)[:400],
            })"""

code = code.replace(old_arm, new_arm)

# Add protection invariant logic at the end of handle_open
# Let's find:
#         result = {
#             "sid": sid, "symbol": symbol, "action": "open",
# And inject before it:

old_end = """        result = {
            "sid": sid, "symbol": symbol, "action": "open",
            "status": status.lower(), "side": logical,
            "qty": filled_qty, "avg_price": avg_price, "binance_order_id": j_final.get("orderId"),
            **prot, **trail, **maker_watchdogs,
        }"""
new_end = """        trail_enabled = _truthy(payload.get("trail_after_tp1")) and bool(tps)
        if not self._protection_confirmed({**prot, **trail}, tps, trail_enabled):
            self._emit_protection_incident(sid, symbol, "entry_filled_without_confirmed_protection")
            emerg = self._emergency_flatten_position(
                sid=sid, symbol=symbol, logical_side=logical, qty=filled_qty,
                client=client, filters=filters,
            )
            prot = {**prot, **emerg, "protection_invariant_failed": True}

        result = {
            "sid": sid, "symbol": symbol, "action": "open",
            "status": status.lower(), "side": logical,
            "qty": filled_qty, "avg_price": avg_price, "binance_order_id": j_final.get("orderId"),
            "execution_policy": policy.name,
            **prot, **trail, **maker_watchdogs,
        }"""

code = code.replace(old_end, new_end)

with open("python-worker/services/binance_executor.py", "w") as f:
    f.write(code)

print("Replaced arm trailing and added protection invariant")
