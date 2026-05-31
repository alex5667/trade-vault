import re

with open("python-worker/services/binance_executor.py", "r") as f:
    code = f.read()

# Replace _place_trailing_stop entirely
code = code.replace(
"""    def _place_trailing_stop(
        self, *, sid: str, symbol: str, logical_side: str,
        qty: float, callback_rate_pct: float,
        client: "BinanceFuturesClient",
        filters: "FiltersCache",
    ) -> Dict[str, Any]:
        \"\"\"Place TRAILING_STOP_MARKET order.

        In one-way mode: reduceOnly=True ensures it only reduces the position.
        In hedge mode: positionSide is set; reduceOnly is forbidden by Binance.
        \"\"\"
        exit_side = "SELL" if logical_side == "LONG" else "BUY"
        pos_side = _position_side_for_mode(self.position_mode, logical_side)
        q, _ = self._quantize(symbol, qty, None, filters=filters)
        if float(q) <= 0:
            raise ValueError("trail qty <= 0")

        p: Dict[str, Any] = {
            "symbol": symbol,
            "side": exit_side,
            "type": "TRAILING_STOP_MARKET",
            "quantity": q,
            "callbackRate": float(callback_rate_pct),
            "newClientOrderId": _make_cid(sid, "trail"),
        }
        if self.position_mode == "oneway":
            p["reduceOnly"] = True
        if pos_side:
            p["positionSide"] = pos_side

        j = client.post_order(p)
        return {
            "trail_order_id": j.get("orderId"),
            "trail_client_id": p["newClientOrderId"],
        }""",
"""    def _place_trailing_stop(
        self, *, sid: str, symbol: str, logical_side: str,
        qty: float, callback_rate_pct: float,
        client: "BinanceFuturesClient",
        filters: "FiltersCache",
    ) -> Dict[str, Any]:
        \"\"\"Place TRAILING_STOP_MARKET through the Algo API with local guards.\"\"\"
        exit_side = "SELL" if logical_side == "LONG" else "BUY"
        pos_side = _position_side_for_mode(self.position_mode, logical_side)
        q, _ = self._quantize(symbol, qty, None, filters=filters)
        if float(q) <= 0:
            raise ValueError("trail qty <= 0")

        latest = float(client.get_working_price(symbol, self.trail_working_type) or 0.0)
        if latest <= 0:
            raise ValueError("latest working price unavailable for trailing activation")
        tick_size = float(filters.get(symbol).tick_size or 0.0)
        activate_price = compute_trailing_activate_price(
            logical_side, latest_price=latest, tick_size=tick_size,
            buffer_bps=self.trail_activate_price_bps,
            user_activate_price=None,
        )

        p: Dict[str, Any] = {
            "symbol": symbol,
            "side": exit_side,
            "type": "TRAILING_STOP_MARKET",
            "quantity": q,
            "callbackRate": float(callback_rate_pct),
            "activatePrice": _format_float(activate_price, tick_size),
            "workingType": self.trail_working_type,
            "clientAlgoId": _make_cid(sid, "trail"),
        }
        if self.position_mode == "oneway":
            p["reduceOnly"] = True
            self._validate_exit_contract(
                position_side=pos_side, reduce_only=True, close_position=False,
                quantity=float(q), order_type="TRAILING_STOP_MARKET",
                working_type=self.trail_working_type, is_algo=True,
            )
        if pos_side:
            p["positionSide"] = pos_side

        j = client.post_algo_order(p)
        return {
            "trail_algo_id": j.get("algoId"),
            "trail_client_algo_id": p["clientAlgoId"],
            "trail_working_type": p["workingType"],
            "trail_activate_price": p["activatePrice"],
        }""")

# Read handle_open to modify it correctly
# We will use simple replacement for the handle_open lines
old_open_1 = """            prot = self._place_protective(
                sid=sid, symbol=symbol, logical_side=logical,
                qty=filled_qty, sl=sl, tps=tps,
                client=client, filters=filters,
                ref_price=avg_price if avg_price > 0 else None,
            )"""
new_open_1 = """            policy = self._resolve_execution_policy(payload, symbol)
            prot = self._place_protective(
                sid=sid, symbol=symbol, logical_side=logical,
                qty=filled_qty, sl=sl, tps=tps, policy=policy,
                client=client, filters=filters,
                ref_price=avg_price if avg_price > 0 else None,
            )"""
code = code.replace(old_open_1, new_open_1)

old_open_2 = """            trail = self._maybe_start_trailing_after_tp1(
                payload=payload, sid=sid, symbol=symbol, logical_side=logical,
                entry_price=avg_price if avg_price > 0 else (p or None),
                sl_order_id=_i(prot.get("sl_algo_id"), 0) or None,
                tp_levels=tps,
                client=client, filters=filters,
            )"""
new_open_2 = """            trail = self._maybe_start_trailing_after_tp1(
                payload=payload, sid=sid, symbol=symbol, logical_side=logical,
                entry_price=avg_price if avg_price > 0 else (p or None), initial_qty=filled_qty,
                sl_algo_id=_i(prot.get("sl_algo_id"), 0) or None,
                tp_levels=tps, tp1_working_type=str(prot.get("tp1_working_type") or policy.tp_working_type),
                policy=policy, client=client, filters=filters,
            )"""
code = code.replace(old_open_2, new_open_2)

# Update _maybe_start_trailing_after_tp1
old_maybe = """    def _maybe_start_trailing_after_tp1(
        self, *, payload: Dict[str, Any], sid: str, symbol: str,
        logical_side: str, entry_price: Optional[float],
        sl_order_id: Optional[int], tp_levels: List[float],
        client: "BinanceFuturesClient",
        filters: "FiltersCache",
    ) -> Dict[str, Any]:"""
new_maybe = """    def _maybe_start_trailing_after_tp1(
        self, *, payload: Dict[str, Any], sid: str, symbol: str,
        logical_side: str, entry_price: Optional[float], initial_qty: float,
        sl_algo_id: Optional[int], tp_levels: List[float], tp1_working_type: str,
        policy: ExecutionPolicyDecision,
        client: "BinanceFuturesClient",
        filters: "FiltersCache",
    ) -> Dict[str, Any]:"""
code = code.replace(old_maybe, new_maybe)

# Update inside _maybe_start_trailing_after_tp1
old_maybe_cb = """        tp1 = float(tp_levels[0])

        t = threading.Thread(
            target=self._arm_trailing_after_tp1_thread,
            kwargs={
                "sid": sid, "symbol": symbol, "logical_side": logical_side,
                "tp1": tp1, "callback_rate_pct": cb,
                "sl_order_id": int(sl_order_id) if sl_order_id else None,
                "client": client,
                "filters": filters,
            },
            daemon=True,  # won't block process exit
        )
        t.start()

        return {
            "trail_after_tp1": True,
            "trail_tp1": tp1,
            "trail_callback_rate_pct": cb,
            "trail_status": "arming",
        }"""
new_maybe_cb = """        if policy.name == MAKER_FIRST:
            return {
                "trail_after_tp1": True,
                "trail_callback_rate_pct": cb,
                "trail_status": "managed_by_tp_watchdog",
                "trail_pending": True,
            }

        tp1 = float(tp_levels[0])
        t = threading.Thread(
            target=self._arm_trailing_after_tp1_thread,
            kwargs={
                "sid": sid, "symbol": symbol, "logical_side": logical_side,
                "tp1": tp1, "callback_rate_pct": cb, "sl_algo_id": sl_algo_id,
                "initial_qty": initial_qty, "tp1_working_type": tp1_working_type,
                "client": client, "filters": filters,
            },
            daemon=True,
        )
        t.start()
        return {
            "trail_after_tp1": True,
            "trail_tp1": tp1,
            "trail_callback_rate_pct": cb,
            "trail_status": "arming",
            "trail_pending": True,
        }"""
code = code.replace(old_maybe_cb, new_maybe_cb)

with open("python-worker/services/binance_executor.py", "w") as f:
    f.write(code)

print("Replaced handle_open and trailing stop methods")
