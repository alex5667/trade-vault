import re

with open("python-worker/services/binance_executor.py", "r") as f:
    code = f.read()

# Replace _place_protective signature
code = code.replace(
"""    def _place_protective(
        self, *, sid: str, symbol: str, logical_side: str,
        qty: float, sl: Optional[float], tps: List[float],
        client: "BinanceFuturesClient",
        filters: "FiltersCache",
        ref_price: Optional[float] = None,
    ) -> Dict[str, Any]:""",
"""    def _place_protective(
        self, *, sid: str, symbol: str, logical_side: str,
        qty: float, sl: Optional[float], tps: List[float],
        policy: ExecutionPolicyDecision,
        client: "BinanceFuturesClient",
        filters: "FiltersCache",
        ref_price: Optional[float] = None,
    ) -> Dict[str, Any]:""")

# We need to replace the `if valid_tps:` block with the new logic, and the `if valid_sl` block.
# Actually, let's just replace the body of _place_protective from `out: Dict[str, Any] = {}` to `return out`
# Let's find the start and end indices of `_place_protective`

lines = code.split("\n")
start_idx = -1
end_idx = -1
for i, l in enumerate(lines):
    if l.startswith("    def _place_protective("):
        start_idx = i
    elif start_idx != -1 and l.startswith("    # --- Order cancellation by token ---"):
        end_idx = i
        break

if start_idx != -1 and end_idx != -1:
    new_method = """    def _place_protective(
        self, *, sid: str, symbol: str, logical_side: str,
        qty: float, sl: Optional[float], tps: List[float],
        policy: ExecutionPolicyDecision,
        client: "BinanceFuturesClient",
        filters: "FiltersCache",
        ref_price: Optional[float] = None,
    ) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "execution_policy": policy.name,
            "execution_policy_reason": policy.reason,
            "tp_watchdog_enabled": bool(policy.tp_watchdog_enabled),
        }
        exit_side = "SELL" if logical_side == "LONG" else "BUY"
        pos_side = _position_side_for_mode(self.position_mode, logical_side)
        reduce_only_allowed = self.position_mode == "oneway"

        check_ref = None
        if sl and sl > 0:
            check_ref = sl
        elif tps:
            check_ref = float(tps[0])
        self._local_headroom_check(client=client, symbol=symbol, qty=qty, reference_price=check_ref)

        valid_sl, valid_tps = self._validate_protective_prices(
            symbol, logical_side, sl, tps,
            client=client, ref_price=ref_price,
        )

        if sl is not None and sl > 0 and valid_sl is None:
            self._exec_event({
                "sid": sid, "symbol": symbol, "action": "sl_skip",
                "status": "warning",
                "msg": f"SL price {sl} already crossed mark price — skipped to avoid -2021",
                "sl_skipped": sl,
            })
        dropped_tps = [tp for tp in tps if tp not in valid_tps]
        if dropped_tps:
            self._exec_event({
                "sid": sid, "symbol": symbol, "action": "tp_skip",
                "status": "warning",
                "msg": f"TP price(s) {dropped_tps} already crossed mark price — skipped to avoid -2021",
                "tp_skipped": str(dropped_tps),
            })

        if valid_sl is not None and valid_sl > 0:
            q_sl, sl_q = self._quantize(symbol, qty, valid_sl, filters=filters)
            p: Dict[str, Any] = {
                "symbol": symbol,
                "side": exit_side,
                "type": "STOP_MARKET",
                "triggerPrice": sl_q,
                "workingType": self.sl_working_type,
                "clientAlgoId": _make_cid(sid, "sl"),
            }
            if reduce_only_allowed:
                p["reduceOnly"] = True
                self._validate_exit_contract(
                    position_side=pos_side, reduce_only=True, close_position=False,
                    quantity=float(q_sl), order_type="STOP_MARKET",
                    working_type=self.sl_working_type, is_algo=True,
                )
                p["quantity"] = q_sl
            elif pos_side:
                p["positionSide"] = pos_side
                p["closePosition"] = True
                self._validate_exit_contract(
                    position_side=pos_side, reduce_only=False, close_position=True,
                    quantity=None, order_type="STOP_MARKET",
                    working_type=self.sl_working_type, is_algo=True,
                )
            j = client.post_algo_order(p)
            out["sl_algo_id"] = j.get("algoId")
            out["sl_client_algo_id"] = p["clientAlgoId"]
            out["sl_working_type"] = p["workingType"]
            out["sl_order_type"] = "STOP_MARKET"

        if valid_tps:
            parts = self._split_tp_qtys(symbol, qty, len(valid_tps), filters=filters)
            filters_obj = filters.get(symbol)
            cumulative = 0.0
            for idx, (tp, q_tp) in enumerate(zip(valid_tps, parts), start=1):
                cumulative += float(q_tp)
                expected_remaining = max(0.0, float(qty) - cumulative)
                q_tp2, tp_q = self._quantize(symbol, q_tp, tp, filters=filters)
                common: Dict[str, Any] = {
                    "symbol": symbol,
                    "side": exit_side,
                    "workingType": policy.tp_working_type,
                    "clientAlgoId": _make_cid(sid, f"tp{idx}"),
                }
                if policy.name == MAKER_FIRST:
                    limit_px = compute_limit_tp_price(
                        float(tp_q), logical_side,
                        offset_bps=self.tp_limit_price_offset_bps,
                        tick_size=float(filters_obj.tick_size or 0.0),
                    )
                    limit_px_s = _format_float(limit_px, float(filters_obj.tick_size or 0.0))
                    p = {
                        **common,
                        "type": "TAKE_PROFIT",
                        "triggerPrice": tp_q,
                        "price": limit_px_s,
                        "timeInForce": policy.tp_limit_time_in_force,
                        "quantity": q_tp2,
                    }
                    if reduce_only_allowed:
                        p["reduceOnly"] = True
                        self._validate_exit_contract(
                            position_side=pos_side, reduce_only=True, close_position=False,
                            quantity=float(q_tp2), order_type="TAKE_PROFIT",
                            working_type=policy.tp_working_type, is_algo=True,
                        )
                    elif pos_side:
                        p["positionSide"] = pos_side
                        self._validate_exit_contract(
                            position_side=pos_side, reduce_only=False, close_position=False,
                            quantity=float(q_tp2), order_type="TAKE_PROFIT",
                            working_type=policy.tp_working_type, is_algo=True,
                        )
                    j = client.post_algo_order(p)
                    out[f"tp{idx}_algo_id"] = j.get("algoId")
                    out[f"tp{idx}_client_algo_id"] = p["clientAlgoId"]
                    out[f"tp{idx}_working_type"] = p["workingType"]
                    out[f"tp{idx}_order_type"] = "TAKE_PROFIT"
                    out[f"tp{idx}_time_in_force"] = p["timeInForce"]
                    out[f"tp{idx}_qty"] = q_tp2
                    out[f"tp{idx}_trigger_price"] = tp_q
                    out[f"tp{idx}_limit_price"] = limit_px_s
                    out[f"tp{idx}_expected_remaining_qty"] = expected_remaining
                    out[f"tp{idx}_state"] = _tp_state_name(idx, "ARMED")
                    self._emit_tp_state(
                        sid, symbol, idx, "ARMED",
                        order_type="TAKE_PROFIT", policy=policy.name,
                        qty=q_tp2, trigger_price=tp_q, limit_price=limit_px_s,
                    )
                else:
                    p = {
                        **common,
                        "type": "TAKE_PROFIT_MARKET",
                        "triggerPrice": tp_q,
                    }
                    if reduce_only_allowed:
                        p["reduceOnly"] = True
                        p["quantity"] = q_tp2
                        self._validate_exit_contract(
                            position_side=pos_side, reduce_only=True, close_position=False,
                            quantity=float(q_tp2), order_type="TAKE_PROFIT_MARKET",
                            working_type=policy.tp_working_type, is_algo=True,
                        )
                    elif pos_side:
                        p["positionSide"] = pos_side
                        p["closePosition"] = True if idx == len(valid_tps) and len(valid_tps) == 1 else False
                        if p["closePosition"]:
                            self._validate_exit_contract(
                                position_side=pos_side, reduce_only=False, close_position=True,
                                quantity=None, order_type="TAKE_PROFIT_MARKET",
                                working_type=policy.tp_working_type, is_algo=True,
                            )
                        else:
                            p["quantity"] = q_tp2
                            self._validate_exit_contract(
                                position_side=pos_side, reduce_only=False, close_position=False,
                                quantity=float(q_tp2), order_type="TAKE_PROFIT_MARKET",
                                working_type=policy.tp_working_type, is_algo=True,
                            )
                    j = client.post_algo_order(p)
                    out[f"tp{idx}_algo_id"] = j.get("algoId")
                    out[f"tp{idx}_client_algo_id"] = p["clientAlgoId"]
                    out[f"tp{idx}_working_type"] = p["workingType"]
                    out[f"tp{idx}_order_type"] = "TAKE_PROFIT_MARKET"
                    out[f"tp{idx}_qty"] = q_tp2
                    out[f"tp{idx}_trigger_price"] = tp_q
                    out[f"tp{idx}_expected_remaining_qty"] = expected_remaining
                    out[f"tp{idx}_state"] = _tp_state_name(idx, "ARMED")
        return out

"""
    lines[start_idx:end_idx] = [new_method]
    code = "\n".join(lines)

with open("python-worker/services/binance_executor.py", "w") as f:
    f.write(code)

print("Replaced _place_protective")
