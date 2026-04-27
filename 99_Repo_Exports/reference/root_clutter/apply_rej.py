import os

base_file = "/tmp/orig/binance_execution/binance_executor.py"
with open(base_file, "r") as f:
    lines = f.readlines()

# Hunk 1: insert _local_headroom_check, _validate_exit_contract, _resolve_execution_policy, _position_qty_tolerance, _emit_tp_state, _submit_reduce_only_market_exit, _emergency_flatten_position, _protection_confirmed, _emit_protection_incident
# These go after _split_tp_qtys at line 785 (or wherever).
# Instead of exact lines, let's find `    # --- Order cancellation by token ---` and insert them BEFORE it, because they are helpers used around protective orders.
# Actually, the original code had them after `_split_tp_qtys`.
idx1 = 0
for i, l in enumerate(lines):
    if "def _split_tp_qtys(" in l:
        idx1 = i
        break
# find end of _split_tp_qtys
for i in range(idx1, len(lines)):
    if "    # --- Order cancellation by token ---" in lines[i] or "    def _cancel_by_token" in lines[i] or "    # --- Protective orders" in lines[i]:
        idx1 = i
        break

hunk1 = """
    def _local_headroom_check(
        self,
        *,
        client: "BinanceFuturesClient",
        symbol: str,
        qty: float,
        reference_price: float | None,
    ) -> None:
        try:
            acct = client.get_account() or {}
            avail = _f(acct.get("availableBalance"), 0.0)
            px = float(reference_price or 0.0)
            notional = abs(float(qty)) * px if px > 0 else 0.0
            reserve = notional * (self.protection_fee_buffer_bps + self.protection_slippage_buffer_bps) / 10000.0
            if avail - reserve < self.account_available_floor_usd:
                raise RuntimeError(
                    f"insufficient protection headroom: available={avail:.8f} reserve={reserve:.8f} "
                    f"floor={self.account_available_floor_usd:.8f}"
                )
        except RuntimeError:
            raise
        except Exception:
            return

    def _validate_exit_contract(
        self,
        *,
        position_side: str | None,
        reduce_only: bool,
        close_position: bool,
        quantity: float | None,
        order_type: str,
        working_type: str | None,
        is_algo: bool,
    ) -> None:
        result = validate_exit_intent(
            position_mode=self.position_mode,
            position_side=position_side,
            exit_intent="close",
            reduce_only=reduce_only,
            close_position=close_position,
            quantity=quantity,
            order_type=order_type,
            working_type=working_type,
            is_algo=is_algo,
        )
        if not result.is_valid_exit_contract:
            raise ValueError(f"invalid_exit_contract:{result.reason}")

    def _resolve_execution_policy(self, payload: dict, symbol: str) -> ExecutionPolicyDecision:
        return resolve_execution_policy(
            payload=payload,
            symbol=symbol,
            default_policy=self.exec_policy_default,
            maker_allowed_symbols=self.exec_policy_maker_allowed_symbols,
            tp_market_working_type=self.tp_market_working_type,
            tp_limit_trigger_working_type=self.tp_limit_trigger_working_type,
            tp_limit_time_in_force=self.tp_limit_time_in_force,
            watchdog_enabled=self.tp_limit_watchdog_enable,
            watchdog_timeout_ms=self.tp_limit_watchdog_timeout_ms,
        )

    def _position_qty_tolerance(self, symbol: str, *, filters: "FiltersCache") -> float:
        try:
            return max(float(filters.get(symbol).step_size or 0.0), 1e-12)
        except Exception:
            return 1e-12

    def _emit_tp_state(self, sid: str, symbol: str, level: int, state: str, **extra) -> None:
        tp_state = _tp_state_name(level, state)
        ev = {
            "sid": sid,
            "symbol": symbol,
            "action": "tp_state",
            "tp_level": int(level),
            "tp_state": tp_state,
            **extra,
        }
        self._exec_event(ev)
        state_doc = {f"tp{int(level)}_state": tp_state}
        for k, v in extra.items():
            state_doc[f"tp{int(level)}_{k}"] = v
        self._save_order_state(sid, state_doc)

    def _submit_reduce_only_market_exit(
        self,
        *,
        sid: str,
        symbol: str,
        logical_side: str,
        qty: float,
        reason_tag: str,
        client: "BinanceFuturesClient",
        filters: "FiltersCache",
    ) -> dict:
        exit_side = "SELL" if logical_side == "LONG" else "BUY"
        pos_side = _position_side_for_mode(self.position_mode, logical_side)
        q_close, _ = self._quantize(symbol, qty, None, filters=filters)
        params = {
            "symbol": symbol,
            "side": exit_side,
            "type": "MARKET",
            "quantity": q_close,
            "newClientOrderId": _make_cid(sid, reason_tag),
            "newOrderRespType": "RESULT",
        }
        if self.position_mode == "oneway":
            params["reduceOnly"] = True
            self._validate_exit_contract(
                position_side=pos_side,
                reduce_only=True,
                close_position=False,
                quantity=float(q_close),
                order_type="MARKET",
                working_type=None,
                is_algo=False,
            )
        elif pos_side:
            params["positionSide"] = pos_side
            self._validate_exit_contract(
                position_side=pos_side,
                reduce_only=False,
                close_position=False,
                quantity=float(q_close),
                order_type="MARKET",
                working_type=None,
                is_algo=False,
            )
        j = client.post_plain_order(params)
        return {
            "close_order_id": j.get("orderId"),
            "close_client_id": params["newClientOrderId"],
            "close_order_status": j.get("status"),
            "close_reason_tag": reason_tag,
        }

    def _protection_confirmed(self, prot: dict, tps: list, trail_enabled: bool) -> bool:
        if prot.get("sl_algo_id") in (None, "", 0):
            return False
        for idx, _ in enumerate(tps, start=1):
            if prot.get(f"tp{idx}_algo_id") in (None, "", 0):
                return False
        if trail_enabled and not (prot.get("trail_client_id") or prot.get("trail_algo_id") or prot.get("trail_pending")):
            return False
        return True

    def _emit_protection_incident(self, sid: str, symbol: str, reason: str) -> None:
        self._exec_event({
            "sid": sid,
            "symbol": symbol,
            "action": "protection_invariant",
            "status": "failed",
            "reason": reason,
            "severity": "critical",
        })
        self._save_order_state(sid, {
            "action": "protection_invariant",
            "status": "failed",
            "symbol": symbol,
            "incident_flag": "protection_missing",
            "incident_reason": reason,
        })

    def _emergency_flatten_position(
        self,
        *,
        sid: str,
        symbol: str,
        logical_side: str,
        qty: float,
        client: "BinanceFuturesClient",
        filters: "FiltersCache",
    ) -> dict:
        close = self._submit_reduce_only_market_exit(
            sid=sid,
            symbol=symbol,
            logical_side=logical_side,
            qty=qty,
            reason_tag="emerg",
            client=client,
            filters=filters,
        )
        return {
            "emergency_order_id": close.get("close_order_id"),
            "emergency_client_id": close.get("close_client_id"),
        }

"""
lines.insert(idx1, hunk1)

# Now, we should also manually replace `_place_protective` entirely, and `_place_trailing_stop` entirely, and `_maybe_start_trailing_after_tp1` entirely.
# Because the other hunks were meant for them!
# Instead of doing that, I'll just apply the un-rejected patch chunks? Oh wait, the `patch` command ALREADY APPLIED THEM to `/tmp/orig/binance_execution/binance_executor.py`!
# The `patch` output was:
# Hunk #12 succeeded at 1543 ...
# That means `/tmp/orig/binance_execution/binance_executor.py` HAS the modified `_place_protective` and `_maybe_start_trailing_after_tp1` OR maybe it DID apply some and rejected others?
# Let's inspect `patch -d /tmp/orig/binance_execution -p4 < ...` output again:
# Hunk #5 FAILED at 785. (This is `_local_headroom_check` and `_place_protective` start)
# Hunk #10 FAILED at 1153. (This is `_place_trailing_stop` and `_arm_trailing_after_tp1_thread`)
# Hunk #11 FAILED at 1304.
# Hunk #16 FAILED at 1468.
