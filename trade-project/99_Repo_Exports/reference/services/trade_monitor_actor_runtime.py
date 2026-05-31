from __future__ import annotations

import os
from typing import Any, Dict, Optional, Callable
from concurrent.futures import Future

from services.sharded_serial_executor import ShardedSerialExecutor


class TradeMonitorActorRuntime:
    """
    Actor-like runtime:
      - fixed number of shard threads
      - each shard owns its TradeMonitorCore instance (state is NOT shared)
      - all calls are routed by symbol (preferred) or sid fallback
      - lossless-friendly: submit -> wait Future -> ACK
    """

    def __init__(self, *, core_factory: Callable[[int], Any], logger=None):
        self.logger = logger
        self.shards = int(os.getenv("TM_ACTOR_SHARDS", "8"))
        self.queue_max = int(os.getenv("TM_ACTOR_QUEUE_MAX", "20000"))
        self.submit_timeout_s = float(os.getenv("TM_ACTOR_SUBMIT_TIMEOUT_S", "2.0"))

        self.exec = ShardedSerialExecutor(
            shards=self.shards,
            queue_max=self.queue_max,
            submit_timeout_s=self.submit_timeout_s,
            name="TMActor",
            logger=logger,
        )
        # One core per shard => state is shard-local
        self.cores = [core_factory(i) for i in range(self.shards)]

    def shutdown(self) -> None:
        try:
            self.exec.shutdown(join_timeout_s=2.0)
        except Exception:
            pass

    def _route_key(self, *, symbol: Optional[str], sid: Optional[str]) -> str:
        if symbol:
            return str(symbol).upper()
        if sid:
            return f"sid:{sid}"
        return "unknown"

    def _core_for_key(self, key: str):
        # must match ShardedSerialExecutor routing: crc32(key) % shards
        sid = self.exec._pick_shard(key)  # intentionally uses executor routing
        return self.cores[int(sid)]

    def submit_tick(self, *, symbol: str, raw_tick: Dict[str, Any]) -> Future:
        key = self._route_key(symbol=symbol, sid=None)
        core = self._core_for_key(key)
        return self.exec.submit(key, lambda: core.on_tick(raw_tick), name=f"tick:{symbol}")

    def submit_signal(self, *, symbol: str, raw_signal: Dict[str, Any]) -> Future:
        key = self._route_key(symbol=symbol, sid=raw_signal.get("sid") or raw_signal.get("signal_id"))
        core = self._core_for_key(key)
        return self.exec.submit(key, lambda: core.on_signal(raw_signal), name=f"signal:{symbol}")

    def submit_event(self, *, symbol: Optional[str], sid: Optional[str], fn_name: str, payload: Dict[str, Any]) -> Future:
        """
        fn_name: which TradeMonitorCore handler to call: 'sl_hit', 'trailing_started', 'tp_hit', ...
        """
        key = self._route_key(symbol=symbol, sid=sid)
        core = self._core_for_key(key)

        def _run():
            # dispatch by name (kept minimal; you can replace with explicit methods)
            if fn_name == "sl_hit":
                return core.apply_external_sl_hit(
                    signal_id=str(sid or ""),
                    price=float(payload.get("price") or 0.0),
                    timestamp=int(payload.get("ts") or 0),
                    source=payload.get("source"),
                    event_id=payload.get("event_id"),
                )
            if fn_name == "trailing_started":
                return core.update_trailing_sl(
                    signal_id=str(sid or ""),
                    new_sl=float(payload.get("new_sl") or 0.0),
                    source=payload.get("source"),
                    profile=payload.get("profile"),
                    event_id=payload.get("event_id"),
                    clear_tp_levels=bool(payload.get("clear_tp_levels") or False),
                )
            if fn_name == "tp_hit":
                return core.apply_external_tp_hit(
                    signal_id=str(sid or ""),
                    tp_level=int(payload.get("tp_level") or 0),
                    price=float(payload.get("price") or 0.0),
                    timestamp=int(payload.get("ts") or 0),
                    event_id=payload.get("event_id"),
                )
            return True

        return self.exec.submit(key, _run, name=f"event:{fn_name}:{symbol or ''}:{sid or ''}")
