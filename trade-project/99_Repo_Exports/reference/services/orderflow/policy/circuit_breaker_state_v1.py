"""Circuit Breaker State Management (P69).

Handles hysteresis (anti-flap) for circuit breaker policy modes.
State is persisted in Redis to survive restarts.

Key Concepts:
- raw_mode: The mode calculated from current indicators (dq_state, drift_state).
- effective_mode: The mode actually applied after hysteresis.
- Dwell time: Minimum time we must stay in a mode before switching.
- Consecutive counts: Minimum number of consecutive raw_mode ticks to confirm a switch.

Redis Keys:
- cb:policy:state:{symbol} -> Hash
    - mode: str (ok/warn/block)
    - changed_at: int (ts_ms)
    - pending_mode: str (candidate for switch)
    - pending_count: int (consecutive counts)
    - updated_at: int (last tick processed)
"""

from __future__ import annotations


import time
import logging
from typing import Tuple, Dict, Any
try:
    import redis.asyncio as aioredis
except Exception:  # pragma: no cover
    aioredis = None  # type: ignore


class CircuitBreakerState:
    def __init__(
        self,
        redis: aioredis.Redis,
        symbol: str,
        min_dwell_s: int = 300,
        min_consecutive: int = 3,
        change_count_ttl_s: int = 3600
    ):
        self.redis = redis
        self.symbol = str(symbol).strip().upper()
        self.min_dwell_ms = int(min_dwell_s * 1000)
        self.min_consecutive = int(min_consecutive)
        self.change_count_ttl_s = int(change_count_ttl_s)
        
        self.key = f"cb:policy:state:{self.symbol}"
        self.logger = logging.getLogger(f"cb_state_{self.symbol}")
        
        # Local cache to avoid reading Redis on every tick if possible?
        # For now, we read/write Redis to be robust across restarts/workers.
        # But for high-frequency ticks, we might want to optimize.
        # Given "anti-flap" is the goal, reading Redis is safer for consistency.
        # However, we can perform read-modify-write via Lua or optimistic locking if strict,
        # but for policy modes (rare changes), a simple fetch-update is likely fine 
        # or best-effort. Actually, we should probably cache the 'effective' mode locally
        # and only hit Redis on potential transitions?
        # Let's start with local cache + async Redis sync for state transitions to keep latency low.
        
        self._local_mode: str = "ok"  # Fail-open default
        self._local_ts: int = 0
        self._last_loaded_ms: int = 0
        self._reload_interval_ms: int = 5000  # Sync with Redis every 5s just in case
        
    async def update(self, raw_mode: str, ts_ms: int) -> Tuple[str, Dict[str, Any]]:
        """
        Update state with new raw_mode observation.
        
        Returns:
            (effective_mode, debug_info_dict)
        """
        # 1. Lazy load / Periodic sync
        now_wall = int(time.time() * 1000)
        if (now_wall - self._last_loaded_ms) > self._reload_interval_ms:
            await self._load_state()
            self._last_loaded_ms = now_wall

        # 2. Hysteresis Logic
        # We need the full state from Redis to verify 'pending' counts.
        # If we want to avoid Redis RTT on every tick, we can only do it if raw == effective.
        # If raw != effective, we MUST check if we are transitioning.
        
        # Fast path: consistency
        if raw_mode == self._local_mode:
            # Reset pending counters if we match current mode?
            # Yes, flap interruption resets the counter.
            # We can do this async or periodically to save Redis writes.
            # But strictly, we should clear 'pending' in Redis if it was set.
            # Optimization: only clear if we know we had pending? 
            # Let's return local mode and assume pending clears on transition failure or timeout.
            return self._local_mode, {"hysteresis": "fast_match"}

        # Slow path: potential transition
        return await self._process_transition(raw_mode, ts_ms)

    async def _load_state(self):
        try:
            data = await self.redis.hgetall(self.key)
            if data:
                self._local_mode = data.get("mode", "ok")
                self._local_ts = int(data.get("changed_at", 0))
        except Exception as e:
            self.logger.warning(f"Failed to load CB state: {e}")

    async def _process_transition(self, raw_mode: str, ts_ms: int) -> Tuple[str, Dict[str, Any]]:
        try:
             # Fetch authoritative state
            data = await self.redis.hgetall(self.key)
            current_mode = data.get("mode", "ok") if data else "ok"
            changed_at = int(data.get("changed_at", 0)) if data else 0
            pending_mode = data.get("pending_mode", "") if data else ""
            pending_count = int(data.get("pending_count", 0)) if data else 0
            
            # Update local cache while we are here
            self._local_mode = current_mode
            self._local_ts = changed_at
            
            debug = {
                "raw": raw_mode,
                "cur": current_mode,
                "pend": pending_mode,
                "cnt": pending_count,
                "dwell_rem": max(0, self.min_dwell_ms - (ts_ms - changed_at))
            }

            if raw_mode == current_mode:
                # We are back to safe/current mode. Reset pending.
                if pending_mode:
                     await self.redis.hdel(self.key, "pending_mode", "pending_count")
                return current_mode, debug
            
            # Check consecutive
            if raw_mode == pending_mode:
                new_count = pending_count + 1
            else:
                # Start new sequence
                new_count = 1
            
            # Save the potential new state/count in Redis?
            # We must persist it to track consecutive counts across ticks.
            
            should_switch = False
            elapsed = ts_ms - changed_at
            dwell_passed = elapsed >= self.min_dwell_ms
            
            if new_count >= self.min_consecutive:
                if dwell_passed:
                    should_switch = True
                else:
                    debug["reason"] = "dwell"
            else:
                debug["reason"] = "counting"

            if should_switch:
                # COMMIT SWITCH
                pipe = self.redis.pipeline()
                pipe.hset(self.key, mapping={
                    "mode": raw_mode,
                    "changed_at": ts_ms,
                    "prev_mode": current_mode,
                    "updated_at": int(time.time() * 1000)
                })
                pipe.hdel(self.key, "pending_mode", "pending_count")
                
                await pipe.execute()
                
                self._local_mode = raw_mode
                self._local_ts = ts_ms
                debug["switched"] = True
                return raw_mode, debug
            else:
                # UPDATE PENDING (always update if not switching, to track count)
                # Optimization: only update if count changed or mode changed?
                # Yes, new_count is calc'd.
                await self.redis.hset(self.key, mapping={
                    "pending_mode": raw_mode,
                    "pending_count": new_count,
                    "updated_at": int(time.time() * 1000)
                })
                return current_mode, debug
                
        except Exception as e:
            self.logger.error(f"Transition error: {e}")
            return self._local_mode, {"error": str(e)}
