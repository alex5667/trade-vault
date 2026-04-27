
import os
import asyncio
import json
import time
from typing import Any, Dict
from collections import defaultdict
from dataclasses import dataclass

import redis.asyncio as aioredis  # type: ignore
from common.log import setup_logger

log = setup_logger("ab_winner_suggester")

@dataclass
class ArmStats:
    wins: int = 0
    losses: int = 0
    total: int = 0
    pnl_sum: float = 0.0
    pnl_sq_sum: float = 0.0 # for variance? maybe overkill
    
    @property
    def win_rate(self) -> float:
        return self.wins / self.total if self.total > 0 else 0.0
    
    @property
    def mean_pnl(self) -> float:
        return self.pnl_sum / self.total if self.total > 0 else 0.0

class ABWinnerSuggesterService:
    """
    Consumes POSITION_CLOSED events from events:trades stream.
    Aggregates stats per (ab_group, regime, ab_arm).
    Suggests winner arm for each context.
    """
    def __init__(self):
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.stream_name = os.getenv("AB_EVENTS_STREAM", "events:trades")
        self.group = os.getenv("AB_EVENTS_GROUP", "ab-suggester")
        self.consumer = os.getenv("AB_EVENTS_CONSUMER", f"c-{os.getpid()}")
        
        self.eval_interval = int(os.getenv("AB_SUGGEST_EVAL_SEC", "60"))
        self.lookback_ms = int(os.getenv("AB_SUGGEST_LOOKBACK_MS", "86400000")) # 24h
        self.min_samples = int(os.getenv("AB_SUGGEST_MIN_SAMPLES", "10"))
        self.key_prefix = os.getenv("AB_SUGGEST_KEY_PREFIX", "cfg:suggestions:entry_policy:ab_winner:v1")
        self.ttl_sec = int(os.getenv("AB_SUGGEST_TTL_SEC", "3600"))

        self.r = aioredis.from_url(self.redis_url, decode_responses=True)
        # In-memory aggregation: context -> arm -> stats
        # Context key: "{ab_group}:{regime}"
        self.stats: Dict[str, Dict[str, ArmStats]] = defaultdict(lambda: defaultdict(ArmStats))
        self.processed_ids: set = set() # Simple dedup for current session, relying on redis for persistence? 
        # Actually, if we restart, using stream consumer group gives us exactly-once mostly.
        # We'll just aggregate what we see in the stream window. But Redis stream has history.
        # Ideally we load history on startup or just roll with stream.
        # For simplicity v1: stream only.
        
        self.last_eval_ts = 0

    async def _ensure_group(self):
        try:
            await self.r.xgroup_create(self.stream_name, self.group, id="0", mkstream=True)
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                log.warning(f"Group create error: {e}")

    def _parse_event(self, msg_id: str, data: Dict[str, Any]) -> None:
        try:
            # Unwrap nested JSON payload if present
            if "payload" in data and isinstance(data["payload"], str):
                try:
                    payload_data = json.loads(data["payload"])
                    # Merge payload data into current data
                    data.update(payload_data)
                except (ValueError, json.JSONDecodeError):
                    pass

            etype = str(data.get("event_type", "")).upper()
            if etype != "POSITION_CLOSED":
                return

            # Handle nested metadata/meta/payload
            meta = {}
            if "meta" in data and isinstance(data["meta"], str):
                try:
                    meta = json.loads(data["meta"])
                except (ValueError, json.JSONDecodeError):
                    pass
            elif "metadata" in data and isinstance(data["metadata"], str):
                try:
                    meta = json.loads(data["metadata"])
                except (ValueError, json.JSONDecodeError):
                    pass
            else:
                 # Check flat fields if meta not present
                 meta = data

            # Extract keys
            group = str(meta.get("ab_group") or "default")
            regime = str(meta.get("regime") or "na")
            arm = str(meta.get("ab_arm") or "A").upper()
            
            pnl = float(data.get("pnl") or data.get("pnl_net") or 0.0)
            
            # Update stats
            ctx_key = f"{group}:{regime}"
            s = self.stats[ctx_key][arm]
            s.total += 1
            s.pnl_sum += pnl
            if pnl > 0:
                s.wins += 1
            else:
                s.losses += 1
                
        except Exception as e:
            log.warning(f"Failed to parse event {msg_id}: {e}")

    def choose_winner(self, arms: Dict[str, ArmStats]) -> str:
        """
        Simple heuristic:
        1. Must have min_samples
        2. Sort by mean PnL (primary) AND WinRate (secondary)
        3. A is default.
        """
        candidates = []
        for arm, s in arms.items():
            if s.total < self.min_samples:
                continue
            candidates.append((arm, s))
        
        if not candidates:
            return "A" # Default fallback
            
        # Score = mean_pnl (maybe handle risk adjusted later)
        # Tie-break with win_rate
        candidates.sort(key=lambda x: (x[1].mean_pnl, x[1].win_rate), reverse=True)
        
        best_arm, best_stats = candidates[0]
        
        # Safety check: if best arm is negative PnL, maybe revert to A? 
        # But maybe A is worse.
        # This suggester blindly follows profit.
        # Strategy logic handles drawdown stops.
        return best_arm

    async def _evaluate_and_publish(self):
        now = time.time()
        count = 0
        for ctx_key, arms in self.stats.items():
            winner = self.choose_winner(arms)
            # ctx_key is "{ab_group}:{regime}"
            redis_key = f"{self.key_prefix}:{ctx_key}"
            
            # Write suggestion with TTL
            # Downstream consumers (policy) will read this.
            # If TTL expires, they revert to default/legacy.
            await self.r.set(redis_key, winner, ex=self.ttl_sec)
            count += 1
            
        log.info(f"Evaluated {len(self.stats)} contexts, updated {count} suggestions.")
        self.stats.clear() # Reset for next window? 
        # CAVEAT: If we clear, we lose history. 
        # Better to keep sliding window? 
        # For V1, let's keep accumulation but maybe decay? 
        # Or simplistic: this service runs continuously, accumulating stats in memory.
        # If it restarts, it learns from scratch.
        # PROD: Should persist aggregates to Redis.
        # For now (MVP): In-memory accumulation.
        pass

    async def run(self):
        await self._ensure_group()
        log.info(f"AB Suggester started: stream={self.stream_name} group={self.group}")
        
        while True:
            try:
                # Read new events
                streams = {self.stream_name: ">"}
                msgs = await self.r.xreadgroup(self.group, self.consumer, streams, count=100, block=1000)
                
                if msgs:
                    for stream, events in msgs:
                        for msg_id, data in events:
                            self._parse_event(msg_id, data)
                            try:
                                await self.r.xack(self.stream_name, self.group, msg_id)
                            except Exception:
                                pass
                
                # Periodic evaluation
                now = time.time()
                if now - self.last_eval_ts > self.eval_interval:
                    await self._evaluate_and_publish()
                    self.last_eval_ts = now
                    
            except Exception as e:
                log.error(f"Loop error: {e}")
                await asyncio.sleep(1)

if __name__ == "__main__":
    svc = ABWinnerSuggesterService()
    asyncio.run(svc.run())
