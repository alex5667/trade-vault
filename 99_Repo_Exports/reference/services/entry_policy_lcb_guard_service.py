from __future__ import annotations

import asyncio
import json
import math
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Tuple

import redis.asyncio as aioredis # type: ignore
from common.log import setup_logger
from core.entry_policy_freeze import EntryPolicyFreezeV1

log = setup_logger("EntryPolicyLcbGuard")

def _now_ms() -> int:
    return int(time.time() * 1000)

@dataclass
class LcbConfig:
    in_stream: str
    group: str
    consumer: str
    min_samples: int
    z_score: float
    lcb_threshold: float
    streak_required: int
    min_freeze_duration_ms: int

    @staticmethod
    def from_env() -> LcbConfig:
        return LcbConfig(
            in_stream=os.getenv("LCB_GUARD_IN_STREAM", "events:trades"),
            group=os.getenv("LCB_GUARD_GROUP", "lcb-guard"),
            consumer=os.getenv("LCB_GUARD_CONSUMER", "c1"),
            min_samples=int(os.getenv("LCB_GUARD_MIN_SAMPLES", "20")),
            z_score=float(os.getenv("LCB_GUARD_Z", "1.96")), # 95% confidence
            lcb_threshold=float(os.getenv("LCB_GUARD_THRESHOLD", "0.0")),
            streak_required=int(os.getenv("LCB_GUARD_STREAK", "2")),
            min_freeze_duration_ms=int(os.getenv("LCB_GUARD_MIN_FREEZE_MS", "300000")), # 5 min
        )

class EntryPolicyLcbGuardService:
    """
    Consumes POSITION_CLOSED events (with R-multiple) to prove regime recovery.
    Uses Welford's algorithm for online variance/standard deviation.
    """
    def __init__(self) -> None:
        redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.r: aioredis.Redis = aioredis.from_url(redis_url, decode_responses=True)
        self.cfg = LcbConfig.from_env()
        self.stats_prefix = "lcb:stats:v1"

    async def _update_stats(self, key: str, r_mult: float) -> Tuple[int, float, float]:
        """
        Welford's algorithm for online variance.
        returns (n, mean, std)
        """
        # Fetch current stats
        raw = await self.r.hgetall(f"{self.stats_prefix}:{key}")
        n = int(raw.get("n", 0))
        mean = float(raw.get("mean", 0.0))
        m2 = float(raw.get("m2", 0.0))

        n += 1
        delta = r_mult - mean
        mean += delta / n
        delta2 = r_mult - mean
        m2 += delta * delta2

        variance = m2 / n if n > 1 else 0.0
        std = math.sqrt(max(0.0, variance))

        # Save back
        await self.r.hset(f"{self.stats_prefix}:{key}", mapping={
            "n": n,
            "mean": mean,
            "m2": m2,
            "last_r": r_mult,
            "updated_ts": _now_ms()
        })
        # Set TTL for stats (e.g. 7 days)
        await self.r.expire(f"{self.stats_prefix}:{key}", 7 * 86400)

        return n, mean, std

    async def _process_event(self, event_type: str, payload: Dict[str, Any]) -> None:
        if event_type != "POSITION_CLOSED":
            return

        # Core fields for LCB segmentation
        sym = str(payload.get("symbol", "")).upper()
        sid = str(payload.get("sid", ""))
        ab_arm = str(payload.get("ab_arm", "A")).upper()
        ab_group = str(payload.get("ab_group", "default")).lower()
        scenario = str(payload.get("scenario", "na")).lower()
        regime = str(payload.get("regime", "na")).lower()
        arm_ver = int(payload.get("arm_ver", 0))
        
        # Performance data
        r_mult = float(payload.get("r_mult", 0.0))
        pnl_net = float(payload.get("pnl", 0.0))
        
        # We only use Arm A to unfreeze, as it's the only one executing during shadow freeze.
        if ab_arm != "A":
            return

        if scenario not in ("reversal", "continuation"):
            return

        # 1) Update stats for this specific slice
        stats_key = f"{sym}:{regime}:{ab_group}:{scenario}:{ab_arm}:{arm_ver}"
        n, mean, std = await self._update_stats(stats_key, r_mult)
        
        # 2) Calculate LCB
        lcb = -10.0 # safe fallback
        if n >= self.cfg.min_samples:
            # LCB = mean - Z * (std / sqrt(n))
            sem = std / math.sqrt(n)
            lcb = mean - (self.cfg.z_score * sem)
        
        log.info(f"📊 [{sym}:{scenario}] R={r_mult:.2f} | N={n} Mean={mean:.2f} Std={std:.2f} LCB={lcb:.3f}")

        # 3) Check for active freeze
        freeze_key = f"cfg:entry_policy:freeze:v1:{sym}:{ab_group}:{scenario}"
        raw_freeze = await self.r.get(freeze_key)
        if not raw_freeze:
            # No freeze -> reset streak
            await self.r.hset(f"{self.stats_prefix}:{stats_key}", "streak", 0)
            return

        fz, _ = EntryPolicyFreezeV1.from_json(raw_freeze)
        if not fz or not fz.is_active():
            return

        # Unfreeze ONLY for shadow-frozen regimes (where Arm A was allowed to provide evidence)
        if fz.mode != "shadow":
            return

        # Check min duration
        now = _now_ms()
        elapsed = now - fz.created_ts_ms
        if elapsed < self.cfg.min_freeze_duration_ms:
            log.debug(f"⏳ [{sym}:{scenario}] Freeze too young: {elapsed/1000:.0f}s < {self.cfg.min_freeze_duration_ms/1000:.0f}s")
            return

        # 4) Update streak
        is_good = (lcb >= self.cfg.lcb_threshold)
        streak = int(await self.r.hget(f"{self.stats_prefix}:{stats_key}", "streak") or 0)
        
        if is_good:
            streak += 1
            await self.r.hset(f"{self.stats_prefix}:{stats_key}", "streak", streak)
            log.info(f"📈 [{sym}:{scenario}] Streak: {streak}/{self.cfg.streak_required} (LCB={lcb:.3f})")
        else:
            streak = 0
            await self.r.hset(f"{self.stats_prefix}:{stats_key}", "streak", 0)

        # 5) UNFREEZE if proof reached
        if streak >= self.cfg.streak_required:
            log.info(f"🔓 UNFREEZING [{sym}:{ab_group}:{scenario}] | Proof reached: LCB={lcb:.3f} N={n} Streak={streak}")
            await self.r.delete(freeze_key)
            # Reset streak after unfreeze
            await self.r.hset(f"{self.stats_prefix}:{stats_key}", "streak", 0)
            
            # Emit audit event for unfreeze?
            audit_payload = {
                "ts_ms": now,
                "event_type": "UNFREEZE_BY_PROOF",
                "symbol": sym,
                "group": ab_group,
                "scenario": scenario,
                "lcb": lcb,
                "n": n,
                "mean": mean,
                "std": std,
                "notes": f"Automatic unfreeze by LCB Guard. Mean R: {mean:.2f}"
            }
            await self.r.xadd("stream:trade:entry_audit", {"data": json.dumps(audit_payload)}, maxlen=10000, approximate=True)

    async def run_forever(self) -> None:
        log.info(f"🚀 LCB Guard Service starting | in={self.cfg.in_stream} group={self.cfg.group}")
        
    async def _ensure_group(self) -> None:
        while True:
            try:
                await self.r.xgroup_create(self.cfg.in_stream, self.cfg.group, id="0", mkstream=True)
                log.info(f"Created consumer group {self.cfg.group} on {self.cfg.in_stream}")
                return
            except Exception as e:
                err_str = str(e)
                if "BUSYGROUP" in err_str:
                    log.info(f"Consumer group {self.cfg.group} already exists")
                    return
                elif "loading the dataset in memory" in err_str.lower() or "busyloading" in err_str.lower():
                    log.warning(f"Redis is loading dataset in memory. Retrying group creation in 5s...")
                    await asyncio.sleep(5)
                else:
                    log.error(f"Failed to create consumer group: {e}. Retrying in 5s...")
                    await asyncio.sleep(5)

    async def run_forever(self) -> None:
        log.info(f"🚀 LCB Guard Service starting | in={self.cfg.in_stream} group={self.cfg.group}")
        
        # Ensure group exists at startup
        await self._ensure_group()

        while True:
            try:
                # Read from stream
                msgs = await self.r.xreadgroup(self.cfg.group, self.cfg.consumer, {self.cfg.in_stream: ">"}, count=50, block=2000)
                if not msgs:
                    continue

                for _, entries in msgs:
                    for msg_id, fields in entries:
                        try:
                            # Stream contains flattened fields if expanded by TradeEventsLogger
                            # or it might have a 'payload' field if it's a different event.
                            event_type = fields.get("event_type")
                            
                            # Reconstruct payload if JSON strings detected
                            payload = {}
                            for k, v in fields.items():
                                try:
                                    if isinstance(v, str) and (v.startswith("{") or v.startswith("[")):
                                        payload[k] = json.loads(v)
                                    else:
                                        payload[k] = v
                                except Exception:
                                    payload[k] = v
                            
                            await self._process_event(event_type, payload)
                        except Exception as e:
                            log.error(f"Error processing message {msg_id}: {e}")
                        finally:
                            await self.r.xack(self.cfg.in_stream, self.cfg.group, msg_id)

            except Exception as e:
                err_str = str(e)
                if "NOGROUP" in err_str:
                    log.warning(f"Consumer group missing (stream key likely expired). Re-creating...")
                    await self._ensure_group()
                elif "loading the dataset in memory" in err_str.lower() or "busyloading" in err_str.lower():
                    log.warning(f"Redis is loading the dataset in memory. Waiting 5s...")
                    await asyncio.sleep(5)
                else:
                    log.error(f"Stream error: {e}")
                    await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(EntryPolicyLcbGuardService().run_forever())
