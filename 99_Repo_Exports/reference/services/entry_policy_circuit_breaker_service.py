"""
Entry Policy Circuit Breaker Service V1

Purpose:
  Auto-freeze entry execution when microstructure degrades.
  Consumes entry_audit stream, tracks P95 metrics, triggers freeze on 2-of-4 bad signals.

Metrics tracked (per symbol×group×scenario):
  - spread_z P95 (wide spreads = adverse selection)
  - obi_age_ms P95 (stale book = unreliable signals)
  - pressure_sps P95 (high churn = noisy market)
  - of_confirm_score EMA (low confirmation = weak setups)

Trigger logic:
  If 2+ metrics bad → freeze for 2-3h (regime-dependent)
  Anti-flap: min 30min gap between freeze activations

Expert review:
  - Financial Analysts: Protects against trading in degraded microstructure
  - Senior Python: P² quantile algorithm (O(1) memory), fail-open design
  - PostgreSQL DBA: Redis-only (no DB writes), stream consumer groups
  - DevOps/SRE: Horizontal scaling via consumer groups, observable freeze keys
  - Professor Statistics: P² algorithm accurate for P95, EMA for of_score smoothing
"""
from __future__ import annotations

import os
import json
import time
import asyncio
from dataclasses import dataclass, field
from typing import Any, Dict

import redis.asyncio as aioredis

from core.entry_policy_freeze import EntryPolicyFreezeV1
from core.switch_budget import utc_day_id


def _now_ms() -> int:
    return int(time.time() * 1000)


def _f(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        if v != v:  # NaN check
            return d
        return v
    except Exception:
        return d


def _s(x: Any, d: str = "") -> str:
    try:
        return str(x)
    except Exception:
        return d


class P2Quantile:
    """
    Lightweight streaming quantile estimator (P² algorithm, Jain & Chlamtac 1985).
    
    O(1) memory, O(1) update time. Good enough for P95 gating without storing full windows.
    
    Algorithm maintains 5 markers (heights + positions) and adjusts them incrementally.
    Accuracy: ~1-2% error for P95 with >100 samples.
    """
    def __init__(self, p: float):
        self.p = float(p)  # Target quantile (e.g., 0.95)
        self.n = 0
        self._x = []  # Bootstrap first 5 samples
        # Markers (after bootstrap)
        self.q = [0.0] * 5  # Heights (quantile estimates)
        self.np = [0.0] * 5  # Desired positions
        self.ni = [0] * 5  # Actual positions
        self.dn = [0.0, p/2.0, p, (1+p)/2.0, 1.0]  # Position increments

    def add(self, x: float) -> None:
        """Add sample to streaming quantile"""
        x = float(x)
        self.n += 1
        
        # Bootstrap: collect first 5 samples
        if len(self._x) < 5:
            self._x.append(x)
            if len(self._x) == 5:
                self._x.sort()
                self.q = self._x[:]
                self.ni = [1, 2, 3, 4, 5]
                self.np = [1.0, 1.0 + 2.0*self.p, 1.0 + 4.0*self.p, 3.0 + 2.0*self.p, 5.0]
            return
        
        # Find cell k where x belongs
        k = 0
        if x < self.q[0]:
            self.q[0] = x
            k = 0
        elif x < self.q[1]:
            k = 0
        elif x < self.q[2]:
            k = 1
        elif x < self.q[3]:
            k = 2
        elif x <= self.q[4]:
            k = 3
        else:
            self.q[4] = x
            k = 3
        
        # Increment positions
        for i in range(k+1, 5):
            self.ni[i] += 1
        for i in range(5):
            self.np[i] += self.dn[i]
        
        # Adjust marker heights (P² algorithm core)
        for i in (1, 2, 3):
            d = self.np[i] - self.ni[i]
            if (d >= 1 and self.ni[i+1] - self.ni[i] > 1) or (d <= -1 and self.ni[i-1] - self.ni[i] < -1):
                s = 1 if d > 0 else -1
                qn = self._parabolic(i, s)
                if self.q[i-1] < qn < self.q[i+1]:
                    self.q[i] = qn
                else:
                    self.q[i] = self._linear(i, s)
                self.ni[i] += s

    def _parabolic(self, i: int, d: int) -> float:
        """Parabolic interpolation for marker adjustment"""
        q = self.q
        n = self.ni
        return q[i] + d / (n[i+1]-n[i-1]) * (
            (n[i]-n[i-1]+d) * (q[i+1]-q[i]) / (n[i+1]-n[i]) +
            (n[i+1]-n[i]-d) * (q[i]-q[i-1]) / (n[i]-n[i-1])
        )

    def _linear(self, i: int, d: int) -> float:
        """Linear interpolation fallback"""
        return self.q[i] + d * (self.q[i+d] - self.q[i]) / (self.ni[i+d] - self.ni[i])

    def value(self) -> float:
        """Get current quantile estimate"""
        if len(self._x) < 5:
            if not self._x:
                return 0.0
            xs = sorted(self._x)
            idx = int(round((len(xs)-1)*self.p))
            return float(xs[max(0, min(len(xs)-1, idx))])
        return float(self.q[2])  # q[2] tracks target quantile


@dataclass
class KeyStats:
    """
    Per-(symbol, group, scenario) statistics aggregation.
    
    Tracks P95 for spread/obi-age/pressure and EMA for of_score.
    """
    n: int = 0
    spread_z_p95: P2Quantile = field(default_factory=lambda: P2Quantile(0.95))
    obi_age_p95: P2Quantile = field(default_factory=lambda: P2Quantile(0.95))
    pressure_p95: P2Quantile = field(default_factory=lambda: P2Quantile(0.95))
    of_score_ema: float = 0.0
    last_freeze_ts_ms: int = 0
    recover_streak: int = 0  # Consecutive samples in recovery zone

    def add(self, *, spread_z: float, obi_age_ms: float, pressure_sps: float, of_score: float) -> None:
        """Add sample to running statistics"""
        self.n += 1
        
        if spread_z > 0:
            self.spread_z_p95.add(spread_z)
        if obi_age_ms > 0:
            self.obi_age_p95.add(obi_age_ms)
        if pressure_sps > 0:
            self.pressure_p95.add(pressure_sps)
        
        # EMA for of_score (0..1 range typically)
        alpha = float(os.getenv("CB_OF_SCORE_EMA_ALPHA", "0.06"))
        if self.n == 1:
            self.of_score_ema = float(of_score)
        else:
            self.of_score_ema = (1.0 - alpha) * float(self.of_score_ema) + alpha * float(of_score)


class EntryPolicyCircuitBreakerService:
    """
    Circuit breaker service: monitors audit stream and triggers freeze on bad metrics.
    
    V2 features:
      - Shadow mode (degrade gracefully) vs hard mode (stop completely)
      - Auto-unfreeze when metrics recover (hysteresis + streak)
    """
    def __init__(self) -> None:
        redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.r = aioredis.from_url(redis_url, decode_responses=True, socket_connect_timeout=10, socket_timeout=30, max_connections=50)
        
        self.stream = os.getenv("CB_AUDIT_STREAM", "stream:trade:entry_audit")
        self.group = os.getenv("CB_GROUP", "entry-cb")
        self.consumer = os.getenv("CB_CONSUMER", f"cb-{os.getpid()}")
        self.block_ms = int(os.getenv("CB_BLOCK_MS", "1000"))
        self.read_count = int(os.getenv("CB_READ_COUNT", "200"))

        # Trigger thresholds
        self.min_n = int(os.getenv("CB_MIN_SAMPLES", "60"))
        self.freeze_ms = int(os.getenv("CB_FREEZE_MS", str(2*60*60*1000)))  # 2h default
        self.freeze_ms_thin = int(os.getenv("CB_FREEZE_MS_THIN", str(3*60*60*1000)))  # 3h thin
        self.min_gap_between_freezes_ms = int(os.getenv("CB_MIN_GAP_BETWEEN_FREEZES_MS", str(30*60*1000)))  # 30min

        # Auto-unfreeze parameters
        self.min_hold_ms = int(os.getenv("CB_FREEZE_MIN_HOLD_MS", str(12*60*1000)))  # 12min min hold
        self.recover_streak_need = int(os.getenv("CB_RECOVER_STREAK", "40"))  # 40 samples
        self.hyst = float(os.getenv("CB_RECOVER_HYST", "0.80"))  # 80% of thresholds
        self.recover_of_bonus = float(os.getenv("CB_RECOVER_OF_BONUS", "0.05"))  # +5% bonus

        # Mode thresholds (hard vs shadow)
        self.hard_bad_cnt = int(os.getenv("CB_HARD_BAD_CNT", "3"))  # 3+ bad → hard, 2 bad → shadow

        # Metric thresholds (2-of-4 must be bad to trigger)
        self.max_spread_z_p95 = float(os.getenv("CB_SPREAD_Z_P95_MAX", "3.0"))
        self.max_obi_age_p95_ms = float(os.getenv("CB_OBI_AGE_P95_MAX_MS", "1500"))
        self.max_pressure_p95 = float(os.getenv("CB_PRESSURE_P95_MAX", "1.4"))
        self.min_of_score_ema = float(os.getenv("CB_OF_SCORE_EMA_MIN", "0.55"))

        self.stats: Dict[str, KeyStats] = {}

    def _key(self, sym: str, grp: str, scn: str) -> str:
        """Stats key: symbol:group:scenario"""
        return f"{sym}:{grp}:{scn}"

    async def ensure_group(self) -> None:
        """Create consumer group if not exists"""
        try:
            await self.r.xgroup_create(self.stream, self.group, id="0", mkstream=True)
        except Exception:
            pass  # Group already exists

    async def run_forever(self) -> None:
        """Main loop: consume audit stream and trigger freezes"""
        await self.ensure_group()
        
        while True:
            try:
                resp = await self.r.xreadgroup(
                    self.group, self.consumer,
                    {self.stream: ">"},
                    count=self.read_count,
                    block=self.block_ms
                )
            except Exception:
                await asyncio.sleep(0.5)
                continue
            
            if not resp:
                continue
            
            for _stream, msgs in resp:
                for msg_id, fields in msgs:
                    await self._process_one(msg_id, fields)

    async def _process_one(self, msg_id: str, fields: Dict[str, Any]) -> None:
        """Process single audit event"""
        try:
            # Filter for entry_policy_audit events
            if str(fields.get("type", "")) != "entry_policy_audit":
                await self.r.xack(self.stream, self.group, msg_id)
                return
            
            payload_raw = fields.get("payload")
            if not payload_raw:
                await self.r.xack(self.stream, self.group, msg_id)
                return
            
            p = json.loads(payload_raw)
        except Exception:
            await self.r.xack(self.stream, self.group, msg_id)
            return

        # Extract dimensions
        sym = _s(p.get("symbol", "")).upper()
        scn = _s(p.get("decision", "")).lower()
        
        if scn not in ("reversal", "continuation"):
            await self.r.xack(self.stream, self.group, msg_id)
            return
        
        # Group extraction (prefer ab_group if available)
        group = _s(p.get("ab_group", "default")).lower()
        if not group:
            group = "default"

        # Extract metrics from payload
        snap = p.get("snap") if isinstance(p.get("snap"), dict) else {}
        
        spread_z = _f(p.get("spread_z", snap.get("spread_z", 0.0)), 0.0)
        obi_age_ms = _f(p.get("obi_age_ms", snap.get("obi_age_ms", 0.0)), 0.0)
        pressure_sps = _f(p.get("pressure_sps", snap.get("pressure_sps", 0.0)), 0.0)
        of_score = _f(p.get("of_confirm_score", snap.get("of_confirm_score", 0.0)), 0.0)

        # Update statistics
        k = self._key(sym, group, scn)
        st = self.stats.get(k)
        if st is None:
            st = KeyStats()
            self.stats[k] = st
        
        st.add(spread_z=spread_z, obi_age_ms=obi_age_ms, pressure_sps=pressure_sps, of_score=of_score)

        # Trigger freeze only after min samples
        if st.n < self.min_n:
            await self.r.xack(self.stream, self.group, msg_id)
            return

        now = _now_ms()
        
        # --- If freeze exists and active, attempt auto-unfreeze based on recovery ---
        fkey = f"cfg:entry_policy:freeze:v1:{sym}:{group}:{scn}"
        fraw = None
        try:
            fraw = await self.r.get(fkey)
        except Exception:
            pass

        if fraw:
            obj, _ = EntryPolicyFreezeV1.from_json(str(fraw))
            if obj and obj.is_active(now):
                # Check minimum hold period
                if obj.created_ts_ms > 0 and (now - int(obj.created_ts_ms)) >= int(self.min_hold_ms):
                    sp95 = st.spread_z_p95.value()
                    ob95 = st.obi_age_p95.value()
                    pr95 = st.pressure_p95.value()
                    of_ema = float(st.of_score_ema)

                    # Hysteresis-based recovery: must be better than threshold * hyst
                    ok_sp = (sp95 <= self.max_spread_z_p95 * self.hyst) if sp95 > 0 else True
                    ok_ob = (ob95 <= self.max_obi_age_p95_ms * self.hyst) if ob95 > 0 else True
                    ok_pr = (pr95 <= self.max_pressure_p95 * self.hyst) if pr95 > 0 else True
                    # of_ema must be better than min + bonus
                    ok_of = (of_ema >= (self.min_of_score_ema + self.recover_of_bonus)) if of_ema > 0 else True

                    recovered = bool(ok_sp and ok_ob and ok_pr and ok_of)
                    if recovered:
                        st.recover_streak += 1
                    else:
                        st.recover_streak = 0
                    
                    if st.recover_streak >= int(self.recover_streak_need):
                        # Recovery confirmed! Unfreeze early
                        try:
                            await self.r.delete(fkey)
                        except Exception:
                            pass
                        st.recover_streak = 0
                
                # If already frozen (even if in recovery process), skip trigger check
                await self.r.xack(self.stream, self.group, msg_id)
                return

        # Anti-flap: don't freeze too often
        if st.last_freeze_ts_ms > 0 and (now - st.last_freeze_ts_ms) < self.min_gap_between_freezes_ms:
            await self.r.xack(self.stream, self.group, msg_id)
            return

        # Evaluate metrics
        sp95 = st.spread_z_p95.value()
        ob95 = st.obi_age_p95.value()
        pr95 = st.pressure_p95.value()
        of_ema = float(st.of_score_ema)

        bad_spread = (sp95 >= self.max_spread_z_p95) if sp95 > 0 else False
        bad_obi = (ob95 >= self.max_obi_age_p95_ms) if ob95 > 0 else False
        bad_pressure = (pr95 >= self.max_pressure_p95) if pr95 > 0 else False
        bad_of = (of_ema <= self.min_of_score_ema) if of_ema > 0 else False

        # Trigger logic: 2-of-4 bad metrics (reduces false positives)
        bad_cnt = int(bad_spread) + int(bad_obi) + int(bad_pressure) + int(bad_of)
        if bad_cnt < 2:
            await self.r.xack(self.stream, self.group, msg_id)
            return

        # Determine mode: 3+ bad → hard, 2 bad → shadow
        mode = "hard" if bad_cnt >= int(self.hard_bad_cnt) else "shadow"
        
        # Determine freeze duration (regime-dependent)
        rg = _s(p.get("regime", "na")).lower()
        if rg in ("thin", "news", "illiquid") and bad_cnt >= 2:
            # Thin/news: prefer shadow degradation (hard only if extremely bad)
            mode = "hard" if bad_cnt >= max(int(self.hard_bad_cnt), 3) else "shadow"
            
        dur = self.freeze_ms_thin if rg in ("thin", "news", "illiquid") else self.freeze_ms
        until = now + int(dur)

        # Create freeze object
        fz = EntryPolicyFreezeV1(
            ver=1,
            symbol=sym,
            group=group,
            scenario=scn,
            until_ts_ms=until,
            mode=mode,
            reason_code="DATA_BAD",
            notes=f"bad_cnt={bad_cnt} spread_p95={sp95:.2f} obi_age_p95={ob95:.0f} pressure_p95={pr95:.2f} of_ema={of_ema:.2f}",
            src="cb_v1",
            created_ts_ms=now,
            metrics={
                "n": int(st.n),
                "spread_z_p95": float(sp95),
                "obi_age_ms_p95": float(ob95),
                "pressure_sps_p95": float(pr95),
                "of_score_ema": float(of_ema),
                "bad_spread": int(bad_spread),
                "bad_obi": int(bad_obi),
                "bad_pressure": int(bad_pressure),
                "bad_of": int(bad_of),
                "day_id": int(utc_day_id(now)),
            },
        )

        # Write freeze to Redis (best-effort)
        try:
            ttl = int(dur // 1000) + 300  # TTL = duration + 5min buffer
            await self.r.set(fkey, fz.to_json(), ex=ttl)
            st.last_freeze_ts_ms = now
        except Exception:
            pass  # Fail-open: don't block on Redis errors

        await self.r.xack(self.stream, self.group, msg_id)


async def _main() -> None:
    svc = EntryPolicyCircuitBreakerService()
    await svc.run_forever()


if __name__ == "__main__":
    asyncio.run(_main())
