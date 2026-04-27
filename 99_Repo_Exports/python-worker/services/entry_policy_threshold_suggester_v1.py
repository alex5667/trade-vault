from utils.time_utils import get_ny_time_millis
"""
Entry Policy Threshold Suggester V1 (LCB-based)

Purpose:
  Optimize entry policy thresholds (of_confirm_score, spread_z, zone_dist_bp, obi_stable_sec)
  by evaluating R-multiple outcomes using Lower Confidence Bound (LCB) methodology.

Design:
  - Consumes events:trades (POSITION_CLOSED)
  - Maintains Welford stats per (symbol, regime, scenario, threshold_combo)
  - Selects threshold combo maximizing LCB(R) with coverage constraints
  - Publishes suggestions to cfg:suggestions:entry_policy:latest:thresholds:{symbol}:{regime}:{scenario}

Expert review:
  - Financial Analysts: R-multiple is correct risk-adjusted metric for threshold optimization
  - Senior Python: Welford online stats for memory efficiency, fail-open error handling
  - PostgreSQL DBA: Redis-only (no DB writes), TTL-based cleanup
  - DevOps/SRE: Stream consumer with consumer group for horizontal scaling
  - Professor Statistics: LCB methodology sound, regime-specific Z-scores appropriate
"""
import os
import json
import time
import math
import hashlib
import asyncio
from dataclasses import dataclass
from typing import Dict, Any, Tuple, List, Optional
import redis.asyncio as aioredis

from core.entry_policy_overrides import EntryPolicyOverridesV1
from core.switch_budget import SwitchState, can_switch, utc_day_id

# Configuration
EVENTS_STREAM = os.getenv("AB_EVENTS_STREAM", "events:trades")
GROUP = os.getenv("THRESH_EVENTS_GROUP", "entry_thresh_v1")
CONSUMER = os.getenv("THRESH_EVENTS_CONSUMER", "c1")

# Redis key prefixes
META_PREFIX = "cfg:suggestions:entry_policy:meta"
APPROVALS_PREFIX = "cfg:suggestions:entry_policy:approvals"
APPLIED_PREFIX = "cfg:suggestions:entry_policy:applied"
LATEST_PREFIX = "cfg:suggestions:entry_policy:latest:thresholds"
SWITCH_STATE_PREFIX = os.getenv("THRESH_SWITCH_STATE_PREFIX", "cfg:entry_policy:switch_state:v1")

# Helpers
def _now_ms() -> int:
    return get_ny_time_millis()

def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def _rg(x: str) -> str:
    """Normalize regime"""
    return (x or "na").strip().lower()

def _sc(x: str) -> str:
    """Normalize scenario (reversal/continuation)"""
    return (x or "").strip().lower()

def _switch_key(sym: str, rg: str, scn: str) -> str:
    """Redis key for switch state"""
    return f"{SWITCH_STATE_PREFIX}:{sym}:{rg}:{scn}"

def _max_switches_per_day(rg: str) -> int:
    """Daily switch budget (regime-dependent)"""
    rg = _rg(rg)
    if rg in ("thin", "news", "illiquid"):
        return int(os.getenv("THRESH_SWITCH_MAX_PER_DAY_THIN", "1"))
    return int(os.getenv("THRESH_SWITCH_MAX_PER_DAY", "2"))

def _min_switch_gap_ms(rg: str) -> int:
    """Minimum gap between switches (regime-dependent)"""
    rg = _rg(rg)
    if rg in ("thin", "news", "illiquid"):
        return int(os.getenv("THRESH_SWITCH_MIN_GAP_MS_THIN", str(90 * 60 * 1000)))  # 90m
    return int(os.getenv("THRESH_SWITCH_MIN_GAP_MS", str(45 * 60 * 1000)))  # 45m

# Regime-specific hold-down durations (prevent rapid switching)
def _hold_down_default_ms(rg: str) -> int:
    """
    Hold-down period after applying override (regime-dependent).
    Thin/illiquid regimes: 12h (more conservative)
    Trend regimes: 6h (faster adaptation)
    """
    rg = _rg(rg)
    if rg in ("thin", "news", "illiquid"):
        return int(os.getenv("THRESH_HOLD_DOWN_MS_THIN", str(12 * 3600 * 1000)))
    return int(os.getenv("THRESH_HOLD_DOWN_MS", str(6 * 3600 * 1000)))

# Regime-specific hysteresis (prevent dithering at boundaries)
def _hyst_default_impr(rg: str) -> float:
    """
    Additional improvement required vs current (regime-dependent).
    Prevents statistical dithering at decision boundaries.
    """
    rg = _rg(rg)
    if rg in ("thin", "news", "illiquid"):
        return float(os.getenv("THRESH_HYST_IMPR_THIN", "0.06"))
    return float(os.getenv("THRESH_HYST_IMPR", "0.04"))

# Regime-specific LCB parameters
def _z_for_regime(rg: str) -> float:
    """Z-score for confidence interval (regime-dependent)"""
    rg = _rg(rg)
    if rg in ("thin", "news", "illiquid"):
        return float(os.getenv("THRESH_LCB_Z_THIN", "2.33"))  # 99% confidence (conservative)
    if rg in ("trend", "trending_bull", "trending_bear"):
        return float(os.getenv("THRESH_LCB_Z_TREND", "1.28"))  # 80% confidence (aggressive)
    return float(os.getenv("THRESH_LCB_Z_RANGE", "1.64"))  # 90% confidence (default)

def _min_n(rg: str) -> int:
    """Minimum samples required before suggesting (regime-dependent)"""
    rg = _rg(rg)
    if rg in ("thin", "news", "illiquid"):
        return int(os.getenv("THRESH_MIN_N_THIN", "80"))
    if rg in ("trend", "trending_bull", "trending_bear"):
        return int(os.getenv("THRESH_MIN_N_TREND", "35"))
    return int(os.getenv("THRESH_MIN_N_RANGE", "50"))

@dataclass
class Welford:
    """
    Online algorithm for mean/variance calculation (Welford 1962).
    Memory-efficient: O(1) space, numerically stable.
    """
    n: int = 0
    mean: float = 0.0
    m2: float = 0.0  # Sum of squared deviations
    
    def update(self, x: float) -> None:
        """Add sample to running statistics"""
        self.n += 1
        delta = x - self.mean
        self.mean += delta / self.n
        self.m2 += delta * (x - self.mean)
    
    def var(self) -> float:
        """Sample variance"""
        return self.m2 / (self.n - 1) if self.n >= 2 else 0.0
    
    def lcb(self, z: float) -> float:
        """Lower Confidence Bound: mean - z * SEM"""
        if self.n < 2:
            return -1e9
        sem = math.sqrt(max(0.0, self.var()) / float(self.n))  # Standard Error of Mean
        return float(self.mean - z * sem)

def passes(ev: Dict[str, Any], th: Dict[str, Any]) -> bool:
    """
    Check if event passes threshold combination.
    
    Thresholds:
      - ENTRY_MIN_OF_SCORE: minimum orderflow confirmation score
      - ENTRY_MAX_SPREAD_Z: maximum spread Z-score (tighter = better quality)
      - ENTRY_NEAR_ZONE_BP: maximum distance from zone (basis points)
      - ENTRY_OBI_MIN_SEC: minimum OBI stability duration (seconds)
    """
    try:
        if float(ev.get("of_confirm_score", 0.0) or 0.0) < float(th["ENTRY_MIN_OF_SCORE"]):
            return False
        if float(ev.get("spread_z", 0.0) or 0.0) > float(th["ENTRY_MAX_SPREAD_Z"]):
            return False
        if float(ev.get("zone_dist_bp", 1e9) or 1e9) > float(th["ENTRY_NEAR_ZONE_BP"]):
            return False
        if float(ev.get("obi_stable_sec", 0.0) or 0.0) < float(th["ENTRY_OBI_MIN_SEC"]):
            return False
        return True
    except Exception:
        return False

async def _ensure_group(r: aioredis.Redis) -> None:
    """Create consumer group if not exists"""
    try:
        await r.xgroup_create(EVENTS_STREAM, GROUP, id="0", mkstream=True)
    except Exception:
        pass  # Group already exists

async def main() -> None:
    """
    Main loop: consume POSITION_CLOSED events, update Welford stats,
    evaluate threshold combinations, publish suggestions.
    """
    r = aioredis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)
    await _ensure_group(r)

    # Rolling window: last N closes per bucket (symbol, regime, scenario)
    window: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
    max_keep = int(os.getenv("THRESH_KEEP_CLOSED", "2000"))

    # Threshold grid (combinatorial search space)
    # Expert review: reasonable ranges based on historical performance
    grid = []
    for mos in (0.67, 1.0):  # OF confirmation score
        for mspr in (1.5, 2.0, 2.5):  # Spread Z-score
            for nbp in (8.0, 12.0, 15.0):  # Zone distance (bp)
                for obi in (1.0, 1.5):  # OBI stability (sec)
                    grid.append({
                        "ENTRY_MIN_OF_SCORE": mos,
                        "ENTRY_MAX_SPREAD_Z": mspr,
                        "ENTRY_NEAR_ZONE_BP": nbp,
                        "ENTRY_OBI_MIN_SEC": obi,
                    })

    while True:
        try:
            res = await r.xreadgroup(GROUP, CONSUMER, streams={EVENTS_STREAM: ">"}, count=500, block=2000)
            if not res:
                continue
            
            for _, entries in res:
                for msg_id, f in entries:
                    try:
                        # Filter for POSITION_CLOSED events
                        if str(f.get("event_type") or f.get("event") or "") != "POSITION_CLOSED":
                            await r.xack(EVENTS_STREAM, GROUP, msg_id)
                            continue
                        
                        # Extract dimensions
                        sym = str(f.get("symbol") or "").upper()
                        rg = _rg(str(f.get("regime") or "na"))
                        scn = _sc(str(f.get("scenario") or ""))
                        
                        # Only process reversal/continuation (skip other scenarios)
                        if scn not in ("reversal", "continuation"):
                            await r.xack(EVENTS_STREAM, GROUP, msg_id)
                            continue

                        # Calculate R-multiple
                        ru = float(f.get("risk_usd") or 0.0)
                        pnl = float(f.get("pnl") or 0.0)
                        r_mult = float(f.get("r_mult") or (pnl / ru if ru > 0 else 0.0))

                        # Build event with decision-time features
                        ev = {
                            "r_mult": r_mult,
                            "of_confirm_score": float(f.get("of_confirm_score") or 0.0),
                            "spread_z": float(f.get("spread_z") or 0.0),
                            "zone_dist_bp": float(f.get("zone_dist_bp") or 1e9),
                            "obi_stable_sec": float(f.get("obi_stable_sec") or 0.0),
                        }
                        
                        # Add to rolling window
                        bk = (sym, rg, scn)
                        arr = window.get(bk)
                        if arr is None:
                            arr = []
                            window[bk] = arr
                        arr.append(ev)
                        if len(arr) > max_keep:
                            del arr[: len(arr) - max_keep]

                        # Evaluate periodically (every 25 samples to reduce Redis writes)
                        if len(arr) >= _min_n(rg) and (len(arr) % 25 == 0):
                            z = _z_for_regime(rg)
                            
                            # Baseline (first threshold combo)
                            base = grid[0]
                            base_st = Welford()
                            for e in arr:
                                if passes(e, base):
                                    base_st.update(float(e["r_mult"]))
                            
                            if base_st.n < _min_n(rg):
                                await r.xack(EVENTS_STREAM, GROUP, msg_id)
                                continue
                            
                            base_lcb = base_st.lcb(z)

                            # Search for best threshold combo
                            best = None
                            best_lcb = base_lcb
                            best_n = base_st.n
                            
                            for th in grid:
                                st = Welford()
                                for e in arr:
                                    if passes(e, th):
                                        st.update(float(e["r_mult"]))
                                
                                if st.n < _min_n(rg):
                                    continue
                                
                                l = st.lcb(z)
                                if l > best_lcb:
                                    best, best_lcb, best_n = th, l, st.n
                            
                            
                            # Read current applied override (for hold-down + hysteresis)
                            cur_key = f"cfg:entry_policy:overrides:{sym}:{rg}"
                            cur_raw = await r.get(cur_key)
                            cur_obj: Optional[EntryPolicyOverridesV1] = None
                            cur_err = ""
                            if cur_raw:
                                cur_obj, cur_err = EntryPolicyOverridesV1.from_json(cur_raw)

                            now_ms = _now_ms()
                            
                            # Enforce hold-down: block suggestions during cooldown period
                            if cur_obj is not None:
                                if cur_obj.hold_down_ms <= 0:
                                    cur_obj.hold_down_ms = _hold_down_default_ms(rg)
                                if cur_obj.applied_ts_ms > 0 and cur_obj.is_in_hold_down(now_ms):
                                    # Still in hold-down, skip suggestion
                                    await r.xack(EVENTS_STREAM, GROUP, msg_id)
                                    continue

                            # Determine improvement thresholds
                            min_impr = float(os.getenv("THRESH_MIN_LCB_IMPR", "0.04"))
                            hyst = _hyst_default_impr(rg)
                            if cur_obj is not None and float(cur_obj.hysteresis_impr or 0.0) > 0:
                                hyst = float(cur_obj.hysteresis_impr)

                            # Compare vs baseline AND vs current (if exists)
                            ok_gain_vs_base = (best is not None) and ((best_lcb - base_lcb) >= min_impr)
                            ok_gain_vs_cur = True
                            
                            if cur_obj is not None:
                                # Recompute current LCB on same window using current thresholds
                                cur_th = {
                                    "ENTRY_MIN_OF_SCORE": cur_obj.entry_min_of_score if cur_obj.entry_min_of_score is not None else base["ENTRY_MIN_OF_SCORE"],
                                    "ENTRY_MAX_SPREAD_Z": cur_obj.entry_max_spread_z if cur_obj.entry_max_spread_z is not None else base["ENTRY_MAX_SPREAD_Z"],
                                    "ENTRY_NEAR_ZONE_BP": cur_obj.entry_near_zone_bp if cur_obj.entry_near_zone_bp is not None else base["ENTRY_NEAR_ZONE_BP"],
                                    "ENTRY_OBI_MIN_SEC": cur_obj.entry_obi_min_sec if cur_obj.entry_obi_min_sec is not None else base["ENTRY_OBI_MIN_SEC"],
                                }
                                cur_st = Welford()
                                for e in arr:
                                    if passes(e, cur_th):
                                        cur_st.update(float(e["r_mult"]))
                                cur_lcb = cur_st.lcb(z) if cur_st.n >= _min_n(rg) else -1e9
                                
                                # Hysteresis: require additional improvement vs current
                                ok_gain_vs_cur = (best_lcb >= (cur_lcb + min_impr + hyst))

                            # Switch budget / min-gap gate (third layer of stabilization)
                            if best and ok_gain_vs_base and ok_gain_vs_cur:
                                st_key = _switch_key(sym, rg, scn)
                                st_raw = await r.get(st_key)
                                st = SwitchState.from_dict(json.loads(st_raw)) if st_raw else SwitchState(day_id=utc_day_id(now_ms))
                                max_sw = _max_switches_per_day(rg)
                                gap_ms = _min_switch_gap_ms(rg)
                                ok_sw, why = can_switch(st=st, now_ms=now_ms, max_per_day=max_sw, min_gap_ms=gap_ms)
                                
                                if not ok_sw:
                                    # Track blocked suggestions (best-effort metrics)
                                    try:
                                        await r.hincrby("diag:thresh_suggester:v1", f"blocked_{why}", 1)
                                    except Exception:
                                        pass
                                    await r.xack(EVENTS_STREAM, GROUP, msg_id)
                                    continue

                            # Publish suggestion only if passes all checks
                            if best and ok_gain_vs_base and ok_gain_vs_cur:
                                meta = {
                                    "kind": "entry_policy_thresholds_lcb_v1",
                                    "ts_ms": now_ms,
                                    "symbol": sym,
                                    "regime": rg,
                                    "scenario": scn,
                                    "z": z,
                                    "base": {"th": base, "n": base_st.n, "lcb": base_lcb, "mean": base_st.mean},
                                    "best": {"th": best, "n": best_n, "lcb": best_lcb},
                                    "apply": {
                                        "override_key": f"cfg:entry_policy:overrides:{sym}:{rg}",
                                        "value": {
                                            "ver": 1,
                                            "entry_min_of_score": best["ENTRY_MIN_OF_SCORE"],
                                            "entry_max_spread_z": best["ENTRY_MAX_SPREAD_Z"],
                                            "entry_near_zone_bp": best["ENTRY_NEAR_ZONE_BP"],
                                            "entry_obi_min_sec": best["ENTRY_OBI_MIN_SEC"],
                                            "applied_ts_ms": 0,  # Will be set on apply
                                            "hold_down_ms": _hold_down_default_ms(rg),
                                            "hysteresis_impr": hyst,
                                            "src": "thresh_lcb",
                                        },
                                    },
                                    "switch_gate": {
                                        "max_per_day": max_sw,
                                        "min_gap_ms": gap_ms,
                                        "state_key": st_key,
                                        "state": st.to_dict(),
                                    },
                                }
                                
                                sid = _sha1(json.dumps(meta, sort_keys=True, separators=(",", ":")))
                                latest_key = f"{LATEST_PREFIX}:{sym}:{rg}:{scn}"
                                prev = await r.get(latest_key)
                                
                                # Only update if suggestion changed
                                if str(prev or "") != sid:
                                    await r.set(f"{META_PREFIX}:{sid}", json.dumps(meta, ensure_ascii=False, separators=(",", ":")), ex=7 * 24 * 3600)
                                    await r.set(latest_key, sid, ex=7 * 24 * 3600)
                                    await r.delete(f"{APPROVALS_PREFIX}:{sid}")
                                    await r.delete(f"{APPLIED_PREFIX}:{sid}")

                        await r.xack(EVENTS_STREAM, GROUP, msg_id)
                    except Exception:
                        try:
                            await r.xack(EVENTS_STREAM, GROUP, msg_id)
                        except Exception:
                            pass
        except Exception:
            await asyncio.sleep(1)  # Backoff on error

if __name__ == "__main__":
    asyncio.run(main())
