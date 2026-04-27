from utils.time_utils import get_ny_time_millis

import asyncio
import hashlib
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Dict, Any, List

import redis.asyncio as aioredis
from core.ab_lcb_evaluator import eval_winner_lcb, RegimeThresholds

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ABWinnerSuggesterLCB")

def _now_ms() -> int:
    return get_ny_time_millis()

def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def regime_group(rg: str) -> str:
    r = (rg or "na").lower()
    return "thin" if r in ("thin","news","illiquid") else "default"

@dataclass
class Cfg:
    events_stream: str = os.getenv("AB_EVENTS_STREAM", "events:trades")
    group: str = os.getenv("AB_EVENTS_GROUP", "ab-winner-lcb")
    consumer: str = os.getenv("AB_EVENTS_CONSUMER", f"ab-winner-{os.getpid()}")
    read_count: int = int(os.getenv("AB_EVENTS_READ_COUNT", "200"))
    read_block_ms: int = int(os.getenv("AB_EVENTS_READ_BLOCK_MS", "1000"))

    agg_prefix: str = os.getenv("AB_AGG_PREFIX", "ab:vals:v1") # Values (lists)

    eval_interval_sec: int = int(os.getenv("AB_EVAL_INTERVAL_SEC", "60"))
    z: float = float(os.getenv("AB_LCB_Z", "1.96"))

    # suggestion keys
    latest_prefix: str = os.getenv("AB_LATEST_PREFIX", "cfg:suggestions:entry_policy:latest:ab_winner")
    meta_prefix: str = os.getenv("AB_META_PREFIX", "cfg:suggestions:entry_policy:meta")
    approvals_prefix: str = os.getenv("AB_APPROVALS_PREFIX", "cfg:suggestions:entry_policy:approvals")
    applied_prefix: str = os.getenv("AB_APPLIED_PREFIX", "cfg:suggestions:entry_policy:applied")

    # winner hysteresis
    winner_hold_down_ms: int = int(os.getenv("AB_WINNER_HOLD_DOWN_MS", "600000"))  # 10m
    winner_min_switch_gap_ms: int = int(os.getenv("AB_WINNER_MIN_SWITCH_GAP_MS", "1800000"))  # 30m

class ABWinnerSuggesterLCB:
    def __init__(self, one_shot: bool = False):
        self.cfg = Cfg()
        self.r = aioredis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)
        self.running = True
        self.one_shot = one_shot
        
        # Hysteresis state
        self._eff: Dict[str, str] = {} # effective winner
        self._eff_ts: Dict[str, int] = {}
        self._cand: Dict[str, str] = {} # candidate winner
        self._cand_ts: Dict[str, int] = {}

    async def _ensure_group(self):
        try:
            await self.r.xgroup_create(self.cfg.events_stream, self.cfg.group, id="$", mkstream=True)
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                logger.warning(f"Group create error: {e}")

    def _thr_map(self) -> Dict[str, RegimeThresholds]:
        # Helper to load thresholds from env (simplified)
        def _th(r: str, d_n=200, d_r=0.05, d_wr=0.45, d_d=0.0):
            n = int(os.getenv(f"AB_MIN_N_{r.upper()}", str(d_n)))
            lr = float(os.getenv(f"AB_MIN_LCB_R_{r.upper()}", str(d_r)))
            lwr = float(os.getenv(f"AB_MIN_LCB_WR_{r.upper()}", str(d_wr)))
            d = float(os.getenv(f"AB_MIN_DELTA_LCB_R_{r.upper()}", str(d_d)))
            # No Z override per regime in env parsing here for brevity, rely on default or extend if needed
            z = float(self.cfg.z) 
            return RegimeThresholds(min_n=n, min_lcb_r=lr, min_lcb_wr=lwr, min_delta_lcb_vs_a=d, z=z)

        return {
            "default": _th("DEFAULT", 200, 0.05, 0.45, 0.0),
            "thin": _th("THIN", 100, 0.02, 0.40, 0.0),
        }

    async def _read_stats(self, symbol: str, regime: str, group: str, scenario: str) -> Dict[str, List[float]]:
        # Read raw R-multiples for A, B, C
        arms = ["A", "B", "C"]
        res = {}
        for arm in arms:
            key = f"{self.cfg.agg_prefix}:{symbol}:{regime}:{group}:{scenario}:{arm}"
            # Limit to last 1000 items?
            vals = await self.r.lrange(key, 0, 999)
            floats = []
            for v in vals:
                try:
                    floats.append(float(v))
                except (ValueError, TypeError):
                    pass
            res[arm] = floats
        return res

    def _bucket_key(self, symbol: str, regime: str, group: str, scenario: str) -> str:
        return f"{symbol}:{regime}:{group}:{scenario}"

    def _winner_hysteresis(self, key: str, raw_winner: str, now_ms: int) -> str:
        eff = self._eff.get(key, "A")
        eff_ts = self._eff_ts.get(key, 0)
        
        # If forcing A (raw_winner "A"), apply immediately if we want fast fallback? 
        # Or apply hysteresis too? Usually fallback to A should be fast if A is better.
        # Expert logic usually implies stable switch.
        
        if raw_winner == eff:
            self._cand.pop(key, None)
            return eff
        
        # Candidate check
        cand = self._cand.get(key, "")
        cand_ts = self._cand_ts.get(key, 0)
        
        if cand != raw_winner:
            self._cand[key] = raw_winner
            self._cand_ts[key] = now_ms
            return eff
            
        # If candidate held long enough
        if cand_ts > 0 and (now_ms - cand_ts) >= self.cfg.winner_hold_down_ms:
            # Also check min switch gap
            if (now_ms - eff_ts) >= self.cfg.winner_min_switch_gap_ms:
                self._eff[key] = raw_winner
                self._eff_ts[key] = now_ms
                return raw_winner
        
        return eff

    async def _propose(self, *, symbol: str, regime: str, group: str, scenario: str, winner: str, dec: dict) -> None:
        sid = _sha1(json.dumps({"sym": symbol, "rg": regime, "g": group, "scn": scenario, "win": winner}, separators=(",", ":")))
        meta_key = f"{self.cfg.meta_prefix}:{sid}"
        appr_key = f"{self.cfg.approvals_prefix}:{sid}"
        appl_key = f"{self.cfg.applied_prefix}:{sid}"
        latest_key = f"{self.cfg.latest_prefix}:{symbol}:{regime}:{group}" # base
        latest_scn_key = f"cfg:suggestions:entry_policy:latest:ab_winner_scn:{symbol}:{regime}:{group}:{scenario}"

        meta = {
            "sid": sid,
            "ts_ms": _now_ms(),
            "symbol": symbol,
            "regime": regime,
            "group": group,
            "winner_arm": winner,
            "scenario": scenario,
            "type": "ab_winner_lcb_v2",
            "decision": dec,
        }
        
        try:
            pipe = self.r.pipeline()
            pipe.set(meta_key, json.dumps(meta, ensure_ascii=False, separators=(",", ":")), ex=14 * 24 * 3600)
            pipe.setnx(appr_key, "")
            pipe.setnx(appl_key, "")
            pipe.set(latest_key, sid, ex=14 * 24 * 3600)
            pipe.set(latest_scn_key, sid, ex=14 * 24 * 3600)
            await pipe.execute()
        except Exception:
            pass

    async def _eval_bucket(self, symbol: str, regime: str, group: str, scenario: str) -> None:
        samples = await self._read_stats(symbol, regime, group, scenario)
        thr_map = self._thr_map()
        
        dec = eval_winner_lcb(
            samples_by_arm=samples,
            regime=regime,
            group=group,
            scenario=scenario,
            thr_by_regime=thr_map,
            default_z=self.cfg.z,
        )

        if not dec.ok and dec.winner == "A":
             # Even if winner is A, we might need to switch BACK to A if currently B/C.
             # So we proceed to hysteresis check with "A"
             pass
        
        now = _now_ms()
        bkey = self._bucket_key(symbol, regime, group, scenario)
        chosen = self._winner_hysteresis(bkey, dec.winner, now)
        
        # Always proposal if it changes stable state or confirms it?
        # Use latest logic: suggest current chosen winner.
        
        # Serialize LCBs for debug
        lcb_r_clean = {k: float(v) for k, v in dec.lcb_r.items()}
        lcb_wr_clean = {k: float(v) for k, v in dec.lcb_wr.items()}
        n_clean = {k: int(v) for k, v in dec.n.items()}

        await self._propose(
            symbol=symbol, regime=regime, group=group, scenario=scenario, winner=chosen,
            dec={
                "ok": int(dec.ok),
                "raw_winner": dec.winner,
                "reason": dec.reason,
                "lcb_r": lcb_r_clean,
                "lcb_wr": lcb_wr_clean,
                "n": n_clean,
                "baseline_a_lcb_r": float(dec.baseline_a_lcb_r),
                "delta_lcb_vs_a": float(dec.delta_lcb_vs_a),
                "z": float(self.cfg.z),
            }
        )

    async def _process_msg(self, msg: Dict[str, Any]):
        try:
            # Flattened payload from TradeMonitor
            p = msg
            if "payload" in p and isinstance(p["payload"], str):
                 try:
                     p.update(json.loads(p["payload"]))
                 except (ValueError, TypeError):
                     pass

            sym = str(p.get("symbol") or "").upper()
            if not sym: return

            arm = str(p.get("ab_arm") or "A").upper()
            group = str(p.get("ab_group") or "default").lower()
            regime = str(p.get("regime") or "na").lower()
            scenario = str(p.get("scenario") or "").lower()
            if not scenario:
                # heuristics if missing
                scenario = "continuation" # default? or "na"

            r_mult = 0.0
            try:
                r_mult = float(p.get("r_mult") or 0.0)
            except (ValueError, TypeError):
                pass
            
            # Store sample
            # List key: ab:vals:v1:{sym}:{regime}:{group}:{scenario}:{arm}
            k = f"{self.cfg.agg_prefix}:{sym}:{regime}:{group}:{scenario}:{arm}"
            await self.r.lpush(k, r_mult)
            await self.r.ltrim(k, 0, 1999) # Keep 2000 samples

            # Mark bucket for update
            reg_key = "ab:agg:registry:v1"
            bk = f"{sym}|{regime}|{group}|{scenario}"
            await self.r.sadd(reg_key, bk)
            
        except Exception as e:
            logger.error(f"process error: {e}")

    async def _eval_all_buckets(self):
        reg_key = "ab:agg:registry:v1"
        try:
            members = await self.r.smembers(reg_key)
        except Exception:
            members = []
        
        for m in members:
            try:
                sym, rg, gr, scn = str(m).split("|", 3)
                await self._eval_bucket(sym, rg, gr, scn)
            except Exception:
                pass

    async def run(self):
        # If one-shot, eval all and exit
        if self.one_shot:
            logger.info("One-shot mode: evaluating all buckets...")
            await self._eval_all_buckets()
            logger.info("Done.")
            return

        # Else consumer loop
        await self._ensure_group()
        last_eval = _now_ms()
        
        while self.running:
            # 1. Read messages
            try:
                msgs = await self.r.xreadgroup(
                    groupname=self.cfg.group,
                    consumername=self.cfg.consumer,
                    streams={self.cfg.events_stream: ">"},
                    count=self.cfg.read_count,
                    block=self.cfg.read_block_ms,
                )
            except Exception:
                msgs = []
                await asyncio.sleep(1.0)

            if msgs:
                for _, entries in msgs:
                    for mid, fields in entries:
                        await self._process_msg(fields)
                        await self.r.xack(self.cfg.events_stream, self.cfg.group, mid)

            # 2. Periodic eval
            now = _now_ms()
            if (now - last_eval) >= self.cfg.eval_interval_sec * 1000:
                await self._eval_all_buckets()
                last_eval = now

if __name__ == "__main__":
    one_shot = "--once" in sys.argv
    svc = ABWinnerSuggesterLCB(one_shot=one_shot)
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(svc.run())
    except KeyboardInterrupt:
        pass
