from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import json
import os
import time
import logging
from dataclasses import dataclass
from typing import Any, Dict, Tuple

import redis.asyncio as aioredis # type: ignore

from services.abc_router import choose_arm_abc
from services.smt_entry_abc_config import ABCPolicyLoader, ArmPolicy
from services.entry_policy_overrides_v1 import EntryPolicyOverridesV1


def _now_ms() -> int:
    return get_ny_time_millis()


def _json_load(s: str) -> Dict[str, Any]:
    try:
        d = json.loads(s)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _json_dump(d: Dict[str, Any]) -> str:
    return json.dumps(d, ensure_ascii=False, separators=(",", ":"))


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return d


def _b(x: Any) -> bool:
    try:
        return int(x) == 1
    except Exception:
        return False


@dataclass
class Setup:
    bundle: str
    kind: str
    leader: str
    pick: str
    trend_dir: str
    ts_ms: int
    ttl_ms: int
    div: str = ""
    coh: float = 0.0
    leader_conf_score: float = 0.0


@dataclass
class RetestState:
    stage: str = "WAIT_TOUCH"     # WAIT_TOUCH|WAIT_AWAY|WAIT_RETEST
    zone_id: str = ""
    touch_ts_ms: int = 0
    away_ts_ms: int = 0
    emitted: int = 0
    # AB routing (locked once touch happens)
    ab_arm: str = "A"
    ab_key: str = ""
    ab_group: str = "default"
    ab_split_reason: str = ""
    ab_split_a: float = 0.0
    ab_split_b: float = 0.0
    ab_split_c: float = 0.0


@dataclass
class Streams:
    in_stream: str
    in_group: str
    in_consumer: str
    out_candidate: str
    out_audit: str


def _desired_side(setup: Setup) -> str:
    k = (setup.kind or "").lower()
    td = (setup.trend_dir or "").upper()
    dv = (setup.div or "").lower()
    if k == "continuation":
        if td == "UP":
            return "LONG"
        if td == "DOWN":
            return "SHORT"
        return "NONE"
    if k == "reversal":
        if dv.startswith("bullish"):
            return "LONG"
        if dv.startswith("bearish"):
            return "SHORT"
        return "NONE"
    return "NONE"


def _inside_band(px: float, lo: float, hi: float) -> bool:
    if px <= 0 or lo <= 0 or hi <= 0:
        return False
    a = min(lo, hi)
    b = max(lo, hi)
    if abs(a - b) < 1e-12:
        return False
    return a <= px <= b


def _mk_ab_key(setup: Any, snap: Dict[str, Any]) -> str:
    try:
        ts_ms = int(snap.get("ts_ms", 0) or 0)
    except Exception:
        ts_ms = 0
    mb = ts_ms // 60000 if ts_ms > 0 else 0
    zid = str(snap.get("zone_id", "") or "")
    bundle = str(getattr(setup, "bundle", "") or "")
    return f"{bundle}|{setup.kind}|{setup.leader}|{setup.pick}|{zid}|{mb}"


def _fsm_step(
    *
    setup: Setup
    st: RetestState
    snap: Dict[str, Any]
    now_ms: int
    touch_bp: float
    away_bp: float
    retest_bp: float
    pol: ArmPolicy = None
) -> Tuple[bool, str]:
    try:
        px = float(snap.get("close_px", 0.0) or 0.0)
        zid = str(snap.get("zone_id", "") or "")
        zlo = float(snap.get("zone_px_lo", 0.0) or 0.0)
        zhi = float(snap.get("zone_px_hi", 0.0) or 0.0)
        dist_bp = float(snap.get("zone_dist_bp", 0.0) or 0.0)
    except Exception:
        return False, "bad_snap_fields"

    if px <= 0 or not zid:
        return False, "no_price_or_zone"

    if st.zone_id and zid and st.zone_id != zid:
        st.stage = "WAIT_TOUCH"
        st.zone_id = ""
        st.touch_ts_ms = 0
        st.away_ts_ms = 0

    inside = _inside_band(px, zlo, zhi)
    near_touch = inside or (dist_bp > 0 and dist_bp <= float(touch_bp))
    far_away = (dist_bp >= float(away_bp)) if dist_bp > 0 else False
    near_retest = inside or (dist_bp > 0 and dist_bp <= float(retest_bp))

    if st.stage == "WAIT_TOUCH":
        if near_touch:
            st.stage = "WAIT_AWAY"
            st.zone_id = zid
            st.touch_ts_ms = now_ms
            return False, "touch"
        return False, "waiting_touch"

    if st.stage == "WAIT_AWAY":
        if far_away:
            st.stage = "WAIT_RETEST"
            st.away_ts_ms = now_ms
            return False, "away"
        return False, "waiting_away"

    if st.stage == "WAIT_RETEST":
        if near_retest:
            want = _desired_side(setup)
            zone_side = str(snap.get("zone_side", "NA") or "NA").upper()
            
            if want == "LONG" and zone_side not in ("SUP", "MID", "NA"):
                return False, "zone_side_mismatch"
            if want == "SHORT" and zone_side not in ("RES", "MID", "NA"):
                return False, "zone_side_mismatch"
            
            regime = str(snap.get("regime", "na") or "na").lower()
            unstable = int(snap.get("abs_lvl_th_unstable", 0) or 0)
            adx_q = float(snap.get("adx_q", 0.5) or 0.5)

            adx_trend_hi = float(os.getenv("SMT_ADX_TREND_HI_Q", "0.75"))
            adx_chop_lo = float(os.getenv("SMT_ADX_CHOP_LO_Q", "0.40"))

            of_score = float(snap.get("of_confirm_score", 0.0) or 0.0)
            min_score = float(os.getenv("SMT_ENTRY_MIN_OF_SCORE", "1.0"))
            if adx_q <= adx_chop_lo:
                min_score = max(min_score, float(os.getenv("SMT_ENTRY_MIN_OF_SCORE_CHOP", "1.0")))
            elif adx_q >= adx_trend_hi:
                min_score = float(os.getenv("SMT_ENTRY_MIN_OF_SCORE_TREND", str(min_score)))
            if of_score < min_score:
                return False, "of_score_low"

            strict_ctx = regime in ("thin", "news", "illiquid") or unstable == 1 or (adx_q <= adx_chop_lo)
            book_ok = int(snap.get("book_health_ok", 1) or 1)

            if strict_ctx:
                if book_ok == 1:
                    obi_sec = float(snap.get("obi_stable_sec", 0.0) or 0.0)
                    ice = int(snap.get("iceberg_strict", 0) or 0)
                    if not (obi_sec >= float(os.getenv("SMT_ENTRY_OBI_MIN_SEC", "1.5")) or ice == 1):
                        return False, "thin_need_obi_or_ice"
                else:
                    reclaim = int(snap.get("reclaim", 0) or 0)
                    abs_lvl_ok = int(snap.get("abs_lvl_ok", 0) or 0)
                    wp = int(snap.get("weak_progress", 0) or 0)
                    if not (reclaim == 1 or abs_lvl_ok == 1 or wp == 1):
                        return False, "unhealthy_book_no_pa_confirm"

            st.emitted = 1
            return True, "retest_ok"
        else:
            return False, "not_near_retest"
    return False, "waiting_retest"


class SmtEntryCandidateService:
    def __init__(self, r: aioredis.Redis) -> None:
        self.r = r
        self._abc = ABCPolicyLoader(cfg_key=os.getenv("CFG_SMT_ENTRY_ABC_KEY", "cfg:smt_entry:abc:config"))
        
        self.streams = Streams(
            in_stream=os.getenv("SMT_SETUP_STREAM", "stream:signals")
            in_group=os.getenv("SMT_SETUP_GROUP", "smt_entry")
            in_consumer=os.getenv("SMT_SETUP_CONSUMER", f"smt_entry:{os.getpid()}")
            out_candidate=os.getenv("SMT_ENTRY_STREAM", "stream:trade:entry_candidate")
            out_audit=os.getenv("SMT_ENTRY_AUDIT_STREAM", "stream:trade:entry_audit")
        )
        self.maxlen = int(os.getenv("SMT_ENTRY_MAXLEN", "20000"))
        self.snap_prefix = os.getenv("SMT_SNAP_PREFIX", "smt:snap:")
        
        self.max_wait_ms = int(os.getenv("SMT_RETEST_MAX_WAIT_MS", "120000"))
        self.dedup_ms = int(os.getenv("SMT_ENTRY_DEDUP_MS", "60000"))
        self.poll_ms = int(os.getenv("SMT_ENTRY_POLL_MS", "100"))
        self._dedup: Dict[str, int] = {}
        
        self._ovr: EntryPolicyOverridesV1 = EntryPolicyOverridesV1()
        self._ovr_loaded_ts_ms: int = 0
        self._ovr_last_apply_ts_ms: int = 0
        
        self.active: Dict[str, Tuple[Setup, RetestState]] = {}

    async def _maybe_poll_overrides(self, now_ms: int, group: str) -> None:
        try:
            hold_ms = int(getattr(self._ovr, "overrides_hold_down_ms", 60000) or 60000)
            if hold_ms > 0 and (now_ms - self._ovr_last_apply_ts_ms) < hold_ms:
                return
        except Exception:
            pass
        g = (group or "default").strip().lower()
        key_g = f"cfg:entry_policy:overrides:v1:{g}"
        key_0 = "cfg:entry_policy:overrides:v1"
        try:
            raw = str(await self.r.get(key_g) or "")
            if not raw:
                raw = str(await self.r.get(key_0) or "")
            if not raw:
                return
            o, status = EntryPolicyOverridesV1.from_json(raw)
            if o is None:
                return
            if int(o.updated_ts_ms or 0) <= self._ovr_loaded_ts_ms:
                return
            self._ovr = o
            self._ovr_loaded_ts_ms = int(o.updated_ts_ms or now_ms)
            self._ovr_last_apply_ts_ms = int(now_ms)
        except Exception:
            pass

    async def run_forever(self) -> None:
        logger = logging.getLogger("smt_entry_candidate")
        
        from core.redis_client import wait_for_redis_async
        if not await wait_for_redis_async(self.r):
            logger.error("❌ Redis is not ready after wait. Exiting.")
            return

        logger.info("🚀 SMT Entry Candidate Service started (Group: %s)", self.streams.in_group)
        
        from core.redis_stream_consumer import AsyncRedisStreamHelper
        helper = AsyncRedisStreamHelper(self.r, self.streams.in_group, self.streams.in_consumer)
        
        # Ensure consumer group exists
        try:
            await helper.ensure_group(self.streams.in_stream, start_id="0")
            logger.info("✅ Consumer group ensured: %s on %s", self.streams.in_group, self.streams.in_stream)
        except Exception as e:
            logger.error("❌ Failed to ensure group: %s", e)
            return

        while True:
            try:
                res = await helper.read({self.streams.in_stream: ">"}, count=10, block=self.poll_ms)
                if res:
                    for _, msgs in res:
                        for mid, data in msgs:
                            try:
                                payload = _json_load(data.get("payload", "{}"))
                                if not payload: continue
                                setup = Setup(
                                    bundle=str(payload.get("bundle", ""))
                                    kind=str(payload.get("kind", ""))
                                    leader=str(payload.get("leader", ""))
                                    pick=str(payload.get("symbol", ""))
                                    trend_dir=str(payload.get("trend_dir", ""))
                                    ts_ms=int(payload.get("ts_ms", _now_ms()))
                                    ttl_ms=int(os.getenv("SMT_SETUP_TTL_MS", "120000"))
                                    div=str(payload.get("div", ""))
                                    coh=float(payload.get("coherence", 0.0))
                                    leader_conf_score=float(payload.get("confidence", 0.0))
                                )
                                if setup.pick:
                                    sid = f"{setup.pick}:{setup.kind}:{setup.leader}"
                                    self.active[sid] = (setup, RetestState())
                            except Exception: pass
                            finally:
                                try: await helper.ack(self.streams.in_stream, mid)
                                except Exception: pass

                now = _now_ms()
                for setup_id in list(self.active.keys()):
                    setup, st = self.active[setup_id]
                    if (now - setup.ts_ms) > self.max_wait_ms:
                        self.active.pop(setup_id, None)
                        continue
                    
                    snap_raw = await self.r.get(f"{self.snap_prefix}{setup.pick}")
                    if not snap_raw: continue
                    snap = _json_load(str(snap_raw))
                    if not snap: continue
                    
                    if st.stage == "WAIT_TOUCH":
                        regime = str(snap.get("regime", "na") or "na").lower()
                        grp_guess = regime if regime in ("thin", "range", "trend") else "default"
                        await self._maybe_poll_overrides(now_ms=now, group=grp_guess)
                        
                        ab_key = _mk_ab_key(setup, snap)
                        sb = int(os.getenv("AB_SPLIT_B", "10"))
                        sc = int(os.getenv("AB_SPLIT_C", "10"))
                        salt = str(os.getenv("AB_SALT", "v1"))
                        try:
                            if int(getattr(self._ovr, "enabled", 1) or 1) == 1:
                                sb = int(getattr(self._ovr, "ab_split_b", sb) or sb)
                                sc = int(getattr(self._ovr, "ab_split_c", sc) or sc)
                                salt = str(getattr(self._ovr, "ab_salt", salt) or salt)
                        except Exception: pass

                        A = 1.0 - (sb + sc)/100.0
                        B = sb/100.0
                        C = sc/100.0
                        arm = choose_arm_abc(key=ab_key, split_b=B, split_c=C, salt=salt)
                        split_reason = "overrides_v1" if self._ovr_loaded_ts_ms > 0 else "env_defaults"
                        
                        if regime in ("thin", "news", "illiquid"): grp = "thin"
                        elif regime == "range": grp = "range"
                        else: grp = "default"
                    else:
                        arm = st.ab_arm; ab_key = st.ab_key; grp = st.ab_group
                        A = st.ab_split_a; B = st.ab_split_b; C = st.ab_split_c; split_reason = st.ab_split_reason

                    pol = self._abc.policy_for(arm)
                    emit, reason = _fsm_step(
                        setup=setup, st=st, snap=snap, now_ms=now
                        touch_bp=float(pol.touch_bp), away_bp=float(pol.away_bp), retest_bp=float(pol.retest_bp)
                        pol=pol
                    )

                    if reason == "touch":
                        st.ab_arm = arm; st.ab_key = ab_key; st.ab_group = grp
                        st.ab_split_reason = split_reason; st.ab_split_a = A; st.ab_split_b = B; st.ab_split_c = C

                    audit_payload = {
                        "type": "smt_entry_fsm_audit", "ts_ms": now, "symbol": setup.pick, "bundle": setup.bundle
                        "kind": setup.kind, "leader": setup.leader, "ab_arm": st.ab_arm, "ab_group": st.ab_group
                        "ab_key": st.ab_key, "ab_split_reason": st.ab_split_reason, "stage": st.stage, "reason": reason
                        "ok": int(1 if emit else 0), "regime": regime, "zone_id": str(snap.get("zone_id", ""))
                    }
                    await self.r.xadd(self.streams.out_audit, {"payload": _json_dump(audit_payload)}, maxlen=self.maxlen, approximate=True)
                    
                    if emit and st.emitted == 1:
                        side = _desired_side(setup)
                        dk = f"{setup.pick}:{str(snap.get('zone_id', ''))}:{side}"
                        last = self._dedup.get(dk, 0)
                        if last > 0 and (now - last) < self.dedup_ms:
                            self.active.pop(setup_id, None); continue
                        self._dedup[dk] = now
                        
                        entry_payload = {
                            "type": "entry_candidate", "ts_ms": now, "symbol": setup.pick, "side": side
                            "bundle": setup.bundle, "kind": setup.kind, "leader": setup.leader
                            "ab_arm": st.ab_arm, "ab_group": st.ab_group, "regime": regime
                        }
                        await self.r.xadd(self.streams.out_candidate, {"payload": _json_dump(entry_payload)}, maxlen=self.maxlen, approximate=True)
                        self.active.pop(setup_id, None)

            except Exception as e:
                logger.error("❌ Loop error: %s", e)
            await asyncio.sleep(max(0.05, self.poll_ms / 1000.0))


async def _main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = aioredis.from_url(redis_url, decode_responses=True) # type: ignore
    try:
        svc = SmtEntryCandidateService(r)
        await svc.run_forever()
    finally:
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(_main())
