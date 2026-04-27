
# -*- coding: utf-8 -*-
"""
Policy Safety Guard (container-friendly).

Reads POSITION_CLOSED from events:trades as a consumer group,
maintains rolling stats per (symbol, regime, scenario, group, arm),
and triggers immediate rollback to A if active arm underperforms with confidence.

Why:
  Hourly autotuner is too slow for damage control.
  Safety guard is the "circuit breaker" layer.
"""

import os
import time
import json
import math
import traceback
from typing import Dict, Any, Tuple, Optional

import redis

from core.lcb_stats import mean_lcb


def _now_ms() -> int:
    return int(time.time() * 1000)


def _sym(x: str) -> str:
    return str(x or "").strip().upper()


def _rg(x: str) -> str:
    return str(x or "na").strip().lower()


def _grp(x: str) -> str:
    return str(x or "default").strip().lower()


def _scn(x: str) -> str:
    x = str(x or "na").strip().lower()
    return x if x in ("continuation", "reversal") else "na"


def _arm(x: str) -> str:
    x = str(x or "A").strip().upper()
    return x if x in ("A", "B", "C") else "A"


def _f(x, d=0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(d)


class PolicySafetyGuard:
    def __init__(self) -> None:
        url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.r = redis.from_url(url, decode_responses=True)
        self.stream = os.getenv("TRADE_EVENTS_STREAM", "events:trades")
        self.group = os.getenv("SAFETY_GUARD_GROUP", "entry-policy-safety")
        self.consumer = os.getenv("SAFETY_GUARD_CONSUMER", f"c-{os.getpid()}")
        self.active_prefix = os.getenv("ENTRY_POLICY_ACTIVE_PREFIX", "cfg:entry_policy:active_arm")
        self.ledger_stream = os.getenv("POLICY_LEDGER_STREAM", "stream:policy:events")
        self.notify_stream = os.getenv("NOTIFY_STREAM", "stream:notify:telegram")

        # Rolling window config (stats TTL)
        self.stats_ttl_sec = int(os.getenv("SAFETY_STATS_TTL_SEC", "21600"))  # 6h
        self.eval_every_sec = float(os.getenv("SAFETY_EVAL_EVERY_SEC", "60"))
        self.min_n = int(os.getenv("SAFETY_MIN_N", "30"))
        self.alpha = float(os.getenv("SAFETY_LCB_ALPHA", "0.05"))
        self.min_edge = float(os.getenv("SAFETY_MIN_EDGE_LCB_R", "0.05"))

        # Rollback freeze (optional)
        self.freeze_enable = int(os.getenv("SAFETY_FREEZE_ENABLE", "1"))
        self.freeze_sec = int(os.getenv("SAFETY_FREEZE_SEC", "3600"))  # 1h
        
        # Create consumer group with retries
        from core.redis_stream_consumer import SyncRedisStreamHelper
        self.helper = SyncRedisStreamHelper(self.r, self.group, self.consumer)
        self._ensure_group()

    def _ensure_group(self):
        """Create consumer group best-effort with retries for Redis startup."""
        from core.redis_client import wait_for_redis
        if not wait_for_redis(self.r):
            print("❌ Redis is not ready after wait. Exiting.")
            return

        try:
            self.helper.ensure_group(self.stream, start_id="0-0")
            print(f"✅ Consumer group ensured: {self.group} on {self.stream}")
        except Exception as e:
            print(f"❌ Failed to ensure group {self.group}: {e}")

    def _ctx(self, sym: str, rg: str, scn: str, grp: str) -> Tuple[str, str, str, str]:
        return (sym, rg, scn, grp)

    def _stats_key(self, ctx: Tuple[str, str, str, str], arm: str) -> str:
        sym, rg, scn, grp = ctx
        return f"autopilot:stats:{sym}:{rg}:{scn}:{grp}:{arm}"

    def _ctx_set_key(self) -> str:
        return "autopilot:contexts"

    def _active_key(self, ctx: Tuple[str, str, str, str]) -> str:
        sym, rg, scn, grp = ctx
        return f"{self.active_prefix}:{sym}:{rg}:{grp}:{scn}"

    def _freeze_key(self, ctx: Tuple[str, str, str, str]) -> str:
        sym, rg, scn, grp = ctx
        # matches your documented scheme
        return f"cfg:entry_policy:freeze:v1:{sym}:{grp}:{scn}"

    def _ledger(self, event: Dict[str, Any]) -> None:
        try:
            msg = {"type": "policy_event", "ts_ms": str(_now_ms()), "payload": json.dumps(event, ensure_ascii=False, separators=(",", ":"))}
            self.r.xadd(self.ledger_stream, msg, maxlen=int(os.getenv("POLICY_LEDGER_MAXLEN", "50000")), approximate=True)
        except Exception:
            pass

    def _notify(self, text: str) -> None:
        try:
            msg = {"type": "report", "ts_ms": str(_now_ms()), "text": str(text)}
            self.r.xadd(self.notify_stream, msg, maxlen=int(os.getenv("NOTIFY_STREAM_MAXLEN", "20000")), approximate=True)
        except Exception:
            pass

    def _update_stats(self, ctx: Tuple[str, str, str, str], arm: str, r_mult: float) -> None:
        k = self._stats_key(ctx, arm)
        pipe = self.r.pipeline()
        pipe.hincrby(k, "n", 1)
        pipe.hincrbyfloat(k, "sum", float(r_mult))
        pipe.hincrbyfloat(k, "sum2", float(r_mult) * float(r_mult))
        pipe.expire(k, self.stats_ttl_sec)
        pipe.sadd(self._ctx_set_key(), "|".join(ctx))
        pipe.expire(self._ctx_set_key(), self.stats_ttl_sec)
        pipe.execute()

    def _read_stats(self, ctx: Tuple[str, str, str, str], arm: str) -> Optional[Dict[str, Any]]:
        k = self._stats_key(ctx, arm)
        d = self.r.hgetall(k) or {}
        if not d:
            return None
        try:
            n = int(d.get("n", 0) or 0)
            s = float(d.get("sum", 0.0) or 0.0)
            s2 = float(d.get("sum2", 0.0) or 0.0)
            return {"n": n, "sum": s, "sum2": s2}
        except Exception:
            return None

    def _lcb_from_moments(self, st: Dict[str, Any]) -> float:
        n = int(st.get("n", 0) or 0)
        if n <= 0:
            return float("-inf")
        mu = float(st.get("sum", 0.0) or 0.0) / float(n)
        # crude variance from moments
        var = (float(st.get("sum2", 0.0) or 0.0) / float(n)) - mu * mu
        if n < 2:
            return mu
        std = math.sqrt(max(0.0, var))  # biased but OK for safety guard
        xs = [mu - std, mu, mu + std]  # fallback sample proxy (avoid storing full list)
        ml = mean_lcb(xs, alpha_one_sided=self.alpha, winsor=(-5.0, 5.0))
        # scale stderr by sqrt(n/3) approx
        if ml.stderr > 0:
            return float(mu - 1.64485 * (std / math.sqrt(float(n))))
        return float(mu)

    def _maybe_rollback(self, ctx: Tuple[str, str, str, str]) -> None:
        # Read active arm
        act = str(self.r.get(self._active_key(ctx)) or "").strip().upper()
        if act not in ("A", "B", "C"):
            return
        if act == "A":
            return
        st_act = self._read_stats(ctx, act)
        st_a = self._read_stats(ctx, "A")
        if not st_act or not st_a:
            return
        if int(st_act["n"]) < self.min_n or int(st_a["n"]) < self.min_n:
            return
        lcb_act = self._lcb_from_moments(st_act)
        lcb_a = self._lcb_from_moments(st_a)
        # rollback if active is confidently worse than A, or LCB < 0
        if (lcb_act < 0.0) or ((lcb_a - lcb_act) >= self.min_edge):
            sym, rg, scn, grp = ctx
            # 1) immediate rollback active_arm -> A
            self.r.set(self._active_key(ctx), "A")
            # 2) optional freeze on this symbol/group/scenario to prevent flapping
            if self.freeze_enable == 1:
                frz = {
                    "v": 1,
                    "active": 1,
                    "mode": "shadow",
                    "until_ts_ms": _now_ms() + int(self.freeze_sec) * 1000,
                    "reason": "safety_rollback",
                }
                self.r.set(self._freeze_key(ctx), json.dumps(frz, ensure_ascii=False, separators=(",", ":")), ex=self.freeze_sec)
            # 3) ledger + telegram
            evt = {
                "event": "ROLLBACK",
                "ctx": {"symbol": sym, "regime": rg, "scenario": scn, "group": grp},
                "from": act,
                "to": "A",
                "lcb_act": float(lcb_act),
                "lcb_a": float(lcb_a),
                "ts_ms": _now_ms(),
            }
            self._ledger(evt)
            self._notify("<pre>" + json.dumps(evt, ensure_ascii=False, indent=2).replace("<", "&lt;").replace(">", "&gt;") + "</pre>")

    def _process_closed(self, fields: Dict[str, Any]) -> None:
        et = str(fields.get("event_type") or fields.get("event") or "").upper()
        if et != "POSITION_CLOSED":
            return
        sym = _sym(fields.get("symbol"))
        rg = _rg(fields.get("regime") or "na")
        scn = _scn(fields.get("scenario") or fields.get("decision") or "na")
        grp = _grp(fields.get("ab_group") or "default")
        arm = _arm(fields.get("ab_arm") or "A")
        r_mult = _f(fields.get("r_mult"), 0.0)
        if not sym or scn == "na":
            return
        ctx = self._ctx(sym, rg, scn, grp)
        self._update_stats(ctx, arm, r_mult)

    def run_forever(self) -> None:
        last_eval = time.time()
        while True:
            try:
                msgs = self.helper.read({self.stream: ">"}, count=200, block=2000)
                if msgs:
                    for _stream, items in msgs:
                        for msg_id, fields in items:
                            self._process_closed(fields or {})
                            # ack
                            try:
                                self.helper.ack(self.stream, msg_id)
                            except Exception:
                                pass
                # periodic evaluation across known contexts
                now = time.time()
                if (now - last_eval) >= self.eval_every_sec:
                    last_eval = now
                    try:
                        ctxs = list(self.r.smembers(self._ctx_set_key()) or [])
                        for s in ctxs:
                            parts = str(s).split("|")
                            if len(parts) != 4:
                                continue
                            self._maybe_rollback((parts[0], parts[1], parts[2], parts[3]))
                    except Exception:
                        pass
            except Exception:
                traceback.print_exc()
                time.sleep(2.0)


if __name__ == "__main__":
    PolicySafetyGuard().run_forever()
