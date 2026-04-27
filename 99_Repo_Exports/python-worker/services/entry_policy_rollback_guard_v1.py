from utils.time_utils import get_ny_time_millis
import asyncio
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

import redis.asyncio as aioredis


def _now_ms() -> int:
    return get_ny_time_millis()


def _sym(x: str) -> str:
    return (x or "").strip().upper()


def _rg(x: str) -> str:
    return (x or "na").strip().lower()


def _grp(x: str) -> str:
    return (x or "default").strip().lower()


def _arm(x: str) -> str:
    a = (x or "").strip().upper()
    return a if a in ("A", "B", "C") else ""


def _median(xs: List[float]) -> float:
    ys = sorted(xs)
    n = len(ys)
    if n == 0:
        return 0.0
    m = n // 2
    return float(ys[m]) if (n % 2 == 1) else 0.5 * (ys[m - 1] + ys[m])


def _mad(xs: List[float], med: float) -> float:
    return _median([abs(x - med) for x in xs]) if xs else 0.0


def _robust_sem(xs: List[float]) -> float:
    # robust sigma ~= 1.4826*MAD
    if len(xs) < 5:
        return 0.0
    med = _median(xs)
    mad = _mad(xs, med)
    sigma = 1.4826 * mad
    return float(sigma / (len(xs) ** 0.5)) if len(xs) > 0 else 0.0


@dataclass
class RollDecision:
    do: bool
    reason: str
    mean_r: float
    lcb_r: float
    n: int


class EntryPolicyRollbackGuardV1:
    """
    Watches POSITION_CLOSED and rolls back active_arm when post-apply EV degrades.

    Keys:
      cfg:entry_policy:last_applied:v1:{SYMBOL}:{regime}:{group} -> {"sid","winner","prev_active","ts_ms","baseline":{...}}
      cfg:entry_policy:active_arm:{SYMBOL}:{regime}:{group} -> "A|B|C"
      cfg:entry_policy:rollback:v1:{SYMBOL}:{regime}:{group} -> last rollback record
    """

    def __init__(self, r: Any) -> None:
        self.r = r
        self.events_stream = os.getenv("TRADE_EVENTS_STREAM", "events:trades")
        self.group = os.getenv("AB_ROLLBACK_GROUP", "ab-rollback")
        self.consumer = os.getenv("AB_ROLLBACK_CONSUMER", f"c-{os.getpid()}")

        self.last_applied_prefix = os.getenv("AB_LAST_APPLIED_PREFIX", "cfg:entry_policy:last_applied:v1")
        self.active_prefix = os.getenv("AB_ACTIVE_PREFIX", "cfg:entry_policy:active_arm")
        self.rollback_prefix = os.getenv("AB_ROLLBACK_PREFIX", "cfg:entry_policy:rollback:v1")
        self.post_list_prefix = os.getenv("AB_POST_LIST_PREFIX", "ab:postapply:r:v1")

        self.mode = os.getenv("AB_ROLLBACK_MODE", "shadow").strip().lower()  # shadow|enforce
        self.min_trades = int(os.getenv("AB_ROLLBACK_MIN_TRADES", "20"))
        self.window_n = int(os.getenv("AB_ROLLBACK_WINDOW_N", "40"))
        self.min_delta_mean_r = float(os.getenv("AB_ROLLBACK_MIN_DELTA_MEAN_R", "-0.05"))  # mean_post - prev_mean
        self.min_delta_lcb_r = float(os.getenv("AB_ROLLBACK_MIN_DELTA_LCB_R", "-0.08"))    # lcb_post - prev_lcb
        self.cooldown_sec = int(os.getenv("AB_ROLLBACK_COOLDOWN_SEC", "7200"))  # 2h
        self.lock_ttl_ms = int(os.getenv("AB_ROLLBACK_LOCK_TTL_MS", "15000"))

    def _k_last_applied(self, sym: str, rg: str, grp: str) -> str:
        return f"{self.last_applied_prefix}:{_sym(sym)}:{_rg(rg)}:{_grp(grp)}"

    def _k_active(self, sym: str, rg: str, grp: str) -> str:
        return f"{self.active_prefix}:{_sym(sym)}:{_rg(rg)}:{_grp(grp)}"

    def _k_rb(self, sym: str, rg: str, grp: str) -> str:
        return f"{self.rollback_prefix}:{_sym(sym)}:{_rg(rg)}:{_grp(grp)}"

    def _k_post(self, sym: str, rg: str, grp: str, sid: str) -> str:
        return f"{self.post_list_prefix}:{_sym(sym)}:{_rg(rg)}:{_grp(grp)}:{sid}"

    def _k_cool(self, sym: str, rg: str, grp: str) -> str:
        return f"{self.rollback_prefix}:cooldown:{_sym(sym)}:{_rg(rg)}:{_grp(grp)}"

    async def _ensure_group(self) -> None:
        try:
            await self.r.xgroup_create(self.events_stream, self.group, id="0", mkstream=True)
        except Exception:
            pass

    async def _read(self) -> List[Tuple[str, List[Tuple[str, Dict[str, Any]]]]]:
        return await self.r.xreadgroup(self.group, self.consumer, streams={self.events_stream: ">"}, count=200, block=1000)

    async def _ack(self, msg_id: str) -> None:
        try:
            await self.r.xack(self.events_stream, self.group, msg_id)
        except Exception:
            pass

    async def _get_last_applied(self, sym: str, rg: str, grp: str) -> Dict[str, Any]:
        try:
            raw = await self.r.get(self._k_last_applied(sym, rg, grp))
            if not raw:
                return {}
            d = json.loads(raw)
            return d if isinstance(d, dict) else {}
        except Exception:
            return {}

    async def _cooldown_ok(self, sym: str, rg: str, grp: str) -> bool:
        try:
            raw = await self.r.get(self._k_cool(sym, rg, grp))
            if not raw:
                return True
            ts = int(json.loads(raw).get("ts_ms", 0) or 0)
            return (_now_ms() - ts) >= self.cooldown_sec * 1000
        except Exception:
            return True

    async def _set_cooldown(self, sym: str, rg: str, grp: str, payload: Dict[str, Any]) -> None:
        try:
            await self.r.set(self._k_cool(sym, rg, grp), json.dumps(payload, separators=(",", ":")), ex=self.cooldown_sec)
        except Exception:
            pass

    async def _append_post_r(self, sym: str, rg: str, grp: str, sid: str, r_val: float) -> List[float]:
        k = self._k_post(sym, rg, grp, sid)
        try:
            pipe = self.r.pipeline()
            pipe.lpush(k, f"{float(r_val):.6f}")
            pipe.ltrim(k, 0, max(0, self.window_n - 1))
            await pipe.execute()
            xs = await self.r.lrange(k, 0, -1)
            return [float(x) for x in xs]
        except Exception:
            return []

    def _decide(self, xs: List[float], baseline: Dict[str, Any]) -> RollDecision:
        if len(xs) < self.min_trades:
            return RollDecision(False, "min_trades_not_reached", 0.0, 0.0, len(xs))

        mean_r = sum(xs) / len(xs)
        sem = _robust_sem(xs)
        # LCB with k=1.0 (conservative). Can be env-configured later.
        lcb = mean_r - 1.0 * sem

        prev_mean = float(((baseline or {}).get("prev_mean_r", 0.0) or 0.0))
        prev_lcb = float(((baseline or {}).get("prev_lcb_r", 0.0) or 0.0))

        d_mean = mean_r - prev_mean
        d_lcb = lcb - prev_lcb

        if d_mean <= self.min_delta_mean_r and d_lcb <= self.min_delta_lcb_r:
            return RollDecision(True, "post_apply_underperforms_baseline", mean_r, lcb, len(xs))
        return RollDecision(False, "ok", mean_r, lcb, len(xs))

    async def _rollback(self, sym: str, rg: str, grp: str, last_applied: Dict[str, Any], dec: RollDecision) -> None:
        winner = _arm(str(last_applied.get("winner") or ""))
        prev = _arm(str(last_applied.get("prev_active") or ""))
        if not prev or prev == winner:
            return

        rb_payload = {
            "ts_ms": _now_ms(),
            "symbol": _sym(sym),
            "regime": _rg(rg),
            "group": _grp(grp),
            "applied_sid": str(last_applied.get("sid") or ""),
            "winner": winner,
            "rollback_to": prev,
            "reason": dec.reason,
            "post_n": dec.n,
            "post_mean_r": dec.mean_r,
            "post_lcb_r": dec.lcb_r,
            "baseline": last_applied.get("baseline") or {},
            "mode": self.mode,
        }

        if self.mode == "shadow":
            # only record rollback suggestion/audit, no config change
            try:
                await self.r.set(self._k_rb(sym, rg, grp), json.dumps(rb_payload, separators=(",", ":")), ex=7 * 24 * 3600)
                await self._set_cooldown(sym, rg, grp, {"ts_ms": rb_payload["ts_ms"], "reason": "shadow"})
            except Exception:
                pass
            return

        # enforce: change active arm back
        try:
            pipe = self.r.pipeline()
            pipe.set(self._k_active(sym, rg, grp), prev)
            pipe.set(self._k_rb(sym, rg, grp), json.dumps(rb_payload, separators=(",", ":")), ex=7 * 24 * 3600)
            # mark cooldown
            pipe.set(self._k_cool(sym, rg, grp), json.dumps({"ts_ms": rb_payload["ts_ms"], "reason": "enforce"}, separators=(",", ":")), ex=self.cooldown_sec)
            # annotate last_applied
            la = dict(last_applied)
            la["rolled_back"] = 1
            la["rollback_ts_ms"] = rb_payload["ts_ms"]
            la["rollback_to"] = prev
            pipe.set(self._k_last_applied(sym, rg, grp), json.dumps(la, separators=(",", ":")), ex=7 * 24 * 3600)
            await pipe.execute()
        except Exception:
            pass

    async def process_one(self, payload: Dict[str, Any]) -> None:
        # Filter close events
        et = str(payload.get("event_type") or payload.get("event") or "")
        if et != "POSITION_CLOSED":
            return

        sym = _sym(str(payload.get("symbol") or ""))
        rg = _rg(str(payload.get("regime") or "na"))
        grp = _grp(str(payload.get("ab_group") or "default"))
        arm = _arm(str(payload.get("ab_arm") or ""))
        if not sym:
            return

        pnl = float(payload.get("pnl", 0.0) or 0.0)
        risk = float(payload.get("risk_usd", 0.0) or 0.0)
        if risk <= 0:
            # can't compute R => fail-open
            return
        r_val = pnl / risk

        last_applied = await self._get_last_applied(sym, rg, grp)
        if not last_applied:
            return

        applied_sid = str(last_applied.get("sid") or "")
        applied_winner = _arm(str(last_applied.get("winner") or ""))
        if not applied_sid or not applied_winner:
            return

        # ensure we only evaluate post-apply trades for the applied winner arm (avoid mixing)
        if arm and arm != applied_winner:
            return

        # cooldown
        if not await self._cooldown_ok(sym, rg, grp):
            return

        xs = await self._append_post_r(sym, rg, grp, applied_sid, float(r_val))
        baseline = (last_applied.get("baseline") or {})
        dec = self._decide(xs, baseline)
        if dec.do:
            await self._rollback(sym, rg, grp, last_applied, dec)

    async def run_forever(self) -> None:
        await self._ensure_group()
        while True:
            try:
                msgs = await self._read()
                if not msgs:
                    continue
                for _, entries in msgs:
                    for msg_id, fields in entries:
                        try:
                            # events:trades expands payload into root; treat fields as dict already
                            await self.process_one(fields)
                        except Exception:
                            pass
                        finally:
                            await self._ack(msg_id)
            except Exception:
                await asyncio.sleep(0.5)


async def _main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = aioredis.from_url(redis_url, decode_responses=True, socket_connect_timeout=10, socket_timeout=30, max_connections=100)
    svc = EntryPolicyRollbackGuardV1(r)
    await svc.run_forever()


if __name__ == "__main__":
    asyncio.run(_main())
