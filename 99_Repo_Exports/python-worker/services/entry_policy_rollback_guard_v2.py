from utils.time_utils import get_ny_time_millis
import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple
import redis.asyncio as aioredis
from common.log import setup_logger
from core.redis_keys import RedisStreams as RS

log = logging.getLogger("ab_rollback_guard_v2")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")


# --- robust helpers (median/MAD) ---
def _now_ms() -> int: return get_ny_time_millis()
def _sym(x: str) -> str: return (x or "").strip().upper()
def _rg(x: str) -> str: return (x or "na").strip().lower()
def _grp(x: str) -> str: return (x or "default").strip().lower()
def _arm(x: str) -> str:
    a = (x or "").strip().upper()
    return a if a in ("A","B","C") else ""

def _median(xs: List[float]) -> float:
    ys = sorted(xs)
    n = len(ys)
    if n == 0: return 0.0
    m = n // 2
    return float(ys[m]) if n % 2 == 1 else 0.5 * (ys[m-1] + ys[m])

def _mad(xs: List[float], med: float) -> float:
    return _median([abs(x - med) for x in xs]) if xs else 0.0

def _robust_sem(xs: List[float]) -> float:
    if len(xs) < 8: return 0.0
    med = _median(xs)
    mad = _mad(xs, med)
    sigma = 1.4826 * mad
    return float(sigma / (len(xs) ** 0.5)) if len(xs) > 0 else 0.0

def _reg_bucket(rg: str) -> str:
    r = (rg or "na").lower()
    if r in ("thin","news","illiquid"): return "THIN"
    if r in ("trend","trending_bull","trending_bear"): return "TREND"
    if r in ("range",): return "RANGE"
    return "MIXED"

@dataclass
class Dec:
    do_rb: bool
    catastrophic: bool
    reason: str
    n: int
    mean_r: float
    lcb_r: float

class RollbackGuardV2:
    """
    Closed-loop AB control:
      - evaluates post-apply performance (R) per {symbol,regime,group}
      - gates rollback by data quality (spread/book/obi staleness)
      - emits suggestion keys for 2-approval apply, or catastrophic auto-enforce
    """
    def __init__(self, r: Any) -> None:
        self.r = r
        self.events_stream = os.getenv("TRADE_EVENTS_STREAM", "events:trades")
        self.group = os.getenv("AB_RB_GROUP", "ab-rb-v2")
        self.consumer = os.getenv("AB_RB_CONSUMER", f"c-{os.getpid()}")

        self.last_applied_prefix = os.getenv("AB_LAST_APPLIED_PREFIX", "cfg:entry_policy:last_applied:v1")
        self.active_prefix = os.getenv("AB_ACTIVE_PREFIX", "cfg:entry_policy:active_arm")
        self.post_prefix = os.getenv("AB_POST_PREFIX", "ab:postapply:r:v2")
        self.audit_stream = os.getenv("AB_RB_AUDIT_STREAM", "stream:ab:rollback_audit")
        self.notify_stream = os.getenv("NOTIFY_STREAM", RS.NOTIFY_TELEGRAM)

        # Suggestion workflow
        self.sug_latest_prefix = os.getenv("AB_RB_SUG_LATEST_PREFIX", "cfg:suggestions:entry_policy:latest:rollback")
        self.sug_meta_prefix = os.getenv("AB_META_PREFIX", "cfg:suggestions:entry_policy:meta")
        self.sug_approvals_prefix = os.getenv("AB_APPROVALS_PREFIX", "cfg:suggestions:entry_policy:approvals")
        self.sug_applied_prefix = os.getenv("AB_APPLIED_PREFIX", "cfg:suggestions:entry_policy:applied")

        self.mode = os.getenv("AB_RB_MODE", "shadow").strip().lower()  # shadow|suggest|enforce
        self.window_n = int(os.getenv("AB_RB_WINDOW_N", "60"))
        self.cooldown_sec = int(os.getenv("AB_RB_COOLDOWN_SEC", "7200"))

        # Per-regime thresholds
        self.cfg = {
            "THIN":  {"min_trades": 30, "k": 1.3, "d_mean": -0.06, "d_lcb": -0.10, "cat_lcb": -0.25,
                      "max_spread_bp": 25.0, "max_book_age_ms": 2500, "max_obi_age_ms": 2500},
            "TREND": {"min_trades": 18, "k": 1.1, "d_mean": -0.04, "d_lcb": -0.07, "cat_lcb": -0.18,
                      "max_spread_bp": 18.0, "max_book_age_ms": 1500, "max_obi_age_ms": 1500},
            "RANGE": {"min_trades": 22, "k": 1.2, "d_mean": -0.05, "d_lcb": -0.08, "cat_lcb": -0.20,
                      "max_spread_bp": 20.0, "max_book_age_ms": 2000, "max_obi_age_ms": 2000},
            "MIXED": {"min_trades": 20, "k": 1.2, "d_mean": -0.05, "d_lcb": -0.08, "cat_lcb": -0.20,
                      "max_spread_bp": 22.0, "max_book_age_ms": 2200, "max_obi_age_ms": 2200},
        }

    def _k_last(self, sym: str, rg: str, grp: str) -> str:
        return f"{self.last_applied_prefix}:{_sym(sym)}:{_rg(rg)}:{_grp(grp)}"

    def _k_active(self, sym: str, rg: str, grp: str) -> str:
        return f"{self.active_prefix}:{_sym(sym)}:{_rg(rg)}:{_grp(grp)}"

    def _k_post(self, sym: str, rg: str, grp: str, sid: str) -> str:
        return f"{self.post_prefix}:{_sym(sym)}:{_rg(rg)}:{_grp(grp)}:{sid}"

    def _k_cool(self, sym: str, rg: str, grp: str) -> str:
        return f"{self.post_prefix}:cool:{_sym(sym)}:{_rg(rg)}:{_grp(grp)}"

    async def _ensure_group(self) -> None:
        try:
            await self.r.xgroup_create(self.events_stream, self.group, id="0", mkstream=True)
        except Exception:  # nosec B110 — BUSYGROUP is expected on restart
            pass

    async def _read(self):
        return await self.r.xreadgroup(self.group, self.consumer, streams={self.events_stream: ">"}, count=200, block=1000)

    async def _ack(self, msg_id: str) -> None:
        try:
            await self.r.xack(self.events_stream, self.group, msg_id)
        except Exception as e:  # nosec B110 — ACK failure is non-fatal, message re-delivered
            log.warning("[ab_rb] ack failed msg=%s err=%s", msg_id, e)

    async def _audit(self, payload: Dict[str, Any]) -> None:
        try:
            msg = {"type": "ab_rollback_audit", "ts_ms": str(_now_ms()), "payload": json.dumps(payload, separators=(",", ":"))}
            await self.r.xadd(self.audit_stream, msg, maxlen=50000, approximate=True)
        except Exception as e:  # nosec B110 — audit write failure is non-fatal
            log.warning("[ab_rb] audit write failed err=%s payload=%s", e, payload)

    async def _cooldown_ok(self, sym: str, rg: str, grp: str) -> bool:
        try:
            raw = await self.r.get(self._k_cool(sym, rg, grp))
            if not raw: return True
            ts = int(json.loads(raw).get("ts_ms", 0) or 0)
            return (_now_ms() - ts) >= self.cooldown_sec * 1000
        except Exception:
            return True

    async def _set_cooldown(self, sym: str, rg: str, grp: str, why: str) -> None:
        try:
            await self.r.set(self._k_cool(sym, rg, grp), json.dumps({"ts_ms": _now_ms(), "why": why}, separators=(",", ":")), ex=self.cooldown_sec)
        except Exception as e:  # nosec B110 — cooldown set failure may cause duplicate evals; log to track
            log.warning("[ab_rb] cooldown set failed sym=%s rg=%s err=%s", sym, rg, e)

    async def _get_last_applied(self, sym: str, rg: str, grp: str) -> Dict[str, Any]:
        try:
            raw = await self.r.get(self._k_last(sym, rg, grp))
            if not raw: return {}
            d = json.loads(raw)
            return d if isinstance(d, dict) else {}
        except Exception:
            return {}

    async def _append_r(self, sym: str, rg: str, grp: str, sid: str, r_val: float) -> List[float]:
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

    def _dq_ok(self, rg: str, ev: Dict[str, Any]) -> Tuple[bool, str]:
        b = _reg_bucket(rg); c = self.cfg.get(b, self.cfg["MIXED"])
        spread_bp = float(ev.get("spread_bp", 0.0) or 0.0)
        book_age = int(ev.get("book_age_ms", 10**9) or 10**9)
        obi_age = int(ev.get("obi_age_ms", 10**9) or 10**9)
        # If missing => treat as bad quality (fail-open in terms of rollback: we won't rollback)
        if book_age >= 10**8 or obi_age >= 10**8:
            return False, "missing_book_or_obi_age"
        if spread_bp > float(c["max_spread_bp"]):
            return False, "spread_too_wide"
        if book_age > int(c["max_book_age_ms"]):
            return False, "book_stale"
        if obi_age > int(c["max_obi_age_ms"]):
            return False, "obi_stale"
        return True, "ok"

    def _decide(self, rg: str, xs: List[float], baseline: Dict[str, Any]) -> Dec:
        b = _reg_bucket(rg); c = self.cfg.get(b, self.cfg["MIXED"])
        if len(xs) < int(c["min_trades"]):
            return Dec(False, False, "min_trades_not_reached", len(xs), 0.0, 0.0)
        mean_r = sum(xs) / len(xs)
        sem = _robust_sem(xs)
        k = float(c["k"])
        lcb = mean_r - k * sem
        prev_mean = float((baseline or {}).get("prev_mean_r", 0.0) or 0.0)
        prev_lcb = float((baseline or {}).get("prev_lcb_r", 0.0) or 0.0)
        d_mean = mean_r - prev_mean
        d_lcb = lcb - prev_lcb
        catastrophic = bool(d_lcb <= float(c["cat_lcb"]))
        if d_mean <= float(c["d_mean"]) and d_lcb <= float(c["d_lcb"]):
            return Dec(True, catastrophic, "post_apply_underperforms_baseline", len(xs), mean_r, lcb)
        return Dec(False, False, "ok", len(xs), mean_r, lcb)

    async def _emit_rollback_suggestion(self, sym: str, rg: str, grp: str, last: Dict[str, Any], dec: Dec) -> None:
        # suggestion sid = hash-like deterministic id
        sid = f"rb:{_sym(sym)}:{_rg(rg)}:{_grp(grp)}:{str(last.get('sid') or '')}"
        winner = _arm(str(last.get("winner") or ""))
        prev = _arm(str(last.get("prev_active") or ""))
        if not prev or prev == winner:
            return
        meta = {
            "type": "rollback_suggestion",
            "sid": sid,
            "ts_ms": _now_ms(),
            "symbol": _sym(sym),
            "regime": _rg(rg),
            "group": _grp(grp),
            "from_arm": winner,
            "to_arm": prev,
            "applied_sid": str(last.get("sid") or ""),
            "reason": dec.reason,
            "post_n": dec.n,
            "post_mean_r": dec.mean_r,
            "post_lcb_r": dec.lcb_r,
            "baseline": last.get("baseline") or {},
        }
        try:
            # store meta
            await self.r.set(f"{self.sug_meta_prefix}:{sid}", json.dumps(meta, separators=(",", ":")), ex=7*24*3600)
            # latest pointer
            await self.r.set(f"{self.sug_latest_prefix}:{_sym(sym)}:{_rg(rg)}:{_grp(grp)}", sid, ex=7*24*3600)
            
            # notify telegram
            msg = (
                f"🚨 <b>Rollback Suggestion</b>\n"
                f"Sym: {meta['symbol']} | Rg: {meta['regime']}\n"
                f"From: {meta['from_arm']} → To: {meta['to_arm']}\n"
                f"Reason: {meta['reason']}\n"
                f"Post-Apply: {meta['post_n']} trades, Mean R={meta['post_mean_r']:.3f}, LCB={meta['post_lcb_r']:.3f}\n"
                f"Baseline: Prev Mean={float((last.get('baseline')or{}).get('prev_mean_r',0)):.3f}\n"
                f"Action: <code>/approve_rollback {sid}</code>"
            )
            await self._emit_telegram(msg)
        except Exception as e:  # nosec B110 — telegram delivery failure must not block rollback logic
            log.warning("[ab_rb] emit_rollback_suggestion failed sym=%s err=%s", sym, e)

    async def _enforce_rollback(self, sym: str, rg: str, grp: str, last: Dict[str, Any], dec: Dec) -> None:
        winner = _arm(str(last.get("winner") or ""))
        prev = _arm(str(last.get("prev_active") or ""))
        if not prev or prev == winner:
            return
        try:
            pipe = self.r.pipeline()
            pipe.set(self._k_active(sym, rg, grp), prev)
            # mark cooldown
            pipe.set(self._k_cool(sym, rg, grp), json.dumps({"ts_ms": _now_ms(), "why": "enforce"}, separators=(",", ":")), ex=self.cooldown_sec)
            await pipe.execute()
            
            # notify telegram
            msg = (
                f"⛔ <b>Rollback ENFORCED</b>\n"
                f"Sym: {_sym(sym)} | Rg: {_rg(rg)}\n"
                f"From: {winner} → To: {prev}\n"
                f"Reason: {dec.reason} (Catastrophic)\n"
                f"Stats: N={dec.n}, Mean={dec.mean_r:.3f}, LCB={dec.lcb_r:.3f}\n"
                f"<i>System automatically enforced protection due to severe degradation.</i>"
            )
            await self._emit_telegram(msg)
        except Exception as e:  # nosec B110 — telegram failure must not block enforce action
            log.warning("[ab_rb] enforce_rollback telegram failed sym=%s err=%s", sym, e)
            
    async def _emit_telegram(self, text: str) -> None:
        try:
            await self.r.xadd(self.notify_stream, {"text": text}, maxlen=20000, approximate=False)
        except Exception as e:  # nosec B110 — notify stream write failure is soft
            log.warning("[ab_rb] telegram notify stream write failed err=%s", e)

    async def process_one(self, ev: Dict[str, Any]) -> None:
        et = str(ev.get("event_type") or ev.get("event") or "")
        if et != "POSITION_CLOSED":
            return
        sym = _sym(str(ev.get("symbol") or ""))
        rg = _rg(str(ev.get("regime") or "na"))
        grp = _grp(str(ev.get("ab_group") or "default"))
        arm = _arm(str(ev.get("ab_arm") or ""))
        if not sym or not arm:
            return
        pnl = float(ev.get("pnl", 0.0) or 0.0)
        risk = float(ev.get("risk_usd", 0.0) or 0.0)
        if risk <= 0:
            await self._audit({"event": "skip_no_risk", "symbol": sym, "regime": rg, "group": grp})
            return
        if not await self._cooldown_ok(sym, rg, grp):
            return

        last = await self._get_last_applied(sym, rg, grp)
        if not last:
            return
        applied_sid = str(last.get("sid") or "")
        applied_w = _arm(str(last.get("winner") or ""))
        if not applied_sid or not applied_w:
            return
        # evaluate only trades from applied winner arm
        if arm != applied_w:
            return

        dq_ok, dq_reason = self._dq_ok(rg, ev)
        if not dq_ok:
            await self._audit({"event": "dq_block", "symbol": sym, "regime": rg, "group": grp, "reason": dq_reason})
            # cooldown prevents spamming on degraded data
            await self._set_cooldown(sym, rg, grp, f"dq:{dq_reason}")
            return

        r_val = pnl / risk
        xs = await self._append_r(sym, rg, grp, applied_sid, float(r_val))
        dec = self._decide(rg, xs, (last.get("baseline") or {}))

        await self._audit({
            "event": "eval",
            "symbol": sym, "regime": rg, "group": grp,
            "n": dec.n, "mean_r": dec.mean_r, "lcb_r": dec.lcb_r,
            "do": int(dec.do_rb), "cat": int(dec.catastrophic),
        })

        if not dec.do_rb:
            return

        if self.mode == "shadow":
            await self._emit_rollback_suggestion(sym, rg, grp, last, dec)
            await self._set_cooldown(sym, rg, grp, "shadow_suggest")
            return

        if self.mode == "suggest":
            await self._emit_rollback_suggestion(sym, rg, grp, last, dec)
            await self._set_cooldown(sym, rg, grp, "suggest")
            return

        # enforce mode
        if dec.catastrophic:
            await self._enforce_rollback(sym, rg, grp, last, dec)
            await self._set_cooldown(sym, rg, grp, "enforce_catastrophic")
        else:
            # non-catastrophic enforce: prefer suggestion to avoid oscillation
            await self._emit_rollback_suggestion(sym, rg, grp, last, dec)
            await self._set_cooldown(sym, rg, grp, "enforce_as_suggest")

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
                            await self.process_one(fields)
                        except Exception as e:  # nosec B110 — per-message isolation: one bad message must not stop the loop
                            log.warning("[ab_rb] process_one failed msg=%s err=%s", msg_id, e)
                        finally:
                            await self._ack(msg_id)
            except Exception:
                await asyncio.sleep(0.5)

async def _main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = aioredis.from_url(redis_url, decode_responses=True, socket_connect_timeout=10, socket_timeout=30, max_connections=10)
    svc = RollbackGuardV2(r)
    await svc.run_forever()

if __name__ == "__main__":
    asyncio.run(_main())
