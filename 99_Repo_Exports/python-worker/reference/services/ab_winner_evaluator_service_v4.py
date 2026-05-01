from __future__ import annotations
from utils.time_utils import get_ny_time_millis
import os, json, time, asyncio
from dataclasses import dataclass
from typing import Any, Dict
import redis.asyncio as aioredis # type: ignore

from core.lcb_r_adj import PenaltyCfg, compute_r_and_adj, thresholds_for, lcb
from core.tail_worstk import WorstK

def _now_ms() -> int:
    return get_ny_time_millis()

def _s(x: Any, d: str = "") -> str:
    try: return str(x) if x is not None else d
    except Exception: return d

def _f(x: Any, d: float = 0.0) -> float:
    try: return float(x)
    except Exception: return d

def _i(x: Any, d: int = 0) -> int:
    try: return int(x)
    except Exception: return d

@dataclass
class Welford:
    n: int = 0
    mean: float = 0.0
    m2: float = 0.0
    def update(self, x: float) -> None:
        self.n += 1
        d = x - self.mean
        self.mean += d / float(self.n)
        d2 = x - self.mean
        self.m2 += d * d2
    def std(self) -> float:
        if self.n <= 1: return 0.0
        return (self.m2 / float(self.n - 1)) ** 0.5
    def to_dict(self) -> Dict[str, Any]:
        return {"n": int(self.n), "mean": float(self.mean), "m2": float(self.m2)}
    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Welford":
        w = Welford()
        try:
            w.n = int(d.get("n", 0) or 0)
            w.mean = float(d.get("mean", 0.0) or 0.0)
            w.m2 = float(d.get("m2", 0.0) or 0.0)
        except Exception:
            pass
        return w

class ABWinnerEvaluatorV4:
    """
    Winner stats on R_adj:
      - mean LCB (Welford)
      - tail LCB (WorstK over R_adj, CVaR-lite)
      - WIN_SCORE = min(LCB_mean, LCB_tail)
    Writes:
      cfg:entry_policy:lcb:v3:{symbol}:{regime}:{group}:{scenario}:{arm}
    """
    def __init__(self) -> None:
        redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.r = aioredis.from_url(redis_url, decode_responses=True, socket_connect_timeout=10, socket_timeout=30, max_connections=200)
        self.stream = os.getenv("AB_EVENTS_STREAM", "events:trades")
        self.group = os.getenv("AB_EVAL_GROUP", "ab-eval-v4")
        self.consumer = os.getenv("AB_EVAL_CONSUMER", f"c-{os.getpid()}")
        self.block_ms = int(os.getenv("AB_EVAL_BLOCK_MS", "2000"))
        self.count = int(os.getenv("AB_EVAL_COUNT", "200"))
        self.prefix = os.getenv("AB_LCB_V3_PREFIX", "cfg:entry_policy:lcb:v3")
        self.ttl_sec = int(os.getenv("AB_LCB_TTL_SEC", str(30 * 24 * 3600)))
        self.tail_k = int(os.getenv("AB_TAIL_WORST_K", "200"))

        self.pen_cfg = PenaltyCfg(
            lam_spread=float(os.getenv("LCB_LAM_SPREAD", "0.03")),
            lam_pressure=float(os.getenv("LCB_LAM_PRESSURE", "0.05")),
            lam_cooldown=float(os.getenv("LCB_LAM_COOLDOWN", "0.04")),
            lam_bookstale=float(os.getenv("LCB_LAM_BOOKSTALE", "0.03")),
            lam_unstable=float(os.getenv("LCB_LAM_UNSTABLE", "0.03")),
            lam_news=float(os.getenv("LCB_LAM_NEWS", "0.05")),
            pressure_hi_sps=float(os.getenv("ENTRY_PRESSURE_HI_SPS", "0.08")),
            cooldown_hi_sps=float(os.getenv("ENTRY_COOLDOWN_HI_SPS", "0.06")),
            obi_ttl_ms=int(os.getenv("OBI_EVENT_TTL_MS", "5000")),
        )

    async def _ensure_group(self) -> None:
        try:
            await self.r.xgroup_create(self.stream, self.group, id="0", mkstream=True)
        except Exception as e:
            if "BUSYGROUP" in str(e):
                return
            raise

    def _key(self, sym: str, rg: str, grp: str, scn: str, arm: str) -> str:
        return f"{self.prefix}:{sym}:{rg}:{grp}:{scn}:{arm}"

    async def _load_state(self, key: str) -> Dict[str, Any]:
        try:
            raw = await self.r.get(key)
            if not raw:
                return {"w": Welford().to_dict(), "tail": WorstK(k=self.tail_k).to_dict()}
            d = json.loads(raw)
            if not isinstance(d, dict):
                return {"w": Welford().to_dict(), "tail": WorstK(k=self.tail_k).to_dict()}
            return d
        except Exception:
            return {"w": Welford().to_dict(), "tail": WorstK(k=self.tail_k).to_dict()}

    async def _save_state(self, key: str, payload: Dict[str, Any]) -> None:
        try:
            await self.r.set(key, json.dumps(payload, separators=(",", ":"), ensure_ascii=False), ex=self.ttl_sec)
        except Exception:
            pass

    async def _process_closed(self, ev: Dict[str, Any]) -> None:
        et = _s(ev.get("event_type", ev.get("event", ""))).upper()
        if et != "POSITION_CLOSED":
            return
        sym = _s(ev.get("symbol", "")).upper()
        rg = _s(ev.get("regime", "na")).lower()
        grp = _s(ev.get("ab_group", "default")).lower()
        arm = _s(ev.get("ab_arm", "A")).upper()
        scn = _s(ev.get("scenario", "na")).lower()
        if not sym or arm not in ("A","B","C"):
            return

        R, R_adj, pen = compute_r_and_adj(ev, self.pen_cfg)
        th = thresholds_for(rg, scn)
        key = self._key(sym, rg, grp, scn, arm)

        st = await self._load_state(key)
        w = Welford.from_dict(st.get("w", {}) if isinstance(st.get("w", {}), dict) else {})
        tail = WorstK.from_dict(st.get("tail", {}) if isinstance(st.get("tail", {}), dict) else {})
        if tail.k != self.tail_k:
            tail.k = self.tail_k

        w.update(float(R_adj))
        tail.push(float(R_adj))

        std = w.std()
        lcb_mean = lcb(w.mean, std, w.n, th.z)
        t_mu, t_std = tail.mean_std()
        t_n = tail.n()
        lcb_tail = lcb(t_mu, t_std, t_n, th.tail_z)

        win_score = float(min(lcb_mean, lcb_tail))

        payload = {
            "ts_ms": _now_ms(),
            "symbol": sym, "regime": rg, "group": grp, "scenario": scn, "arm": arm,
            "n": int(w.n),
            "mean_r_adj": float(w.mean),
            "std_r_adj": float(std),
            "lcb_mean": float(lcb_mean),
            "tail_n": int(t_n),
            "tail_mean": float(t_mu),
            "tail_std": float(t_std),
            "lcb_tail": float(lcb_tail),
            "win_score": float(win_score),
            "z": float(th.z),
            "tail_z": float(th.tail_z),
            "min_n": int(th.min_n),
            "min_tail_n": int(th.min_tail_n),
            "margin": float(th.margin),
            "pen_last": pen,   # transparency: last penalties
            "w": w.to_dict(),
            "tail": tail.to_dict(),
        }
        await self._save_state(key, payload)

    async def run_forever(self) -> None:
        await self._ensure_group()
        while True:
            try:
                res = await self.r.xreadgroup(self.group, self.consumer, streams={self.stream: ">"}, count=self.count, block=self.block_ms)
            except Exception:
                await asyncio.sleep(0.5)
                continue
            if not res:
                continue
            for _, entries in res:
                for mid, fields in entries:
                    try:
                        if isinstance(fields, dict):
                            await self._process_closed(fields)
                    finally:
                        try:
                            await self.r.xack(self.stream, self.group, mid)
                        except Exception:
                            pass

async def _main() -> None:
    await ABWinnerEvaluatorV4().run_forever()

if __name__ == "__main__":
    asyncio.run(_main())
