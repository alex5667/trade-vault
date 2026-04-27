from utils.time_utils import get_ny_time_millis
import asyncio
from utils.task_manager import safe_create_task

import hashlib
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Tuple

import redis.asyncio as aioredis

from core.redis_lock import RedisLock
from core.lcb_evaluator import ArmAgg, evaluate_winner_lcb, regime_thresholds
from core.entry_policy_suggestion_meta_v1 import EntryPolicySuggestionMetaV1


def _now_ms() -> int:
    return get_ny_time_millis()


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _s(x: Any) -> str:
    return str(x or "").strip()


def _sym(x: Any) -> str:
    return _s(x).upper()


def _rg(x: Any) -> str:
    return _s(x).lower() or "na"


def _grp(x: Any) -> str:
    g = _s(x).lower()
    return g if g else "default"


def _scn(x: Any) -> str:
    v = _s(x).lower()
    return v if v in ("continuation", "reversal") else "na"


def _arm(x: Any) -> str:
    v = _s(x).upper()
    return v if v in ("A", "B", "C") else "A"


async def _send_telegram_report(r: aioredis.Redis, text: str) -> None:
    """
    Telegram worker contract (as per your notify_worker.py):
      {"type":"report","text":"..."}
    Stream/key name is env-tunable because deployments differ.
    """
    stream = os.getenv("TELEGRAM_NOTIFY_STREAM", "notify:telegram")
    try:
        await r.xadd(stream, {"type": "report", "text": str(text)}, maxlen=20000, approximate=True)
    except Exception:
        pass


@dataclass
class KeyCtx:
    symbol: str
    regime: str
    group: str
    scenario: str


class ABWinnerSuggesterV3:
    """
    Reads events:trades (POSITION_CLOSED) and produces proposals:
      cfg:suggestions:entry_policy:meta:{sid}  (JSON meta schema V1)
      cfg:suggestions:entry_policy:latest:ab_winner:{sym}:{rg}:{grp}:{scn} -> sid
      cfg:suggestions:entry_policy:stream  (audit stream for UI/ops)

    No silent apply. ApplyRunner performs approval-gated apply.
    """

    def __init__(self) -> None:
        redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.r: aioredis.Redis = aioredis.from_url(redis_url, decode_responses=True)

        self.events_stream = os.getenv("TRADE_EVENTS_STREAM", "events:trades")
        self.audit_stream = os.getenv("AB_SUGGEST_STREAM", "cfg:suggestions:entry_policy:stream")
        self.latest_prefix = os.getenv(
            "AB_LATEST_PREFIX",
            "cfg:suggestions:entry_policy:latest:ab_winner",
        )
        self.meta_prefix = os.getenv("AB_META_PREFIX", "cfg:suggestions:entry_policy:meta")
        self.approvals_required = int(os.getenv("ENTRY_POLICY_APPROVALS_REQUIRED", "2"))

        # ingestion cursor
        self.cursor_key = os.getenv("AB_INGEST_CURSOR_KEY", "state:ab_winner_suggester_v3:cursor")
        self._last_id = "0-0"

        # in-memory aggregation: (sym,rg,grp,scn)-> arm -> agg
        self._agg: Dict[Tuple[str, str, str, str], Dict[str, ArmAgg]] = {}
        self._seen_keys: Dict[Tuple[str, str, str, str], int] = {}

        # scheduling
        self.eval_every_sec = int(os.getenv("AB_EVAL_EVERY_SEC", "3600"))  # hourly
        self.lookback_hours = int(os.getenv("AB_LOOKBACK_HOURS", "168"))   # 7d for cold start scan

        # lock
        self.lock = RedisLock(key=os.getenv("AB_SUGGEST_LOCK_KEY", "lock:ab_winner_suggester_v3"), ttl_sec=55)

    async def _load_cursor(self) -> None:
        try:
            v = await self.r.get(self.cursor_key)
            if v:
                self._last_id = str(v)
        except Exception:
            pass

    async def _save_cursor(self, x: str) -> None:
        try:
            await self.r.set(self.cursor_key, str(x))
        except Exception:
            pass

    def _ingest_event(self, ev: Dict[str, Any]) -> None:
        """
        Expected flattened fields in events:trades (your logger expands payload to root):
          event_type, sid, symbol, ts, r_mult, ab_arm, ab_group, regime, scenario
        """
        try:
            if str(ev.get("event_type") or "") != "POSITION_CLOSED":
                return
            sym = _sym(ev.get("symbol"))
            rg = _rg(ev.get("regime", "na"))
            scn = _scn(ev.get("scenario", "na"))
            if scn == "na":
                return
            grp = _grp(ev.get("ab_group", "default"))
            arm = _arm(ev.get("ab_arm", "A"))
            r_mult = float(ev.get("r_mult", 0.0) or 0.0)

            k = (sym, rg, grp, scn)
            d = self._agg.get(k)
            if d is None:
                d = {}
                self._agg[k] = d
            a = d.get(arm)
            if a is None:
                a = ArmAgg()
                d[arm] = a
            a.add(r_mult)
            self._seen_keys[k] = int(_now_ms())
        except Exception:
            return

    async def ingest_forever(self) -> None:
        await self._load_cursor()
        while True:
            try:
                resp = await self.r.xread({self.events_stream: self._last_id}, count=2000, block=2000)
                if not resp:
                    continue
                for _stream, items in resp:
                    for msg_id, fields in items:
                        self._last_id = msg_id
                        self._ingest_event(fields or {})
                await self._save_cursor(self._last_id)
            except Exception:
                await asyncio.sleep(1.0)

    async def _emit_proposal(self, ctx: KeyCtx, winner: str, metrics: Dict[str, Any], reason: str, min_n: int, alpha: float, min_edge_r: float) -> str:
        now = _now_ms()
        sid = _sha1(f"abwinner|v3|{ctx.symbol}|{ctx.regime}|{ctx.group}|{ctx.scenario}|{winner}|{int(now/1000/60)}")

        meta = EntryPolicySuggestionMetaV1(
            v=1,
            sid=sid,
            created_ts_ms=now,
            updated_ts_ms=now,
            expires_ts_ms=now + int(os.getenv("AB_SUGGEST_EXPIRE_MS", "604800000")),  # 7d
            symbol=ctx.symbol,
            regime=ctx.regime,
            group=ctx.group,
            scenario=ctx.scenario,
            winner_arm=winner,
            baseline_arm="A",
            min_n=min_n,
            alpha=float(alpha),
            min_edge_r=float(min_edge_r),
            reason=str(reason),
            arm_metrics=metrics,
            approvals_required=int(self.approvals_required),
        )
        ok, why = meta.validate()
        if not ok:
            return ""

        latest_key = f"{self.latest_prefix}:{ctx.symbol}:{ctx.regime}:{ctx.group}:{ctx.scenario}"
        meta_key = f"{self.meta_prefix}:{sid}"
        # write atomically
        try:
            pipe = self.r.pipeline()
            pipe.set(meta_key, meta.to_json(), ex=int(os.getenv("AB_META_TTL_SEC", "1209600")))  # 14d
            pipe.set(latest_key, sid, ex=int(os.getenv("AB_LATEST_TTL_SEC", "1209600")))
            # audit stream
            pipe.xadd(
                self.audit_stream,
                {
                    "type": "entry_policy_suggestion",
                    "ts_ms": str(now),
                    "sid": sid,
                    "symbol": ctx.symbol,
                    "regime": ctx.regime,
                    "group": ctx.group,
                    "scenario": ctx.scenario,
                    "winner": winner,
                    "payload": meta.to_json(),
                },
                maxlen=50000,
                approximate=True,
            )
            await pipe.execute()
        except Exception:
            return ""

        # telegram
        try:
            msg = (
                f"<b>AB Winner Proposal</b>\n"
                f"key: {ctx.symbol}/{ctx.regime}/{ctx.group}/{ctx.scenario}\n"
                f"winner: <b>{winner}</b>\n"
                f"reason: {reason}\n"
                f"sid: <code>{sid}</code>\n"
                f"approvals_required: {self.approvals_required}\n"
            )
            await _send_telegram_report(self.r, msg)
        except Exception:
            pass
        return sid

    async def evaluate_once(self) -> int:
        """
        Evaluate all seen keys and emit proposals if winner exists.
        Guarded by Redis lock to avoid double-run in scaled containers.
        """
        if not await self.lock.acquire(self.r):
            return 0
        try:
            n_emit = 0
            keys = list(self._agg.keys())
            for k in keys:
                sym, rg, grp, scn = k
                per_arm = self._agg.get(k) or {}
                if not per_arm:
                    continue

                # regime thresholds (world-practice: stricter in thin/news)
                min_n, alpha, min_edge_r = regime_thresholds(rg)
                # ENV overrides (optional)
                try:
                    min_n = int(os.getenv(f"AB_MIN_N_{rg.upper()}", str(min_n)))
                except Exception:
                    pass
                try:
                    alpha = float(os.getenv(f"AB_ALPHA_{rg.upper()}", str(alpha)))
                except Exception:
                    pass
                try:
                    min_edge_r = float(os.getenv(f"AB_MIN_EDGE_R_{rg.upper()}", str(min_edge_r)))
                except Exception:
                    pass

                winner, res, reason = evaluate_winner_lcb(
                    stats_by_arm=per_arm,
                    baseline_arm="A",
                    min_n=min_n,
                    alpha=alpha,
                    min_edge_r=min_edge_r,
                )
                if not winner:
                    continue

                # compress metrics for meta
                m: Dict[str, Any] = {}
                for arm, rr in (res or {}).items():
                    m[arm] = {
                        "n": rr.n,
                        "mean_r": round(float(rr.mean_r), 4),
                        "lcb_r": round(float(rr.lcb_r), 4),
                        "winrate": round(float(rr.winrate), 4),
                        "std_r": round(float(rr.std_r), 4),
                    }

                ctx = KeyCtx(symbol=sym, regime=rg, group=grp, scenario=scn)
                sid = await self._emit_proposal(ctx, winner, m, reason, min_n, alpha, min_edge_r)
                if sid:
                    n_emit += 1
            return n_emit
        finally:
            await self.lock.release(self.r)

    async def run_forever(self) -> None:
        """
        Two loops:
          - ingest_forever: keeps aggregation fresh
          - evaluator loop: hourly evaluate and propose
        """
        safe_create_task(self.ingest_forever())
        # align to hour boundary (best practice: stable cadence)
        while True:
            now = time.time()
            # next hour + small jitter
            next_run = (int(now // 3600) + 1) * 3600 + 5
            await asyncio.sleep(max(1.0, next_run - time.time()))
            await self.evaluate_once()


async def main() -> None:
    svc = ABWinnerSuggesterV3()
    await svc.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
