# -*- coding: utf-8 -*-
import os
import json
import time
import asyncio
import argparse
import hashlib
from typing import Any, Dict, Tuple, List, Optional, Deque
from collections import deque

import redis.asyncio as aioredis

from common.log import setup_logger
from core.ab_lcb_evaluator import choose_winner_lcb, default_regime_policy
from core.redis_lock import try_acquire_lock, release_lock
from core.cost_aware_lcb import compute_r_adj, compute_arm_stats
from core.winner_hysteresis import WinnerHysteresis
from services.observability.metrics_registry import lcb_winner_changes_total, lcb_margin

log = setup_logger("ab_winner_suggester_v2")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _sf(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _si(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d


def _ss(x: Any, d: str = "") -> str:
    try:
        return str(x)
    except Exception:
        return d


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _norm_sym(s: str) -> str:
    return str(s).strip().upper()


def _norm_rg(s: str) -> str:
    return str(s).strip().lower()


def _norm_grp(s: str) -> str:
    return str(s).strip().lower()


def _norm_scn(s: str) -> str:
    v = str(s).strip().lower()
    return v if v in ("continuation", "reversal") else "na"


class ABWinnerSuggesterV2:
    """
    Reads events:trades and manages AB winner suggestions.
    """

    def __init__(self, redis_client: Optional[aioredis.Redis] = None, redis_url: Optional[str] = None) -> None:
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.r = redis_client or aioredis.from_url(self.redis_url, decode_responses=True)

        self.stream = os.getenv("AB_EVENTS_STREAM", "events:trades")
        self.group = os.getenv("AB_WINNER_GROUP", "ab-winner-v2")
        self.consumer = os.getenv("AB_WINNER_CONSUMER", f"c-{os.getpid()}")

        # Retention and eval
        self.eval_every_ms = int(os.getenv("AB_WINNER_EVAL_EVERY_MS", "300000"))
        self._last_eval_ms = 0

        # Memory buffer for stats: (sym, rg, grp, scn) -> arm -> Deque[float]
        self._samples: Dict[Tuple[str, str, str, str], Dict[str, Deque[float]]] = {}
        self._ts_range: Dict[Tuple[str, str, str, str], Tuple[int, int]] = {}

        # --- LCB evaluator policy ---
        self._policy_overrides: Dict[str, Dict[str, Any]] = {}
        try:
            self._policy_overrides["trend"] = {
                "conf": float(os.getenv("AB_LCB_CONF_TREND", "0.90")),
                "min_n": int(os.getenv("AB_LCB_MINN_TREND", "60")),
                "min_edge_lcb": float(os.getenv("AB_LCB_EDGE_TREND", "0.07")),
            }
            self._policy_overrides["range"] = {
                "conf": float(os.getenv("AB_LCB_CONF_RANGE", "0.95")),
                "min_n": int(os.getenv("AB_LCB_MINN_RANGE", "80")),
                "min_edge_lcb": float(os.getenv("AB_LCB_EDGE_RANGE", "0.10")),
            }
            self._policy_overrides["thin"] = {
                "conf": float(os.getenv("AB_LCB_CONF_THIN", "0.975")),
                "min_n": int(os.getenv("AB_LCB_MINN_THIN", "120")),
                "min_edge_lcb": float(os.getenv("AB_LCB_EDGE_THIN", "0.15")),
            }
        except Exception:
            self._policy_overrides = {}

        # latest pointer prefix
        self.latest_prefix = "cfg:suggestions:entry_policy:latest:ab_winner"
        self.meta_prefix = "cfg:suggestions:entry_policy:meta"

        # --- Step 2: Cost-aware LCB ---
        self._cost_aware = os.getenv("LCB_COST_AWARE_ENABLE", "0") == "1"
        self._lcb_z = float(os.getenv("LCB_Z", "1.28"))
        self._min_n = int(os.getenv("LCB_MIN_N", "40"))
        self._floor = float(os.getenv("LCB_FLOOR_R", "0.00"))
        self._lookback_h = int(os.getenv("LCB_LOOKBACK_HOURS", "24"))
        self._hyst = WinnerHysteresis(self.r)

    def _collect_stats(self, symbol: str, regime: str, group: str, scenario: str) -> Dict[str, Any]:
        k = (_norm_sym(symbol), _norm_rg(regime), _norm_grp(group), _norm_scn(scenario))
        s = self._samples.get(k, {})
        tr = self._ts_range.get(k, (0, 0))
        return {
            "A": s.get("A", []),
            "B": s.get("B", []),
            "C": s.get("C", []),
            "ts_lo": tr[0],
            "ts_hi": tr[1]
        }

    async def _score_key_async(self, k: Tuple[str, str, str, str]) -> Dict[str, Any]:
        symbol, regime, group, scenario = k
        stats = self._collect_stats(symbol=symbol, regime=regime, group=group, scenario=scenario)
        samples_by_arm = {
            "A": list(stats.get("A", []) or []),
            "B": list(stats.get("B", []) or []),
            "C": list(stats.get("C", []) or []),
        }

        # --- Step 2: Cost-aware LCB with hysteresis ---
        if self._cost_aware:
            bucket_key = f"{symbol}|{regime}|{group}|{scenario}"
            stats_list = compute_arm_stats(samples_by_arm, z=self._lcb_z, min_n=self._min_n, floor=self._floor)
            if not stats_list:
                # Fallback to old method if no eligible arms
                winner = "A"
                reason = "no_eligible_arms_cost_aware"
                scores_dict = {}
            else:
                cand = stats_list[0]  # Best LCB
                # Apply hysteresis (async)
                res = await self._hyst.apply_async(bucket=bucket_key, candidate=cand.arm, candidate_lcb=cand.lcb)
                
                winner = res.winner
                reason = f"cost_aware_lcb_{res.reason}"
                
                # Metrics: track winner changes
                if res.changed:
                    lcb_winner_changes_total.labels(symbol=symbol, regime=regime, scenario=scenario).inc()
                
                # Metrics: track LCB margin (winner - runner-up)
                margin = 0.0
                if len(stats_list) >= 2:
                    # Find winner's LCB and runner-up LCB
                    winner_lcb = next((s.lcb for s in stats_list if s.arm == winner), 0.0)
                    # Runner-up is the second best (stats_list is sorted by LCB desc)
                    runner_up_lcb = stats_list[1].lcb if stats_list[0].arm != winner else stats_list[0].lcb
                    margin = winner_lcb - runner_up_lcb
                elif len(stats_list) == 1:
                    margin = stats_list[0].lcb
                lcb_margin.labels(symbol=symbol, regime=regime, scenario=scenario).set(margin)
                
                # Build scores dict from stats_list
                scores_dict = {
                    s.arm: {
                        "n": s.n,
                        "mean": s.mean,
                        "stdev": s.std,
                        "stderr": s.stderr,
                        "lcb": s.lcb
                    }
                    for s in stats_list
                }
                # Fill missing arms
                for arm in ["A", "B", "C"]:
                    if arm not in scores_dict:
                        n = len(samples_by_arm.get(arm, []))
                        scores_dict[arm] = {"n": n, "mean": 0.0, "stdev": 0.0, "stderr": 0.0, "lcb": -1e9}

                # -----------------------------
                # SRE metrics (Redis-backed)
                # -----------------------------
                try:
                    r = getattr(self, "r", None)
                    if r is not None:
                        # winner changes counter (per symbol|regime|scenario)
                        if getattr(res, "changed", False):
                            k1 = f"metrics:lcb_winner_changes_total:{symbol}|{regime}|{scenario}"
                            await r.incr(k1)
                            await r.expire(k1, int(os.getenv("METRICS_COUNTER_TTL_SEC", "604800")))
                        # margin = winner_lcb - runner_up_lcb (stats_list is sorted by LCB desc)
                        margin = 0.0
                        if len(stats_list) >= 2:
                            # find winner lcb
                            winner_lcb = next((s.lcb for s in stats_list if s.arm == winner), float(stats_list[0].lcb))
                            runner_up_lcb = float(stats_list[1].lcb) if stats_list[0].arm == winner else float(stats_list[0].lcb)
                            margin = float(winner_lcb - runner_up_lcb)
                        k2 = f"metrics:lcb_margin:{symbol}|{regime}|{scenario}"
                        await r.set(k2, str(margin), ex=int(os.getenv("METRICS_COUNTER_TTL_SEC", "604800")))
                        # discovery set for exporter/alerts
                        await r.sadd("metrics:lcb:keys", f"{symbol}|{regime}|{scenario}")
                        await r.expire("metrics:lcb:keys", int(os.getenv("METRICS_COUNTER_TTL_SEC", "604800")))
                except Exception:
                    pass
        else:
            # Original LCB method
            pol = default_regime_policy(regime)
            rg = (regime or "na").lower()
            if rg in ("trend", "trending_bull", "trending_bear") and "trend" in self._policy_overrides:
                o = self._policy_overrides["trend"]
                pol.conf, pol.min_n, pol.min_edge_lcb = o["conf"], o["min_n"], o["min_edge_lcb"]
            elif rg in ("range", "mixed") and "range" in self._policy_overrides:
                o = self._policy_overrides["range"]
                pol.conf, pol.min_n, pol.min_edge_lcb = o["conf"], o["min_n"], o["min_edge_lcb"]
            elif rg in ("thin", "news", "illiquid") and "thin" in self._policy_overrides:
                o = self._policy_overrides["thin"]
                pol.conf, pol.min_n, pol.min_edge_lcb = o["conf"], o["min_n"], o["min_edge_lcb"]

            winner, scores, reason = choose_winner_lcb(samples_by_arm=samples_by_arm, regime=regime, policy=pol)
            scores_dict = {
                a: {"n": s.n, "mean": s.mean, "stdev": s.stdev, "stderr": s.stderr, "lcb": s.lcb}
                for a, s in scores.items()
            }
            
            # Metrics: track LCB margin (winner - runner-up) for non-cost-aware mode
            margin = 0.0
            if len(scores) >= 2:
                winner_score = scores.get(winner)
                if winner_score:
                    # Find runner-up (second best LCB)
                    sorted_arms = sorted(scores.items(), key=lambda x: x[1].lcb, reverse=True)
                    # sorted_arms[0] is the best, sorted_arms[1] is runner-up
                    if len(sorted_arms) >= 2:
                        runner_up_lcb = sorted_arms[1][1].lcb
                        margin = winner_score.lcb - runner_up_lcb
                    else:
                        margin = winner_score.lcb
            elif len(scores) == 1:
                margin = list(scores.values())[0].lcb
            lcb_margin.labels(symbol=symbol, regime=regime, scenario=scenario).set(margin)

            # Metrics: LCB margin for non-cost-aware mode too (Redis-backed)
            try:
                r = getattr(self, "r", None)
                if r is not None and scores:
                    # sort by lcb desc
                    sorted_arms = sorted(scores.items(), key=lambda x: x[1].lcb, reverse=True)
                    margin = 0.0
                    if len(sorted_arms) >= 2:
                        margin = float(sorted_arms[0][1].lcb - sorted_arms[1][1].lcb)
                    k2 = f"metrics:lcb_margin:{symbol}|{regime}|{scenario}"
                    await r.set(k2, str(margin), ex=int(os.getenv("METRICS_COUNTER_TTL_SEC", "604800")))
                    await r.sadd("metrics:lcb:keys", f"{symbol}|{regime}|{scenario}")
                    await r.expire("metrics:lcb:keys", int(os.getenv("METRICS_COUNTER_TTL_SEC", "604800")))
            except Exception:
                pass

        ts_lo = int(stats.get("ts_lo", 0) or 0)
        ts_hi = int(stats.get("ts_hi", 0) or 0)
        sid_src = f"{symbol}|{regime}|{group}|{scenario}|{winner}|{ts_lo}|{ts_hi}"
        sid = hashlib.sha1(sid_src.encode("utf-8")).hexdigest()

        meta = {
            "v": 2,
            "sid": sid,
            "symbol": str(symbol),
            "regime": str(regime),
            "group": str(group),
            "scenario": str(scenario),
            "winner_arm": str(winner),
            "reason": str(reason),
            "cost_aware": bool(self._cost_aware),
            "scores": scores_dict,
            "window": {"ts_lo": ts_lo, "ts_hi": ts_hi},
            "updated_ts_ms": int(time.time() * 1000),
        }
        if not self._cost_aware:
            # Add policy info for non-cost-aware mode
            pol = default_regime_policy(regime)
            meta["policy"] = {"conf": float(pol.conf), "min_n": int(pol.min_n), "min_edge_lcb": float(pol.min_edge_lcb)}
        return {"sid": sid, "meta": meta, "winner": winner, "reason": reason}

    async def publish_suggestion(self, *, symbol: str, regime: str, group: str, scenario: str) -> Optional[str]:
        try:
            res = await self._score_key_async((symbol, regime, group, scenario))
            sid = str(res.get("sid") or "")
            meta = res.get("meta") or {}
            if not sid or not isinstance(meta, dict):
                return None

            meta_key = f"{self.meta_prefix}:{sid}"
            latest_key = f"{self.latest_prefix}:{symbol}:{regime}:{group}:{scenario}"
            ttl = int(os.getenv("AB_WINNER_META_TTL_SEC", "604800"))  # 7d
            pipe = self.r.pipeline()
            pipe.set(meta_key, json.dumps(meta, ensure_ascii=False, separators=(",", ":")), ex=ttl)
            pipe.set(latest_key, sid, ex=ttl)
            await pipe.execute()
            return sid
        except Exception:
            return None

    def _extract_sample_value(self, payload: Dict[str, Any]) -> float:
        """Extract R-multiple value, optionally cost-aware adjusted."""
        if self._cost_aware:
            return float(compute_r_adj(payload))
        return float(payload.get("r_mult", 0.0) or 0.0)

    def _ingest(self, msg: Dict[str, Any]) -> None:
        et = _ss(msg.get("event_type") or msg.get("event"))
        if et != "POSITION_CLOSED":
            return
        sym = _norm_sym(msg.get("symbol", ""))
        rg = _norm_rg(msg.get("regime", "na"))
        grp = _norm_grp(msg.get("ab_group", "default"))
        scn = _norm_scn(msg.get("scenario", msg.get("decision", "na")))
        arm = _ss(msg.get("ab_arm", "A")).upper()
        # Use cost-aware extraction if enabled
        r_value = self._extract_sample_value(msg)
        ts = _si(msg.get("ts", 0), 0)

        if not sym or scn == "na":
            return

        k = (sym, rg, grp, scn)
        if k not in self._samples:
            self._samples[k] = {"A": deque(maxlen=2000), "B": deque(maxlen=2000), "C": deque(maxlen=2000)}
            self._ts_range[k] = (ts, ts)
        
        if arm not in self._samples[k]:
            self._samples[k][arm] = deque(maxlen=2000)
            
        self._samples[k][arm].append(r_value)
        lo, hi = self._ts_range[k]
        self._ts_range[k] = (min(lo, ts) if lo > 0 else ts, max(hi, ts))

    async def run_once(self) -> None:
        await self.run_once_impl()

    async def run_once_impl(self) -> None:
        # Drain stream
        from core.redis_stream_consumer import AsyncRedisStreamHelper
        helper = AsyncRedisStreamHelper(self.r, self.group, self.consumer)
        try:
            await helper.ensure_group(self.stream, start_id="0-0")
        except Exception:
            pass
        
        # Read a large batch
        batch = await self.r.xreadgroup(self.group, self.consumer, {self.stream: ">"}, count=1000, block=1)
        if batch:
            for s, msgs in batch:
                for mid, fields in msgs:
                    self._ingest(fields)
                    await self.r.xack(self.stream, self.group, mid)

        # Evaluate all keys in memory
        for k in list(self._samples.keys()):
            await self.publish_suggestion(symbol=k[0], regime=k[1], group=k[2], scenario=k[3])

    async def run_forever(self) -> None:
        from core.redis_client import wait_for_redis_async
        if not await wait_for_redis_async(self.r):
            log.error("❌ Redis is not ready after wait. Exiting.")
            return

        from core.redis_stream_consumer import AsyncRedisStreamHelper
        helper = AsyncRedisStreamHelper(self.r, self.group, self.consumer)
        try:
            await helper.ensure_group(self.stream, start_id="0-0")
        except Exception:
            pass

        while True:
            batch = await self.r.xreadgroup(self.group, self.consumer, {self.stream: ">"}, count=200, block=1000)
            if batch:
                for s, msgs in batch:
                    for mid, fields in msgs:
                        try:
                            self._ingest(fields)
                        except Exception:
                            pass
                        await self.r.xack(self.stream, self.group, mid)

            now = _now_ms()
            if now - self._last_eval_ms >= self.eval_every_ms:
                for k in list(self._samples.keys()):
                    try:
                        await self.publish_suggestion(symbol=k[0], regime=k[1], group=k[2], scenario=k[3])
                    except Exception:
                        pass
                self._last_eval_ms = now
            await asyncio.sleep(0.1)


async def _run_once_main() -> None:
    """
    Entry point for scheduled evaluator.
    - acquires Redis lock
    - runs run_once() to update winners/proposals
    """
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    # ensure clean redis client
    r = aioredis.from_url(redis_url, decode_responses=True)

    lock_key = os.getenv("AB_WINNER_LOCK_KEY", "lock:ab_winner_evaluator:v1")
    ttl = int(os.getenv("AB_WINNER_LOCK_TTL_SEC", "3500"))  # ~58 min
    lock = await try_acquire_lock(r, key=lock_key, ttl_sec=ttl)
    if lock is None:
        # already running or locked
        return
    try:
        svc = ABWinnerSuggesterV2(redis_client=r)
        await svc.run_once()
    finally:
        await release_lock(r, lock, key=lock_key)
        try:
            await r.close()
        except Exception:
            pass


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--run-once", action="store_true", help="Run evaluator once and exit (with Redis lock).")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    if args.run_once:
        asyncio.run(_run_once_main())
    else:
        # legacy behavior: long-running
        asyncio.run(ABWinnerSuggesterV2().run_forever())
