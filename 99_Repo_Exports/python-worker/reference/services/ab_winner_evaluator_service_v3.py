# -*- coding: utf-8 -*-
"""
AB Winner Evaluator Service V3 (Continuous Loop)
===============================================

Запуск:
  python -m services.ab_winner_evaluator_service_v3

Назначение:
  - Периодически (раз в час) сканировать events:trades.
  - "Ещё выше": инкрементальный ingest, scenario aggregation, hysteresis.
  - Писать Suggestions в Redis.

Отличие от oneshot:
  - Работает в бесконечном цикле с sleep.
  - Использует ABWinnerEvalStore.
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import signal
import time
import uuid
from typing import Any, Dict, List, Tuple

import redis

from services.ab_winner_evaluator_core import (
    WinnerDecision,
    aggregate_scenario_winners,
    choose_winner_lcb,
    hysteresis_should_publish,
    make_meta_payload,
    norm_regime,
    regime_bucket,
)
from services.ab_winner_eval_store import ABWinnerEvalStore


def _now_ms() -> int:
    return get_ny_time_millis()


def _lua_unlock() -> str:
    # release lock only if token matches
    return """
    if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
    else
        return 0
    end
    """


class ABWinnerEvaluatorLogic:
    def __init__(self) -> None:
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.r = redis.from_url(self.redis_url, decode_responses=True)

        self.stream = os.getenv("AB_EVAL_STREAM", os.getenv("TRADE_EVENTS_STREAM", "events:trades"))
        self.max_events = int(os.getenv("AB_EVAL_MAX_EVENTS", "200000"))
        self.window_hours = float(os.getenv("AB_EVAL_WINDOW_HOURS", os.getenv("AB_EVAL_LOOKBACK_HOURS", "24")))
        self.ignore_last_min = int(os.getenv("AB_EVAL_IGNORE_LAST_MIN", "5"))
        self.max_items_per_zset = int(os.getenv("AB_EVAL_MAX_ITEMS_PER_ZSET", "8000"))

        # Winner controls
        self.min_n = int(os.getenv("AB_EVAL_MIN_N", "40"))
        self.arm_ver = int(os.getenv("AB_EVAL_ARM_VER", "1"))

        # LCB params per regime bucket
        self.alpha_by_bucket = {
            "trend": float(os.getenv("AB_EVAL_ALPHA_TREND", "0.10")),
            "range": float(os.getenv("AB_EVAL_ALPHA_RANGE", "0.10")),
            "mixed": float(os.getenv("AB_EVAL_ALPHA_MIXED", "0.10")),
            "thin": float(os.getenv("AB_EVAL_ALPHA_THIN", "0.05")),
        }
        self.min_edge_by_bucket = {
            "trend": float(os.getenv("AB_EVAL_MIN_EDGE_TREND_R", "0.05")),
            "range": float(os.getenv("AB_EVAL_MIN_EDGE_RANGE_R", "0.08")),
            "mixed": float(os.getenv("AB_EVAL_MIN_EDGE_MIXED_R", "0.08")),
            "thin": float(os.getenv("AB_EVAL_MIN_EDGE_THIN_R", "0.12")),
        }

        # "Ещё выше": hysteresis / hold-down
        self.hold_down_ms = int(os.getenv("AB_EVAL_HOLD_DOWN_MS", str(6 * 3600 * 1000)))  # 6h
        self.switch_min_margin_r = float(os.getenv("AB_EVAL_SWITCH_MIN_MARGIN_R", "0.12"))
        self.scenario_disagree_margin_r = float(os.getenv("AB_EVAL_SCENARIO_DISAGREE_MARGIN_R", "0.18"))

        # Suggestions keys
        self.meta_prefix = os.getenv("AB_SUGG_META_PREFIX", "cfg:suggestions:entry_policy:meta")
        self.latest_prefix = os.getenv("AB_SUGG_LATEST_PREFIX", "cfg:suggestions:entry_policy:latest:ab_winner")
        self.meta_ttl_sec = int(os.getenv("AB_SUGG_META_TTL_SEC", str(30 * 24 * 3600)))
        
        # Report artefact
        self.report_key = os.getenv("AB_EVAL_REPORT_KEY", "ab:winner_report:latest")

        # Locking
        self.lock_key = os.getenv("AB_EVAL_LOCK_KEY", "lock:ab_winner_evaluator:v1")
        self.lock_ttl_sec = int(os.getenv("AB_EVAL_LOCK_TTL_SEC", "3300"))

    def _acquire_lock(self) -> Tuple[bool, str]:
        token = str(uuid.uuid4())
        try:
            ok = self.r.set(self.lock_key, token, nx=True, ex=self.lock_ttl_sec)
            return (bool(ok), token)
        except Exception:
            return (False, token)

    def _release_lock(self, token: str) -> None:
        try:
            self.r.eval(_lua_unlock(), 1, self.lock_key, token)
        except Exception:
            pass
            
    def _load_prev_meta(self, symbol: str, regime: str, group: str) -> Dict[str, Any]:
        latest_key = f"{self.latest_prefix}:{symbol}:{regime}:{group}"
        try:
            sid = str(self.r.get(latest_key) or "")
            if not sid:
                return {}
            raw = self.r.get(f"{self.meta_prefix}:{sid}")
            if not raw:
                return {}
            d = json.loads(raw)
            return d if isinstance(d, dict) else {}
        except Exception:
            return {}

    def _write_suggestion(self, meta: Dict[str, Any]) -> None:
        sid = str(meta.get("sid") or "")
        symbol = str(meta.get("symbol") or "").upper()
        regime = norm_regime(str(meta.get("regime") or "na"))
        group = str(meta.get("group") or "default").strip().lower()

        meta_key = f"{self.meta_prefix}:{sid}"
        latest_key = f"{self.latest_prefix}:{symbol}:{regime}:{group}"

        raw = json.dumps(meta, ensure_ascii=False, separators=(",", ":"))
        try:
            pipe = self.r.pipeline()
            pipe.set(meta_key, raw, ex=self.meta_ttl_sec)
            pipe.set(latest_key, sid, ex=self.meta_ttl_sec)
            pipe.execute()
        except Exception:
            pass

    def run_once(self) -> int:
        ok, token = self._acquire_lock()
        if not ok:
            return 0
        try:
            now = _now_ms()
            end = now - int(max(0, self.ignore_last_min)) * 60_000
            window_ms = int(self.window_hours * 3600.0 * 1000.0)
            start = end - window_ms

            store = ABWinnerEvalStore(r=self.r, stream=self.stream)
            ing = store.ingest_from_stream(
                end_ms=end,
                window_ms=window_ms,
                max_items_per_zset=self.max_items_per_zset,
                hard_cap_msgs=self.max_events,
            )

            # Evaluate per ctx (symbol|regime|group), using scenario splits
            ctxs = store.list_contexts()
            if not ctxs:
                return 0

            n_written = 0
            n_suppressed = 0
            window_sec = int(self.window_hours * 3600.0)
            report_rows: List[Dict[str, Any]] = []

            for ctx_id in ctxs:
                try:
                    symbol, regime, group = ctx_id.split("|", 2)
                except Exception:
                    continue
                symbol = str(symbol).upper()
                regime = norm_regime(regime)
                group = str(group).strip().lower()

                rb = regime_bucket(regime)
                alpha = float(self.alpha_by_bucket.get(rb, 0.10))
                min_edge = float(self.min_edge_by_bucket.get(rb, 0.08))

                # load per-scenario series
                per_scn: Dict[str, Dict[str, List[float]]] = {}
                for scn in ("continuation", "reversal"):
                    arm_to_r = {"A": [], "B": [], "C": []}
                    for arm in ("A", "B", "C"):
                        xs = store.load_r_mult_series(
                            symbol=symbol, regime=regime, group=group,
                            scenario=scn, arm=arm,
                            start_ms=start, end_ms=end,
                        )
                        arm_to_r[arm] = xs
                    # Keep scenario only if has any data
                    if any(len(arm_to_r[a]) > 0 for a in ("A", "B", "C")):
                        per_scn[scn] = arm_to_r

                # pooled (across scenarios)
                pooled_arm_to_r = {"A": [], "B": [], "C": []}
                for scn, arm_to_r in per_scn.items():
                    for arm in ("A", "B", "C"):
                        pooled_arm_to_r[arm].extend(list(arm_to_r.get(arm) or []))

                if not any(len(pooled_arm_to_r[a]) > 0 for a in ("A", "B", "C")):
                    continue

                pooled_dec = choose_winner_lcb(
                    regime=regime,
                    arm_to_r=pooled_arm_to_r,
                    min_n=self.min_n,
                    min_edge_by_bucket=self.min_edge_by_bucket,
                    alpha_by_bucket=self.alpha_by_bucket,
                    require_lcb_gt0_for_non_a=True,
                )
                per_scn_dec: Dict[str, WinnerDecision] = {}
                for scn, arm_to_r in per_scn.items():
                    per_scn_dec[scn] = choose_winner_lcb(
                        regime=regime,
                        arm_to_r=arm_to_r,
                        min_n=self.min_n,
                        min_edge_by_bucket=self.min_edge_by_bucket,
                        alpha_by_bucket=self.alpha_by_bucket,
                        require_lcb_gt0_for_non_a=True,
                    )

                final_dec = aggregate_scenario_winners(
                    regime=regime,
                    pooled=pooled_dec,
                    per_scn=per_scn_dec,
                    require_same_winner_when_non_a=True,
                    disagree_allow_margin_r=self.scenario_disagree_margin_r,
                )

                prev = self._load_prev_meta(symbol, regime, group)
                ok_pub, why = hysteresis_should_publish(
                    now_ms=now,
                    prev_meta=(prev if prev else None),
                    new_winner=final_dec,
                    hold_down_ms=self.hold_down_ms,
                    switch_min_margin_r=self.switch_min_margin_r,
                )

                # Build meta; include scenario breakdown for audit
                meta = make_meta_payload(
                    now_ms=now,
                    symbol=symbol,
                    regime=regime,
                    group=group,
                    arm_ver=self.arm_ver,
                    window_sec=window_sec,
                    min_n=self.min_n,
                    decision=final_dec,
                    rbucket=rb,
                    min_edge=min_edge,
                    alpha=alpha,
                )
                meta["scenario"] = {
                    scn: {
                        "winner": per_scn_dec[scn].winner,
                        "reason": per_scn_dec[scn].reason,
                        "stats": {a: per_scn_dec[scn].stats[a].__dict__ for a in ("A","B","C")},
                    }
                    for scn in per_scn_dec.keys()
                }
                meta["hysteresis"] = {"publish": int(ok_pub), "reason": why}
                if isinstance(prev, dict) and prev:
                    meta["prev"] = {"winner_arm": prev.get("winner_arm"), "ts_ms": prev.get("ts_ms")}

                if ok_pub:
                    self._write_suggestion(meta)
                    n_written += 1
                else:
                    n_suppressed += 1

                report_rows.append({
                    "symbol": symbol, "regime": regime, "group": group,
                    "bucket": rb,
                    "winner": final_dec.winner,
                    "reason": final_dec.reason,
                    "publish": int(ok_pub),
                    "publish_reason": why,
                    "nA": final_dec.stats["A"].n, "nB": final_dec.stats["B"].n, "nC": final_dec.stats["C"].n,
                    # "lcbA": final_dec.stats["A"].lcb, 
                })

            # Store run report artefact (JSON)
            try:
                rep = {
                    "ts_ms": now,
                    "stream": self.stream,
                    "ingest": {"n_msgs": ing.n_msgs, "n_closed": ing.n_closed, "last_id": ing.last_id},
                    "written": n_written,
                    "suppressed": n_suppressed,
                    "rows": report_rows[:2000], 
                }
                self.r.set(self.report_key, json.dumps(rep, ensure_ascii=False, separators=(",", ":")), ex=24 * 3600)
            except Exception:
                pass

            return n_written
        finally:
            self._release_lock(token)


class ABWinnerEvaluatorService(ABWinnerEvaluatorLogic):
    def __init__(self) -> None:
        super().__init__()
        self.interval_sec = int(os.getenv("AB_EVAL_INTERVAL_SEC", "3600"))
        self._running = True
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum: int, frame: Any) -> None:
        print(f"ab_winner_evaluator: received signal {signum}, stopping...")
        self._running = False

    def run_loop(self) -> None:
        print(f"ab_winner_evaluator: starting loop, interval={self.interval_sec}s, window={self.window_hours}h")
        while self._running:
            try:
                start_ts = time.time()
                n = self.run_once()
                elapsed = time.time() - start_ts
                print(f"ab_winner_evaluator: wrote {n} suggestions in {elapsed:.2f}s (report_key={self.report_key})")
            except Exception as e:
                print(f"ab_winner_evaluator: error in loop: {e}")
                import traceback
                traceback.print_exc()
            
            # Smart sleep
            sleep_rem = self.interval_sec
            step = 1.0
            while self._running and sleep_rem > 0:
                time.sleep(min(step, sleep_rem))
                sleep_rem -= step
        print("ab_winner_evaluator: shutdown complete.")


def main() -> None:
    svc = ABWinnerEvaluatorService()
    svc.run_loop()


if __name__ == "__main__":
    main()
