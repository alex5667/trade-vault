#!/usr/bin/env python3
"""tools/signal_quality_kpi_worker_v1.py

P47: Compute 24h rolling Signal Quality KPIs from `trades:closed` and write into cfg2 (`settings:dynamic_cfg`).
P64: Adds per-regime breakdown (ok/warn/block) based on DQ+Drift states.
P70: Adds per-policy-mode breakdown (ok/warn/block/unknown) based on `policy_effective_mode` (breaker).

Outputs (cfg2 keys):
- signal_quality_expectancy_r_24h
- signal_quality_precision_top5p_24h
- signal_quality_ece_24h
- signal_quality_n_24h
- signal_quality_last_ts_ms
- *_regime_{ok|warn|block}
- *_policy_{ok|warn|block|unknown}
- signal_quality_policy_mode_counts_24h_json
- signal_quality_policy_mode_last_ts_ms

ENV:
  REDIS_URL (default redis://redis-worker-1:6379/0)
  DYN_CFG_KEY (default settings:dynamic_cfg)
  TRADES_CLOSED_STREAM (default trades:closed)
  SIGNAL_QUALITY_LOOKBACK_H (default 24)
  SIGNAL_QUALITY_MAX_SCAN (default 200000)
  SIGNAL_QUALITY_TOP_P (default 0.05)
  SIGNAL_QUALITY_ECE_BINS (default 10)
  SIGNAL_QUALITY_SCORE_FIELDS (optional, comma-separated override)
  SIGNAL_QUALITY_R_FIELDS (optional, comma-separated override)
  SIGNAL_QUALITY_POLICY_MODE_FIELD (optional override, default policy_effective_mode)

Run:
  python3 -m tools.signal_quality_kpi_worker_v1 --once
"""

from __future__ import annotations

import argparse
import json
import math
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Tuple

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore


def now_ms() -> int:
    return int(time.time() * 1000)



def _csv_env_list(name: str) -> list[str]:
    v = (os.getenv(name) or "").strip()
    if not v:
        return []
    return [s.strip() for s in v.split(',') if s.strip()]


def _to_str(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, (bytes, bytearray)):
        try:
            return x.decode("utf-8", "replace")
        except Exception:
            return ""
    return str(x)


def _f(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, (bytes, bytearray)):
            x = _to_str(x)
        return float(x)
    except Exception:
        return default


def _i(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        if isinstance(x, (bytes, bytearray)):
            x = _to_str(x)
        return int(float(x))
    except Exception:
        return default


def clamp01(x: float) -> float:
    if x != x:  # NaN
        return 0.0
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


def _loads_json_maybe(s: str) -> Any:
    s = (s or "").strip()
    if not s:
        return None
    if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
        try:
            return json.loads(s)
        except Exception:
            return s
    return s


def _parse_stream_id_ms(msg_id: str) -> int:
    # Redis Streams id: <ms>-<seq>
    try:
        return int(msg_id.split("-", 1)[0])
    except Exception:
        return 0


def _decode_fields(raw: Mapping[Any, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in raw.items():
        ks = _to_str(k)
        if isinstance(v, (bytes, bytearray)):
            vs = _to_str(v)
            # attempt JSON decode for structured values
            out[ks] = _loads_json_maybe(vs)
        else:
            out[ks] = v
    # Optional nested payload pattern
    pj = out.get("payload_json")
    if isinstance(pj, str):
        maybe = _loads_json_maybe(pj)
        if isinstance(maybe, dict):
            out.update(maybe)
    elif isinstance(pj, dict):
        out.update(pj)
    return out


def _pick_ts_ms(msg_id: str, f: Mapping[str, Any]) -> int:
    # Prefer explicit timestamps from payload; fallback to stream id.
    for k in (
        "exit_ts_ms",
        "close_ts_ms",
        "ts_ms",
        "decision_ts_ms",
        "open_ts_ms",
        "ts",
    ):
        ts = _i(f.get(k), 0)
        if ts > 0:
            # Accept seconds timestamps too
            if ts < 10_000_000_000:  # < ~2286-11-20 in seconds
                # Heuristic: if ts is 10 digits => seconds
                if ts < 1_000_000_000_000:
                    return ts * 1000
            return ts
    return _parse_stream_id_ms(msg_id)


def _normalize_mode(x: Any) -> str:
    s = _to_str(x).strip().lower()
    if not s:
        return "unknown"
    # allow common synonyms
    if s in ("ok", "pass", "allow", "green", "normal"):
        return "ok"
    if s in ("warn", "warning", "yellow", "degraded"):
        return "warn"
    if s in ("block", "blocked", "deny", "red", "halt", "stop"):
        return "block"
    return "unknown"


def _regime_from_fields(f: Mapping[str, Any]) -> str:
    # If explicit combined regime exists
    for k in ("regime", "dqdrift_regime", "decision_regime"):
        s = _normalize_mode(f.get(k))
        if s != "unknown":
            return s

    # Combine dq/drift states: pick worst (ok < warn < block)
    dq = _normalize_mode(f.get("dq_state") or f.get("dq_regime"))
    dr = _normalize_mode(f.get("drift_state") or f.get("drift_regime"))

    order = {"ok": 0, "warn": 1, "block": 2, "unknown": -1}
    # If both unknown => unknown; else take max severity among known
    cand = [dq, dr]
    known = [c for c in cand if c != "unknown"]
    if not known:
        return "unknown"
    return max(known, key=lambda m: order.get(m, -1))



def _policy_mode_from_fields(f: Mapping[str, Any], policy_field: str) -> str:
    if policy_field:
        return _normalize_mode(f.get(policy_field))
    return _normalize_mode(
        f.get("policy_effective_mode")
        or f.get("policy_mode")
        or f.get("policy_effective_state")
        or f.get("breaker_mode")
    )




def _score_from_fields(f: Mapping[str, Any], score_fields: list[str]) -> Optional[float]:
    # Priority list of candidate fields. Expect score in [0..1] but clamp.
    for k in score_fields:
        if k in f and f.get(k) is not None:
            v = _f(f.get(k), float("nan"))
            if v == v:
                # If scores are 0..100, map to 0..1
                if v > 1.5 and v <= 100.0:
                    v = v / 100.0
                return clamp01(v)
    return None




def _r_mult_from_fields(f: Mapping[str, Any], r_fields: list[str]) -> Optional[float]:
    for k in r_fields:
        if k in f and f.get(k) is not None:
            v = _f(f.get(k), float("nan"))
            if v == v:
                return float(v)
    return None



def precision_at_top_p(scores: List[float], y: List[int], p: float) -> float:
    n = len(scores)
    if n == 0:
        return 0.0
    k = max(1, int(math.ceil(p * n)))
    idx = sorted(range(n), key=lambda i: scores[i], reverse=True)
    wins = sum(y[i] for i in idx[:k])
    return wins / float(k)


def ece(scores: List[float], y: List[int], n_bins: int) -> float:
    n = len(scores)
    if n == 0:
        return 0.0
    n_bins = max(2, int(n_bins))
    bins_cnt = [0] * n_bins
    bins_conf = [0.0] * n_bins
    bins_acc = [0.0] * n_bins
    for s, yy in zip(scores, y):
        ss = clamp01(float(s))
        b = int(min(n_bins - 1, math.floor(ss * n_bins)))
        bins_cnt[b] += 1
        bins_conf[b] += ss
        bins_acc[b] += float(yy)

    out = 0.0
    for cnt, conf_sum, acc_sum in zip(bins_cnt, bins_conf, bins_acc):
        if cnt <= 0:
            continue
        avg_conf = conf_sum / cnt
        avg_acc = acc_sum / cnt
        out += (cnt / float(n)) * abs(avg_acc - avg_conf)
    return float(out)


@dataclass
class Acc:
    scores: List[float]
    y: List[int]
    r: List[float]


def _acc_add(acc: Acc, score: float, y: int, r_mult: float) -> None:
    acc.scores.append(score)
    acc.y.append(y)
    acc.r.append(r_mult)


def _acc_metrics(acc: Acc, top_p: float, ece_bins: int) -> Tuple[float, float, float, int]:
    n = len(acc.scores)
    if n == 0:
        return 0.0, 0.0, 0.0, 0
    exp_r = sum(acc.r) / float(n)
    pr_top = precision_at_top_p(acc.scores, acc.y, top_p)
    e = ece(acc.scores, acc.y, ece_bins)
    return exp_r, pr_top, e, n


def compute_and_write_once() -> int:
    if redis is None:
        raise RuntimeError("redis package is not available")

    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    dyn_cfg_key = os.getenv("DYN_CFG_KEY", "settings:dynamic_cfg")
    stream_key = os.getenv("TRADES_CLOSED_STREAM", "trades:closed")

    lookback_h = float(os.getenv("SIGNAL_QUALITY_LOOKBACK_H", "24") or 24)
    max_scan = int(os.getenv("SIGNAL_QUALITY_MAX_SCAN", "200000") or 200000)
    top_p = float(os.getenv("SIGNAL_QUALITY_TOP_P", "0.05") or 0.05)
    ece_bins = int(os.getenv("SIGNAL_QUALITY_ECE_BINS", "10") or 10)



    # Optional overrides (comma-separated)

    score_fields = _csv_env_list("SIGNAL_QUALITY_SCORE_FIELDS") or [

        "score",

        "ml_score",

        "confidence",

        "confidence01",

        "prob",

        "p",

        "p_hat",

        "proba",

        "probability",

    ]

    r_fields = _csv_env_list("SIGNAL_QUALITY_R_FIELDS") or ["r_mult", "r_multiple", "r", "R"]

    policy_field = (os.getenv("SIGNAL_QUALITY_POLICY_MODE_FIELD") or "").strip()


    now = now_ms()
    since_ms = int(now - lookback_h * 3600.0 * 1000.0)

    _redis_delay = 1.0
    for _attempt in range(3):
        try:
            r = redis.Redis.from_url(redis_url, decode_responses=False)
            r.ping()
            break
        except Exception as _e:
            if _attempt == 2:
                raise
            print(f"⚠️ Redis not ready (attempt {_attempt + 1}/3): {_e}. Retry in {_redis_delay:.0f}s...")
            time.sleep(_redis_delay)
            _redis_delay = min(_redis_delay * 2, 10.0)

    # Accumulators
    all_acc = Acc([], [], [])
    by_regime: Dict[str, Acc] = {k: Acc([], [], []) for k in ("ok", "warn", "block", "unknown")}
    by_policy: Dict[str, Acc] = {k: Acc([], [], []) for k in ("ok", "warn", "block", "unknown")}

    scanned = 0
    kept = 0

    # Reverse scan newest -> oldest; stop when ts below since_ms.
    start = "+"
    while scanned < max_scan:
        rows = r.xrevrange(stream_key, max= start, min= "-", count=1000)
        if not rows:
            break

        # xrevrange returns newest..oldest for this page
        for msg_id_b, raw_fields in rows:
            scanned += 1
            msg_id = _to_str(msg_id_b)
            f = _decode_fields(raw_fields)
            ts = _pick_ts_ms(msg_id, f)

            # update `start` to paginate further (exclusive)
            start = f"({msg_id}"

            if ts < since_ms:
                # stop fully: remaining are older
                scanned = max_scan
                break

            score = _score_from_fields(f, score_fields)
            r_mult = _r_mult_from_fields(f, r_fields)
            if score is None or r_mult is None:
                continue

            y = 1 if r_mult > 0.0 else 0
            _acc_add(all_acc, score, y, float(r_mult))

            regime = _regime_from_fields(f)
            regime = regime if regime in by_regime else "unknown"
            _acc_add(by_regime[regime], score, y, float(r_mult))

            mode = _policy_mode_from_fields(f, policy_field)
            mode = mode if mode in by_policy else "unknown"
            _acc_add(by_policy[mode], score, y, float(r_mult))

            kept += 1

        if scanned >= max_scan:
            break

        # If fewer than 1000 returned, done.
        if len(rows) < 1000:
            break

    # Compute metrics
    exp_r, pr_top, e, n = _acc_metrics(all_acc, top_p, ece_bins)

    out: Dict[str, Any] = {
        "signal_quality_expectancy_r_24h": exp_r,
        "signal_quality_precision_top5p_24h": pr_top,
        "signal_quality_ece_24h": e,
        "signal_quality_n_24h": n,
        "signal_quality_last_ts_ms": now,
    }

    # Regime
    for k, acc in by_regime.items():
        if k not in ("ok", "warn", "block"):
            continue  # keep P64 low-cardinality
        exp_r_k, pr_k, e_k, n_k = _acc_metrics(acc, top_p, ece_bins)
        out[f"signal_quality_expectancy_r_24h_regime_{k}"] = exp_r_k
        out[f"signal_quality_precision_top5p_24h_regime_{k}"] = pr_k
        out[f"signal_quality_ece_24h_regime_{k}"] = e_k
        out[f"signal_quality_n_24h_regime_{k}"] = n_k

    # Policy mode (P70)
    counts: Dict[str, int] = {}
    for k, acc in by_policy.items():
        exp_r_k, pr_k, e_k, n_k = _acc_metrics(acc, top_p, ece_bins)
        out[f"signal_quality_expectancy_r_24h_policy_{k}"] = exp_r_k
        out[f"signal_quality_precision_top5p_24h_policy_{k}"] = pr_k
        out[f"signal_quality_ece_24h_policy_{k}"] = e_k
        out[f"signal_quality_n_24h_policy_{k}"] = n_k
        counts[k] = n_k

    out["signal_quality_policy_mode_counts_24h_json"] = json.dumps(counts, sort_keys=True)
    out["signal_quality_policy_mode_last_ts_ms"] = now

    # Write to cfg2
    mapping: Dict[str, Any] = {}
    for kk, vv in out.items():
        if isinstance(vv, (dict, list)):
            mapping[kk] = json.dumps(vv)
        else:
            mapping[kk] = vv

    if mapping:
        r.hset(dyn_cfg_key, mapping=mapping)

    print(
        json.dumps(
            {
                "ts_ms": now,
                "since_ms": since_ms,
                "scanned": scanned,
                "kept": kept,
                "n": n,
                "policy_counts": counts,
            },
            sort_keys=True,
        )
    )
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="Run once and exit")
    args = ap.parse_args(argv)

    if not args.once:
        # This worker is expected to be run by a timer; for safety, default to once.
        args.once = True

    if args.once:
        return compute_and_write_once()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
