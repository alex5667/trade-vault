"""
Signal Quality KPI Worker (v3)

Reads: TRADES_CLOSED_STREAM (default: trades:closed), expects each entry has field:
  - payload: JSON
Relative lookback: lookback_h (default 24h)

Computes for rolling window:
  - expectancy_r_24h = mean(r_mult)
  - precision_top5p_24h (rank by score field)
  - ece_24h (if prob fields exist)

Breakdowns:
  A) by (drift_mode, dq_state)  -> exported to Prometheus
  B) by (meta_enforce_cov_bucket, meta_enforce_applied) -> exported to Prometheus (safe cardinality)
  C) by rule_reason_code_top1 -> stored in Redis only (top-K by n), not exported

Writes:
  - cfg2 (settings:dynamic_cfg) global keys (for exporters/alerts)
  - hashes:
      metrics:signal_quality:24h:by_mode
      metrics:signal_quality:24h:by_bucket
      metrics:signal_quality:24h:by_reason
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import redis
from prometheus_client import Counter

_runs_total = Counter("signal_quality_kpi_v3_runs_total", "KPI v3 runs", ["result"])

def _now_ms() -> int:
    return int(time.time() * 1000)

def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if (v is not None and str(v).strip() != "") else default

def _env_int(name: str, default: str) -> int:
    try:
        return int(_env(name, default))
    except Exception:
        return int(default)

def _env_float(name: str, default: str) -> float:
    try:
        return float(_env(name, default))
    except Exception:
        return float(default)

def _loads(s: Any) -> Dict[str, Any]:
    if s is None:
        return {}
    if isinstance(s, dict):
        return s
    try:
        return json.loads(s)
    except Exception:
        return {}

def _pick(d: Dict[str, Any], *keys: str) -> Optional[Any]:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None

def _as_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None

def _state(v: Any) -> str:
    if v is None:
        return "unknown"
    s = str(v).strip().lower()
    if s in ("ok","warn","block","unknown"):
        return s
    if s in ("warning","blocked"):
        return "warn" if s=="warning" else "block"
    if s.isdigit():
        return { "0":"ok","1":"warn","2":"block","3":"unknown"}.get(s, "unknown")
    return "unknown"

def _win_label(r_mult: Optional[float], win_r_min: float) -> Optional[int]:
    if r_mult is None:
        return None
    return 1 if r_mult > win_r_min else 0

def _ece(probs: List[float], ys: List[int], bins: int) -> Optional[float]:
    n = len(probs)
    if n == 0:
        return None
    # clamp
    ps = [0.0 if p < 0 else 1.0 if p > 1 else p for p in probs]
    # bins equally spaced
    out = 0.0
    for b in range(bins):
        lo = b / bins
        hi = (b + 1) / bins
        idx = [i for i,p in enumerate(ps) if (p >= lo and (p < hi or (b==bins-1 and p<=hi)))]
        if not idx:
            continue
        p_avg = sum(ps[i] for i in idx) / len(idx)
        y_avg = sum(ys[i] for i in idx) / len(idx)
        out += (len(idx)/n) * abs(p_avg - y_avg)
    return out

def _precision_top_p(scores: List[float], ys: List[int], top_p: float) -> Optional[float]:
    n = len(scores)
    if n == 0:
        return None
    k = max(1, int(n * top_p))
    order = sorted(range(n), key=lambda i: scores[i], reverse=True)
    top = order[:k]
    return sum(ys[i] for i in top) / k

def _mean(xs: List[float]) -> Optional[float]:
    if not xs:
        return None
    return sum(xs)/len(xs)

def _extract_score_prob(d: Dict[str, Any], score_fields: List[str], prob_fields: List[str]) -> Tuple[Optional[float], Optional[float]]:
    s = None
    for k in score_fields:
        v = _as_float(_pick(d, k))
        if v is not None:
            s = v
            break
    p = None
    for k in prob_fields:
        v = _as_float(_pick(d, k))
        if v is not None:
            p = v
            break
    return s, p

def _write_hash(cli: redis.Redis, key: str, mapping: Dict[str, str]) -> None:
    if not mapping:
        return
    pipe = cli.pipeline(transaction=False)
    pipe.delete(key)
    pipe.hset(key, mapping=mapping)
    pipe.execute()

def compute_once() -> None:
    redis_url = _env("REDIS_URL", "redis://localhost:6379/0")
    stream = _env("TRADES_CLOSED_STREAM", "trades:closed")
    lookback_h = _env_float("SIGNAL_QUALITY_LOOKBACK_H", "24")
    max_scan = _env_int("SIGNAL_QUALITY_MAX_SCAN", "200000")
    min_n = _env_int("SIGNAL_QUALITY_MIN_N", "30")
    top_p = _env_float("SIGNAL_QUALITY_TOP_P", "0.05")
    ece_bins = _env_int("SIGNAL_QUALITY_ECE_BINS", "10")
    win_r_min = _env_float("SIGNAL_QUALITY_WIN_R_MIN", "0.0")
    max_groups = _env_int("SIGNAL_QUALITY_MAX_GROUPS", "200")
    top_reason_k = _env_int("SIGNAL_QUALITY_TOP_REASON_K", "30")

    dyn_cfg_key = _env("DYN_CFG_KEY", "settings:dynamic_cfg")

    score_fields = [s.strip() for s in _env("SIGNAL_QUALITY_SCORE_FIELDS", "ml_p_cal,ml_p,rule_score,score").split(",") if s.strip()]
    prob_fields  = [s.strip() for s in _env("SIGNAL_QUALITY_PROB_FIELDS",  "ml_p_cal,ml_p,p").split(",") if s.strip()]

    by_mode_key = _env("SIGNAL_QUALITY_BY_MODE_HASH", "metrics:signal_quality:24h:by_mode")
    by_bucket_key = _env("SIGNAL_QUALITY_BY_BUCKET_HASH", "metrics:signal_quality:24h:by_bucket")
    by_reason_key = _env("SIGNAL_QUALITY_BY_REASON_HASH", "metrics:signal_quality:24h:by_reason")

    cli = redis.Redis.from_url(redis_url, decode_responses=True)
    now_ms = _now_ms()
    min_ts = now_ms - int(lookback_h * 3600 * 1000)

    # scan tail
    rows = cli.xrevrange(stream, max="+", min="-", count=max_scan)

    # collectors
    g_scores: List[float] = []
    g_probs: List[float] = []
    g_ys: List[int] = []
    g_rs: List[float] = []
    last_ts = 0

    mode_bins: Dict[Tuple[str,str], Dict[str, Any]] = {}
    bucket_bins: Dict[Tuple[str,str], Dict[str, Any]] = {}
    reason_bins: Dict[str, Dict[str, Any]] = {}

    def _acc(binmap, key):
        if key not in binmap:
            binmap[key] = {"scores": [], "probs": [], "ys": [], "rs": [], "n": 0, "last_ts": 0}
        return binmap[key]

    for msg_id, fields in rows:
        d = _loads(fields.get("payload"))
        ts_ms = _as_float(_pick(d, "close_ts_ms", "ts_ms", "closed_ts_ms"))
        if ts_ms is None:
            # stream-id time
            try:
                ts_ms = float(str(msg_id).split("-")[0])
            except Exception:
                continue
        ts_ms_i = int(ts_ms)
        if ts_ms_i < min_ts:
            break
        last_ts = max(last_ts, ts_ms_i)

        r_mult = _as_float(_pick(d, "r_mult", "r"))
        y = _win_label(r_mult, win_r_min)
        if y is None:
            continue
        score, prob = _extract_score_prob(d, score_fields, prob_fields)
        if score is None:
            continue

        g_scores.append(score)
        g_ys.append(y)
        g_rs.append(r_mult if r_mult is not None else 0.0)
        if prob is not None:
            g_probs.append(prob)

        drift_mode = str(_pick(d, "drift_mode") or "unknown").strip().lower()
        dq_state = _state(_pick(d, "dq_state"))
        mb = _acc(mode_bins, (drift_mode, dq_state))
        mb["scores"].append(score); mb["ys"].append(y); mb["rs"].append(r_mult or 0.0); mb["n"] += 1; mb["last_ts"]=max(mb["last_ts"], ts_ms_i)
        if prob is not None: mb["probs"].append(prob)

        bucket = str(_pick(d, "meta_enforce_cov_bucket") or "na").strip()
        applied = str(_pick(d, "meta_enforce_applied") or "na").strip()
        bb = _acc(bucket_bins, (bucket, applied))
        bb["scores"].append(score); bb["ys"].append(y); bb["rs"].append(r_mult or 0.0); bb["n"] += 1; bb["last_ts"]=max(bb["last_ts"], ts_ms_i)
        if prob is not None: bb["probs"].append(prob)

        reason = str(_pick(d, "rule_reason_code_top1") or "na").strip()
        rb = _acc(reason_bins, reason)
        rb["scores"].append(score); rb["ys"].append(y); rb["rs"].append(r_mult or 0.0); rb["n"] += 1; rb["last_ts"]=max(rb["last_ts"], ts_ms_i)
        if prob is not None: rb["probs"].append(prob)

    n = len(g_scores)
    if n < min_n:
        _runs_total.labels(result="skip_n").inc()
        # still write last_ts for staleness checks
        cli.hset(dyn_cfg_key, mapping={"signal_quality_n_24h": str(n), "signal_quality_last_ts_ms": str(last_ts or 0)})
        return

    g_expect = _mean(g_rs)
    g_prec = _precision_top_p(g_scores, g_ys, top_p)
    # global ece needs aligned arrays; use probs only where present by recomputing paired lists
    # For simplicity: if prob count != n, do not compute ece globally (requires consistent pairing)
    g_ece = _ece(g_probs, g_ys[:len(g_probs)], ece_bins) if len(g_probs) == n else None

    # Write cfg2
    cfg_map = {
        "signal_quality_expectancy_r_24h": f"{g_expect:.6f}" if g_expect is not None else "",
        "signal_quality_precision_top5p_24h": f"{g_prec:.6f}" if g_prec is not None else "",
        "signal_quality_ece_24h": f"{g_ece:.6f}" if g_ece is not None else "",
        "signal_quality_n_24h": str(n),
        "signal_quality_last_ts_ms": str(last_ts),
    }
    cli.hset(dyn_cfg_key, mapping={k:v for k,v in cfg_map.items() if v != ""})

    # Breakdown helpers
    def _pack(binobj: Dict[str, Any]) -> Optional[str]:
        if binobj["n"] < min_n:
            return None
        exp = _mean(binobj["rs"])
        prec = _precision_top_p(binobj["scores"], binobj["ys"], top_p)
        ece = _ece(binobj["probs"], binobj["ys"][:len(binobj["probs"])], ece_bins) if len(binobj["probs"]) == binobj["n"] else None
        out = {
            "n": binobj["n"],
            "expectancy_r": exp,
            "precision_top5p": prec,
            "ece": ece,
            "last_ts_ms": binobj["last_ts"],
        }
        return json.dumps(out, ensure_ascii=False)

    # by_mode
    mode_items = sorted(mode_bins.items(), key=lambda kv: kv[1]["n"], reverse=True)[:max_groups]
    mode_map: Dict[str,str] = {}
    for (dm, dq), b in mode_items:
        v = _pack(b)
        if v:
            mode_map[f"mode={dm}|dq={dq}"] = v
    _write_hash(cli, by_mode_key, mode_map)

    # by_bucket (safe to keep all but cap anyway)
    bucket_items = sorted(bucket_bins.items(), key=lambda kv: kv[1]["n"], reverse=True)[:max_groups]
    bucket_map: Dict[str,str] = {}
    for (bucket, applied), b in bucket_items:
        v = _pack(b)
        if v:
            bucket_map[f"bucket={bucket}|applied={applied}"] = v
    _write_hash(cli, by_bucket_key, bucket_map)

    # by_reason (top-K only)
    reason_items = sorted(reason_bins.items(), key=lambda kv: kv[1]["n"], reverse=True)[:top_reason_k]
    reason_map: Dict[str,str] = {}
    for reason, b in reason_items:
        v = _pack(b)
        if v:
            reason_map[f"reason={reason}"] = v
    _write_hash(cli, by_reason_key, reason_map)

    _runs_total.labels(result="ok").inc()

def main() -> None:
    once = "--once" in os.sys.argv
    loop_s = _env_int("SIGNAL_QUALITY_LOOP_S", "900")
    if once:
        compute_once()
        return
    while True:
        try:
            compute_once()
        except Exception:
            _runs_total.labels(result="err").inc()
        time.sleep(loop_s)

if __name__ == "__main__":
    main()
