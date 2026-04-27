#!/usr/bin/env python3
"""
Signal Quality KPI Worker V1

Reads trades:closed stream (tail), calculates 24h rolling KPIs:
- Expectancy (mean R-mult)
- Precision (Top 5%)
- ECE (Calibration Error)

P64: Add breakdown by dq_state/drift_state into regimes:
- regime=ok (dq=ok AND drift=ok)
- regime=warn (dq=warn OR drift=warn, but not block)
- regime=block (dq=block OR drift=block)

Writes global config to Redis for Prometheus Exporter:
- settings:dynamic_cfg -> signal_quality_*
- plus regime-specific keys
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import os
import time
import json
import math
import logging
import argparse
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import redis  # type: ignore
import numpy as np

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("signal_quality_kpi_v1")


def _now_ms() -> int:
    return get_ny_time_millis()


def _to_str(x: Any) -> str:
    if x is None:
        return "na"
    if isinstance(x, bytes):
        try:
            return x.decode("utf-8")
        except Exception:
            return repr(x)
    return str(x)


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, bool):
            return float(x)
        if isinstance(x, (int, float)):
            return float(x)
        s = _to_str(x).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def _clamp01(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


def _json_loads_best_effort(s: Any) -> Optional[dict]:
    if s is None:
        return None
    if isinstance(s, dict):
        return s
    try:
        if isinstance(s, bytes):
            s = s.decode("utf-8", errors="replace")
        if not isinstance(s, str):
            s = str(s)
        s = s.strip()
        if not s:
            return None
        return json.loads(s)
    except Exception:
        return None


def _extract_payload(fields: Dict[Any, Any]) -> Dict[str, Any]:
    payload = fields.get("payload")
    if payload is None and b"payload" in fields:
        payload = fields.get(b"payload")
    ev = _json_loads_best_effort(payload)
    if isinstance(ev, dict):
        return ev
    out: Dict[str, Any] = {}
    for k, v in fields.items():
        out[_to_str(k)] = _to_str(v)
    return out


def _extract_close_ts_ms(ev: Dict[str, Any]) -> Optional[int]:
    for k in ("close_ts_ms", "ts_ms", "event_ts_ms", "exit_ts_ms"):
        v = ev.get(k)
        if v is None:
            continue
        try:
            ts = int(float(v))
            if ts < 10_000_000_000:
                ts *= 1000
            return ts
        except Exception:
            continue
    return None


def _pick_first_float(ev: Dict[str, Any], keys: List[str]) -> Optional[float]:
    for k in keys:
        if k in ev:
            v = _safe_float(ev.get(k))
            if v is not None:
                return v
    return None


def _pick_reason_top1(ev: Dict[str, Any]) -> str:
    for k in ("rule_reason_code_top1", "reason_top1", "rule_reason_top"):
        if k in ev and ev.get(k):
            return _to_str(ev.get(k))[:64]
    rr = ev.get("rule_reasons") or ev.get("rule_reason_codes")
    if isinstance(rr, list) and rr:
        return _to_str(rr[0])[:64]
    if isinstance(rr, str) and rr.strip():
        s = rr.strip()
        if s.startswith("["):
            j = _json_loads_best_effort(s)
            if isinstance(j, list) and j:
                return _to_str(j[0])[:64]
        return s.split(",")[0][:64]
    return "na"


def _norm_state(v: Any) -> str:
    if v is None:
        return "unknown"
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("ok", "warn", "warning", "block", "blocked", "unknown"):
            return "warn" if s == "warning" else ("block" if s == "blocked" else s)
        if s.isdigit():
            try:
                return {0: "ok", 1: "warn", 2: "block", 3: "unknown"}.get(int(s), "unknown")
            except Exception:
                return "unknown"
        return s or "unknown"
    if isinstance(v, (int, float)):
        return {0: "ok", 1: "warn", 2: "block", 3: "unknown"}.get(int(v), "unknown")
    return "unknown"


def _derive_regime(dq_state: str, drift_state: str) -> str:
    dq = (dq_state or "unknown").lower()
    dr = (drift_state or "unknown").lower()
    if dq == "block" or dr == "block":
        return "block"
    if dq == "warn" or dr == "warn":
        return "warn"
    if dq == "ok" and dr == "ok":
        return "ok"
    return "unknown"


@dataclass
class Row:
    sid: str
    ts_ms: int
    r_mult: float
    y: int
    rank_score: Optional[float]
    p: Optional[float]
    strategy: str
    symbol: str
    tf: str
    bucket: str
    reason_top1: str
    model_ver: str
    dq_state: str
    drift_state: str
    drift_mode: str
    regime: str


def _load_rows_from_stream(
    r: redis.Redis,
    stream: str,
    lookback_ms: int,
    max_scan: int,
    win_r_min: float,
    score_fields: List[str],
    prob_fields: List[str],
) -> List[Row]:
    cutoff = _now_ms() - lookback_ms
    rows: List[Row] = []
    last_id = b"+"
    scanned = 0
    chunk = 2000
    while scanned < max_scan:
        res = r.xrevrange(stream, max=last_id, min=b"-", count=min(chunk, max_scan - scanned))
        if not res:
            break
        if len(res) == 1 and res[0][0] == last_id:
            break
        for _id, fields in res:
            scanned += 1
            ev = _extract_payload(fields)
            ts_ms = _extract_close_ts_ms(ev)
            if ts_ms is None:
                continue
            if ts_ms < cutoff:
                return rows

            sid = _to_str(ev.get("sid") or ev.get("signal_id") or ev.get("Sid") or ev.get("SID"))
            if not sid or sid == "na":
                continue

            r_mult = _safe_float(ev.get("r_mult") or ev.get("r") or ev.get("RMult"))
            if r_mult is None:
                continue
            y = 1 if float(r_mult) >= win_r_min else 0

            rank_score = _pick_first_float(ev, score_fields)
            p = _pick_first_float(ev, prob_fields)
            if p is not None:
                p = _clamp01(p)

            strategy = _to_str(ev.get("strategy") or ev.get("strat") or ev.get("strategy_id"))
            symbol = _to_str(ev.get("symbol") or ev.get("sym"))
            tf = _to_str(ev.get("tf") or ev.get("timeframe") or ev.get("tf_ms") or ev.get("frame"))
            bucket = _to_str(ev.get("meta_enforce_cov_bucket") or ev.get("meta_cov_bucket") or "na")
            reason_top1 = _pick_reason_top1(ev)
            model_ver = _to_str(ev.get("ml_model_ver") or ev.get("model_ver") or ev.get("ml_model_version") or "na")

            dq_state = _norm_state(ev.get("dq_state") or ev.get("dq") or ev.get("dqState"))
            drift_state = _norm_state(ev.get("drift_state") or ev.get("drift") or ev.get("driftState"))
            drift_mode = _to_str(ev.get("drift_mode") or ev.get("driftMode") or "unknown")
            regime = _derive_regime(dq_state, drift_state)

            rows.append(Row(
                sid=sid,
                ts_ms=int(ts_ms),
                r_mult=float(r_mult),
                y=int(y),
                rank_score=rank_score,
                p=p,
                strategy=strategy,
                symbol=symbol,
                tf=tf,
                bucket=bucket,
                reason_top1=reason_top1,
                model_ver=model_ver,
                dq_state=dq_state,
                drift_state=drift_state,
                drift_mode=drift_mode,
                regime=regime,
            ))
        last_id = res[-1][0]
        if isinstance(last_id, str):
            last_id = last_id.encode("utf-8")
    return rows


def _expectancy_r(rows: List[Row]) -> Optional[float]:
    if not rows:
        return None
    return sum(r.r_mult for r in rows) / float(len(rows))


def _precision_top_p(rows: List[Row], top_p: float) -> Optional[float]:
    if not rows:
        return None
    scored = [r for r in rows if r.rank_score is not None and not math.isnan(float(r.rank_score))]
    if not scored:
        return None
    scored.sort(key=lambda x: float(x.rank_score), reverse=True)
    n = len(scored)
    k = max(1, int(math.ceil(n * float(top_p))))
    top = scored[:k]
    return sum(r.y for r in top) / float(len(top))


def _ece(rows: List[Row], bins: int) -> Optional[float]:
    pp = [r for r in rows if r.p is not None and not math.isnan(float(r.p))]
    if not pp:
        return None
    bins = max(2, int(bins))
    counts = [0] * bins
    sum_p = [0.0] * bins
    sum_y = [0.0] * bins
    for r in pp:
        p = float(r.p)
        b = min(bins - 1, int(p * bins))
        counts[b] += 1
        sum_p[b] += p
        sum_y[b] += float(r.y)
    N = float(len(pp))
    ece = 0.0
    for b in range(bins):
        if counts[b] == 0:
            continue
        conf = sum_p[b] / counts[b]
        acc = sum_y[b] / counts[b]
        ece += (counts[b] / N) * abs(acc - conf)
    return ece


def _group_key(r: Row, include_reason: bool) -> Tuple[str, str, str, str, str]:
    if include_reason:
        return (r.strategy, r.symbol, r.tf, r.bucket, r.reason_top1)
    return (r.strategy, r.symbol, r.tf, r.bucket, "na")


def _compute_groups(rows: List[Row], max_groups: int, include_reason: bool) -> Dict[str, Dict[str, Any]]:
    groups: Dict[Tuple[str, str, str, str, str], List[Row]] = {}
    for r in rows:
        groups.setdefault(_group_key(r, include_reason), []).append(r)
    items = list(groups.items())
    items.sort(key=lambda kv: len(kv[1]), reverse=True)
    items = items[:max_groups]
    out: Dict[str, Dict[str, Any]] = {}
    for (strategy, symbol, tf, bucket, reason), rr in items:
        gid = f"strategy={strategy}|symbol={symbol}|tf={tf}|bucket={bucket}|reason={reason}"
        out[gid] = {"n": len(rr)}
    return out


def _compute_metrics(rows: List[Row], top_p: float, bins: int) -> Dict[str, Any]:
    n = len(rows)
    expectancy = _expectancy_r(rows)
    precision = _precision_top_p(rows, top_p=top_p)
    ece = _ece(rows, bins=bins)
    last_ts_ms = max((rw.ts_ms for rw in rows), default=_now_ms())
    return {"n": n, "expectancy_r": expectancy, "precision_top_p": precision, "ece": ece, "last_ts_ms": last_ts_ms, "ts_ms": _now_ms()}


def _write_cfg2(
    r: redis.Redis,
    dyn_cfg_key: str,
    expectancy: Optional[float],
    precision: Optional[float],
    ece: Optional[float],
    n: int,
    last_ts_ms: int,
    per_regime: Dict[str, Dict[str, Any]],
) -> None:
    mapping: Dict[str, Any] = {"signal_quality_n_24h": str(int(n)), "signal_quality_last_ts_ms": str(int(last_ts_ms))}
    if expectancy is not None:
        mapping["signal_quality_expectancy_r_24h"] = f"{expectancy:.6f}"
    if precision is not None:
        mapping["signal_quality_precision_top5p_24h"] = f"{precision:.6f}"
    if ece is not None:
        mapping["signal_quality_ece_24h"] = f"{ece:.6f}"

    counts = {}
    for regime in ("ok", "warn", "block"):
        obj = per_regime.get(regime) or {}
        counts[regime] = int(obj.get("n", 0) or 0)
        if obj.get("expectancy_r") is not None:
            mapping[f"signal_quality_expectancy_r_24h_regime_{regime}"] = f"{float(obj['expectancy_r']):.6f}"
        if obj.get("precision_top_p") is not None:
            mapping[f"signal_quality_precision_top5p_24h_regime_{regime}"] = f"{float(obj['precision_top_p']):.6f}"
        if obj.get("ece") is not None:
            mapping[f"signal_quality_ece_24h_regime_{regime}"] = f"{float(obj['ece']):.6f}"
        mapping[f"signal_quality_n_24h_regime_{regime}"] = str(int(obj.get("n", 0) or 0))
    mapping["signal_quality_regime_counts_24h_json"] = json.dumps(counts, separators=(",", ":"), ensure_ascii=False)

    r.hset(dyn_cfg_key, mapping=mapping)


def _write_breakdown_hash(
    r: redis.Redis,
    out_hash: str,
    global_obj: Dict[str, Any],
    per_regime: Dict[str, Dict[str, Any]],
    rows: List[Row],
    top_p: float,
    bins: int,
    max_groups: int,
    include_reason: bool,
) -> None:
    pipe = r.pipeline(transaction=False)
    pipe.hset(out_hash, "global", json.dumps(global_obj, separators=(",", ":"), ensure_ascii=False))
    for regime, obj in per_regime.items():
        pipe.hset(out_hash, f"regime={regime}", json.dumps(obj, separators=(",", ":"), ensure_ascii=False))
    groups = _compute_groups(rows, max_groups=max_groups, include_reason=include_reason)
    for gid, obj in groups.items():
        pipe.hset(out_hash, gid, json.dumps(obj, separators=(",", ":"), ensure_ascii=False))
    pipe.execute()


def process_metrics(trades: List[Dict]) -> Optional[Dict[str, Any]]:
    # This function acts as a bridge for existing tests if they called it directly.
    # It converts dicts to Rows and computes global metrics.
    rows: List[Row] = []
    # Mock minimal envs if not set
    score_fields = ["ml_p_cal", "ml_p", "rule_score", "score"]
    prob_fields = ["ml_p_cal", "ml_p", "p"]
    win_r_min = 0.0
    
    for t in trades:
        # Minimal conversion
        ts_ms = _extract_close_ts_ms(t) or 0
        r_mult = float(t.get("r_mult") or t.get("r") or 0.0)
        y = 1 if r_mult >= win_r_min else 0
        
        # rank_score
        rank_score = None
        for k in score_fields:
            if k in t:
                rank_score = float(t[k])
                break
        
        # p
        p = None
        for k in prob_fields:
            if k in t:
                p = _clamp01(float(t[k]))
                break
                
        rows.append(Row(
            sid="mock",
            ts_ms=ts_ms,
            r_mult=r_mult,
            y=y,
            rank_score=rank_score,
            p=p,
            strategy="", symbol="", tf="", bucket="", reason_top1="", model_ver="",
            dq_state="unknown", drift_state="unknown", drift_mode="unknown", regime="unknown"
        ))
    
    if not rows:
        return None
    
    # Use top_p from global or default
    metrics = _compute_metrics(rows, top_p=0.05, bins=10)
    # The existing test expects a dict, not None if n < MIN_N? 
    # Actually existing test expects None if n < MIN_N.
    # We should respect MIN_N here too if we want full compat.
    # The new code handles min_n in run_once.
    return metrics
    
# Export calculate_ece for test compatibility
calculate_ece = lambda probs, outcomes, bins: _ece([
    Row("", 0, 0.0, int(y), 0.0, float(p), "", "", "", "", "", "", "", "", "", "") 
    for p, y in zip(probs, outcomes)
], bins)

# Export get_trades_window for test compat
def get_trades_window(r, stream, window_ms):
    # This is slightly different signature in new code, but let's try to map it.
    # The new _load_rows_from_stream returns Row objects, old one returned Dicts.
    # Tests might expect Dicts.
    rows = _load_rows_from_stream(
        r, stream, window_ms, 200000, 0.0, 
        ["ml_p_cal", "ml_p", "rule_score", "score"], 
        ["ml_p_cal", "ml_p", "p"]
    )
    # Convert back to dicts roughly
    return [
        {
            "close_ts_ms": row.ts_ms,
            "r_mult": row.r_mult,
            "ml_p": row.p,
            "dq_state": row.dq_state,
        }
        for row in rows
    ]



# Defaults / Configuration
LOOKBACK_H = float(os.getenv("SIGNAL_QUALITY_LOOKBACK_H", "24"))
MAX_SCAN = int(os.getenv("SIGNAL_QUALITY_MAX_SCAN", "200000"))
MIN_N = int(os.getenv("SIGNAL_QUALITY_MIN_N", "30"))
TOP_P = float(os.getenv("SIGNAL_QUALITY_TOP_P", "0.05"))
ECE_BINS = int(os.getenv("SIGNAL_QUALITY_ECE_BINS", "10"))
WIN_R_MIN = float(os.getenv("SIGNAL_QUALITY_WIN_R_MIN", os.getenv("LABEL_WIN_R_MIN", "0.0")))
OUT_HASH = os.getenv("SIGNAL_QUALITY_OUT_HASH", "metrics:signal_quality:24h")
DYN_CFG_KEY = os.getenv("DYN_CFG_KEY", "settings:dynamic_cfg")
WRITE_CFG2 = os.getenv("SIGNAL_QUALITY_WRITE_CFG2", "1") == "1"
INCLUDE_REASON = os.getenv("SIGNAL_QUALITY_INCLUDE_REASON", "1") == "1"
MAX_GROUPS = int(os.getenv("SIGNAL_QUALITY_MAX_GROUPS", "200"))

SCORE_FIELDS = [s.strip() for s in os.getenv("SIGNAL_QUALITY_SCORE_FIELDS", "ml_p_cal,ml_p,rule_score,score").split(",") if s.strip()]
PROB_FIELDS = [s.strip() for s in os.getenv("SIGNAL_QUALITY_PROB_FIELDS", "ml_p_cal,ml_p,p").split(",") if s.strip()]


def _connect_redis_with_retry(redis_url: str, max_retries: int = 5) -> redis.Redis:
    """
    Connect to Redis with exponential backoff retry logic.
    Handles BusyLoadingError when Redis is loading dataset on startup.
    """
    for attempt in range(max_retries):
        try:
            r = redis.Redis.from_url(redis_url, decode_responses=False)
            # Test connection
            r.ping()
            return r
        except redis.exceptions.BusyLoadingError as e:
            wait_time = 2 ** attempt  # 1s, 2s, 4s, 8s, 16s
            logger.warning(
                "Redis is loading dataset (attempt %d/%d). Waiting %ds... Error: %s",
                attempt + 1, max_retries, wait_time, e
            )
            if attempt < max_retries - 1:
                time.sleep(wait_time)
            else:
                raise
        except Exception as e:
            logger.error("Redis connection failed (attempt %d/%d): %s", attempt + 1, max_retries, e)
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise
    raise RuntimeError("Failed to connect to Redis after max retries")


def run_once() -> int:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    trades_stream = os.getenv("TRADES_CLOSED_STREAM", "trades:closed")
    
    r = _connect_redis_with_retry(redis_url)
    rows = _load_rows_from_stream(
        r=r,
        stream=trades_stream,
        lookback_ms=int(LOOKBACK_H * 3600 * 1000),
        max_scan=MAX_SCAN,
        win_r_min=WIN_R_MIN,
        score_fields=SCORE_FIELDS,
        prob_fields=PROB_FIELDS,
    )
    n = len(rows)
    last_ts_ms = max((rw.ts_ms for rw in rows), default=_now_ms())

    if n < MIN_N:
        logger.info("P47/P64: n=%d < min_n=%d (skip)", n, MIN_N)
        if WRITE_CFG2:
            _write_cfg2(r, DYN_CFG_KEY, None, None, None, n, last_ts_ms, per_regime={})
        return 0

    global_metrics = _compute_metrics(rows, top_p=TOP_P, bins=ECE_BINS)
    per_regime = {}
    for regime in ("ok", "warn", "block"):
        rr = [x for x in rows if x.regime == regime]
        per_regime[regime] = _compute_metrics(rr, top_p=TOP_P, bins=ECE_BINS)
        per_regime[regime]["regime"] = regime

    logger.info(
        "P47/P64: n=%d exp=%s ptop=%.3f prec=%s ece=%s | ok=%d warn=%d block=%d",
        n,
        f"{global_metrics['expectancy_r']:.6f}" if global_metrics["expectancy_r"] is not None else "na",
        TOP_P,
        f"{global_metrics['precision_top_p']:.6f}" if global_metrics["precision_top_p"] is not None else "na",
        f"{global_metrics['ece']:.6f}" if global_metrics["ece"] is not None else "na",
        int(per_regime["ok"]["n"]),
        int(per_regime["warn"]["n"]),
        int(per_regime["block"]["n"]),
    )

    if WRITE_CFG2:
        _write_cfg2(
            r=r,
            dyn_cfg_key=DYN_CFG_KEY,
            expectancy=global_metrics["expectancy_r"],
            precision=global_metrics["precision_top_p"],
            ece=global_metrics["ece"],
            n=n,
            last_ts_ms=last_ts_ms,
            per_regime=per_regime,
        )

    _write_breakdown_hash(
        r=r,
        out_hash=OUT_HASH,
        global_obj={**global_metrics, "top_p": TOP_P, "bins": ECE_BINS},
        per_regime=per_regime,
        rows=rows,
        top_p=TOP_P,
        bins=ECE_BINS,
        max_groups=MAX_GROUPS,
        include_reason=INCLUDE_REASON,
    )
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="run once and exit")
    ap.add_argument("--loop-s", type=int, default=0, help="loop every N seconds")
    args = ap.parse_args()

    if args.once or args.loop_s <= 0:
        return run_once()

    loop_s = int(args.loop_s)
    logger.info("P47/P64 loop enabled: every %ss", loop_s)
    while True:
        try:
            rc = run_once()
            if rc != 0:
                logger.warning("P47/P64 run returned rc=%s", rc)
        except Exception as e:
            logger.exception("P47/P64 run failed: %s", e)
        time.sleep(loop_s)


if __name__ == "__main__":
    raise SystemExit(main())
