from __future__ import annotations

import argparse
import json
import os
from collections import Counter, defaultdict
from typing import Any

import redis

from utils.time_utils import get_ny_time_millis
import contextlib
from core.redis_keys import RedisStreams as RS


def _now_ms() -> int:
    """Get current timestamp in milliseconds."""
    return get_ny_time_millis()


def _f(x: Any, d: float = 0.0) -> float:
    """Safe float conversion with default value."""
    try:
        if x is None:
            return d
        return float(x)
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    """Safe int conversion with default value."""
    try:
        if x is None:
            return d
        return int(float(x))
    except Exception:
        return d


def pctl(xs: list[float], q: float) -> float:
    """
    Calculate percentile from sorted list.
    
    Args:
        xs: List of float values
        q: Quantile (0.0 to 1.0)
    
    Returns:
        Percentile value
    """
    if not xs:
        return 0.0
    xs = sorted(xs)
    i = int(round((len(xs) - 1) * q))
    i = max(0, min(len(xs) - 1, i))
    return float(xs[i])


def _read_stream_window(r: redis.Redis, stream: str, start_ms: int, window_ms: int, *, max_scan: int = 600000) -> list[dict[str, Any]]:
    """
    Read messages from Redis stream within time window.
    
    Scans stream backwards from latest, collecting messages within [start_ms, start_ms + window_ms].
    Stops early if timestamp goes below start_ms.
    
    Args:
        r: Redis client
        stream: Stream name
        start_ms: Start timestamp (ms)
        window_ms: Window size (ms)
        max_scan: Maximum messages to scan (safety limit)
    
    Returns:
        List of message dicts with _ts_ms field added, sorted by timestamp
    """
    end_ms = start_ms + window_ms
    rows: list[dict[str, Any]] = []
    last_id = "+"
    scanned = 0
    while scanned < max_scan:
        batch = r.xrevrange(stream, max=last_id, min="-", count=2000)
        if not batch:
            break
        if len(batch) == 1 and batch[0][0] == last_id:
            break
        for msg_id, fields in batch:
            scanned += 1
            if msg_id == last_id:
                continue
            last_id = msg_id
            d = dict(fields or {})
            ts = _i(d.get("ts_ms", d.get("ts", d.get("timestamp", 0))), 0)
            if ts <= 0:
                continue
            if ts < start_ms:
                scanned = max_scan
                break
            if ts <= end_ms:
                d["_ts_ms"] = ts
                rows.append(d)
        if len(batch) < 2000:
            break
    rows.sort(key=lambda x: int(x.get("_ts_ms", 0)))
    return rows


def _key_group(d: dict[str, Any]) -> str:
    """
    Generate stable low-cardinality grouping key for topdiff analysis.
    
    Groups by symbol and bucket (if available) or scenario.
    Format: "SYMBOL|BUCKET" or "SYMBOL|SCENARIO"
    """
    sym = (d.get("symbol", "") or "").upper() or "NA"
    sc = (d.get("scenario_v4", d.get("scenario", "")) or "") or "na"
    bucket = (d.get("bucket", "") or "")
    if bucket:
        return f"{sym}|{bucket}"
    return f"{sym}|{sc}"


def compute_snapshot(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Compute aggregated snapshot statistics from stream rows.
    
    Computes:
    - Overall metrics: p_edge, latency, allow/abstain/missing/error rates
    - Top status/model_run/kind counters
    - Per-group metrics (top 200 by volume) for drift detection
    
    Args:
        rows: List of message dicts from stream
    
    Returns:
        Snapshot dict with metrics and group summaries
    """
    n = len(rows)
    pedge: list[float] = []
    lat_ms: list[float] = []
    allow = 0
    abstain = 0
    miss = 0
    err = 0
    conf: list[float] = []
    status = Counter()
    model_run = Counter()
    kind = Counter()

    # group stats for topdiff
    g_n = defaultdict(int)
    g_pedge = defaultdict(list)
    g_allow = defaultdict(int)
    g_abst = defaultdict(int)
    g_lat = defaultdict(list)

    for d in rows:
        pe = _f(d.get("p_edge", 0.0), 0.0)
        pedge.append(pe)
        if (d.get("latency_ms", "") or "").strip() != "":
            lm = _f(d.get("latency_ms", 0.0), 0.0)
        else:
            lm = _f(d.get("latency_us", 0.0), 0.0) / 1000.0
        lat_ms.append(lm)

        al = 1 if _i(d.get("allow", 0), 0) == 1 else 0
        ab = 1 if _i(d.get("abstain", 0), 0) == 1 else 0
        allow += al
        abstain += ab

        st = (d.get("status", "") or "").upper()
        status[st or ""] += 1

        miss_flag = _i(d.get("missing", d.get("missing_n", 0)), 0) > 0 or st.startswith("MISSING")
        miss += 1 if miss_flag else 0

        err_s = (d.get("err", d.get("error", "")) or "").strip()
        err += 1 if err_s != "" else 0

        if (d.get("conf", "") or "").strip() != "":
            conf.append(_f(d.get("conf", 0.0), 0.0))

        mr = (d.get("model_run_id", "") or "")
        if mr:
            model_run[mr] += 1
        kd = (d.get("kind", "") or "")
        if kd:
            kind[kd] += 1

        gk = _key_group(d)
        g_n[gk] += 1
        g_pedge[gk].append(pe)
        g_allow[gk] += al
        g_abst[gk] += ab
        g_lat[gk].append(lm)

    snap = {
        "ts_ms": _now_ms(),
        "n": int(n),
        "p_edge": {"p10": pctl(pedge, 0.10), "p50": pctl(pedge, 0.50), "p90": pctl(pedge, 0.90)},
        "latency_ms": {"p50": pctl(lat_ms, 0.50), "p95": pctl(lat_ms, 0.95), "p99": pctl(lat_ms, 0.99)},
        "allow_rate": float(allow / n) if n > 0 else 0.0,
        "abstain_rate": float(abstain / n) if n > 0 else 0.0,
        "missing_rate": float(miss / n) if n > 0 else 0.0,
        "err_rate": float(err / n) if n > 0 else 0.0,
        "conf": {"p10": pctl(conf, 0.10), "p50": pctl(conf, 0.50)} if conf else {"p10": 0.0, "p50": 0.0},
        "status_top": status.most_common(12),
        "model_run_top": model_run.most_common(5),
        "kind_top": kind.most_common(5),
    }

    # group summary for topdiff (keep top 200 by volume)
    g_items = sorted(g_n.items(), key=lambda kv: kv[1], reverse=True)[:200]
    groups = {}
    for k, gn in g_items:
        groups[k] = {
            "n": int(gn),
            "p50": pctl(g_pedge[k], 0.50),
            "allow_rate": float(g_allow[k] / gn) if gn > 0 else 0.0,
            "abstain_rate": float(g_abst[k] / gn) if gn > 0 else 0.0,
            "lat_p99": pctl(g_lat[k], 0.99),
        }
    snap["groups"] = groups
    return snap


def diff_snapshot(base: dict[str, Any], cur: dict[str, Any], *, topk: int = 20) -> dict[str, Any]:
    """
    Compute difference between baseline and candidate snapshots.
    
    Computes:
    - Core deltas: p50, allow_rate, missing_rate, error_rate, latency_p99, conf_p50
    - Top-k group diffs sorted by score (abs(dp50) + 0.5*abs(dallow) + 0.1*abs(dlat_p99))
    
    Args:
        base: Baseline snapshot dict
        cur: Current/candidate snapshot dict
        topk: Number of top group diffs to return
    
    Returns:
        Diff dict with core deltas and topdiff groups
    """
    def g(base_path: list[str], dflt: float = 0.0) -> float:
        """Get nested value from base dict by path."""
        x: Any = base
        for p in base_path:
            if not isinstance(x, dict) or p not in x:
                return dflt
            x = x[p]
        return float(x) if x is not None else dflt

    def h(cur_path: list[str], dflt: float = 0.0) -> float:
        """Get nested value from cur dict by path."""
        x: Any = cur
        for p in cur_path:
            if not isinstance(x, dict) or p not in x:
                return dflt
            x = x[p]
        return float(x) if x is not None else dflt

    core = {
        "n_base": int(base.get("n", 0) or 0),
        "n_cur": int(cur.get("n", 0) or 0),
        "delta_p50": h(["p_edge", "p50"]) - g(["p_edge", "p50"]),
        "delta_p10": h(["p_edge", "p10"]) - g(["p_edge", "p10"]),
        "delta_allow": h(["allow_rate"]) - g(["allow_rate"]),
        "delta_abstain": h(["abstain_rate"]) - g(["abstain_rate"]),
        "delta_missing": h(["missing_rate"]) - g(["missing_rate"]),
        "delta_err": h(["err_rate"]) - g(["err_rate"]),
        "delta_lat_p99": h(["latency_ms", "p99"]) - g(["latency_ms", "p99"]),
        "delta_conf_p50": h(["conf", "p50"]) - g(["conf", "p50"]),
    }

    # topdiff by group (abs delta p50 + abs delta allow)
    bgs = base.get("groups", {}) if isinstance(base.get("groups", {}), dict) else {}
    cgs = cur.get("groups", {}) if isinstance(cur.get("groups", {}), dict) else {}
    keys = set(bgs.keys()) | set(cgs.keys())
    diffs = []
    for k in keys:
        b = bgs.get(k, {}) if isinstance(bgs.get(k, {}), dict) else {}
        c = cgs.get(k, {}) if isinstance(cgs.get(k, {}), dict) else {}
        bn = int(b.get("n", 0) or 0)
        cn = int(c.get("n", 0) or 0)
        if bn < 50 and cn < 50:
            continue
        dp = float(c.get("p50", 0.0) or 0.0) - float(b.get("p50", 0.0) or 0.0)
        da = float(c.get("allow_rate", 0.0) or 0.0) - float(b.get("allow_rate", 0.0) or 0.0)
        dl = float(c.get("lat_p99", 0.0) or 0.0) - float(b.get("lat_p99", 0.0) or 0.0)
        score = abs(dp) + 0.5 * abs(da) + 0.1 * abs(dl)
        diffs.append((score, k, {"dp50": dp, "dallow": da, "dlat_p99": dl, "n_base": bn, "n_cur": cn}))
    diffs.sort(key=lambda x: x[0], reverse=True)
    top = [{"key": k, **v} for _, k, v in diffs[:topk]]

    return {"core": core, "topdiff": top}


def main() -> None:
    """
    ML Golden Snapshot: baseline vs candidate drift detection.
    
    Reads metrics:ml_confirm stream for last N hours, computes snapshot,
    compares with baseline, detects drift, optionally notifies and fails.
    
    Modes:
    - --write-baseline 1: Write current snapshot as baseline and exit
    - Normal: Compare candidate with baseline, detect drift, notify if enabled
    
    Drift thresholds (tunable via ENV/args):
    - max_abs_dp50: Max absolute delta in p_edge p50 (default 0.03)
    - max_abs_dallow: Max absolute delta in allow_rate (default 0.10)
    - max_dlat_p99: Max increase in latency p99 ms (default 2.5)
    - max_dmissing: Max increase in missing_rate (default 0.02)
    - max_derr: Max increase in error_rate (default 0.01)
    
    Exits with code 2 if drift detected and --fail-on-drift=1.
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--stream", default=os.getenv("ML_CONFIRM_METRICS_STREAM", "metrics:ml_confirm"))
    ap.add_argument("--window-hours", type=float, default=float(os.getenv("ML_GOLDEN_WINDOW_HOURS", "24")))
    ap.add_argument("--baseline", default=os.getenv("ML_GOLDEN_BASELINE", "/var/lib/trade/ml_golden/baseline.json"))
    ap.add_argument("--out-dir", default=os.getenv("ML_GOLDEN_OUT_DIR", "/var/lib/trade/ml_golden"))
    ap.add_argument("--write-baseline", type=int, default=int(os.getenv("ML_GOLDEN_WRITE_BASELINE", "0")))
    ap.add_argument("--fail-on-drift", type=int, default=int(os.getenv("ML_GOLDEN_FAIL_ON_DRIFT", "1")))
    ap.add_argument("--notify", type=int, default=int(os.getenv("ML_GOLDEN_NOTIFY", "1")))
    ap.add_argument("--notify-stream", default=os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM))

    # drift thresholds (core)
    ap.add_argument("--min-n", type=int, default=int(os.getenv("ML_GOLDEN_MIN_N", "500")))
    ap.add_argument("--max-abs-dp50", type=float, default=float(os.getenv("ML_GOLDEN_MAX_ABS_DP50", "0.03")))
    ap.add_argument("--max-abs-dallow", type=float, default=float(os.getenv("ML_GOLDEN_MAX_ABS_DALLOW", "0.10")))
    ap.add_argument("--max-dlat-p99", type=float, default=float(os.getenv("ML_GOLDEN_MAX_DLAT_P99", "2.5")))
    ap.add_argument("--max-dmissing", type=float, default=float(os.getenv("ML_GOLDEN_MAX_DMISSING", "0.02")))
    ap.add_argument("--max-derr", type=float, default=float(os.getenv("ML_GOLDEN_MAX_DERR", "0.01")))
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    window_ms = int(float(args.window_hours) * 3600_000)
    start_ms = _now_ms() - window_ms
    rows = _read_stream_window(r, args.stream, start_ms, window_ms)

    cur = compute_snapshot(rows)
    cur_path = os.path.join(args.out_dir, f"candidate_{_now_ms()}.json")
    with open(cur_path, "w", encoding="utf-8") as f:
        json.dump(cur, f, ensure_ascii=False, indent=2)

    # if requested: write baseline and exit
    if int(args.write_baseline) == 1 or not os.path.exists(args.baseline):
        os.makedirs(os.path.dirname(args.baseline) or ".", exist_ok=True)
        with open(args.baseline, "w", encoding="utf-8") as f:
            json.dump(cur, f, ensure_ascii=False, indent=2)
        print(json.dumps({"ok": True, "mode": "baseline_written", "baseline": args.baseline, "candidate": cur_path}, ensure_ascii=False, indent=2))
        return

    with open(args.baseline, encoding="utf-8") as f:
        base = json.load(f)

    diff = diff_snapshot(base, cur, topk=20)
    diff_path = os.path.join(args.out_dir, f"diff_{_now_ms()}.json")
    with open(diff_path, "w", encoding="utf-8") as f:
        json.dump(diff, f, ensure_ascii=False, indent=2)

    # drift decision
    core = diff.get("core", {}) if isinstance(diff.get("core", {}), dict) else {}
    n_cur = int(cur.get("n", 0) or 0)
    fail = False
    reasons = []
    if n_cur < int(args.min_n):
        # not enough data -> do not fail
        pass
    else:
        if abs(float(core.get("delta_p50", 0.0) or 0.0)) > float(args.max_abs_dp50):
            fail = True; reasons.append("abs(dp50) too high")
        if abs(float(core.get("delta_allow", 0.0) or 0.0)) > float(args.max_abs_dallow):
            fail = True; reasons.append("abs(dallow) too high")
        if float(core.get("delta_lat_p99", 0.0) or 0.0) > float(args.max_dlat_p99):
            fail = True; reasons.append("dlat_p99 too high")
        if float(core.get("delta_missing", 0.0) or 0.0) > float(args.max_dmissing):
            fail = True; reasons.append("dmissing too high")
        if float(core.get("delta_err", 0.0) or 0.0) > float(args.max_derr):
            fail = True; reasons.append("derr too high")

    if int(args.notify) == 1 and fail:
        msg = (
            f"ML GOLDEN drift (last {args.window_hours}h) n={n_cur}\n"
            f"dp50={float(core.get('delta_p50',0.0) or 0.0):+.4f} "
            f"dallow={float(core.get('delta_allow',0.0) or 0.0):+.4f} "
            f"dlat_p99={float(core.get('delta_lat_p99',0.0) or 0.0):+.2f}ms "
            f"dmiss={float(core.get('delta_missing',0.0) or 0.0):+.4f} "
            f"derr={float(core.get('delta_err',0.0) or 0.0):+.4f}\n"
            f"reasons: {', '.join(reasons)}\n"
            f"topdiff: {diff.get('topdiff', [])[:5]}"
        )
        import html
        safe_msg = html.escape(msg)
        with contextlib.suppress(Exception):
            r.xadd(args.notify_stream, {"type": "report", "subtype": "ml_golden", "ts_ms": str(_now_ms()), "text": safe_msg}, maxlen=200000, approximate=True)

    out = {"ok": True, "candidate": cur_path, "diff": diff_path, "fail": bool(fail), "reasons": reasons}
    print(json.dumps(out, ensure_ascii=False, indent=2))
    if int(args.fail_on_drift) == 1 and fail:
        raise SystemExit(2)


if __name__ == "__main__":
    main()

