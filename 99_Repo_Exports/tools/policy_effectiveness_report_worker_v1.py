#!/usr/bin/env python3
"""tools/policy_effectiveness_report_worker_v1.py

P71: Build a 24h rolling "policy effectiveness report" (CSV/JSON) to calibrate policy thresholds safely.

What it does:
- Scans `trades:closed` (last N hours, default 24h).
- Groups trades by `policy_effective_mode` (ok/warn/block/unknown).
- Computes per-mode:
  - n_24h, share_24h
  - expectancy_r_24h (mean R multiple)
  - precision_top5p_24h (win-rate among top P% by score)
  - ece_24h (calibration error for win probability)

- Computes deltas vs ok baseline:
  - delta_expectancy_r_24h_{mode} = expectancy_r(mode) - expectancy_r(ok)
  - delta_precision_top5p_24h_{mode} = precision(mode) - precision(ok)
  - delta_ece_24h_{mode} = ece(mode) - ece(ok)  (positive => worse)

Outputs:
- Writes a compact numeric snapshot into cfg2 (`settings:dynamic_cfg`) for Prometheus export:
  - policy_effectiveness_last_ts_ms
  - policy_effectiveness_baseline_ok_present
  - policy_effectiveness_share_24h_{ok|warn|block|unknown}
  - policy_effectiveness_expectancy_r_delta_24h_{ok|warn|block|unknown}
  - policy_effectiveness_precision_top5p_delta_24h_{ok|warn|block|unknown}
  - policy_effectiveness_ece_delta_24h_{ok|warn|block|unknown}
  - policy_effectiveness_n_24h_{ok|warn|block|unknown}
  - policy_effectiveness_report_key (Redis key with JSON)

- Writes full report JSON and CSV into Redis string keys:
  - reports:policy_effectiveness:p71:last_json
  - reports:policy_effectiveness:p71:last_csv

ENV:
  REDIS_URL (default redis://redis-worker-1:6379/0)
  DYN_CFG_KEY (default settings:dynamic_cfg)
  TRADES_CLOSED_STREAM (default trades:closed)
  POLICY_EFF_LOOKBACK_H (default 24)
  POLICY_EFF_MAX_SCAN (default 200000)
  POLICY_EFF_TOP_P (default 0.05)
  POLICY_EFF_ECE_BINS (default 10)
  POLICY_EFF_SCORE_FIELDS (optional comma-separated override)
  POLICY_EFF_R_FIELDS (optional comma-separated override)
  POLICY_EFF_POLICY_MODE_FIELD (optional override, default policy_effective_mode)
  POLICY_EFF_REPORT_JSON_KEY (default reports:policy_effectiveness:p71:last_json)
  POLICY_EFF_REPORT_CSV_KEY (default reports:policy_effectiveness:p71:last_csv)

Run:
  python3 tools/policy_effectiveness_report_worker_v1.py --once
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import math
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional

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
    return [s.strip() for s in v.split(",") if s.strip()]


def _to_str(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, (bytes, bytearray)):
        try:
            return x.decode("utf-8", "replace")
        except Exception:
            return ""
    return str(x)


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (bytes, bytearray)):
        x = _to_str(x)
    if isinstance(x, (int, float)):
        try:
            return float(x)
        except Exception:
            return None
    s = str(x).strip()
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _loads_maybe_json(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode("utf-8", "replace")
        except Exception:
            return None
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
            try:
                return json.loads(s)
            except Exception:
                return v
        return v
    return v


def _decode_fields(raw_fields: Mapping[Any, Any]) -> Dict[str, Any]:
    d: Dict[str, Any] = {}
    for k, v in raw_fields.items():
        ks = _to_str(k)
        d[ks] = _loads_maybe_json(v)
    # merge nested payload/json if present
    p = d.get("payload") or d.get("json")
    if isinstance(p, dict):
        for kk, vv in p.items():
            if kk not in d:
                d[str(kk)] = vv
    return d


def _pick_ts_ms(stream_id: str, fields: Mapping[str, Any]) -> int:
    # prefer explicit ts fields if available
    for k in ("ts_ms", "timestamp_ms", "ts", "timestamp"):
        v = fields.get(k)
        fv = _to_float(v)
        if fv is None:
            continue
        # heuristic: if seconds, convert to ms
        if fv < 10_000_000_000:
            return int(fv * 1000)
        return int(fv)
    # fallback: redis stream id "ms-seq"
    try:
        return int(stream_id.split("-", 1)[0])
    except Exception:
        return 0


def _score_from_fields(fields: Mapping[str, Any], score_fields: List[str]) -> Optional[float]:
    for k in score_fields:
        v = _to_float(fields.get(k))
        if v is None:
            continue
        return float(v)
    return None


def _r_mult_from_fields(fields: Mapping[str, Any], r_fields: List[str]) -> Optional[float]:
    for k in r_fields:
        v = _to_float(fields.get(k))
        if v is None:
            continue
        return float(v)
    return None


def _policy_mode_from_fields(fields: Mapping[str, Any], policy_field: str) -> str:
    v = fields.get(policy_field)
    s = _to_str(v).strip().lower()
    if not s:
        return "unknown"
    # normalize common variants
    if s in ("ok", "pass", "allow", "good", "green"):
        return "ok"
    if s in ("warn", "warning", "soft", "yellow", "degrade"):
        return "warn"
    if s in ("block", "blocked", "hard", "red", "deny"):
        return "block"
    return s


def _clamp01(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


@dataclass
class Acc:
    scores: List[float]
    ys: List[int]
    rs: List[float]

    def __init__(self) -> None:
        self.scores = []
        self.ys = []
        self.rs = []


def _acc_add(acc: Acc, score: float, y: int, r: float) -> None:
    acc.scores.append(float(score))
    acc.ys.append(int(y))
    acc.rs.append(float(r))


def _mean(xs: List[float]) -> float:
    if not xs:
        return 0.0
    return float(sum(xs) / float(len(xs)))


def _precision_top_p(acc: Acc, top_p: float) -> float:
    n = len(acc.scores)
    if n <= 0:
        return 0.0
    k = int(math.ceil(max(1.0, top_p * n)))
    idx = sorted(range(n), key=lambda i: acc.scores[i], reverse=True)[:k]
    if not idx:
        return 0.0
    wins = sum(acc.ys[i] for i in idx)
    return float(wins / float(len(idx)))


def _ece(acc: Acc, bins: int) -> float:
    n = len(acc.scores)
    if n <= 0:
        return 0.0
    bins = max(1, int(bins))
    # buckets by score in [0,1]
    bucket_sum_p = [0.0] * bins
    bucket_sum_y = [0.0] * bins
    bucket_n = [0] * bins

    for s, y in zip(acc.scores, acc.ys):
        p = _clamp01(float(s))
        b = int(min(bins - 1, max(0, math.floor(p * bins))))
        bucket_sum_p[b] += p
        bucket_sum_y[b] += float(y)
        bucket_n[b] += 1

    ece = 0.0
    for b in range(bins):
        nb = bucket_n[b]
        if nb <= 0:
            continue
        p_hat = bucket_sum_p[b] / nb
        y_hat = bucket_sum_y[b] / nb
        ece += abs(p_hat - y_hat) * (nb / n)
    return float(ece)


def _metrics(acc: Acc, top_p: float, ece_bins: int) -> Dict[str, float]:
    n = len(acc.scores)
    if n <= 0:
        return {"n": 0.0, "expectancy_r": 0.0, "precision_top5p": 0.0, "ece": 0.0}
    exp_r = _mean(acc.rs)
    pr = _precision_top_p(acc, top_p)
    e = _ece(acc, ece_bins)
    return {"n": float(n), "expectancy_r": float(exp_r), "precision_top5p": float(pr), "ece": float(e)}


def _make_csv(rows: List[Dict[str, Any]]) -> str:
    out = io.StringIO()
    if not rows:
        return ""
    fieldnames = list(rows[0].keys())
    w = csv.DictWriter(out, fieldnames=fieldnames)
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return out.getvalue()


def compute_and_write_once() -> int:
    if not redis:
        raise RuntimeError("redis module is not available")

    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    dyn_cfg_key = os.getenv("DYN_CFG_KEY", "settings:dynamic_cfg")
    stream_key = os.getenv("TRADES_CLOSED_STREAM", "trades:closed")

    lookback_h = float(os.getenv("POLICY_EFF_LOOKBACK_H", "24") or 24)
    max_scan = int(os.getenv("POLICY_EFF_MAX_SCAN", "200000") or 200000)
    top_p = float(os.getenv("POLICY_EFF_TOP_P", "0.05") or 0.05)
    ece_bins = int(os.getenv("POLICY_EFF_ECE_BINS", "10") or 10)

    score_fields = _csv_env_list("POLICY_EFF_SCORE_FIELDS") or [
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
    r_fields = _csv_env_list("POLICY_EFF_R_FIELDS") or ["r_mult", "r_multiple", "r", "R"]
    policy_field = (os.getenv("POLICY_EFF_POLICY_MODE_FIELD") or "").strip() or "policy_effective_mode"

    report_json_key = os.getenv("POLICY_EFF_REPORT_JSON_KEY", "reports:policy_effectiveness:p71:last_json")
    report_csv_key = os.getenv("POLICY_EFF_REPORT_CSV_KEY", "reports:policy_effectiveness:p71:last_csv")

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

    now = now_ms()
    since_ms = int(now - lookback_h * 3600 * 1000)

    by_mode: Dict[str, Acc] = {m: Acc() for m in ("ok", "warn", "block", "unknown")}
    scanned = 0
    kept = 0
    start = "+"

    while scanned < max_scan:
        rows = r.xrevrange(stream_key, max=start, min="-", count=1000)
        if not rows:
            break

        for msg_id_b, raw_fields in rows:
            scanned += 1
            msg_id = _to_str(msg_id_b)
            f = _decode_fields(raw_fields)
            ts = _pick_ts_ms(msg_id, f)
            start = f"({msg_id}"  # paginate further (exclusive)

            if ts < since_ms:
                scanned = max_scan
                break

            score = _score_from_fields(f, score_fields)
            r_mult = _r_mult_from_fields(f, r_fields)
            if score is None or r_mult is None:
                continue

            mode = _policy_mode_from_fields(f, policy_field)
            if mode not in by_mode:
                mode = "unknown"
            y = 1 if float(r_mult) > 0.0 else 0
            _acc_add(by_mode[mode], float(score), y, float(r_mult))
            kept += 1

    # Metrics per mode
    per_mode: Dict[str, Dict[str, float]] = {}
    total_n = 0.0
    for mode, acc in by_mode.items():
        m = _metrics(acc, top_p=top_p, ece_bins=ece_bins)
        per_mode[mode] = m
        total_n += m["n"]

    # shares
    for mode, m in per_mode.items():
        m["share"] = float(m["n"] / total_n) if total_n > 0 else 0.0

    # baseline ok
    ok_present = 1 if per_mode.get("ok", {}).get("n", 0.0) > 0 else 0
    ok_exp = per_mode.get("ok", {}).get("expectancy_r", 0.0)
    ok_pr = per_mode.get("ok", {}).get("precision_top5p", 0.0)
    ok_ece = per_mode.get("ok", {}).get("ece", 0.0)

    deltas: Dict[str, Dict[str, float]] = {}
    for mode in ("ok", "warn", "block", "unknown"):
        m = per_mode.get(mode, {"n": 0.0, "share": 0.0, "expectancy_r": 0.0, "precision_top5p": 0.0, "ece": 0.0})
        if ok_present:
            deltas[mode] = {
                "expectancy_r_delta": float(m["expectancy_r"] - ok_exp),
                "precision_top5p_delta": float(m["precision_top5p"] - ok_pr),
                "ece_delta": float(m["ece"] - ok_ece),
            }
        else:
            deltas[mode] = {"expectancy_r_delta": 0.0, "precision_top5p_delta": 0.0, "ece_delta": 0.0}

    # Build full report rows (for CSV)
    csv_rows: List[Dict[str, Any]] = []
    for mode in ("ok", "warn", "block", "unknown"):
        m = per_mode[mode]
        d = deltas[mode]
        csv_rows.append(
            {
                "mode": mode,
                "n_24h": int(m["n"]),
                "share_24h": round(m["share"], 6),
                "expectancy_r_24h": round(m["expectancy_r"], 6),
                "precision_top5p_24h": round(m["precision_top5p"], 6),
                "ece_24h": round(m["ece"], 6),
                "delta_expectancy_r_vs_ok": round(d["expectancy_r_delta"], 6),
                "delta_precision_top5p_vs_ok": round(d["precision_top5p_delta"], 6),
                "delta_ece_vs_ok": round(d["ece_delta"], 6),
            }
        )

    report = {
        "ts_ms": now,
        "window_h": lookback_h,
        "since_ms": since_ms,
        "scanned": scanned,
        "kept": kept,
        "total_n": int(total_n),
        "baseline_ok_present": int(ok_present),
        "per_mode": {
            mode: {
                "n_24h": int(per_mode[mode]["n"]),
                "share_24h": float(per_mode[mode]["share"]),
                "expectancy_r_24h": float(per_mode[mode]["expectancy_r"]),
                "precision_top5p_24h": float(per_mode[mode]["precision_top5p"]),
                "ece_24h": float(per_mode[mode]["ece"]),
                "delta_expectancy_r_vs_ok": float(deltas[mode]["expectancy_r_delta"]),
                "delta_precision_top5p_vs_ok": float(deltas[mode]["precision_top5p_delta"]),
                "delta_ece_vs_ok": float(deltas[mode]["ece_delta"]),
            }
            for mode in ("ok", "warn", "block", "unknown")
        },
    }

    # Write report strings
    report_json = json.dumps(report, sort_keys=True)
    report_csv = _make_csv(csv_rows)

    r.set(report_json_key, report_json)
    r.set(report_csv_key, report_csv)

    # Write cfg2 snapshot (numeric keys only; store report key for retrieval)
    cfg: Dict[str, Any] = {
        "policy_effectiveness_last_ts_ms": now,
        "policy_effectiveness_baseline_ok_present": int(ok_present),
        "policy_effectiveness_report_key": report_json_key,
    }
    for mode in ("ok", "warn", "block", "unknown"):
        cfg[f"policy_effectiveness_share_24h_{mode}"] = float(per_mode[mode]["share"])
        cfg[f"policy_effectiveness_n_24h_{mode}"] = int(per_mode[mode]["n"])
        cfg[f"policy_effectiveness_expectancy_r_delta_24h_{mode}"] = float(deltas[mode]["expectancy_r_delta"])
        cfg[f"policy_effectiveness_precision_top5p_delta_24h_{mode}"] = float(deltas[mode]["precision_top5p_delta"])
        cfg[f"policy_effectiveness_ece_delta_24h_{mode}"] = float(deltas[mode]["ece_delta"])

    r.hset(dyn_cfg_key, mapping={k: (json.dumps(v) if isinstance(v, (dict, list)) else v) for k, v in cfg.items()})

    print(report_json)
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="Run once and exit")
    args = ap.parse_args(argv)

    if not args.once:
        args.once = True

    if args.once:
        return compute_and_write_once()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
