#!/usr/bin/env python3
"""tools/policy_regime_effectiveness_report_worker_p72.py

P72: 24h rolling report to reduce confounding in policy calibration.

What it does
  Reads `trades:closed` and computes KPIs grouped by
    (dq_state, drift_state, policy_effective_mode)
  Then, for each (dq_state, drift_state) cell, computes deltas vs policy=ok
  *within the same cell* (when ok baseline exists).

Why
  P71 provides global deltas per policy mode. Those can be biased by a different
  regime mix (dq/drift) in warn/block. This report helps answer:
    "Within the same dq/drift state, does warn/block look better or worse than ok?"

Outputs
  - Redis strings (full report):
      reports:policy_regime_effectiveness:p72:last_json
      reports:policy_regime_effectiveness:p72:last_csv
  - cfg2 snapshot keys (exported by meta_cov_rollout_exporter_v1):
      policy_regime_effectiveness_last_ts_ms
      policy_regime_effectiveness_cells_total
      policy_regime_effectiveness_cells_ok_baseline
      policy_regime_effectiveness_worst_warn_expectancy_r_delta
      policy_regime_effectiveness_worst_warn_precision_top5p_delta
      policy_regime_effectiveness_worst_warn_ece_delta
      policy_regime_effectiveness_worst_block_expectancy_r_delta
      policy_regime_effectiveness_worst_block_precision_top5p_delta
      policy_regime_effectiveness_worst_block_ece_delta
      policy_regime_effectiveness_report_key

Run
  python3 tools/policy_regime_effectiveness_report_worker_p72.py --once
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
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore


def now_ms() -> int:
    return int(time.time() * 1000)


def _env_csv_list(name: str) -> List[str]:
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
        v = _to_str(v)
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


def _extract_from_signal_payload(sp: Dict[str, Any]) -> Dict[str, Any]:
    """Extract score / policy / dq / drift fields buried in signal_payload.

    trades:closed stream stores signal context inside signal_payload JSON blob.
    Top-level stream fields take priority; we only fill gaps.
    """
    extracted: Dict[str, Any] = {}

    def _dig(d: Any, *path: str) -> Any:
        cur = d
        for key in path:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(key)
        return cur

    # ── score / probability ──────────────────────────────────────────────────
    # Prefer calibrated final score from score_breakdown > of_confirm_v3 score
    score = _to_float(_dig(sp, "config_snapshot", "indicators", "score_breakdown", "final_score"))
    if score is None:
        score = _to_float(_dig(sp, "indicators", "of_confirm_v3", "score"))
    if score is None:
        score = _to_float(_dig(sp, "indicators", "of_confirm", "score"))
    if score is None:
        score = _to_float(_dig(sp, "score"))
    if score is not None:
        extracted["score"] = score

    # ML meta score (calibrated probability)
    ml_p = _to_float(_dig(sp, "indicators", "of_confirm_v3", "evidence", "ml", "p_edge_cal"))
    if ml_p is None:
        ml_p = _to_float(_dig(sp, "indicators", "of_confirm", "evidence", "ml", "p_edge_cal"))
    if ml_p is not None:
        extracted["ml_p_cal"] = ml_p
        extracted["ml_p"] = ml_p

    # ── policy_effective_mode ────────────────────────────────────────────────
    # of_gate_mode lives at signal_payload.indicators.of_confirm_v3.of_gate_mode
    gate_mode = _to_str(_dig(sp, "indicators", "of_confirm_v3", "of_gate_mode"))
    if not gate_mode:
        gate_mode = _to_str(_dig(sp, "indicators", "of_confirm", "of_gate_mode"))
    if not gate_mode:
        gate_mode = _to_str(_dig(sp, "indicators", "of_confirm_v3", "evidence", "meta_mode"))
    if not gate_mode:
        gate_mode = _to_str(_dig(sp, "indicators", "of_confirm", "evidence", "meta_mode"))
    if not gate_mode:
        gate_mode = _to_str(_dig(sp, "of_gate_mode"))  # top-level fallback
    if gate_mode:
        extracted["policy_effective_mode"] = gate_mode
        extracted["ml_state"] = gate_mode

    # ML status field (OK/WARN/BLOCK) — secondary fallback
    ml_status = _to_str(_dig(sp, "indicators", "of_confirm_v3", "evidence", "ml", "status"))
    if not ml_status:
        ml_status = _to_str(_dig(sp, "indicators", "of_confirm", "evidence", "ml", "status"))
    if ml_status and "policy_effective_mode" not in extracted:
        extracted["policy_effective_mode"] = ml_status

    # ── dq_state ─────────────────────────────────────────────────────────────
    dq_bucket = _to_str(_dig(sp, "config_snapshot", "indicators", "dq_reason_bucket"))
    if not dq_bucket:
        dq_bucket = _to_str(_dig(sp, "indicators", "dq_reason_bucket"))
    if not dq_bucket:
        dq_bucket = _to_str(_dig(sp, "config_snapshot", "indicators", "dq_reason"))
    if dq_bucket:
        extracted["dq_state"] = dq_bucket
        extracted["dq"] = dq_bucket

    # ── drift_state ──────────────────────────────────────────────────────────
    drift = _to_str(_dig(sp, "drift_state") or _dig(sp, "indicators", "drift_state")
                    or _dig(sp, "config_snapshot", "drift_state"))
    if drift:
        extracted["drift_state"] = drift
        extracted["drift"] = drift

    return extracted


def _decode_fields(raw_fields: Mapping[Any, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in raw_fields.items():
        kk = _to_str(k)
        out[kk] = _loads_maybe_json(v)

    if "payload" in out and isinstance(out["payload"], dict):
        pl = out.pop("payload")
        for pk, pv in pl.items():
            if pk not in out:
                out[pk] = pv

    # Expand signal_payload: fill missing score/policy/dq/drift from nested blob
    sp = out.get("signal_payload")
    if isinstance(sp, dict):
        for ek, ev in _extract_from_signal_payload(sp).items():
            if ek not in out or out[ek] is None or _to_str(out[ek]).strip() == "":
                out[ek] = ev

    return out


def _pick_ts_ms(msg_id: str, fields: Mapping[str, Any]) -> int:
    # Prefer explicit ts fields when present
    for k in ("ts_ms", "timestamp_ms", "close_ts_ms", "event_ts_ms"):
        tv = _to_float(fields.get(k))
        if tv is not None and tv > 0:
            return int(tv)
    # Redis stream id: ms-seq
    try:
        return int(msg_id.split("-")[0])
    except Exception:
        return 0


def _norm_policy_mode(v: Any) -> str:
    s = _to_str(v).strip().lower()
    if not s:
        return "unknown"
    if s in ("ok", "normal", "green", "shadow", "live"):
        # SHADOW = system is live but ML gate is in shadow mode → treat as ok baseline
        return "ok"
    if s in ("warn", "warning", "yellow", "degraded"):
        return "warn"
    if s in ("block", "blocked", "red"):
        return "block"
    return "unknown"


def _norm_state(v: Any) -> str:
    s = _to_str(v).strip().lower()
    if not s:
        return "unknown"
    if s in ("ok", "good", "pass", "green", "healthy", "none"):
        return "ok"
    if s in (
        "warn",
        "warning",
        "yellow",
        "soft",
        "stale",
        "suspect",
        "degrade",
        "degraded",
    ):
        return "warn"
    if s in ("block", "blocked", "red", "hard", "fail", "failed", "bad", "quarantine"):
        return "block"
    return "unknown"


def _pick_first(fields: Mapping[str, Any], candidates: Iterable[str]) -> Any:
    for k in candidates:
        if k in fields:
            v = fields.get(k)
            if v is not None and _to_str(v).strip() != "":
                return v
    return None


def _score_from_fields(fields: Mapping[str, Any], score_fields: List[str]) -> Optional[float]:
    v = _pick_first(fields, score_fields)
    score = _to_float(v)
    if score is None:
        return None
    # clamp to [0,1] if appears to be a probability
    if score < 0.0:
        score = 0.0
    if score > 1.0 and score <= 100.0:
        # some pipelines may store percent
        score = score / 100.0
    if score > 1.0:
        score = 1.0
    return score


def _r_mult_from_fields(fields: Mapping[str, Any], r_fields: List[str]) -> Optional[float]:
    v = _pick_first(fields, r_fields)
    return _to_float(v)


@dataclass
class Acc:
    scores: List[float]
    ys: List[int]
    rs: List[float]

    def __init__(self) -> None:
        self.scores = []
        self.ys = []
        self.rs = []


def _acc_add(acc: Acc, score: float, y: int, r_mult: float) -> None:
    acc.scores.append(float(score))
    acc.ys.append(int(y))
    acc.rs.append(float(r_mult))


def _ece(scores: List[float], ys: List[int], bins: int) -> float:
    if not scores or not ys or len(scores) != len(ys):
        return 0.0
    bins = max(1, int(bins))
    n = len(scores)
    ece = 0.0
    for b in range(bins):
        lo = b / bins
        hi = (b + 1) / bins
        idx = [i for i, s in enumerate(scores) if (s >= lo and (s < hi if b < bins - 1 else s <= hi))]
        if not idx:
            continue
        p_hat = sum(scores[i] for i in idx) / len(idx)
        y_hat = sum(ys[i] for i in idx) / len(idx)
        ece += (len(idx) / n) * abs(p_hat - y_hat)
    return float(ece)


def _precision_top_p(scores: List[float], ys: List[int], top_p: float) -> float:
    if not scores or not ys or len(scores) != len(ys):
        return 0.0
    n = len(scores)
    k = int(math.ceil(max(1.0, float(n) * float(top_p))))
    pairs = sorted(zip(scores, ys), key=lambda x: x[0], reverse=True)
    top = pairs[:k]
    if not top:
        return 0.0
    return float(sum(y for _, y in top) / len(top))


def _metrics(acc: Acc, top_p: float, ece_bins: int) -> Dict[str, float]:
    n = float(len(acc.rs))
    if n <= 0:
        return {"n": 0.0, "expectancy_r": 0.0, "precision_top5p": 0.0, "ece": 0.0}
    exp_r = float(sum(acc.rs) / n)
    pr = _precision_top_p(acc.scores, acc.ys, top_p=top_p)
    ece = _ece(acc.scores, acc.ys, bins=ece_bins)
    return {"n": n, "expectancy_r": exp_r, "precision_top5p": pr, "ece": ece}


def _make_csv(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return ""
    buf = io.StringIO()
    cols = list(rows[0].keys())
    w = csv.DictWriter(buf, fieldnames=cols)
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return buf.getvalue()


def compute_and_write_once() -> int:
    if redis is None:
        raise RuntimeError("redis library is required")

    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    dyn_cfg_key = os.getenv("DYN_CFG_KEY", "settings:dynamic_cfg")
    stream_key = os.getenv("TRADES_CLOSED_STREAM", "trades:closed")

    lookback_h = float(os.getenv("POLICY_REGIME_EFF_LOOKBACK_H", "24") or 24)
    max_scan = int(os.getenv("POLICY_REGIME_EFF_MAX_SCAN", "200000") or 200000)
    top_p = float(os.getenv("POLICY_REGIME_EFF_TOP_P", "0.05") or 0.05)
    ece_bins = int(os.getenv("POLICY_REGIME_EFF_ECE_BINS", "10") or 10)
    min_n_ok = int(os.getenv("POLICY_REGIME_EFF_MIN_N_OK", "20") or 20)
    min_n_other = int(os.getenv("POLICY_REGIME_EFF_MIN_N_MODE", "10") or 10)

    score_fields = _env_csv_list("POLICY_REGIME_EFF_SCORE_FIELDS") or [
        "score",
        "p",
        "prob",
        "probability",
        "ml_score",
        "meta_model_score",
        "y_prob",
        "yhat",
        "proba",
        "ml_p_cal",
        "ml_p",
    ]
    r_fields = _env_csv_list("POLICY_REGIME_EFF_R_FIELDS") or [
        "r_mult",
        "r_multiple",
        "r",
        "R",
        "trade_r",
        "result_r",
    ]
    policy_fields = _env_csv_list("POLICY_REGIME_EFF_POLICY_MODE_FIELDS") or [
        (os.getenv("POLICY_REGIME_EFF_POLICY_MODE_FIELD") or "policy_effective_mode").strip(),
        "ml_state",
        "policy_mode",
    ]
    dq_fields = _env_csv_list("POLICY_REGIME_EFF_DQ_FIELDS") or ["dq_state", "dq", "dq_mode", "dq_status"]
    drift_fields = _env_csv_list("POLICY_REGIME_EFF_DRIFT_FIELDS") or [
        "drift_state",
        "drift",
        "drift_mode",
        "drift_status",
    ]

    report_json_key = os.getenv(
        "POLICY_REGIME_EFF_REPORT_JSON_KEY", "reports:policy_regime_effectiveness:p72:last_json"
    )
    report_csv_key = os.getenv(
        "POLICY_REGIME_EFF_REPORT_CSV_KEY", "reports:policy_regime_effectiveness:p72:last_csv"
    )

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
    since_ms = int(now - lookback_h * 3600.0 * 1000.0)

    by_cell: Dict[Tuple[str, str, str], Acc] = {}
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
            start = f"({msg_id}"

            if ts < since_ms:
                scanned = max_scan
                break

            score = _score_from_fields(f, score_fields)
            r_mult = _r_mult_from_fields(f, r_fields)
            if score is None or r_mult is None:
                continue

            pm = _norm_policy_mode(_pick_first(f, policy_fields))
            dq = _norm_state(_pick_first(f, dq_fields))
            drift = _norm_state(_pick_first(f, drift_fields))

            acc = by_cell.get((dq, drift, pm))
            if acc is None:
                acc = Acc()
                by_cell[(dq, drift, pm)] = acc

            y = 1 if float(r_mult) > 0.0 else 0
            _acc_add(acc, float(score), int(y), float(r_mult))
            kept += 1

    # Aggregate totals per (dq, drift), and ok baselines
    per_regime_total_n: Dict[Tuple[str, str], float] = {}
    per_regime_ok: Dict[Tuple[str, str], Dict[str, float]] = {}
    for (dq, drift, pm), acc in by_cell.items():
        m = _metrics(acc, top_p=top_p, ece_bins=ece_bins)
        per_regime_total_n[(dq, drift)] = per_regime_total_n.get((dq, drift), 0.0) + float(m["n"])
        if pm == "ok":
            per_regime_ok[(dq, drift)] = m

    cells_total = len(per_regime_total_n)
    cells_ok = 0
    for (dq, drift), ok_m in per_regime_ok.items():
        if float(ok_m.get("n", 0.0)) >= float(min_n_ok):
            cells_ok += 1

    # Build rows and track worst deltas across regimes
    rows_out: List[Dict[str, Any]] = []

    worst_warn_exp: Optional[float] = None
    worst_warn_pr: Optional[float] = None
    worst_warn_ece: Optional[float] = None
    worst_block_exp: Optional[float] = None
    worst_block_pr: Optional[float] = None
    worst_block_ece: Optional[float] = None

    for (dq, drift, pm), acc in by_cell.items():
        m = _metrics(acc, top_p=top_p, ece_bins=ece_bins)
        total_n = float(per_regime_total_n.get((dq, drift), 0.0))
        share_in_regime = float(m["n"] / total_n) if total_n > 0 else 0.0

        ok_m = per_regime_ok.get((dq, drift))
        ok_present = 1 if ok_m and float(ok_m.get("n", 0.0)) > 0 else 0
        if ok_present:
            d_exp = float(m["expectancy_r"] - float(ok_m.get("expectancy_r", 0.0)))
            d_pr = float(m["precision_top5p"] - float(ok_m.get("precision_top5p", 0.0)))
            d_ece = float(m["ece"] - float(ok_m.get("ece", 0.0)))
        else:
            d_exp = d_pr = d_ece = 0.0

        rows_out.append(
            {
                "dq_state": dq,
                "drift_state": drift,
                "policy_mode": pm,
                "n_24h": int(m["n"]),
                "share_in_regime_24h": round(share_in_regime, 6),
                "expectancy_r_24h": round(float(m["expectancy_r"]), 6),
                "precision_top5p_24h": round(float(m["precision_top5p"]), 6),
                "ece_24h": round(float(m["ece"]), 6),
                "delta_expectancy_r_vs_ok_in_regime": round(d_exp, 6),
                "delta_precision_top5p_vs_ok_in_regime": round(d_pr, 6),
                "delta_ece_vs_ok_in_regime": round(d_ece, 6),
                "ok_baseline_present": int(ok_present),
            }
        )

        # worst-case tracking (only meaningful samples)
        if pm in ("warn", "block") and ok_present and ok_m is not None:
            if int(float(ok_m.get("n", 0.0))) < min_n_ok:
                continue
            if int(float(m.get("n", 0.0))) < min_n_other:
                continue

            if pm == "warn":
                worst_warn_exp = d_exp if worst_warn_exp is None else min(worst_warn_exp, d_exp)
                worst_warn_pr = d_pr if worst_warn_pr is None else min(worst_warn_pr, d_pr)
                worst_warn_ece = d_ece if worst_warn_ece is None else max(worst_warn_ece, d_ece)
            if pm == "block":
                worst_block_exp = d_exp if worst_block_exp is None else min(worst_block_exp, d_exp)
                worst_block_pr = d_pr if worst_block_pr is None else min(worst_block_pr, d_pr)
                worst_block_ece = d_ece if worst_block_ece is None else max(worst_block_ece, d_ece)

    rows_out.sort(key=lambda r: (r["dq_state"], r["drift_state"], r["policy_mode"]))

    report = {
        "ts_ms": now,
        "window_h": lookback_h,
        "since_ms": since_ms,
        "scanned": scanned,
        "kept": kept,
        "cells_total": int(cells_total),
        "cells_ok_baseline": int(cells_ok),
        "min_n_ok": int(min_n_ok),
        "min_n_mode": int(min_n_other),
        "worst": {
            "warn": {
                "expectancy_r_delta": float(worst_warn_exp or 0.0),
                "precision_top5p_delta": float(worst_warn_pr or 0.0),
                "ece_delta": float(worst_warn_ece or 0.0),
            },
            "block": {
                "expectancy_r_delta": float(worst_block_exp or 0.0),
                "precision_top5p_delta": float(worst_block_pr or 0.0),
                "ece_delta": float(worst_block_ece or 0.0),
            },
        },
        "rows": rows_out,
    }

    report_json = json.dumps(report, sort_keys=True)
    report_csv = _make_csv(rows_out)

    r.set(report_json_key, report_json)
    r.set(report_csv_key, report_csv)

    cfg_snapshot: Dict[str, Any] = {
        "policy_regime_effectiveness_last_ts_ms": now,
        "policy_regime_effectiveness_cells_total": int(cells_total),
        "policy_regime_effectiveness_cells_ok_baseline": int(cells_ok),
        "policy_regime_effectiveness_worst_warn_expectancy_r_delta": float(worst_warn_exp or 0.0),
        "policy_regime_effectiveness_worst_warn_precision_top5p_delta": float(worst_warn_pr or 0.0),
        "policy_regime_effectiveness_worst_warn_ece_delta": float(worst_warn_ece or 0.0),
        "policy_regime_effectiveness_worst_block_expectancy_r_delta": float(worst_block_exp or 0.0),
        "policy_regime_effectiveness_worst_block_precision_top5p_delta": float(worst_block_pr or 0.0),
        "policy_regime_effectiveness_worst_block_ece_delta": float(worst_block_ece or 0.0),
        "policy_regime_effectiveness_report_key": report_json_key,
    }

    r.hset(
        dyn_cfg_key,
        mapping={
            k: (json.dumps(v) if isinstance(v, (dict, list)) else v)  # cfg2 convention
            for k, v in cfg_snapshot.items()
        },
    )

    print(report_json)
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="Run once and exit")
    args = ap.parse_args(argv)
    return compute_and_write_once() if args.once or True else 0


if __name__ == "__main__":
    raise SystemExit(main())
