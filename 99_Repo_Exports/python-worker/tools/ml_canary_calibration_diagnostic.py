"""ml_canary_calibration_diagnostic.py — one-shot diagnostic.

Answers four questions about the v14_of ML scorer in shadow mode:

  1. Is `ml_shadow_conf01` predictive of trade outcome at all?
     (AUC vs hit, Spearman vs r_multiple)

  2. Is it calibrated as a probability?
     (Reliability curve — predicted vs actual hit rate in deciles)

  3. How does its scale compare to rule-based `confidence_v1`?
     (Distribution overlap, quantile mapping recipe)

  4. Where does ML disagree with the rule, and which side wins?
     (4-cell cross-tab on outcome)

Reads:
  - signals:of:inputs (XRANGE last LOOKBACK_H hours)
  - trades:closed     (XRANGE last LOOKBACK_H hours)

Output: report to stdout. No state writes.

Usage:
    REDIS_URL=redis://...:63791/0 python -m tools.ml_canary_calibration_diagnostic
    LOOKBACK_H=72 python -m tools.ml_canary_calibration_diagnostic
"""

from __future__ import annotations

import json
import math
import os
import statistics
import sys
import time
from typing import Any

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
LOOKBACK_H = float(os.getenv("LOOKBACK_H", "72"))
HIT_THRESHOLD_R = float(os.getenv("HIT_THRESHOLD_R", "0.3"))


def _norm_sid(raw: str | None) -> str | None:
    if not raw:
        return None
    parts = str(raw).strip().split(":")
    if len(parts) < 3:
        return None
    sym_idx = 0
    if (
        parts[0].replace("-", "").isalpha()
        and parts[0] == parts[0].lower()
        and parts[1].isalnum()
        and parts[1] == parts[1].upper()
    ):
        sym_idx = 1
    if sym_idx + 1 >= len(parts):
        return None
    symbol = parts[sym_idx]
    ts = parts[sym_idx + 1]
    if not (symbol.isalnum() and ts.isdigit()):
        return None
    return f"{symbol.upper()}:{ts}"


def _next_cursor(entry_id: str) -> str:
    try:
        ms, seq = entry_id.split("-")
        return f"{ms}-{int(seq) + 1}"
    except Exception:
        return entry_id


def load_signals(r, since_ms: int) -> dict[str, dict[str, Any]]:
    """Return sid_norm → {ml, rule, mode, symbol, regime}."""
    out: dict[str, dict[str, Any]] = {}
    cursor = f"{since_ms}-0"
    BATCH = 5000
    while True:
        chunk = r.xrange("signals:of:inputs", min=cursor, count=BATCH)
        if not chunk:
            break
        last_id = chunk[-1][0]
        for _, fields in chunk:
            payload = fields.get("payload") if isinstance(fields, dict) else None
            if not payload:
                continue
            try:
                p = json.loads(payload)
            except Exception:
                continue
            inner = p.get("data", p) if isinstance(p, dict) else p
            if isinstance(inner, str):
                try:
                    inner = json.loads(inner)
                except Exception:
                    continue
            if not isinstance(inner, dict):
                continue
            sid = _norm_sid(inner.get("sid") or inner.get("signal_id"))
            if not sid:
                continue
            ind = inner.get("indicators") or {}
            cb = ind.get("confidence_breakdown") or {} if isinstance(ind, dict) else {}
            ml = cb.get("ml_shadow_conf01")
            if ml is None:
                continue
            try:
                ml_v = float(ml)
            except Exception:
                continue
            rule = inner.get("confidence") or ind.get("confidence_v1") or ind.get("confidence")
            try:
                rule_v = float(rule) if rule is not None else None
            except Exception:
                rule_v = None
            out[sid] = {
                "ml": ml_v,
                "rule": rule_v,
                "mode": cb.get("scorer_mode", ""),
                "symbol": inner.get("symbol", ""),
                "regime": ind.get("regime") or "",
            }
        if len(chunk) < BATCH:
            break
        cursor = _next_cursor(last_id)
    return out


def load_trades(r, since_ms: int) -> dict[str, float]:
    out: dict[str, float] = {}
    cursor = f"{since_ms}-0"
    BATCH = 5000
    while True:
        chunk = r.xrange("trades:closed", min=cursor, count=BATCH)
        if not chunk:
            break
        last_id = chunk[-1][0]
        for _, fields in chunk:
            if not isinstance(fields, dict):
                continue
            sid = _norm_sid(fields.get("sid") or fields.get("signal_id"))
            if not sid:
                continue
            r_raw = fields.get("r_multiple")
            if r_raw is None or r_raw == "":
                continue
            try:
                rv = float(r_raw)
            except Exception:
                continue
            if not math.isfinite(rv):
                continue
            out[sid] = max(-5.0, min(5.0, rv))
        if len(chunk) < BATCH:
            break
        cursor = _next_cursor(last_id)
    return out


# ── Statistics helpers ────────────────────────────────────────────────────────


def auc_binary(scores: list[float], labels: list[int]) -> float:
    """Mann-Whitney AUC. O(n log n)."""
    if not scores or len(scores) != len(labels):
        return 0.5
    n_pos = sum(labels)
    n_neg = len(labels) - n_pos
    if n_pos == 0 or n_neg == 0:
        return 0.5
    # rank scores, average for ties
    paired = sorted(zip(scores, labels), key=lambda x: x[0])
    ranks = [0.0] * len(paired)
    i = 0
    while i < len(paired):
        j = i
        while j + 1 < len(paired) and paired[j + 1][0] == paired[i][0]:
            j += 1
        avg = (i + j) / 2 + 1  # 1-indexed average
        for k in range(i, j + 1):
            ranks[k] = avg
        i = j + 1
    sum_pos = sum(rank for rank, (_, lab) in zip(ranks, paired) if lab == 1)
    return (sum_pos - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg)


def spearman(xs: list[float], ys: list[float]) -> float:
    if len(xs) < 3 or len(xs) != len(ys):
        return 0.0
    def _rank(vs):
        idx = sorted(range(len(vs)), key=lambda i: vs[i])
        ranks = [0.0] * len(vs)
        i = 0
        while i < len(idx):
            j = i
            while j + 1 < len(idx) and vs[idx[j + 1]] == vs[idx[i]]:
                j += 1
            avg = (i + j) / 2 + 1
            for k in range(i, j + 1):
                ranks[idx[k]] = avg
            i = j + 1
        return ranks
    rx, ry = _rank(xs), _rank(ys)
    mx = sum(rx) / len(rx)
    my = sum(ry) / len(ry)
    num = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    dx = math.sqrt(sum((a - mx) ** 2 for a in rx))
    dy = math.sqrt(sum((b - my) ** 2 for b in ry))
    return num / (dx * dy) if dx > 0 and dy > 0 else 0.0


def quantiles(vs: list[float], qs: list[float]) -> list[float]:
    if not vs:
        return [0.0] * len(qs)
    sv = sorted(vs)
    out = []
    for q in qs:
        if q <= 0:
            out.append(sv[0])
        elif q >= 1:
            out.append(sv[-1])
        else:
            idx = q * (len(sv) - 1)
            lo = int(idx)
            hi = min(lo + 1, len(sv) - 1)
            frac = idx - lo
            out.append(sv[lo] * (1 - frac) + sv[hi] * frac)
    return out


def bin_calibration(scores: list[float], outcomes: list[float], n_bins: int = 10) -> list[dict]:
    """Equal-frequency bins, return per-bin {n, mean_score, mean_outcome}."""
    if not scores:
        return []
    paired = sorted(zip(scores, outcomes), key=lambda x: x[0])
    bin_size = len(paired) / n_bins
    bins = []
    for b in range(n_bins):
        lo = int(b * bin_size)
        hi = int((b + 1) * bin_size) if b < n_bins - 1 else len(paired)
        chunk = paired[lo:hi]
        if not chunk:
            continue
        bins.append({
            "n": len(chunk),
            "score_lo": chunk[0][0],
            "score_hi": chunk[-1][0],
            "mean_score": sum(s for s, _ in chunk) / len(chunk),
            "mean_outcome": sum(o for _, o in chunk) / len(chunk),
        })
    return bins


# ── Diagnostic report ─────────────────────────────────────────────────────────


def diagnose() -> None:
    try:
        import redis
    except ImportError:
        print("redis-py not installed", file=sys.stderr)
        sys.exit(2)

    r = redis.from_url(REDIS_URL, decode_responses=True)
    since_ms = int(time.time() * 1000) - int(LOOKBACK_H * 3600 * 1000)

    print(f"=== ML canary calibration diagnostic — lookback {LOOKBACK_H}h ===")
    print(f"Redis: {REDIS_URL.split('@')[-1] if '@' in REDIS_URL else REDIS_URL}")
    print(f"Hit threshold: r_multiple >= {HIT_THRESHOLD_R}")
    print()

    sigs = load_signals(r, since_ms)
    trades = load_trades(r, since_ms)
    print(f"loaded: {len(sigs)} signals w/ ml_shadow_conf01, {len(trades)} closed trades")

    # Join
    rows = []
    for sid, s in sigs.items():
        rv = trades.get(sid)
        if rv is None:
            continue
        if s["ml"] is None or s["rule"] is None:
            continue
        rows.append({
            "sid": sid,
            "ml": s["ml"],
            "rule": s["rule"],
            "mode": s["mode"],
            "symbol": s["symbol"],
            "regime": s["regime"],
            "r": rv,
            "hit": 1 if rv >= HIT_THRESHOLD_R else 0,
        })

    n = len(rows)
    print(f"JOIN signals∩trades: {n}")
    if n < 30:
        print()
        print("⚠️  insufficient JOIN sample (<30) — extend LOOKBACK_H or wait for more trades")
        return

    print()
    print("─── 1. PREDICTIVE POWER ──────────────────────────────────────────")
    ml_scores = [r["ml"] for r in rows]
    rule_scores = [r["rule"] for r in rows]
    hits = [r["hit"] for r in rows]
    r_mults = [r["r"] for r in rows]

    auc_ml = auc_binary(ml_scores, hits)
    auc_rule = auc_binary(rule_scores, hits)
    spr_ml = spearman(ml_scores, r_mults)
    spr_rule = spearman(rule_scores, r_mults)
    hit_rate = sum(hits) / n

    print(f"  base hit rate (r≥{HIT_THRESHOLD_R}):  {hit_rate*100:5.1f}%")
    print(f"  ml_shadow_conf01: AUC vs hit = {auc_ml:.3f}   Spearman vs r = {spr_ml:+.3f}")
    print(f"  rule_conf01:      AUC vs hit = {auc_rule:.3f}   Spearman vs r = {spr_rule:+.3f}")
    print()
    if auc_ml < 0.45:
        print("  ⚠️  ML inversely correlated — model is BROKEN or labels flipped")
    elif auc_ml < 0.52:
        print("  ⚠️  ML ≈ random — has no signal above noise")
    elif auc_ml < 0.58:
        print("  • ML weakly predictive (AUC 0.52-0.58)")
    else:
        print("  ✓  ML predictive (AUC ≥ 0.58)")

    print()
    print("─── 2. RELIABILITY (calibration curve) ──────────────────────────")
    ml_bins = bin_calibration(ml_scores, [float(h) for h in hits], n_bins=10)
    print(f"  ML deciles (predicted vs actual hit rate):")
    print(f"  {'bin':>3s} {'n':>4s} {'score_lo':>10s} {'score_hi':>10s} {'mean_score':>11s} {'actual_hit':>10s}")
    for i, b in enumerate(ml_bins):
        print(f"  {i+1:>3d} {b['n']:>4d} {b['score_lo']:>10.4f} {b['score_hi']:>10.4f} "
              f"{b['mean_score']:>11.4f} {b['mean_outcome']*100:>9.1f}%")
    # Monotonicity check
    means = [b["mean_outcome"] for b in ml_bins]
    if len(means) >= 3:
        # count direction concordance
        up = sum(1 for i in range(1, len(means)) if means[i] > means[i-1])
        dn = sum(1 for i in range(1, len(means)) if means[i] < means[i-1])
        print(f"  monotonicity: {up} up / {dn} down between adjacent deciles")
        if up >= len(means) - 2:
            print("  ✓  monotone increasing — ML rank is informative; sigmoid/isotonic calibration will work")
        elif dn >= len(means) - 2:
            print("  ⚠️  monotone DECREASING — labels are inverted")
        else:
            print("  • non-monotone — direction unstable across deciles")

    print()
    print("─── 3. SCALE COMPARISON (ml vs rule) ────────────────────────────")
    qs = [0.0, 0.10, 0.25, 0.50, 0.75, 0.90, 1.0]
    ml_q = quantiles(ml_scores, qs)
    rule_q = quantiles(rule_scores, qs)
    print(f"  {'quantile':>10s}  {'ml':>8s}  {'rule':>8s}")
    for q, m, ru in zip(qs, ml_q, rule_q):
        print(f"  {q*100:>9.0f}%  {m:>8.4f}  {ru:>8.4f}")
    ml_range = ml_q[-1] - ml_q[0]
    rule_range = rule_q[-1] - rule_q[0]
    print(f"  ml range:   {ml_range:.4f}   rule range: {rule_range:.4f}")
    if ml_q[-1] < 0.35:
        print("  ⚠️  ML's max < 0.35 — even loosest min_conf rejects 100% of enforce signals")
        print("     → quantile-map ML to rule's [{:.2f}, {:.2f}] is REQUIRED before enforce".format(
            rule_q[1], rule_q[-2]))

    print()
    print("─── 4. AGREEMENT — 4-cell cross-tab ──────────────────────────────")
    ml_med = ml_q[3]
    rule_med = rule_q[3]
    cells = {(0, 0): [], (0, 1): [], (1, 0): [], (1, 1): []}
    for row in rows:
        mh = 1 if row["ml"] >= ml_med else 0
        rh = 1 if row["rule"] >= rule_med else 0
        cells[(mh, rh)].append(row["r"])
    print(f"  using ML median ≈ {ml_med:.4f}, rule median ≈ {rule_med:.4f}")
    print(f"  {'ml':>4s} {'rule':>5s} {'n':>5s} {'mean_R':>9s} {'hit%':>7s}")
    for (mh, rh), rs in cells.items():
        if not rs:
            print(f"  {'hi' if mh else 'lo':>4s} {'hi' if rh else 'lo':>5s} {'0':>5s} {'-':>9s} {'-':>7s}")
            continue
        h = sum(1 for v in rs if v >= HIT_THRESHOLD_R) / len(rs)
        print(f"  {'hi' if mh else 'lo':>4s} {'hi' if rh else 'lo':>5s} "
              f"{len(rs):>5d} {statistics.mean(rs):>+9.3f} {h*100:>6.1f}%")

    # Disagreement zones
    n_disagree_ml_hi = len(cells[(1, 0)])
    n_disagree_rule_hi = len(cells[(0, 1)])
    if n_disagree_ml_hi >= 5 and n_disagree_rule_hi >= 5:
        avg_ml_hi = statistics.mean(cells[(1, 0)])
        avg_rule_hi = statistics.mean(cells[(0, 1)])
        diff = avg_ml_hi - avg_rule_hi
        print()
        if diff > 0.05:
            print(f"  ✓  ML-hi/rule-lo wins by {diff:+.3f}R — ML catches setups rule misses")
        elif diff < -0.05:
            print(f"  ⚠️  rule-hi/ml-lo wins by {-diff:+.3f}R — ML throws away setups rule keeps")
        else:
            print(f"  • disagreement zones equal ({diff:+.3f}R) — neither dominates")

    print()
    print("─── 5. RECOMMENDATION ────────────────────────────────────────────")
    if auc_ml < 0.52:
        print("  STOP. ML has no signal. Do NOT promote canary. Investigate:")
        print("    - feature drift (compare training-time vs serve-time feature distributions)")
        print("    - schema mismatch (n_features_in_ vs feature_cols)")
        print("    - label-time horizon mismatch (model trained on different barrier)")
    elif ml_q[-1] < 0.35:
        print("  ML predictive but scale incompatible with min_conf threshold.")
        print("  REQUIRED before any canary expansion:")
        print("  → Add quantile-calibration layer in MLScoringGate.score():")
        print(f"     ml_calibrated = quantile_transform(ml_proba, rule_distribution)")
        print(f"     where rule_distribution from p10..p90: [{rule_q[1]:.3f}, {rule_q[-2]:.3f}]")
        print(f"  → Or fit Platt scaling: rule_equiv = sigmoid(a*ml + b)")
        print("    on the joined (ml_proba, hit) pairs from this diagnostic.")
    else:
        print("  ML scale fits rule's range — calibration not blocking.")
        print("  → Safe to expand canary as planned.")


if __name__ == "__main__":
    diagnose()
