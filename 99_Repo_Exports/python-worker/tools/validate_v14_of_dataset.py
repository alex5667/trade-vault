"""validate_v14_of_dataset.py — Production-readiness checks for v14_of training dataset.

Checks (matching the 5.1–5.4 spec):
  5.1 Dataset integrity  — row counts, duplicates, join-lag leakage
  5.2 Per-fold metrics   — fold-level CV: AUC, PR-AUC, Brier, LogLoss, Precision@5%, ECE
  5.3 Precision@K        — Top-1/3/5/10% precision vs baseline
  5.4 Calibration        — reliability table (10 p_edge buckets)

Usage:
  python -m tools.validate_v14_of_dataset
  python -m tools.validate_v14_of_dataset --work-dir /tmp/v14_of_train
  python -m tools.validate_v14_of_dataset --dataset /tmp/v14_of_train/ml_dataset_v14.jsonl

Exit codes:
  0  all gates pass
  1  one or more CRITICAL gates fail (leakage / too few positives per fold)
  2  warning-only (minor issues)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _ff(v: Any, d: float = 0.0) -> float:
    try:
        return float(v) if v is not None else d
    except Exception:
        return d


def _read_ndjson(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open() as f:
        for line in f:
            s = line.strip()
            if s:
                try:
                    rows.append(json.loads(s))
                except Exception:
                    pass
    return rows


def _hline(char: str = "─", width: int = 72) -> str:
    return char * width


def _section(title: str) -> None:
    print(f"\n{'═' * 72}")
    print(f"  {title}")
    print(f"{'═' * 72}")


def _pass(msg: str) -> None:
    print(f"  ✓  {msg}")


def _warn(msg: str) -> None:
    print(f"  ⚠  {msg}")


def _fail(msg: str) -> None:
    print(f"  ✗  {msg}")


# ──────────────────────────────────────────────────────────────────────────────
# 5.1  Dataset integrity
# ──────────────────────────────────────────────────────────────────────────────

def check_integrity(
    dataset_rows: list[dict[str, Any]],
    tb_rows: list[dict[str, Any]],
) -> tuple[bool, list[str]]:
    """Returns (critical_fail, warning_messages)."""
    _section("5.1  Dataset integrity")
    critical = False
    warns: list[str] = []

    # ── Basic counts ──────────────────────────────────────────────────────────
    n = len(dataset_rows)
    pos = sum(int(r.get("y_edge", 0) or 0) for r in dataset_rows)
    pos_rate = pos / n if n else 0.0
    print(f"\n  n_rows      : {n:,}")
    print(f"  positives   : {pos:,}")
    print(f"  pos_rate    : {pos_rate:.4f}  ({pos_rate*100:.2f}%)")
    if n == 0:
        _fail("Dataset is empty — cannot proceed")
        return True, warns
    if pos < 20:
        _fail(f"Too few positives ({pos}) — training unreliable")
        critical = True
    elif pos < 50:
        _warn(f"Low positives ({pos}) — consider more data")
        warns.append(f"low_positives:{pos}")
    else:
        _pass(f"Positives OK ({pos})")

    # ── Duplicate input_id (sid) ──────────────────────────────────────────────
    sid_counts: Counter[str] = Counter(r.get("sid", "") for r in dataset_rows)
    dup_sids = {sid: cnt for sid, cnt in sid_counts.items() if cnt > 1 and sid}
    if dup_sids:
        top5 = sorted(dup_sids.items(), key=lambda x: -x[1])[:5]
        _warn(f"Duplicate sids (input_id): {len(dup_sids)} — top: {top5}")
        warns.append(f"dup_sids:{len(dup_sids)}")
    else:
        _pass("No duplicate sids (input_id)")

    # ── Duplicate label_id (sid in tb_labels) ────────────────────────────────
    def _norm_sid(s: str) -> str:
        return s[len("crypto-of:"):] if s.startswith("crypto-of:") else s

    if tb_rows:
        tb_sid_counts: Counter[str] = Counter(
            _norm_sid(r.get("sid", "") or "") for r in tb_rows
        )
        dup_tb_sids = {sid: cnt for sid, cnt in tb_sid_counts.items() if cnt > 1 and sid}
        if dup_tb_sids:
            top5 = sorted(dup_tb_sids.items(), key=lambda x: -x[1])[:5]
            _warn(f"Duplicate label sids: {len(dup_tb_sids)} — top: {top5}")
            warns.append(f"dup_label_sids:{len(dup_tb_sids)}")
        else:
            _pass("No duplicate label sids")
    else:
        _warn("tb_labels.ndjson not available — skipping label duplicate check")

    # ── Join lag / leakage check ──────────────────────────────────────────────
    # tb_hit_ms in tb_labels is the relative time from entry to barrier hit.
    # Must be >= 0 (otherwise label was resolved before entry — leakage).
    if tb_rows:
        hit_ms_vals = [_ff(r.get("tb_hit_ms", 0)) for r in tb_rows]
        negatives = [v for v in hit_ms_vals if v < 0]
        valid_hits = [v for v in hit_ms_vals if v > 0]

        if negatives:
            _fail(f"LEAKAGE: {len(negatives)} tb_hit_ms < 0 (barrier before entry)")
            critical = True
        else:
            _pass("No negative tb_hit_ms (no leakage)")

        if valid_hits:
            import statistics
            valid_hits_sorted = sorted(valid_hits)
            n_h = len(valid_hits_sorted)

            def _pct(lst: list[float], p: float) -> float:
                idx = int(len(lst) * p)
                return lst[min(idx, len(lst) - 1)]

            p50 = _pct(valid_hits_sorted, 0.50)
            p95 = _pct(valid_hits_sorted, 0.95)
            p99 = _pct(valid_hits_sorted, 0.99)
            mn = valid_hits_sorted[0]
            mx = valid_hits_sorted[-1]
            print(f"\n  Join lag (tb_hit_ms)  n={n_h:,}")
            print(f"    min={mn/1000:.1f}s  p50={p50/1000:.1f}s  p95={p95/1000:.1f}s"
                  f"  p99={p99/1000:.1f}s  max={mx/1000:.1f}s")
            if mn < 0:
                _fail("min lag < 0 — leakage")
                critical = True
            else:
                _pass("min lag >= 0")
    else:
        _warn("tb_labels.ndjson not available — skipping lag check")

    # ── Symbol distribution ───────────────────────────────────────────────────
    sym_counts: Counter[str] = Counter(r.get("symbol", "?") for r in dataset_rows)
    top_syms = sym_counts.most_common(10)
    print(f"\n  Symbols ({len(sym_counts)} unique): {top_syms}")

    # ── TB outcome distribution ───────────────────────────────────────────────
    outcome_counts: Counter[str] = Counter(r.get("tb_outcome", "?") for r in dataset_rows)
    print(f"  TB outcomes: {dict(outcome_counts.most_common())}")

    return critical, warns


# ──────────────────────────────────────────────────────────────────────────────
# 5.2  Per-fold metrics
# ──────────────────────────────────────────────────────────────────────────────

def _ece(y_true: Any, p_pred: Any, n_bins: int = 10) -> float:
    """Expected calibration error (uniform bins)."""
    import numpy as np
    ece_val = 0.0
    n = len(y_true)
    for i in range(n_bins):
        lo = i / n_bins
        hi = (i + 1) / n_bins
        mask = (p_pred >= lo) & (p_pred < hi)
        if i == n_bins - 1:
            mask = (p_pred >= lo) & (p_pred <= hi)
        if mask.sum() == 0:
            continue
        acc = float(np.mean(y_true[mask]))
        conf = float(np.mean(p_pred[mask]))
        ece_val += (mask.sum() / n) * abs(acc - conf)
    return ece_val


def _precision_at_k(y_true: Any, p: Any, k_frac: float) -> float:
    import numpy as np
    n = len(y_true)
    k = max(1, int(n * k_frac))
    idx = np.argsort(p)[::-1][:k]
    return float(np.mean(np.asarray(y_true)[idx]))


def check_folds(
    dataset_rows: list[dict[str, Any]],
    feature_cols: list[str],
) -> tuple[bool, list[dict[str, Any]]]:
    """Returns (critical_fail, fold_results)."""
    _section("5.2  Per-fold metrics (5-fold stratified CV)")
    critical = False

    try:
        import numpy as np
        from sklearn.linear_model import LogisticRegression
        from sklearn.metrics import (
            average_precision_score,
            brier_score_loss,
            log_loss,
            roc_auc_score,
        )
        from sklearn.model_selection import StratifiedKFold
        from sklearn.preprocessing import StandardScaler
    except ImportError as e:
        _warn(f"sklearn not available — skipping fold metrics: {e}")
        return False, []

    def _ff_local(v: Any, d: float = 0.0) -> float:
        try:
            return float(v) if v is not None else d
        except Exception:
            return d

    import numpy as np

    X = np.array([
        [_ff_local((r.get("indicators") or {}).get(k)) for k in feature_cols]
        for r in dataset_rows
    ], dtype=np.float64)
    y = np.array([int(r.get("y_edge", 0) or 0) for r in dataset_rows], dtype=np.int64)

    # Replace NaN with column median
    col_medians = np.nanmedian(X, axis=0)
    for j in range(X.shape[1]):
        nan_mask = np.isnan(X[:, j])
        X[nan_mask, j] = col_medians[j]

    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    fold_results: list[dict[str, Any]] = []
    oof_p = np.zeros(len(y), dtype=np.float64)

    for fold_i, (tr, te) in enumerate(skf.split(X, y)):
        n_tr_pos = int(y[tr].sum())
        n_te_pos = int(y[te].sum())

        if n_te_pos < 3:
            _warn(f"Fold {fold_i}: only {n_te_pos} positives in test — metrics unreliable")
            fold_results.append({
                "fold": fold_i, "n_rows": len(te), "n_pos": n_te_pos,
                "roc_auc": None, "pr_auc": None, "brier": None,
                "log_loss": None, "prec_top5pct": None, "ece": None,
            })
            continue

        sc = StandardScaler().fit(X[tr])
        Xtr: Any = sc.transform(X[tr])
        Xte: Any = sc.transform(X[te])
        lr = LogisticRegression(C=1.0, max_iter=2000, random_state=42)
        lr.fit(Xtr, y[tr])
        p = lr.predict_proba(Xte)[:, 1]
        oof_p[te] = p

        fold_res: dict[str, Any] = {
            "fold": fold_i,
            "n_rows": len(te),
            "n_pos": n_te_pos,
            "roc_auc": round(float(roc_auc_score(y[te], p)), 4),
            "pr_auc": round(float(average_precision_score(y[te], p)), 4),
            "brier": round(float(brier_score_loss(y[te], p)), 4),
            "log_loss": round(float(log_loss(y[te], p, labels=[0, 1])), 4),
            "prec_top5pct": round(_precision_at_k(y[te], p, 0.05), 4),
            "ece": round(_ece(y[te].astype(float), p), 4),
        }
        fold_results.append(fold_res)

    # Print table
    header = (
        f"  {'Fold':>4}  {'N':>6}  {'Pos':>5}  {'AUC':>6}  "
        f"{'PR-AUC':>6}  {'Brier':>6}  {'LogLoss':>8}  {'P@5%':>6}  {'ECE':>6}"
    )
    print(f"\n{header}")
    print(f"  {_hline('-', len(header.rstrip()) - 2)}")

    auc_vals = []
    brier_vals = []
    min_pos = 999

    for fr in fold_results:
        roc = f"{fr['roc_auc']:.4f}" if fr["roc_auc"] is not None else "  N/A "
        pra = f"{fr['pr_auc']:.4f}" if fr["pr_auc"] is not None else "  N/A "
        bri = f"{fr['brier']:.4f}" if fr["brier"] is not None else "  N/A "
        ll_ = f"{fr['log_loss']:.4f}" if fr["log_loss"] is not None else "   N/A  "
        p5 = f"{fr['prec_top5pct']:.4f}" if fr["prec_top5pct"] is not None else "  N/A "
        ece = f"{fr['ece']:.4f}" if fr["ece"] is not None else "  N/A "
        print(f"  {fr['fold']:>4}  {fr['n_rows']:>6,}  {fr['n_pos']:>5}  "
              f"{roc:>6}  {pra:>6}  {bri:>6}  {ll_:>8}  {p5:>6}  {ece:>6}")
        if fr["roc_auc"] is not None:
            auc_vals.append(fr["roc_auc"])
            brier_vals.append(fr["brier"])  # type: ignore[arg-type]
        min_pos = min(min_pos, fr["n_pos"])

    # Aggregate OOF row
    n_valid_oof = int((oof_p > 0).sum())
    if n_valid_oof > 10 and len(set(y.tolist())) > 1:
        oof_mask = oof_p > 0
        oof_auc = float(roc_auc_score(y[oof_mask], oof_p[oof_mask]))
        oof_pr = float(average_precision_score(y[oof_mask], oof_p[oof_mask]))
        oof_brier = float(brier_score_loss(y[oof_mask], oof_p[oof_mask]))
        oof_ll = float(log_loss(y[oof_mask], oof_p[oof_mask], labels=[0, 1]))
        oof_p5 = _precision_at_k(y[oof_mask], oof_p[oof_mask], 0.05)
        oof_ece = _ece(y[oof_mask].astype(float), oof_p[oof_mask])
        print(f"  {_hline('-', len(header.rstrip()) - 2)}")
        print(f"  {'OOF':>4}  {int(oof_mask.sum()):>6,}  {int(y[oof_mask].sum()):>5}  "
              f"{oof_auc:.4f}  {oof_pr:.4f}  {oof_brier:.4f}  {oof_ll:.6f}  "
              f"{oof_p5:.4f}  {oof_ece:.4f}")

    # Gate checks
    print()
    if min_pos < 5:
        _fail(f"Min fold positives={min_pos} — some folds have too few positives (need ≥ 5)")
        critical = True
    else:
        _pass(f"Min fold positives={min_pos} ≥ 5")

    if auc_vals:
        auc_range = max(auc_vals) - min(auc_vals)
        if auc_range > 0.15:
            _warn(f"High AUC variance across folds: range={auc_range:.4f} — potential non-stationarity")
        else:
            _pass(f"AUC fold range={auc_range:.4f} within tolerance")

        if max(brier_vals) > min(brier_vals) * 2.5 and len(brier_vals) > 1:
            _warn("Brier score diverges across folds (>2.5× ratio)")

    return critical, fold_results


# ──────────────────────────────────────────────────────────────────────────────
# 5.3  Precision@K
# ──────────────────────────────────────────────────────────────────────────────

def check_precision_at_k(
    dataset_rows: list[dict[str, Any]],
    feature_cols: list[str],
) -> None:
    _section("5.3  Precision@K (OOF predictions)")

    try:
        import numpy as np
        from sklearn.linear_model import LogisticRegression
        from sklearn.model_selection import StratifiedKFold
        from sklearn.preprocessing import StandardScaler
    except ImportError as e:
        _warn(f"sklearn not available: {e}")
        return

    import numpy as np

    def _ff_local(v: Any, d: float = 0.0) -> float:
        try:
            return float(v) if v is not None else d
        except Exception:
            return d

    X = np.array([
        [_ff_local((r.get("indicators") or {}).get(k)) for k in feature_cols]
        for r in dataset_rows
    ], dtype=np.float64)
    y = np.array([int(r.get("y_edge", 0) or 0) for r in dataset_rows], dtype=np.int64)

    col_medians = np.nanmedian(X, axis=0)
    for j in range(X.shape[1]):
        nan_mask = np.isnan(X[:, j])
        X[nan_mask, j] = col_medians[j]

    oof_p = np.zeros(len(y), dtype=np.float64)
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    for tr, te in skf.split(X, y):
        sc = StandardScaler().fit(X[tr])
        lr = LogisticRegression(C=1.0, max_iter=2000, random_state=42)
        lr.fit(sc.transform(X[tr]), y[tr])
        oof_p[te] = lr.predict_proba(sc.transform(X[te]))[:, 1]

    baseline = float(y.mean())
    n = len(y)
    print(f"\n  Baseline pos_rate: {baseline:.4f}  ({baseline*100:.2f}%)")
    print(f"\n  {'k':>6}  {'k_count':>8}  {'Prec@k':>8}  {'vs_base':>9}  {'lift':>6}")
    print(f"  {_hline('-', 44)}")
    for k_frac in [0.01, 0.03, 0.05, 0.10]:
        k_count = max(1, int(n * k_frac))
        prec = _precision_at_k(y, oof_p, k_frac)
        lift = prec / baseline if baseline > 0 else 0.0
        delta = prec - baseline
        flag = "✓" if lift >= 2.0 else ("⚠" if lift >= 1.2 else "✗")
        print(f"  {k_frac*100:>5.0f}%  {k_count:>8,}  {prec:>8.4f}  "
              f"{delta:>+9.4f}  {lift:>5.2f}×  {flag}")

    print(f"\n  Threshold: lift ≥ 2× = good, ≥ 1.2× = acceptable, < 1.2× = weak")


# ──────────────────────────────────────────────────────────────────────────────
# 5.4  Calibration reliability table
# ──────────────────────────────────────────────────────────────────────────────

def check_calibration(
    dataset_rows: list[dict[str, Any]],
    feature_cols: list[str],
    n_bins: int = 10,
) -> None:
    _section("5.4  Calibration — reliability table (OOF predictions)")

    try:
        import numpy as np
        from sklearn.linear_model import LogisticRegression
        from sklearn.model_selection import StratifiedKFold
        from sklearn.preprocessing import StandardScaler
    except ImportError as e:
        _warn(f"sklearn not available: {e}")
        return

    import numpy as np

    def _ff_local(v: Any, d: float = 0.0) -> float:
        try:
            return float(v) if v is not None else d
        except Exception:
            return d

    X = np.array([
        [_ff_local((r.get("indicators") or {}).get(k)) for k in feature_cols]
        for r in dataset_rows
    ], dtype=np.float64)
    y = np.array([int(r.get("y_edge", 0) or 0) for r in dataset_rows], dtype=np.int64)

    col_medians = np.nanmedian(X, axis=0)
    for j in range(X.shape[1]):
        nan_mask = np.isnan(X[:, j])
        X[nan_mask, j] = col_medians[j]

    oof_p = np.zeros(len(y), dtype=np.float64)
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    for tr, te in skf.split(X, y):
        sc = StandardScaler().fit(X[tr])
        lr = LogisticRegression(C=1.0, max_iter=2000, random_state=42)
        lr.fit(sc.transform(X[tr]), y[tr])
        oof_p[te] = lr.predict_proba(sc.transform(X[te]))[:, 1]

    print(f"\n  {'Bucket':>12}  {'n':>6}  {'pred_avg':>9}  {'actual_rate':>12}  {'delta':>8}  {'status':>6}")
    print(f"  {_hline('-', 60)}")

    max_gap = 0.0
    for i in range(n_bins):
        lo = i / n_bins
        hi = (i + 1) / n_bins
        if i == n_bins - 1:
            mask = (oof_p >= lo) & (oof_p <= hi)
        else:
            mask = (oof_p >= lo) & (oof_p < hi)
        n_bucket = int(mask.sum())
        if n_bucket == 0:
            print(f"  {lo:.2f}-{hi:.2f}      {'':>6}  {'':>9}  {'':>12}  {'':>8}  empty")
            continue
        pred_avg = float(np.mean(oof_p[mask]))
        actual = float(np.mean(y[mask]))
        delta = actual - pred_avg
        max_gap = max(max_gap, abs(delta))
        flag = "ok" if abs(delta) < 0.10 else ("⚠" if abs(delta) < 0.20 else "✗")
        print(f"  {lo:.2f}-{hi:.2f}  {n_bucket:>6,}  {pred_avg:>9.4f}  {actual:>12.4f}"
              f"  {delta:>+8.4f}  {flag:>6}")

    print()
    if max_gap >= 0.20:
        _warn(f"Max calibration gap={max_gap:.4f} — model probabilities need isotonic recal before use as scores")
    elif max_gap >= 0.10:
        _warn(f"Moderate calibration gap={max_gap:.4f} — useful for ranking, not as literal probabilities")
    else:
        _pass(f"Calibration gap={max_gap:.4f} — well-calibrated")

    ece = _ece(y.astype(float), oof_p)
    print(f"  ECE (overall): {ece:.4f}")


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description="v14_of dataset production-readiness checks")
    ap.add_argument("--work-dir", default=os.environ.get("V14_WORK_DIR", "/tmp/v14_of_train"))
    ap.add_argument("--dataset", default="", help="Explicit path to ml_dataset_v14.jsonl")
    ap.add_argument("--tb-labels", default="", help="Explicit path to tb_labels.ndjson")
    ap.add_argument("--schema-ver", default=os.environ.get("V14_FEATURE_SCHEMA_VER", "v15_of"))
    ap.add_argument("--skip-folds", action="store_true", help="Skip CV fold metrics (fast mode)")
    args = ap.parse_args()

    work_dir = Path(args.work_dir)

    dataset_path = Path(args.dataset) if args.dataset else work_dir / "ml_dataset_v14.jsonl"
    tb_path = Path(args.tb_labels) if args.tb_labels else work_dir / "tb_labels.ndjson"

    print(f"\n{'═' * 72}")
    print(f"  v14_of Dataset Validation Report")
    print(f"  dataset : {dataset_path}")
    print(f"  schema  : {args.schema_ver}")
    print(f"{'═' * 72}")

    if not dataset_path.exists():
        print(f"\n  ERROR: Dataset not found: {dataset_path}")
        print("  Run the nightly bundle first:")
        print("    python -m tools.nightly_v14_of_train_bundle")
        return 2

    dataset_rows = _read_ndjson(dataset_path)
    tb_rows = _read_ndjson(tb_path) if tb_path.exists() else []

    if not dataset_rows:
        print("\n  ERROR: Dataset is empty")
        return 2

    print(f"\n  Loaded {len(dataset_rows):,} dataset rows, {len(tb_rows):,} tb_label rows")

    # Load feature cols
    try:
        if args.schema_ver == "v14_of":
            from core.ml_feature_schema_v14_of import get_v14_of_numeric_keys
            feature_cols = get_v14_of_numeric_keys()
        else:
            from core.ml_feature_schema_v15_of import get_v15_of_numeric_keys
            feature_cols = get_v15_of_numeric_keys()
        print(f"  Feature cols: {len(feature_cols)} ({args.schema_ver})")
    except Exception as e:
        print(f"  WARNING: Could not load feature schema ({e}) — using indicators keys from first row")
        indicators_ex = dataset_rows[0].get("indicators") or {}
        feature_cols = [k for k, v in indicators_ex.items()
                        if isinstance(v, (int, float)) and not isinstance(v, bool)]
        print(f"  Feature cols: {len(feature_cols)} (auto-detected)")

    any_critical = False
    exit_code = 0

    # ── 5.1 ──────────────────────────────────────────────────────────────────
    crit, warns = check_integrity(dataset_rows, tb_rows)
    if crit:
        any_critical = True

    # ── 5.2 ──────────────────────────────────────────────────────────────────
    if not args.skip_folds:
        crit2, _ = check_folds(dataset_rows, feature_cols)
        if crit2:
            any_critical = True

        # ── 5.3 ──────────────────────────────────────────────────────────────
        check_precision_at_k(dataset_rows, feature_cols)

        # ── 5.4 ──────────────────────────────────────────────────────────────
        check_calibration(dataset_rows, feature_cols)
    else:
        _section("5.2–5.4  Skipped (--skip-folds)")

    # ── Summary ──────────────────────────────────────────────────────────────
    _section("Summary")
    if any_critical:
        _fail("CRITICAL gates failed — NOT production-ready")
        exit_code = 1
    elif warns:
        _warn(f"Warnings ({len(warns)}) — review before promoting: {warns}")
        exit_code = 2
    else:
        _pass("All gates passed — dataset is production-ready")
        exit_code = 0

    print()
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
