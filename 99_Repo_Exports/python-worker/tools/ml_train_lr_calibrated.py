
from __future__ import annotations

import argparse
import json
import os
from typing import Any

import joblib  # type: ignore
import numpy as np
from sklearn.calibration import CalibratedClassifierCV
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, log_loss

from core.ml_feature_schema import build_feature_vector, feature_names
from core.ml_metrics_utils import brier_score, ece_score

# Passed-trade tokens mirror core.reject_reason_weights._PASSED_TOKENS so we
# don't need a hard dependency on it just for the gate.
_PASSED_TOKENS = frozenset({"", "OK", "ok", "PASSED", "passed", "ALLOW", "allow"})


def _is_real_passed(row: dict[str, Any]) -> bool:
    """Real (not virtual) trade that passed every gate."""
    if int(row.get("is_virtual", 0) or 0) == 1:
        return False
    reason = str(row.get("v_gate_reason", "") or "").strip()
    return reason in _PASSED_TOKENS


def _env_flag(name: str, default: str = "0") -> bool:
    return (os.environ.get(name, default) or default).strip().lower() in ("1", "true", "yes", "on")


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, "") or default)
    except (TypeError, ValueError):
        return default


def _resolve_slippage_bps(row: dict[str, Any], fallback_bps: float) -> float:
    """Match resolution rules of tools/ml_confirm_cost_aware_label_v1.py."""
    for key in ("slippage_realized_bps", "expected_slippage_bps"):
        v = row.get(key)
        if v is None:
            continue
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if f >= 0.0:
            return f
    return max(0.0, fallback_bps)


def _cost_aware_y(row: dict[str, Any], *, fee_mul: float, slippage_bps_fallback: float) -> int:
    """y_cost_aware = pnl_net - fee_mul*fees - slippage_realized_usd > 0.

    Falls back to y_edge / r_mult > 0 when cost fields are not present.
    """
    pnl_net = row.get("pnl_net")
    fees = row.get("fees")
    risk_usd = row.get("risk_usd") or 0.0
    if pnl_net is None and fees is None:
        # Legacy row — keep behaviour identical to original trainer.
        return int(row.get("y_edge", 0) or 0)
    try:
        pnl_net_f = float(pnl_net or 0.0)
        fees_f = float(fees or 0.0)
        risk_f = float(risk_usd or 0.0)
    except (TypeError, ValueError):
        return int(row.get("y_edge", 0) or 0)
    bps = _resolve_slippage_bps(row, slippage_bps_fallback)
    slip_usd = (bps / 10_000.0) * abs(risk_f)
    cost = fee_mul * fees_f + slip_usd
    return 1 if (pnl_net_f - cost) > 0.0 else 0


def load_dataset(path: str) -> list[dict[str, Any]]:
    out = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out


def ece_score(y_true: np.ndarray, p: np.ndarray, n_bins: int = 15) -> float:
    # Expected Calibration Error
    bins = np.linspace(0.0, 1.0, n_bins + 1)
    ece = 0.0
    for i in range(n_bins):
        lo, hi = bins[i], bins[i+1]
        mask = (p >= lo) & (p < hi) if i < n_bins - 1 else (p >= lo) & (p <= hi)
        if not np.any(mask):
            continue
        acc = float(np.mean(y_true[mask]))
        conf = float(np.mean(p[mask]))
        w = float(np.mean(mask))
        ece += w * abs(acc - conf)
    return float(ece)


def build_xy(
    rows: list[dict[str, Any]],
    *,
    use_cost_aware_label: bool = False,
    cost_aware_fee_mul: float = 2.0,
    cost_aware_slip_bps_fallback: float = 4.0,
) -> tuple[np.ndarray, np.ndarray, list[int], np.ndarray]:
    """Returns (X, y, ts, w). w is the per-sample IPS weight (defaults to 1.0)."""
    X: list[Any] = []
    y: list[int] = []
    ts: list[int] = []
    w: list[float] = []
    for r in rows:
        xraw = r.get("x")
        # Support both formats: list of features (from nightly pipeline) or dict (legacy)
        if isinstance(xraw, list):
            # Already extracted features from nightly pipeline
            X.append(xraw)
        elif isinstance(xraw, dict):
            # Legacy format: extract features from dict
            indicators = dict(xraw)
            vec, _miss = build_feature_vector(
                symbol=(xraw.get("symbol","")),
                ts_ms=int(xraw.get("ts_ms", 0)),
                direction=(xraw.get("direction","")),
                scenario=(xraw.get("scenario","")),
                indicators=indicators,
                rule_score=float(xraw.get("score", xraw.get("rule_score", 0.0)) or 0.0),
                rule_have=int(xraw.get("have", xraw.get("rule_have", 0)) or 0),
                rule_need=int(xraw.get("need", xraw.get("rule_need", 0)) or 0),
                cancel_spike_veto=int(xraw.get("cancel_spike_veto", 0) or 0),
            )
            X.append(vec)
        else:
            continue
        if use_cost_aware_label:
            y.append(_cost_aware_y(
                r,
                fee_mul=cost_aware_fee_mul,
                slippage_bps_fallback=cost_aware_slip_bps_fallback,
            ))
        else:
            y.append(int(r.get("y_edge", 0) or r.get("y", 0) or 0))
        ts.append(int(r.get("ts_ms", 0)))
        try:
            wv = float(r.get("ips_weight", 1.0) or 1.0)
        except (TypeError, ValueError):
            wv = 1.0
        if wv <= 0.0:
            wv = 1.0
        w.append(wv)
    Xn = np.asarray(X, dtype=np.float32)
    yn = np.asarray(y, dtype=np.int32)
    wn = np.asarray(w, dtype=np.float32)
    return Xn, yn, ts, wn


def time_split(rows: list[dict[str, Any]], test_share: float, calib_share: float) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    rows = sorted(rows, key=lambda r: int(r.get("ts_ms", 0)))
    n = len(rows)
    n_test = int(max(1, n * test_share))
    test = rows[-n_test:]
    train_all = rows[:-n_test]
    n_cal = int(max(1, len(train_all) * calib_share))
    calib = train_all[-n_cal:]
    train_fit = train_all[:-n_cal]
    return train_fit, calib, test


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--out-model", required=True)
    ap.add_argument("--out-meta", required=True)
    ap.add_argument("--calib", choices=["sigmoid","isotonic"], default="sigmoid")
    ap.add_argument("--test-share", type=float, default=0.30)
    ap.add_argument("--calib-share", type=float, default=0.20)
    ap.add_argument("--C", type=float, default=1.0)
    ap.add_argument("--max-iter", type=int, default=500)
    ap.add_argument(
        "--use-ips-weights",
        action="store_true",
        default=_env_flag("ML_TRAIN_USE_IPS_WEIGHTS", "1"),
        help="Pass sample_weight=ips_weight to lr.fit and cal.fit (default on; env ML_TRAIN_USE_IPS_WEIGHTS).",
    )
    ap.add_argument(
        "--calibrate-real-only",
        action="store_true",
        default=_env_flag("ML_TRAIN_CALIBRATION_REAL_ONLY", "1"),
        help="Restrict the calibration fold to real-passed rows (is_virtual=0 AND v_gate_reason in passed). Default on.",
    )
    ap.add_argument(
        "--use-cost-aware-label",
        action="store_true",
        default=_env_flag("ML_TRAIN_USE_COST_AWARE_LABEL", "0"),
        help="Switch y from y_edge to y_cost_aware (pnl_net - fee_mul*fees - slippage_realized_usd > 0).",
    )
    ap.add_argument("--cost-aware-fee-mul", type=float, default=_env_float("COSTAWARE_FEE_MUL", 2.0))
    ap.add_argument("--cost-aware-slip-bps-fallback", type=float, default=_env_float("COSTAWARE_SLIPPAGE_BPS_FALLBACK", 4.0))
    args = ap.parse_args()

    rows = load_dataset(args.dataset)
    train_fit, calib, test = time_split(rows, args.test_share, args.calib_share)

    # Calibration layer is fit ONLY on real-passed trades by default — Platt/
    # isotonic must reflect prod-distribution semantics, not the widened
    # virtual+shadow training pool. See Hand & Henley 1997 + López de Prado AFML ch.3.
    if args.calibrate_real_only:
        calib_filtered = [r for r in calib if _is_real_passed(r)]
        if not calib_filtered:
            # Fallback: if filter empties the calibration fold, keep original so
            # nightly does not crash; emit a meta flag for downstream alerting.
            calib_filtered = calib
            calib_real_only_applied = False
        else:
            calib_real_only_applied = True
        calib = calib_filtered
    else:
        calib_real_only_applied = False

    X_train, y_train, _, w_train = build_xy(
        train_fit,
        use_cost_aware_label=bool(args.use_cost_aware_label),
        cost_aware_fee_mul=float(args.cost_aware_fee_mul),
        cost_aware_slip_bps_fallback=float(args.cost_aware_slip_bps_fallback),
    )
    X_cal, y_cal, _, w_cal = build_xy(
        calib,
        use_cost_aware_label=bool(args.use_cost_aware_label),
        cost_aware_fee_mul=float(args.cost_aware_fee_mul),
        cost_aware_slip_bps_fallback=float(args.cost_aware_slip_bps_fallback),
    )
    X_test, y_test, _, _w_test = build_xy(
        test,
        use_cost_aware_label=bool(args.use_cost_aware_label),
        cost_aware_fee_mul=float(args.cost_aware_fee_mul),
        cost_aware_slip_bps_fallback=float(args.cost_aware_slip_bps_fallback),
    )

    # Base LR
    lr = LogisticRegression(
        C=float(args.C),
        max_iter=int(args.max_iter),
        solver="lbfgs",
        n_jobs=1,
    )
    _sw_train = w_train if args.use_ips_weights else None
    lr.fit(X_train, y_train, sample_weight=_sw_train)

    # Platt/Isotonic calibration on temporal holdout (calib set)
    cal = CalibratedClassifierCV(lr, method=args.calib, cv="prefit")
    _sw_cal = w_cal if args.use_ips_weights else None
    cal.fit(X_cal, y_cal, sample_weight=_sw_cal)

    # Evaluate on test
    p_test = cal.predict_proba(X_test)[:, 1]
    pr_auc = float(average_precision_score(y_test, p_test)) if len(set(y_test.tolist())) > 1 else 0.0
    ll = float(log_loss(y_test, p_test, eps=1e-12))
    brier = float(brier_score(y_test.tolist(), p_test.tolist()))
    ece = float(ece_score(y_test.tolist(), p_test.tolist()))

    # Per-slice diagnostics: virtual vs real on the test fold.
    real_idx = [i for i, r in enumerate(test) if _is_real_passed(r)]
    virt_idx = [i for i, r in enumerate(test) if int(r.get("is_virtual", 0) or 0) == 1]
    def _slice_metrics(idx: list[int]) -> dict[str, float | int]:
        if not idx:
            return {"n": 0, "brier": 0.0, "ece": 0.0, "pr_auc": 0.0}
        y_s = y_test[idx].tolist()
        p_s = p_test[idx].tolist()
        try:
            pr = float(average_precision_score(y_s, p_s)) if len(set(y_s)) > 1 else 0.0
        except Exception:
            pr = 0.0
        return {
            "n": int(len(idx)),
            "brier": float(brier_score(y_s, p_s)),
            "ece": float(ece_score(y_s, p_s)),
            "pr_auc": pr,
        }
    slice_real = _slice_metrics(real_idx)
    slice_virt = _slice_metrics(virt_idx)
    virtual_share_train = float(sum(1 for r in train_fit if int(r.get("is_virtual", 0) or 0) == 1)) / float(max(1, len(train_fit)))

    meta = {
        "model_ver": "ml_confirm_lr_cal_v1",
        "calib": args.calib,
        "feature_names": feature_names(),
        "sizes": {"train": int(len(train_fit)), "calib": int(len(calib)), "test": int(len(test))},
        "metrics": {"pr_auc": pr_auc, "logloss": ll, "brier": brier, "ece": ece},
        "metrics_test": {"pr_auc": pr_auc, "logloss": ll, "brier": brier, "ece": ece},
        "metrics_test_slice_real_passed": slice_real,
        "metrics_test_slice_virtual": slice_virt,
        "train_virtual_share": virtual_share_train,
        "train_config": {
            "use_ips_weights": bool(args.use_ips_weights),
            "calibrate_real_only": bool(args.calibrate_real_only),
            "calibrate_real_only_applied": bool(calib_real_only_applied),
            "use_cost_aware_label": bool(args.use_cost_aware_label),
            "cost_aware_fee_mul": float(args.cost_aware_fee_mul),
            "cost_aware_slip_bps_fallback": float(args.cost_aware_slip_bps_fallback),
            "w_train_p50": float(np.percentile(w_train, 50)) if len(w_train) else 1.0,
            "w_train_p99": float(np.percentile(w_train, 99)) if len(w_train) else 1.0,
            "w_train_min": float(np.min(w_train)) if len(w_train) else 1.0,
        },
        "ts_range": {"train_start": int(train_fit[0]["ts_ms"]) if train_fit else 0,
                     "train_end": int(train_fit[-1]["ts_ms"]) if train_fit else 0,
                     "test_start": int(test[0]["ts_ms"]) if test else 0,
                     "test_end": int(test[-1]["ts_ms"]) if test else 0},
    }

    joblib.dump(cal, args.out_model)

    with open(args.out_meta, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(json.dumps(meta["metrics_test"], indent=2))

if __name__ == "__main__":
    main()
