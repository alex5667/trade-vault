"""
Phase-3 scorer ML fusion trainer (stub v1).

See docs/PHASE3_ML_FUSION_REDESIGN.md for the full contract.

What this script does:
  1. Loads labeled signals from edge_live JSONL window.
  2. Builds feature matrix from a curated whitelist of OrderflowSignalContext fields.
  3. Trains LightGBM binary classifier P(y=1 | features) with chronological split.
  4. Validates on holdout (AUC, precision@top5%, expectancy_R, ECE).
  5. Writes scorer_model.lgb + scorer_model.features ONLY if validation gates pass.
  6. Always writes scorer_model.report.json for debug.

This is a STUB: the feature whitelist and hyperparameters are fixed defaults.
Tune after first successful training run on real data.

Not enabled in compose by default (PHASE3_TRAINER_ENABLE=0).
"""

from __future__ import annotations

import argparse
import glob
import json
import logging
import math
import os
import time
from typing import Any

from core.scorer_categorical_features import (
    SCORER_CATEGORICAL_FEATURES,
    encode_categorical_from_record,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("phase3_trainer")

DEFAULT_INPUT_DIR = "/var/lib/trade/of_reports/out/confidence_cal_live"
DEFAULT_OUTPUT_DIR = "python-worker/ml_models"
MODEL_FILENAME = "scorer_model.lgb"
FEATURES_FILENAME = "scorer_model.features"
REPORT_FILENAME = "scorer_model.report.json"
READY_MARKER = "scorer_model.ready"

# Feature whitelist. Names must match OrderflowSignalContext attributes
# (python-worker/contexts.py) so scorer's getattr(ctx, fn, 0.0) works.
FEATURE_WHITELIST: list[str] = [
    "delta_z",
    "obi_z",
    "weak_ratio",
    "atr_q",
    "atr_local_q",
    "cvd_tick",
    "cvd_ema",
    "cvd_slope",
    "delta_tick",
    "delta_ema",
    "pressure_per_min_ema",
    "pressure_sps",
    "spread_bps",
    "spread_bps_z",
    "microprice_shift_bps_20",
    "obi_avg_20",
    "obi_local_q",
    "delta_spike_z",
    "delta_spike_z_local_q",
    "wall_bid_dist_bps",
    "wall_ask_dist_bps",
    "liq_score",
    "liq_spread_bps",
    "depth_bid_5",
    "depth_ask_5",
    "depth_bid_20",
    "depth_ask_20",
    "slope_bid_20",
    "slope_ask_20",
    "trend_score",
    "range_score",
]


def _f(x: Any, default: float = float("nan")) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except (TypeError, ValueError):
        return default


def load_dataset(
    input_dir: str,
    since_ms: int,
    features: list[str],
    *,
    y_min_r_override: float | None = None,
    max_samples_per_symbol: int = 0,
) -> tuple[list[list[float]], list[int], list[float], list[int], list[str]]:
    """Returns (X, y, r_mult, ts_ms, kept_features). NaN rows are dropped.

    max_samples_per_symbol: if > 0, keep only the most-recent N rows per symbol.
    Prevents a high-volume, low-quality symbol from dominating the training set
    and corrupting feature-scale distributions for other symbols.
    """
    paths = sorted(glob.glob(os.path.join(input_dir, "edge_live_[0-9]*.jsonl")))
    if not paths:
        logger.warning("no JSONL files under %s", input_dir)
        return [], [], [], [], features

    # First pass: count per-feature non-null coverage across a representative sample.
    # Sample files spread across the whole window (not just the newest 3) so that
    # recently-added features don't inflate coverage and then NaN-drop old rows.
    n_cov = max(3, min(20, len(paths) // 5 + 1))
    step = max(1, len(paths) // n_cov)
    coverage_paths = paths[::step][:n_cov]

    coverage: dict[str, int] = {f: 0 for f in features}
    total = 0
    for p in coverage_paths:
        try:
            with open(p, encoding="utf-8") as f:
                for line in f:
                    try:
                        d = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    inds = d.get("indicators") or {}
                    if not isinstance(inds, dict):
                        continue
                    ts_val = int(d.get("ts_ms", 0) or 0)
                    if ts_val > 0 and ts_val < since_ms:
                        continue
                    total += 1
                    for feat in features:
                        if feat in inds and _f(inds[feat], float("nan")) == _f(inds[feat], float("nan")):
                            coverage[feat] += 1
        except OSError:
            continue

    # Keep features with > 50% coverage on the sample. Logs filtered ones.
    kept_features = []
    dropped = []
    for f, c in coverage.items():
        ratio = c / total if total > 0 else 0.0
        if ratio >= 0.5:
            kept_features.append(f)
        else:
            dropped.append((f, ratio))
    if dropped:
        logger.info("dropped %d features below 50%% coverage: %s", len(dropped), dropped[:10])
    logger.info("kept %d features", len(kept_features))

    # Collect all rows with symbol tag so per-symbol cap can be applied.
    # Each entry: (ts_ms_val, symbol, feat_row, y_val, r_val)
    # Row layout: [continuous features in kept_features order] + [categorical in SCORER_CATEGORICAL_FEATURES order]
    all_rows: list[tuple[int, str, list[float], int, float]] = []

    for p in paths:
        try:
            stem = os.path.basename(p)
            ts_str = stem.replace("edge_live_", "").replace(".jsonl", "")
            file_ts_ms = int(ts_str)
            if file_ts_ms + 6 * 3600 * 1000 < since_ms:
                continue
        except ValueError:
            pass
        try:
            with open(p, encoding="utf-8") as fp:
                for line in fp:
                    try:
                        d = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    inds = d.get("indicators") or {}
                    if not isinstance(inds, dict):
                        continue
                    if int(d.get("ts_ms", 0) or 0) < since_ms:
                        continue
                    r = d.get("r_mult", d.get("r_multiple"))
                    if r is None:
                        continue
                    row: list[float] = []
                    has_nan = False
                    for feat in kept_features:
                        v = _f(inds.get(feat), float("nan"))
                        if v != v:  # NaN check
                            has_nan = True
                            break
                        row.append(v)
                    if has_nan:
                        continue
                    # Append categorical features (always derivable; never NaN).
                    cat = encode_categorical_from_record(d, inds)
                    for cat_name in SCORER_CATEGORICAL_FEATURES:
                        row.append(float(cat[cat_name]))
                    rv = _f(r, float("nan"))
                    if rv != rv:
                        continue
                    if y_min_r_override is not None:
                        yy = 1 if rv >= y_min_r_override else 0
                    else:
                        yy_raw = d.get("y")
                        if yy_raw is not None:
                            try:
                                yy = 1 if int(yy_raw) else 0
                            except (TypeError, ValueError):
                                yy = 1 if rv > 0 else 0
                        else:
                            yy = 1 if rv > 0 else 0
                    sym = str(d.get("symbol") or "").upper()
                    all_rows.append((int(d.get("ts_ms", 0) or 0), sym, row, yy, rv))
        except OSError as e:
            logger.warning("cannot read %s: %s", p, e)

    # Sort chronologically before capping so we keep the most-recent N per symbol.
    all_rows.sort(key=lambda r: r[0])

    if max_samples_per_symbol > 0:
        # Walk backwards (newest first), keep up to max per symbol.
        sym_counts: dict[str, int] = {}
        kept: list[tuple[int, str, list[float], int, float]] = []
        for entry in reversed(all_rows):
            sym = entry[1]
            if sym_counts.get(sym, 0) < max_samples_per_symbol:
                sym_counts[sym] = sym_counts.get(sym, 0) + 1
                kept.append(entry)
        all_rows = sorted(kept, key=lambda r: r[0])
        logger.info(
            "per-symbol cap=%d applied: %s",
            max_samples_per_symbol,
            {k: v for k, v in sorted(sym_counts.items())},
        )

    X = [r[2] for r in all_rows]
    y = [r[3] for r in all_rows]
    r_mult = [r[4] for r in all_rows]
    ts_ms = [r[0] for r in all_rows]

    # Append categorical feature names to the public output so the scorer
    # (inference path) writes them into scorer_model.features and can route
    # `_cat_*` lookups through `encode_categorical_from_ctx`.
    final_features = kept_features + list(SCORER_CATEGORICAL_FEATURES)
    return X, y, r_mult, ts_ms, final_features


def _auc(y_true: list[int], y_score: list[float]) -> float:
    pos = [s for s, yy in zip(y_score, y_true) if yy == 1]
    neg = [s for s, yy in zip(y_score, y_true) if yy == 0]
    if not pos or not neg:
        return 0.5
    wins = 0
    for p in pos:
        for n in neg:
            if p > n:
                wins += 1
            elif p == n:
                wins += 0.5
    return wins / (len(pos) * len(neg))


def _ece(y_true: list[int], y_prob: list[float], bins: int = 10) -> float:
    n = len(y_true)
    if n == 0:
        return 1.0
    bucket_sum = [0.0] * bins
    bucket_pos = [0] * bins
    bucket_n = [0] * bins
    for yp, yt in zip(y_prob, y_true):
        idx = min(int(yp * bins), bins - 1)
        bucket_sum[idx] += yp
        bucket_pos[idx] += yt
        bucket_n[idx] += 1
    ece = 0.0
    for i in range(bins):
        if bucket_n[i] == 0:
            continue
        avg_p = bucket_sum[i] / bucket_n[i]
        avg_y = bucket_pos[i] / bucket_n[i]
        ece += (bucket_n[i] / n) * abs(avg_p - avg_y)
    return ece


def evaluate(y_true: list[int], y_prob: list[float], r_mult: list[float]) -> dict[str, float]:
    n = len(y_true)
    if n == 0:
        return {"n": 0, "auc": 0.5, "precision_top5pct": 0.0, "expectancy_r_top5pct": 0.0, "ece": 1.0}
    top_n = max(1, int(n * 0.05))
    order = sorted(range(n), key=lambda i: y_prob[i], reverse=True)
    top = order[:top_n]
    precision = sum(y_true[i] for i in top) / top_n
    expectancy = sum(r_mult[i] for i in top) / top_n
    return {
        "n": n,
        "auc": _auc(y_true, y_prob),
        "precision_top5pct": precision,
        "expectancy_r_top5pct": expectancy,
        "ece": _ece(y_true, y_prob),
    }


def validate_gates(holdout_m: dict[str, float], baseline_precision: float) -> tuple[bool, list[str]]:
    fail: list[str] = []
    if int(holdout_m["n"]) < 1500:
        fail.append(f"holdout_n={int(holdout_m['n'])}<2000")
    if holdout_m["auc"] < 0.55:
        fail.append(f"auc={holdout_m['auc']:.3f}<0.55")
    if holdout_m["precision_top5pct"] < baseline_precision + 0.03:
        fail.append(f"precision_gain={holdout_m['precision_top5pct']-baseline_precision:+.3f}<0.03")
    if holdout_m["expectancy_r_top5pct"] < 0.0:
        fail.append(f"expectancy_r={holdout_m['expectancy_r_top5pct']:+.3f}<0")
    if holdout_m["ece"] > 0.10:
        fail.append(f"ece={holdout_m['ece']:.3f}>0.10")
    return (len(fail) == 0, fail)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--input-dir", default=os.getenv("PHASE3_INPUT_DIR", DEFAULT_INPUT_DIR))
    p.add_argument("--output-dir", default=os.getenv("PHASE3_OUTPUT_DIR", DEFAULT_OUTPUT_DIR))
    p.add_argument("--lookback-hours", type=int, default=int(os.getenv("PHASE3_LOOKBACK_HOURS", "336")))
    p.add_argument("--min-total-n", type=int, default=int(os.getenv("PHASE3_MIN_TOTAL_N", "8000")))
    p.add_argument("--holdout-frac", type=float, default=float(os.getenv("PHASE3_HOLDOUT_FRAC", "0.30")))
    p.add_argument(
        "--max-samples-per-symbol",
        type=int,
        default=int(os.getenv("PHASE3_MAX_SAMPLES_PER_SYMBOL", "0")),
        help="Cap training rows per symbol to most-recent N (0 = unlimited). "
             "Prevents high-volume symbols from contaminating feature scales.",
    )
    p.add_argument("--dry-run", action="store_true", help="compute but do not write artifacts")
    p.add_argument(
        "--y-min-r-override",
        type=float,
        default=None if not os.getenv("PHASE3_Y_MIN_R_OVERRIDE") else float(os.getenv("PHASE3_Y_MIN_R_OVERRIDE", "0")),
        help="Cost-aware label: override JSONL y with r_mult >= threshold (e.g. 0.25 covers fees+slip)",
    )
    args = p.parse_args()

    enabled = os.getenv("PHASE3_TRAINER_ENABLE", "0").strip().lower() in ("1", "true", "yes", "on")
    write_artifacts = enabled and not args.dry_run

    now_ms = int(time.time() * 1000)
    since_ms = now_ms - args.lookback_hours * 3600 * 1000

    X, y, r_mult, _ts_ms, kept_features = load_dataset(
        args.input_dir,
        since_ms,
        FEATURE_WHITELIST,
        y_min_r_override=args.y_min_r_override,
        max_samples_per_symbol=args.max_samples_per_symbol,
    )
    logger.info(
        "loaded %d samples; features kept=%d; y_min_r_override=%s",
        len(X), len(kept_features), args.y_min_r_override
    )

    os.makedirs(args.output_dir, exist_ok=True)
    report_path = os.path.join(args.output_dir, REPORT_FILENAME)

    if len(X) < args.min_total_n:
        report = {
            "version": "3.0.0-stub",
            "generated_at_ms": now_ms,
            "lookback_hours": args.lookback_hours,
            "n_samples": len(X),
            "kept_features": kept_features,
            "validation": {"pass": False, "reasons": [f"insufficient_samples({len(X)}<{args.min_total_n})"]},
        }
        _write_report(report, report_path)
        return 0

    try:
        import joblib  # type: ignore
        from lightgbm import LGBMClassifier  # type: ignore
    except ImportError as e:
        report = {
            "version": "3.0.0-stub",
            "generated_at_ms": now_ms,
            "validation": {"pass": False, "reasons": [f"missing_dependency: {e}"]},
        }
        _write_report(report, report_path)
        logger.error("missing dependency: %s", e)
        return 2

    split = int(len(X) * (1.0 - args.holdout_frac))
    X_train, X_hold = X[:split], X[split:]
    y_train, y_hold = y[:split], y[split:]
    r_hold = r_mult[split:]
    logger.info("split: train=%d holdout=%d", len(X_train), len(X_hold))

    # Train baseline: predict overall positive rate (so we can measure precision gain).
    base_p = sum(y_hold) / max(1, len(y_hold))
    # Baseline precision_top5pct on a "random" sort equals overall positive rate.
    baseline_precision = base_p
    logger.info("baseline holdout positive_rate=%.4f", base_p)

    model = LGBMClassifier(
        num_leaves=15,
        max_depth=4,
        learning_rate=0.05,
        n_estimators=200,
        min_child_samples=50,
        reg_alpha=0.1,
        reg_lambda=0.1,
        random_state=42,
        verbose=-1,
    )
    # LightGBM native categorical handling: indices of `_cat_*` columns.
    cat_indices = [i for i, fn in enumerate(kept_features) if fn.startswith("_cat_")]
    t0 = time.time()
    model.fit(X_train, y_train, categorical_feature=cat_indices if cat_indices else "auto")
    fit_sec = time.time() - t0
    logger.info("fit done in %.2fs (categorical_indices=%s)", fit_sec, cat_indices)

    y_hold_prob = [float(p) for p in model.predict_proba(X_hold)[:, 1]]
    holdout_m = evaluate(y_hold, y_hold_prob, r_hold)
    logger.info("holdout metrics: %s", holdout_m)

    ok, fails = validate_gates(holdout_m, baseline_precision)

    importance = list(zip(kept_features, model.feature_importances_.tolist()))
    importance.sort(key=lambda x: x[1], reverse=True)
    top_imp = importance[:10]
    if importance and importance[0][1] > 0:
        total_imp = sum(imp for _, imp in importance)
        if total_imp > 0 and importance[0][1] / total_imp > 0.60:
            fails.append(f"single_feature_dominance: {importance[0][0]}={importance[0][1]/total_imp:.2%}")
            ok = False

    report = {
        "version": "3.0.0-stub",
        "generated_at_ms": now_ms,
        "lookback_hours": args.lookback_hours,
        "n_samples": len(X),
        "n_train": len(X_train),
        "n_holdout": len(X_hold),
        "kept_features": kept_features,
        "label_config": {"y_min_r_override": args.y_min_r_override, "source": "cost_aware_r_thresh" if args.y_min_r_override else "jsonl_y"},
        "baseline_precision_top5pct": baseline_precision,
        "holdout_metrics": holdout_m,
        "fit_seconds": round(fit_sec, 2),
        "top_feature_importance": [{"feature": f, "importance": int(i)} for f, i in top_imp],
        "validation": {"pass": ok, "reasons": fails, "holdout_split": args.holdout_frac},
    }

    if ok and write_artifacts:
        model_path = os.path.join(args.output_dir, MODEL_FILENAME)
        features_path = os.path.join(args.output_dir, FEATURES_FILENAME)
        joblib.dump(model, model_path)
        joblib.dump(kept_features, features_path)
        marker_path = os.path.join(args.output_dir, READY_MARKER)
        with open(marker_path, "w", encoding="utf-8") as f:
            f.write(str(now_ms))
        logger.info("wrote model + features + ready marker (validation pass)")
        report["artifacts"] = {
            "model": model_path,
            "features": features_path,
            "ready_marker": marker_path,
        }
    elif ok and not write_artifacts:
        logger.info("validation pass BUT PHASE3_TRAINER_ENABLE!=1 or --dry-run; not writing model")
        report["artifacts"] = "not_written_disabled"
    else:
        logger.warning("validation FAILED: %s", fails)
        report["artifacts"] = "not_written_validation_failed"

    _write_report(report, report_path)
    return 0


def _write_report(report: dict[str, Any], path: str) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=float)
    os.replace(tmp, path)
    logger.info("wrote %s (pass=%s, n=%d)", path,
                report.get("validation", {}).get("pass"),
                report.get("n_samples", 0))


if __name__ == "__main__":
    raise SystemExit(main())
