"""calibration/conf_meta_gate_trainer.py — Plan 1 Phase 2 trainer.

Fits a logistic-regression scorer + calibrator on the dataset produced by
`conf_meta_gate_dataset.build_dataset`, evaluates it with the project's
purged walk-forward CV, and emits a JSON artifact compatible with
`services.confidence_meta_gate.model.MetaGateArtifact.load_artifact`.

Promotion is intentionally *external*: the trainer computes OOS metrics
and calls `calibration.promotion_gate.can_promote`. The decision —
write the artifact under the live path or only under the candidate path —
is the caller's responsibility (mirrors the v15_lgbm pattern).

Library footprint: numpy + sklearn (LR, isotonic), already in requirements.
"""
from __future__ import annotations

import json
import logging
import math
import os
import time
from dataclasses import dataclass, field
from typing import Any, Iterable

import numpy as np

from calibration.promotion_gate import (
    PromotionMetrics,
    PromotionThresholds,
    can_promote,
)
from calibration.purged_cv import purged_walkforward

log = logging.getLogger("conf_meta_gate.trainer")


# ── Config ──────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class TrainConfig:
    target: str = "y_util_pos"        # "y_win" | "y_util_pos"
    calibrator: str = "platt"         # "platt" | "isotonic" | "identity"
    n_cv_blocks: int = 6
    embargo_ms: int = 600_000
    min_rows: int = 1000
    # LR regularisation. Mirrors sklearn default but tightened — the
    # dataset is small + correlated, so a stronger prior is safer.
    l2_C: float = 0.5
    # Feature pre-selection: drop columns with > drop_missing_frac NaNs.
    drop_missing_frac: float = 0.5
    # Minimum coverage threshold; features below it are dropped.
    min_coverage: float = 0.30
    # Schema metadata propagated to the artifact.
    schema_name: str = "conf_meta_gate_schema_v1"
    model_ver_prefix: str = "conf_meta_gate_lr"


# ── Metrics ─────────────────────────────────────────────────────────────────


def expected_calibration_error(p: np.ndarray, y: np.ndarray, n_bins: int = 10) -> float:
    """Equal-width-binned ECE."""
    if p.size == 0:
        return 0.0
    bins = np.linspace(0.0, 1.0, n_bins + 1)
    idx = np.digitize(p, bins[1:-1], right=False)
    total = 0.0
    n = len(p)
    for b in range(n_bins):
        mask = idx == b
        if not mask.any():
            continue
        conf = p[mask].mean()
        acc = y[mask].mean()
        total += (mask.sum() / n) * abs(conf - acc)
    return float(total)


def brier_score(p: np.ndarray, y: np.ndarray) -> float:
    if p.size == 0:
        return 0.0
    return float(np.mean((p - y) ** 2))


def roc_auc(p: np.ndarray, y: np.ndarray) -> float:
    """AUC via the rank formula. Returns 0.5 when only one class is present."""
    if p.size == 0 or y.size == 0:
        return 0.5
    pos = (y == 1)
    neg = (y == 0)
    n_pos = int(pos.sum())
    n_neg = int(neg.sum())
    if n_pos == 0 or n_neg == 0:
        return 0.5
    order = np.argsort(p)
    ranks = np.empty_like(order, dtype=np.float64)
    ranks[order] = np.arange(1, len(p) + 1)
    sum_ranks_pos = float(ranks[pos].sum())
    return (sum_ranks_pos - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg)


def top_pct_expectancy(p: np.ndarray, r: np.ndarray, top_pct: float = 0.05) -> float:
    if p.size == 0 or r.size == 0:
        return 0.0
    n = len(p)
    take_n = max(1, round(n * top_pct))
    order = np.argsort(p)[::-1]
    return float(np.mean(r[order[:take_n]]))


def pass_rate(p: np.ndarray, threshold: float) -> float:
    if p.size == 0:
        return 0.0
    return float(np.mean(p >= threshold))


# ── Calibrator fitters ─────────────────────────────────────────────────────


@dataclass
class CalibratorBlock:
    type: str
    a: float = 1.0
    b: float = 0.0
    points: list[tuple[float, float]] = field(default_factory=list)
    ece: float | None = None


def fit_platt(p_raw: np.ndarray, y: np.ndarray) -> CalibratorBlock:
    """Single-feature LR on logit(p_raw)."""
    from sklearn.linear_model import LogisticRegression

    eps = 1e-6
    p = np.clip(p_raw, eps, 1.0 - eps)
    z = np.log(p / (1.0 - p)).reshape(-1, 1)
    lr = LogisticRegression(C=1.0, solver="lbfgs", max_iter=200)
    lr.fit(z, y)
    a = float(lr.coef_[0, 0])
    b = float(lr.intercept_[0])
    return CalibratorBlock(type="platt", a=a, b=b)


def fit_isotonic(p_raw: np.ndarray, y: np.ndarray) -> CalibratorBlock:
    """Piecewise isotonic regression — exported as a compact (x_i, y_i) list."""
    from sklearn.isotonic import IsotonicRegression

    iso = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
    iso.fit(p_raw, y)
    # Down-sample the step function to ~30 anchor points to keep the JSON small.
    xs = np.linspace(0.0, 1.0, 31)
    ys = iso.predict(xs)
    points = [(float(x), float(y)) for x, y in zip(xs, ys)]
    return CalibratorBlock(type="isotonic", points=points)


def fit_calibrator(kind: str, p_raw: np.ndarray, y: np.ndarray,
                   ece_eval_bins: int = 10) -> CalibratorBlock:
    if kind == "identity":
        blk = CalibratorBlock(type="identity")
    elif kind == "platt":
        blk = fit_platt(p_raw, y)
    elif kind == "isotonic":
        blk = fit_isotonic(p_raw, y)
    else:
        raise ValueError(f"unknown calibrator type: {kind}")
    p_cal = apply_calibrator(blk, p_raw)
    blk.ece = expected_calibration_error(p_cal, y, n_bins=ece_eval_bins)
    return blk


def apply_calibrator(blk: CalibratorBlock, p_raw: np.ndarray) -> np.ndarray:
    if blk.type == "identity":
        return np.clip(p_raw, 0.0, 1.0)
    if blk.type == "platt":
        eps = 1e-6
        p = np.clip(p_raw, eps, 1.0 - eps)
        z = blk.a * np.log(p / (1.0 - p)) + blk.b
        return 1.0 / (1.0 + np.exp(-z))
    if blk.type == "isotonic":
        if not blk.points:
            return np.clip(p_raw, 0.0, 1.0)
        xs = np.array([x for x, _ in blk.points])
        ys = np.array([y for _, y in blk.points])
        return np.clip(np.interp(p_raw, xs, ys), 0.0, 1.0)
    raise ValueError(f"unknown calibrator block: {blk.type}")


# ── Feature selection ──────────────────────────────────────────────────────


# Canonical training feature set. Trainer drops columns missing in
# >drop_missing_frac of rows; remaining set lands in the artifact.
DEFAULT_FEATURES: tuple[str, ...] = (
    "legacy_confidence",
    "p_edge_raw",
    "p_edge_cal",
    "rule_score",
    "have_need_ratio",
    "spread_bps",
    "expected_slippage_bps",
    "fee_bps",
    "exec_cost_bps",
    "expected_edge_bps",
    "exec_risk_norm",
    "dq_score",
    "dq_flag_count",
    "regime_code",
    "session_asia",
    "session_europe",
    "session_us",
    "weekend_flag",
)


def select_features(
    rows: list[dict], *,
    candidates: Iterable[str] = DEFAULT_FEATURES,
    min_coverage: float = 0.30,
) -> tuple[str, ...]:
    """Keep features with finite, non-zero coverage ≥ min_coverage."""
    if not rows:
        return tuple(candidates)
    out: list[str] = []
    n = len(rows)
    for name in candidates:
        present = 0
        for r in rows:
            v = r.get(name)
            if v is None:
                continue
            try:
                f = float(v)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(f):
                continue
            present += 1
        if present / n >= min_coverage:
            out.append(name)
    return tuple(out)


def rows_to_arrays(
    rows: list[dict], feature_cols: tuple[str, ...], target: str,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Vectorise rows into (X, y, r, decision_ms, resolved_ms).

    Missing/non-finite values become 0.0 — the SL-bps fall-back logic in
    the runtime gate uses the same convention.
    """
    n = len(rows)
    f = len(feature_cols)
    X = np.zeros((n, f), dtype=np.float64)
    y = np.zeros(n, dtype=np.int8)
    r = np.zeros(n, dtype=np.float64)
    d = np.zeros(n, dtype=np.int64)
    rs = np.zeros(n, dtype=np.int64)
    for i, row in enumerate(rows):
        for j, name in enumerate(feature_cols):
            v = row.get(name)
            if v is None:
                continue
            try:
                fv = float(v)
            except (TypeError, ValueError):
                continue
            if math.isfinite(fv):
                X[i, j] = fv
        y[i] = int(row.get(target, 0) or 0)
        r[i] = float(row.get("r_mult", 0.0) or 0.0)
        d[i] = int(row.get("ts_ms", 0) or 0)
        rs[i] = d[i] + int(row.get("horizon_ms", 0) or 0)
    return X, y, r, d, rs


# ── Trainer ─────────────────────────────────────────────────────────────────


@dataclass
class TrainResult:
    """What the trainer produced, regardless of promotion outcome."""

    feature_cols: tuple[str, ...]
    intercept: float
    coef: tuple[float, ...]
    calibrator: CalibratorBlock
    n_rows: int
    pos_rate: float
    oos_auc: float
    oos_brier: float
    oos_ece: float
    top5_expectancy_r: float
    pass_rate_at_default: float
    fold_returns: list[float]
    promotion_passed: bool
    promotion_reasons: list[str]


def _fit_lr(X: np.ndarray, y: np.ndarray, *, l2_C: float, sample_weight: np.ndarray | None = None):
    from sklearn.linear_model import LogisticRegression

    lr = LogisticRegression(
        C=l2_C,
        class_weight="balanced",
        solver="lbfgs",
        max_iter=500,
    )
    lr.fit(X, y, sample_weight=sample_weight)
    return lr


def train(
    rows: list[dict],
    *,
    cfg: TrainConfig | None = None,
    thr: PromotionThresholds | None = None,
) -> TrainResult:
    cfg = cfg or TrainConfig()
    thr = thr or PromotionThresholds()
    if len(rows) < cfg.min_rows:
        raise ValueError(f"need at least {cfg.min_rows} rows; got {len(rows)}")

    feature_cols = select_features(
        rows, candidates=DEFAULT_FEATURES, min_coverage=cfg.min_coverage,
    )
    if not feature_cols:
        raise ValueError("no features passed the coverage gate")

    X, y, r, decision_ms, resolved_ms = rows_to_arrays(rows, feature_cols, cfg.target)
    pos_rate = float(np.mean(y))

    # Walk-forward OOS evaluation: train per fold, score test, accumulate.
    oos_p = np.zeros_like(y, dtype=np.float64)
    oos_used = np.zeros_like(y, dtype=bool)
    fold_returns: list[float] = []
    n_folds = 0
    for train_idx, test_idx in purged_walkforward(
        decision_ms, resolved_ms,
        n_blocks=cfg.n_cv_blocks, embargo_ms=cfg.embargo_ms,
    ):
        n_folds += 1
        if len(train_idx) < 50 or len(test_idx) < 25:
            continue
        if len(np.unique(y[train_idx])) < 2:
            continue
        try:
            lr = _fit_lr(X[train_idx], y[train_idx], l2_C=cfg.l2_C)
        except Exception as e:  # pragma: no cover — sklearn raises on degenerate input
            log.warning("fold %d LR fit failed: %s", n_folds, e)
            continue
        p_test = lr.predict_proba(X[test_idx])[:, 1]
        oos_p[test_idx] = p_test
        oos_used[test_idx] = True

        # Fold return = mean realized_r over the top-percentile selected by p.
        order = np.argsort(p_test)[::-1]
        take = max(1, round(len(order) * 0.05))
        fold_returns.append(float(np.mean(r[test_idx][order[:take]])))

    if not oos_used.any():
        raise ValueError("walk-forward CV produced no OOS predictions")

    p_oos = oos_p[oos_used]
    y_oos = y[oos_used]
    r_oos = r[oos_used]

    # Calibrate on OOS predictions to avoid in-sample over-fitting.
    calibrator = fit_calibrator(cfg.calibrator, p_oos, y_oos.astype(np.float64))
    p_cal = apply_calibrator(calibrator, p_oos)

    auc = roc_auc(p_oos, y_oos.astype(np.int64))
    brier = brier_score(p_cal, y_oos.astype(np.float64))
    ece = expected_calibration_error(p_cal, y_oos.astype(np.float64))
    top5 = top_pct_expectancy(p_cal, r_oos, top_pct=0.05)
    pass_rate_default = pass_rate(p_cal, 0.56)

    # Final model: fit once on the full dataset so the artifact captures
    # all available signal (OOS metrics already established trustworthiness).
    final_lr = _fit_lr(X, y, l2_C=cfg.l2_C)
    intercept = float(final_lr.intercept_[0])
    coef = tuple(float(c) for c in final_lr.coef_[0])

    metrics = PromotionMetrics(
        n_oos_trades=int(oos_used.sum()),
        n_oos_days=max(
            1,
            int((decision_ms[oos_used].max() - decision_ms[oos_used].min()) // 86_400_000),
        ),
        mean_oos_profit_factor=0.0,  # we don't track PF here; rely on top5 + DSR
        mean_oos_sharpe=0.0,
        deflated_sharpe=0.0,
        pbo=0.0,
        ece=ece,
        brier=brier,
        pass_rate=pass_rate_default,
    )
    passed, reasons = can_promote(metrics, thr)
    # An extra rule that's specific to this gate: AUC must be > 0.55.
    if auc <= 0.55:
        passed = False
        reasons = list(reasons) + ["auc_too_low"]
    # And the top-5% expectancy must be strictly positive.
    if top5 <= 0.0:
        passed = False
        reasons = list(reasons) + ["top5_expectancy_non_positive"]

    return TrainResult(
        feature_cols=feature_cols,
        intercept=intercept,
        coef=coef,
        calibrator=calibrator,
        n_rows=len(rows),
        pos_rate=pos_rate,
        oos_auc=auc,
        oos_brier=brier,
        oos_ece=ece,
        top5_expectancy_r=top5,
        pass_rate_at_default=pass_rate_default,
        fold_returns=fold_returns,
        promotion_passed=passed,
        promotion_reasons=list(reasons),
    )


# ── Artifact emission ──────────────────────────────────────────────────────


def build_artifact_json(
    result: TrainResult,
    *,
    cfg: TrainConfig,
    min_p_win: float = 0.56,
    min_expected_r: float = 0.02,
    min_expected_edge_bps: float = 1.5,
    now_ms: int | None = None,
) -> dict[str, Any]:
    """Build a JSON-serialisable dict in the exact MetaGateArtifact schema.

    `services.confidence_meta_gate.model.load_artifact` will round-trip this.
    """
    now_ms = now_ms or int(time.time() * 1000)
    ver = f"{cfg.model_ver_prefix}_{now_ms}"
    payload: dict[str, Any] = {
        "model_ver": ver,
        "schema": cfg.schema_name,
        "feature_cols": list(result.feature_cols),
        "model": {
            "type": "logistic_regression",
            "intercept": result.intercept,
            "coef": list(result.coef),
        },
        "calibrator": {
            "type": result.calibrator.type,
            "a": result.calibrator.a,
            "b": result.calibrator.b,
            "points": [list(p) for p in result.calibrator.points],
            "ece": result.calibrator.ece,
        },
        "thresholds": {
            "min_p_win": min_p_win,
            "min_expected_r": min_expected_r,
            "min_expected_edge_bps": min_expected_edge_bps,
        },
        "training_summary": {
            "n_rows": result.n_rows,
            "pos_rate": result.pos_rate,
            "oos_auc": result.oos_auc,
            "oos_brier": result.oos_brier,
            "oos_ece": result.oos_ece,
            "top5_expectancy_r": result.top5_expectancy_r,
            "created_ms": now_ms,
        },
        "promotion": {
            "passed": result.promotion_passed,
            "reasons": list(result.promotion_reasons),
        },
    }
    return payload


def write_artifact(payload: dict[str, Any], path: str) -> None:
    """Atomic write — tmp file then os.replace to avoid torn JSON."""
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    os.replace(tmp, path)


__all__ = [
    "DEFAULT_FEATURES",
    "TrainConfig",
    "TrainResult",
    "CalibratorBlock",
    "apply_calibrator",
    "brier_score",
    "build_artifact_json",
    "expected_calibration_error",
    "fit_calibrator",
    "fit_isotonic",
    "fit_platt",
    "pass_rate",
    "roc_auc",
    "rows_to_arrays",
    "select_features",
    "top_pct_expectancy",
    "train",
    "write_artifact",
]
