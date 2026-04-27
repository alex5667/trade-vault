#!/usr/bin/env python3
"""
Walk-Forward Threshold Calibrator — OOS-validated grid search.

Replaces the in-sample calibrate_thresholds_pnl.py with expanding-window
walk-forward validation to prevent overfitting.

Usage:
    python3 calibrate_thresholds_wf.py \
        --data joined_features_exec.parquet \
        --out-env config/calibrated_wf.env \
        --min-train-days 30 \
        --test-days 7 \
        --step-days 7

Architecture:
    - Expanding window over timestamps (day-indexed for parquet data).
    - For each fold: run grid search on train window, evaluate on test window.
    - Aggregate: median of stable fold params, stability score, deploy gate.
    - Only emit config if OOS performance is stable across folds.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import product
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd


@dataclass
class FoldThresholdResult:
    """Result of one walk-forward fold."""
    fold_idx: int
    train_end_date: str
    test_end_date: str
    train_n: int
    test_n: int
    best_dz: float
    best_obi: float
    # In-sample metrics
    train_mean_profit: float
    train_sharpe: float
    # Out-of-sample metrics
    oos_mean_profit: float
    oos_sharpe: float
    oos_win_rate: float
    oos_profit_factor: float
    oos_n_trades: int


@dataclass
class WFThresholdResult:
    """Aggregated walk-forward result for threshold calibration."""
    robust_dz: float
    robust_obi: float
    stability_score: float     # std of OOS Sharpe
    deploy: bool
    n_folds: int
    n_stable_folds: int
    mean_oos_sharpe: float
    overfit_ratio: float
    folds: List[FoldThresholdResult]


def _objective(
    df: pd.DataFrame,
    dz: float,
    obi: float,
    weight_profit: float = 0.7,
) -> Tuple[float, float, float, int]:
    """
    Calculate composite score for given thresholds.

    Returns (score, mean_profit, sharpe, n_samples).
    """
    mask = np.abs(df["delta_z"]) >= dz
    if "obi" in df.columns:
        mask &= np.abs(df["obi"]) >= obi

    sel = df[mask]
    if sel.empty or "profit" not in sel.columns:
        return -1e9, 0.0, 0.0, 0

    mu = sel["profit"].mean()
    sd = sel["profit"].std(ddof=0)
    if pd.isna(sd) or sd <= 0:
        sd = 1e-9

    sharpe = mu / sd
    score = weight_profit * mu + (1 - weight_profit) * sharpe
    return score, float(mu), float(sharpe), len(sel)


def _evaluate_oos(
    df: pd.DataFrame,
    dz: float,
    obi: float,
) -> Tuple[float, float, float, float, int]:
    """
    Evaluate thresholds on OOS data.

    Returns (mean_profit, sharpe, win_rate, profit_factor, n_trades).
    """
    mask = np.abs(df["delta_z"]) >= dz
    if "obi" in df.columns:
        mask &= np.abs(df["obi"]) >= obi

    sel = df[mask]
    if sel.empty or "profit" not in sel.columns:
        return 0.0, 0.0, 0.0, 0.0, 0

    profits = sel["profit"]
    n = len(profits)
    mu = float(profits.mean())
    sd = float(profits.std(ddof=0))
    if pd.isna(sd) or sd <= 0:
        sd = 1e-9
    sharpe = mu / sd

    wins = int((profits > 0).sum())
    win_rate = wins / n if n > 0 else 0.0

    total_pos = float(profits[profits > 0].sum())
    total_neg = abs(float(profits[profits <= 0].sum()))
    pf = total_pos / total_neg if total_neg > 1e-9 else (
        10.0 if total_pos > 0 else 0.0
    )

    return mu, sharpe, win_rate, pf, n


def run_walk_forward(
    df: pd.DataFrame,
    dz_grid: List[float],
    obi_grid: List[float],
    min_train_days: int = 30,
    test_days: int = 7,
    step_days: int = 7,
    weight_profit: float = 0.7,
    stability_threshold: float = 0.5,
    min_oos_pf: float = 1.0,
    min_folds_to_deploy: int = 2,
) -> WFThresholdResult:
    """
    Run walk-forward validation over threshold grid.

    Args:
        df: DataFrame with 'delta_z', 'profit', optional 'obi',
            and a datetime column ('ts', 'timestamp', or 'date').
        dz_grid: List of delta_z threshold candidates.
        obi_grid: List of OBI threshold candidates.
        min_train_days: Minimum training window in days.
        test_days: Test window size in days.
        step_days: Step size between folds in days.
        weight_profit: Weight for profit in composite score (0-1).
        stability_threshold: Max std(OOS Sharpe) for deploy gate.
        min_oos_pf: Min OOS profit factor for a fold to be "stable".
        min_folds_to_deploy: Min stable folds required for deployment.

    Returns:
        WFThresholdResult with robust thresholds and stability metrics.
    """
    # Identify date column
    date_col = None
    for col in ("ts", "timestamp", "date", "exit_ts", "entry_ts"):
        if col in df.columns:
            date_col = col
            break

    if date_col is None:
        raise ValueError(
            f"No date column found in DataFrame. "
            f"Expected one of: ts, timestamp, date, exit_ts, entry_ts. "
            f"Got: {list(df.columns)}"
        )

    df = df.copy()
    df["_dt"] = pd.to_datetime(df[date_col])
    df = df.sort_values("_dt").reset_index(drop=True)

    min_dt = df["_dt"].min()
    max_dt = df["_dt"].max()
    total_days = (max_dt - min_dt).days

    if total_days < min_train_days + test_days:
        print(
            f"⚠️  Insufficient data span: {total_days} days "
            f"< min_train({min_train_days}) + test({test_days})"
        )
        return WFThresholdResult(
            robust_dz=dz_grid[len(dz_grid) // 2] if dz_grid else 3.0,
            robust_obi=obi_grid[len(obi_grid) // 2] if obi_grid else 0.5,
            stability_score=999.0,
            deploy=False,
            n_folds=0,
            n_stable_folds=0,
            mean_oos_sharpe=0.0,
            overfit_ratio=0.0,
            folds=[],
        )

    # Generate expanding windows
    folds: List[FoldThresholdResult] = []
    fold_idx = 0
    train_end_offset = pd.Timedelta(days=min_train_days)

    while True:
        train_end_dt = min_dt + train_end_offset
        test_start_dt = train_end_dt
        test_end_dt = test_start_dt + pd.Timedelta(days=test_days)

        if test_end_dt > max_dt + pd.Timedelta(days=1):
            break

        train_df = df[df["_dt"] < train_end_dt]
        test_df = df[(df["_dt"] >= test_start_dt) & (df["_dt"] < test_end_dt)]

        if len(train_df) < 20 or len(test_df) < 5:
            train_end_offset += pd.Timedelta(days=step_days)
            continue

        # Grid search on train
        best_score = -1e9
        best_dz = dz_grid[0]
        best_obi = obi_grid[0]
        best_mu = 0.0
        best_sh = 0.0

        for dz, ob in product(dz_grid, obi_grid):
            score, mu, sh, n = _objective(train_df, dz, ob, weight_profit)
            if score > best_score and n >= 5:
                best_score = score
                best_dz = dz
                best_obi = ob
                best_mu = mu
                best_sh = sh

        # Evaluate on test
        oos_mu, oos_sh, oos_wr, oos_pf, oos_n = _evaluate_oos(
            test_df, best_dz, best_obi,
        )

        fold_result = FoldThresholdResult(
            fold_idx=fold_idx,
            train_end_date=str(train_end_dt.date()),
            test_end_date=str(test_end_dt.date()),
            train_n=len(train_df),
            test_n=len(test_df),
            best_dz=best_dz,
            best_obi=best_obi,
            train_mean_profit=best_mu,
            train_sharpe=best_sh,
            oos_mean_profit=oos_mu,
            oos_sharpe=oos_sh,
            oos_win_rate=oos_wr,
            oos_profit_factor=oos_pf,
            oos_n_trades=oos_n,
        )
        folds.append(fold_result)

        print(
            f"  Fold {fold_idx}: train→{train_end_dt.date()} ({len(train_df)}), "
            f"test→{test_end_dt.date()} ({len(test_df)}) | "
            f"dz={best_dz}, obi={best_obi} | "
            f"train_sh={best_sh:.3f}, oos_sh={oos_sh:.3f}, "
            f"oos_pf={oos_pf:.2f}, oos_wr={oos_wr:.1%}"
        )

        fold_idx += 1
        train_end_offset += pd.Timedelta(days=step_days)

    if not folds:
        return WFThresholdResult(
            robust_dz=dz_grid[len(dz_grid) // 2] if dz_grid else 3.0,
            robust_obi=obi_grid[len(obi_grid) // 2] if obi_grid else 0.5,
            stability_score=999.0,
            deploy=False, n_folds=0, n_stable_folds=0,
            mean_oos_sharpe=0.0, overfit_ratio=0.0, folds=[],
        )

    # Aggregate
    stable_folds = [f for f in folds if f.oos_profit_factor > min_oos_pf]
    n_stable = len(stable_folds)

    if stable_folds:
        robust_dz = float(np.median([f.best_dz for f in stable_folds]))
        robust_obi = float(np.median([f.best_obi for f in stable_folds]))
    else:
        robust_dz = float(np.median([f.best_dz for f in folds]))
        robust_obi = float(np.median([f.best_obi for f in folds]))

    oos_sharpes = [f.oos_sharpe for f in folds]
    stability_score = float(np.std(oos_sharpes)) if len(oos_sharpes) > 1 else 999.0
    mean_oos_sharpe = float(np.mean(oos_sharpes))

    # Overfit ratio
    mean_train_sh = float(np.mean([f.train_sharpe for f in folds]))
    if abs(mean_oos_sharpe) > 1e-9:
        overfit_ratio = mean_train_sh / mean_oos_sharpe
    else:
        overfit_ratio = 0.0

    deploy = (
        stability_score < stability_threshold
        and n_stable >= min_folds_to_deploy
    )

    return WFThresholdResult(
        robust_dz=robust_dz,
        robust_obi=robust_obi,
        stability_score=round(stability_score, 4),
        deploy=deploy,
        n_folds=len(folds),
        n_stable_folds=n_stable,
        mean_oos_sharpe=round(mean_oos_sharpe, 4),
        overfit_ratio=round(overfit_ratio, 4),
        folds=folds,
    )


def main():
    """CLI entry point."""
    ap = argparse.ArgumentParser(
        description="Walk-Forward Threshold Calibrator v1"
    )
    ap.add_argument("--data", required=True, help="Input data (parquet/csv)")
    ap.add_argument("--out-env", required=True, help="Output .env file")
    ap.add_argument("--dz-grid", default="1.5,2,2.5,3,3.5,4", help="Delta Z grid")
    ap.add_argument("--obi-grid", default="0,0.2,0.3,0.4,0.5,0.6,0.7", help="OBI grid")
    ap.add_argument("--min-train-days", type=int, default=30, help="Min training window (days)")
    ap.add_argument("--test-days", type=int, default=7, help="Test window (days)")
    ap.add_argument("--step-days", type=int, default=7, help="Step between folds (days)")
    ap.add_argument("--weight-profit", type=float, default=0.7, help="Profit weight (0-1)")
    ap.add_argument("--stability-threshold", type=float, default=0.5, help="Max OOS Sharpe std for deploy")
    args = ap.parse_args()

    print("=" * 80)
    print("🎯 Walk-Forward Threshold Calibrator v1")
    print("=" * 80)
    print()

    # Load data
    print(f"📊 Loading data from {args.data}...")
    if args.data.endswith(".parquet"):
        df = pd.read_parquet(args.data)
    else:
        df = pd.read_csv(args.data)

    print(f"✅ Loaded {len(df)} records")
    print(f"   Columns: {list(df.columns)}")
    print()

    # Check required columns
    required = ["delta_z", "profit"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"❌ Missing required columns: {missing}")
        return

    # Parse grids
    dzs = [float(x) for x in args.dz_grid.split(",")]
    obis = [float(x) for x in args.obi_grid.split(",")]

    print(f"🔍 Walk-Forward parameters:")
    print(f"   Delta Z grid:  {dzs}")
    print(f"   OBI grid:      {obis}")
    print(f"   Min train:     {args.min_train_days} days")
    print(f"   Test window:   {args.test_days} days")
    print(f"   Step:          {args.step_days} days")
    print(f"   Combinations:  {len(dzs) * len(obis)}")
    print()

    # Run walk-forward
    result = run_walk_forward(
        df=df,
        dz_grid=dzs,
        obi_grid=obis,
        min_train_days=args.min_train_days,
        test_days=args.test_days,
        step_days=args.step_days,
        weight_profit=args.weight_profit,
        stability_threshold=args.stability_threshold,
    )

    print()
    print("=" * 80)
    print("📊 WALK-FORWARD RESULTS")
    print("=" * 80)
    print(f"  Folds:              {result.n_folds}")
    print(f"  Stable folds:       {result.n_stable_folds}")
    print(f"  Stability score:    {result.stability_score:.4f} (std of OOS Sharpe)")
    print(f"  Mean OOS Sharpe:    {result.mean_oos_sharpe:.4f}")
    print(f"  Overfit ratio:      {result.overfit_ratio:.2f}")
    print(f"  Deploy:             {'✅ YES' if result.deploy else '❌ NO (unstable)'}")
    print()
    print(f"  Robust DZ:          {result.robust_dz}")
    print(f"  Robust OBI:         {result.robust_obi}")
    print()

    if not result.deploy:
        print("⚠️  Walk-forward validation REJECTED these thresholds as unstable.")
        print("    Consider: more data, wider grid, or lower stability threshold.")
        print("    Writing defaults to output file.")
        with open(args.out_env, "w") as f:
            f.write("# Walk-forward REJECTED — using defaults\n")
            f.write(f"# stability_score={result.stability_score:.4f}\n")
            f.write("DELTA_Z_THRESHOLD=3.0\n")
            f.write("OBI_THRESHOLD=0.5\n")
        print(f"✅ Wrote defaults to {args.out_env}")
        return

    # Compare with in-sample best
    print("📊 Comparing WF robust vs in-sample best:")
    is_score, is_dz, is_obi = -1e9, dzs[0], obis[0]
    for dz, ob in product(dzs, obis):
        score, _, _, n = _objective(df, dz, ob, args.weight_profit)
        if score > is_score and n >= 5:
            is_score, is_dz, is_obi = score, dz, ob

    print(f"  In-sample best:   DZ={is_dz}, OBI={is_obi}")
    print(f"  WF robust:        DZ={result.robust_dz}, OBI={result.robust_obi}")
    if is_dz != result.robust_dz or is_obi != result.robust_obi:
        print("  ⚠️  WF picked different thresholds — in-sample was likely overfit!")
    else:
        print("  ✅ Same thresholds — in-sample was stable.")
    print()

    # Write to env file
    with open(args.out_env, "w") as f:
        f.write(f"# Walk-forward calibrated thresholds\n")
        f.write(f"# Generated from {len(df)} records, {result.n_folds} folds\n")
        f.write(f"# Stability score: {result.stability_score:.4f}\n")
        f.write(f"# Mean OOS Sharpe: {result.mean_oos_sharpe:.4f}\n")
        f.write(f"# Overfit ratio: {result.overfit_ratio:.2f}\n")
        f.write(f"# Deploy: {result.deploy}\n\n")
        f.write(f"DELTA_Z_THRESHOLD={result.robust_dz}\n")
        f.write(f"OBI_THRESHOLD={result.robust_obi}\n")

    print(f"✅ Wrote WF-validated configuration to {args.out_env}")
    print()


if __name__ == "__main__":
    main()
