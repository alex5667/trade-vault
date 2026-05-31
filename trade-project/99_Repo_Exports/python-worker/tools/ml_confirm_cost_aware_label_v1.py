"""Task 2.1 — cost-aware label augmentation for ML-confirm refit.

Reads a base dataset (parquet) produced by
`ml_analysis/tools/build_edge_stack_dataset_from_redis.py` (or any other tool
that joins `signals:of:inputs` with `trades_closed`) and writes a derived
parquet with an extra column:

    y_cost_aware = ((pnl_net − fee_mul*fees − slippage_realized_usd) > 0).astype(int)

This is the "cost-aware label" the ML-confirm refit should train against
(per low-win-rate analysis: голый pnl даёт оптимистичный сигнал; нужно
доплачивать комиссию × fee_mul и realized slippage).

ENV / CLI overrides
-------------------
COSTAWARE_FEE_MUL                 Multiplier on `fees` column (default 2.0 = round-trip)
COSTAWARE_SLIPPAGE_BPS_FALLBACK   bps used when realized_slippage is absent (default 4.0)
COSTAWARE_SLIPPAGE_BPS_COL        Indicator column with realized slippage in bps
                                  (default: slippage_realized_bps, falls back to
                                   expected_slippage_bps then fallback bps)

Usage
-----
    python -m tools.ml_confirm_cost_aware_label_v1 \
        --input /tmp/edge_stack_dataset.parquet \
        --out   /tmp/edge_stack_dataset_costaware.parquet

The output parquet preserves all input columns and adds:
    - y_cost_aware (int 0/1)
    - cost_total_usd (float, debug)
    - slippage_realized_usd (float, debug)
    - slippage_bps_used (float, debug)
    - slippage_bps_source (str, "realized"|"expected"|"fallback")

Existing tests / contracts: see tests/test_ml_confirm_cost_aware_label.py
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from typing import Any


def _as_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        f = float(x)
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except (TypeError, ValueError):
        return default


def _resolve_slippage_bps(
    row: dict[str, Any],
    *,
    col: str,
    fallback_bps: float,
) -> tuple[float, str]:
    """Returns (bps, source). source ∈ {realized, expected, fallback}."""
    val = row.get(col)
    if val is not None:
        f = _as_float(val, -1.0)
        if f >= 0.0:
            return f, "realized"
    val = row.get("expected_slippage_bps")
    if val is not None:
        f = _as_float(val, -1.0)
        if f >= 0.0:
            return f, "expected"
    return max(0.0, fallback_bps), "fallback"


def apply_cost_aware_label(
    df,
    *,
    fee_mul: float,
    slippage_bps_fallback: float,
    slippage_bps_col: str,
):
    """In-place augmentation of df with cost-aware label columns.

    Mirrors the formula from low-win-rate analysis:
        y_cost_aware = ((pnl_net - fee_mul*fees - slip_usd) > 0).astype(int)

    Required columns: `pnl_net`, `fees`. Optional: `notional_usd`,
    `slippage_realized_bps`, `expected_slippage_bps`.

    Returns the same df (mutated).
    """
    if "pnl_net" not in df.columns:
        raise ValueError("input df missing required column: pnl_net")
    if "fees" not in df.columns:
        raise ValueError("input df missing required column: fees")

    pnl_net = df["pnl_net"].astype(float).fillna(0.0)
    fees = df["fees"].astype(float).fillna(0.0)
    notional = (
        df["notional_usd"].astype(float).fillna(0.0)
        if "notional_usd" in df.columns
        else pnl_net * 0.0
    )

    slippage_bps_vals: list[float] = []
    slippage_source_vals: list[str] = []
    for row in df.to_dict(orient="records"):
        bps, src = _resolve_slippage_bps(
            row, col=slippage_bps_col, fallback_bps=slippage_bps_fallback
        )
        slippage_bps_vals.append(bps)
        slippage_source_vals.append(src)

    import pandas as pd
    slip_bps_series = pd.Series(slippage_bps_vals, index=df.index, dtype=float)
    slip_usd = (slip_bps_series / 1e4) * notional.abs()
    cost_total = (fee_mul * fees.abs()) + slip_usd
    y_cost_aware = ((pnl_net - cost_total) > 0).astype(int)

    df["slippage_bps_used"] = slip_bps_series
    df["slippage_bps_source"] = slippage_source_vals
    df["slippage_realized_usd"] = slip_usd
    df["cost_total_usd"] = cost_total
    df["y_cost_aware"] = y_cost_aware
    return df


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Input parquet (joined dataset)")
    ap.add_argument("--out", required=True, help="Output parquet path")
    ap.add_argument(
        "--fee-mul",
        type=float,
        default=float(os.getenv("COSTAWARE_FEE_MUL", "2.0") or 2.0),
        help="Fee multiplier (round-trip default 2.0)",
    )
    ap.add_argument(
        "--slippage-bps-fallback",
        type=float,
        default=float(os.getenv("COSTAWARE_SLIPPAGE_BPS_FALLBACK", "4.0") or 4.0),
        help="bps used when realized_slippage absent",
    )
    ap.add_argument(
        "--slippage-bps-col",
        default=os.getenv("COSTAWARE_SLIPPAGE_BPS_COL", "slippage_realized_bps"),
        help="Column name with realized slippage bps",
    )
    args = ap.parse_args(argv)

    try:
        import pandas as pd
    except ImportError:
        print("ERROR: pandas required", file=sys.stderr)
        return 2

    df = pd.read_parquet(args.input)
    if df.empty:
        print(f"WARN: input dataset {args.input} is empty — writing empty output")
        df.to_parquet(args.out, index=False)
        return 0

    df = apply_cost_aware_label(
        df,
        fee_mul=float(args.fee_mul),
        slippage_bps_fallback=float(args.slippage_bps_fallback),
        slippage_bps_col=str(args.slippage_bps_col),
    )

    df.to_parquet(args.out, index=False)
    n_pos = int(df["y_cost_aware"].sum())
    n_total = len(df)
    print(
        f"wrote {args.out}: n_total={n_total} n_pos={n_pos} "
        f"pos_rate={n_pos / max(1, n_total):.4f} "
        f"fee_mul={args.fee_mul} slip_fallback_bps={args.slippage_bps_fallback}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
