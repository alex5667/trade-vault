#!/usr/bin/env python3
"""
Offline signal validation for XAUUSD order flow.

Validates signal generation rules against historical data using forward returns.
Provides metrics: signal count, win rate, average edge, percentile edges.

Usage:
    python3 validate_signals.py --data features.parquet \\
                                 --horizon 60 \\
                                 --delta_z 3.0 \\
                                 --obi 0.5
"""

import argparse
from typing import Dict, List, Tuple

try:
    import pandas as pd
    import numpy as np
except ImportError:
    print("Error: pandas and numpy required. Run: pip install pandas numpy pyarrow")
    exit(1)

# Опциональный GPU сервис (ленивая загрузка)
_GPU_AVAILABLE = False
try:
    from services.gpu_compute_service import get_gpu_service

    _gpu_service = get_gpu_service()
    _GPU_AVAILABLE = bool(_gpu_service and _gpu_service.is_gpu_available())
except Exception:
    _gpu_service = None
    _GPU_AVAILABLE = False


def _compute_quantiles(values: List[float], probs: List[float], use_gpu: bool) -> List[float]:
    """
    Безопасно вычисляет квантили с GPU при доступности.
    """
    arr = np.asarray(values, dtype=np.float32)
    if arr.size == 0:
        return [0.0 for _ in probs]
    if use_gpu and _GPU_AVAILABLE and _gpu_service:
        try:
            qs = _gpu_service.compute_quantiles(arr, probs)
            return [float(x) for x in qs]
        except Exception:
            pass
    qs_cpu = np.nanquantile(arr, probs)
    return [float(x) for x in qs_cpu]


def forward_return(mid: pd.Series, horizon: int = 60) -> pd.Series:
    """
    Compute forward return over horizon.
    
    Assumes samples are ~1s apart.
    
    Args:
        mid: Mid price series
        horizon: Forward window size (samples)
        
    Returns:
        Forward return series (percentage)
    """
    return (mid.shift(-horizon) - mid) / mid


def run_rules(df: pd.DataFrame, cfg: Dict, use_gpu: bool) -> List[Tuple[int, int, str]]:
    """
    Apply signal generation rules to data.
    
    Rules:
    1. Delta spike + OBI continuation
    2. Absorption at pivot (weak progress + delta spike)
    
    Args:
        df: Feature DataFrame
        cfg: Configuration dict with thresholds
        
    Returns:
        List of (index, side, signal_type) tuples
            side: 1 = LONG, -1 = SHORT
    """
    # GPU-вариант: векторизованная фильтрация (без питоновских циклов)
    if use_gpu and _GPU_AVAILABLE:
        try:
            import cupy as cp

            z = cp.asarray(df["delta_z"].fillna(0.0).to_numpy(dtype=np.float32))
            obi_raw = cp.asarray(df["obi"].fillna(0.0).to_numpy(dtype=np.float32))

            if z.size == 0:
                return []

            # rolling mean с окном 2, min_periods=1:
            # obi_mean[0] = obi[0], далее среднее текущего и предыдущего
            obi_mean = cp.empty_like(obi_raw)
            obi_mean[0] = obi_raw[0]
            if obi_raw.size > 1:
                obi_mean[1:] = (obi_raw[1:] + obi_raw[:-1]) * 0.5

            side = cp.sign(z)
            cond_z = cp.abs(z) >= cfg["DELTA_Z_THRESHOLD"]
            cond_obi = (side * obi_mean) >= cfg["OBI_THRESHOLD"]
            mask = cond_z & cond_obi

            idxs = cp.nonzero(mask)[0]
            if idxs.size == 0:
                return []

            idxs_cpu = idxs.get().tolist()
            sides_cpu = side[idxs].get().tolist()
            return [
                (int(i), 1 if s > 0 else -1, "delta_spike+obi")
                for i, s in zip(idxs_cpu, sides_cpu)
            ]
        except Exception:
            # В случае любой ошибки — fallback на CPU путь ниже
            pass

    # CPU fallback (оригинальная логика)
    sigs = []
    obi = df["obi"].fillna(0.0).rolling(2, min_periods=1).mean()
    for i, row in df.iterrows():
        z = row.get("delta_z", 0.0)
        if abs(z) < cfg["DELTA_Z_THRESHOLD"]:
            continue
        side = 1 if z > 0 else -1
        obi_val = obi.iloc[i]
        if (side * obi_val) >= cfg["OBI_THRESHOLD"]:
            sigs.append((i, side, "delta_spike+obi"))
    return sigs


def evaluate(
    df: pd.DataFrame,
    sigs: List[Tuple[int, int, str]],
    horizon: int = 60,
    min_edge: float = 0.0,
    use_gpu: bool = False,
) -> Dict:
    """
    Evaluate signal performance using forward returns.
    
    Args:
        df: Feature DataFrame
        sigs: List of signals
        horizon: Forward window (samples)
        min_edge: Minimum edge to consider a win (default: 0)
        
    Returns:
        Dictionary of metrics
    """
    if not sigs:
        return {
            "signals": 0,
            "evaluated": 0,
            "win_rate": 0.0,
            "avg_edge": 0.0,
            "p50_edge": 0.0,
            "p75_edge": 0.0,
            "p95_edge": 0.0,
            "max_edge": 0.0,
            "min_edge": 0.0,
        }

    # GPU-вариант: векторно считаем forward returns и метрики
    if use_gpu and _GPU_AVAILABLE:
        try:
            import cupy as cp

            mid = cp.asarray(df["mid"].to_numpy(dtype=np.float32))
            n = mid.size
            if n == 0 or horizon <= 0 or horizon >= n:
                return {
                    "signals": len(sigs),
                    "evaluated": 0,
                    "win_rate": 0.0,
                    "avg_edge": 0.0,
                    "p50_edge": 0.0,
                    "p75_edge": 0.0,
                    "p95_edge": 0.0,
                    "max_edge": 0.0,
                    "min_edge": 0.0,
                }

            # forward return для валидных индексов
            idxs_cpu = np.array([i for i, _, _ in sigs], dtype=np.int32)
            sides_cpu = np.array([1 if s > 0 else -1 for _, s, _ in sigs], dtype=np.int8)

            idxs = cp.asarray(idxs_cpu)
            sides = cp.asarray(sides_cpu, dtype=cp.float32)

            valid_mask = idxs + horizon < n
            if not bool(cp.any(valid_mask)):
                return {
                    "signals": len(sigs),
                    "evaluated": 0,
                    "win_rate": 0.0,
                    "avg_edge": 0.0,
                    "p50_edge": 0.0,
                    "p75_edge": 0.0,
                    "p95_edge": 0.0,
                    "max_edge": 0.0,
                    "min_edge": 0.0,
                }

            idxs = idxs[valid_mask]
            sides = sides[valid_mask]

            fwd = (mid[idxs + horizon] - mid[idxs]) / mid[idxs]
            edges = fwd * sides

            edges_cpu = edges.get()
            wins = int(np.sum(edges_cpu > min_edge))
            losses = len(edges_cpu) - wins
            total = wins + losses if wins + losses > 0 else 1

            qs = _compute_quantiles(edges_cpu, [0.50, 0.75, 0.95], use_gpu=True)
            return {
                "signals": len(sigs),
                "evaluated": len(edges_cpu),
                "win_rate": wins / total,
                "avg_edge": float(np.nanmean(edges_cpu)) if edges_cpu.size else 0.0,
                "p50_edge": qs[0],
                "p75_edge": qs[1],
                "p95_edge": qs[2],
                "max_edge": float(np.nanmax(edges_cpu)) if edges_cpu.size else 0.0,
                "min_edge": float(np.nanmin(edges_cpu)) if edges_cpu.size else 0.0,
            }
        except Exception:
            # fallback на CPU ниже
            pass

    # CPU fallback
    fwd = forward_return(df["mid"], horizon)

    wins = 0
    losses = 0
    edges: List[float] = []

    for idx, side, _ in sigs:
        r = fwd.iloc[idx] * side
        if pd.isna(r):
            continue
        edges.append(float(r))
        if r > min_edge:
            wins += 1
        else:
            losses += 1

    total = wins + losses if wins + losses > 0 else 1

    qs_cpu = _compute_quantiles(edges, [0.50, 0.75, 0.95], use_gpu=False)
    return {
        "signals": len(sigs),
        "evaluated": len(edges),
        "win_rate": wins / total,
        "avg_edge": float(np.nanmean(edges)) if edges else 0.0,
        "p50_edge": qs_cpu[0],
        "p75_edge": qs_cpu[1],
        "p95_edge": qs_cpu[2],
        "max_edge": float(np.nanmax(edges)) if edges else 0.0,
        "min_edge": float(np.nanmin(edges)) if edges else 0.0,
    }


def main():
    """Main entry point."""
    ap = argparse.ArgumentParser(
        description="Validate XAUUSD signal rules against historical data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic validation
  python3 validate_signals.py --data features.parquet
  
  # Custom thresholds
  python3 validate_signals.py --data features.csv \\
                               --horizon 120 \\
                               --delta_z 2.5 \\
                               --obi 0.6
        """
    )
    ap.add_argument("--data", required=True, help="Features file (.parquet or .csv)")
    ap.add_argument(
        "--horizon",
        type=int,
        default=60,
        help="Forward horizon in samples (default: 60 = ~1 min)",
    )
    ap.add_argument(
        "--delta_z", type=float, default=3.0, help="Delta Z-score threshold (default: 3.0)"
    )
    ap.add_argument("--obi", type=float, default=0.5, help="OBI threshold (default: 0.5)")
    ap.add_argument(
        "--min_edge", type=float, default=0.0, help="Minimum edge for win (default: 0.0)"
    )
    ap.add_argument(
        "--use-gpu",
        action="store_true",
        default=False,
        help="Enable GPU acceleration (requires cupy and available GPU).",
    )
    args = ap.parse_args()
    
    # Load data
    print(f"📂 Loading data from {args.data}...")
    if args.data.endswith(".parquet"):
        df = pd.read_parquet(args.data)
    elif args.data.endswith(".csv"):
        df = pd.read_csv(args.data)
    else:
        raise SystemExit("Error: Data file must be .parquet or .csv")
    
    print(f"✅ Loaded {len(df)} rows\n")
    
    # Configuration
    cfg = {
        "DELTA_Z_THRESHOLD": args.delta_z,
        "OBI_THRESHOLD": args.obi
    }
    
    print("🔧 Configuration:")
    print(f"   DELTA_Z_THRESHOLD = {cfg['DELTA_Z_THRESHOLD']}")
    print(f"   OBI_THRESHOLD     = {cfg['OBI_THRESHOLD']}")
    print(f"   Forward horizon   = {args.horizon} samples (~{args.horizon}s)")
    print(f"   Min edge for win  = {args.min_edge}")
    print()
    
    use_gpu = bool(args.use_gpu and _GPU_AVAILABLE)
    if args.use_gpu and not _GPU_AVAILABLE:
        print("⚠️ GPU requested but not available, falling back to CPU\n")
    elif use_gpu:
        print("🚀 GPU acceleration enabled\n")

    # Run rules
    print("🔍 Generating signals...")
    sigs = run_rules(df, cfg, use_gpu=use_gpu)
    print(f"✅ Generated {len(sigs)} signals\n")
    
    # Evaluate
    print("📊 Evaluating performance...")
    metrics = evaluate(
        df,
        sigs,
        horizon=args.horizon,
        min_edge=args.min_edge,
        use_gpu=use_gpu,
    )
    
    # Display results
    print("\n" + "=" * 60)
    print("📈 VALIDATION RESULTS")
    print("=" * 60)
    print(f"  Total signals:       {metrics['signals']}")
    print(f"  Evaluated:           {metrics['evaluated']}")
    print(f"  Win rate:            {metrics['win_rate']:.2%}")
    print(f"  Avg edge:            {metrics['avg_edge']:.5f} ({metrics['avg_edge']*100:.3f}%)")
    print(f"  Median edge (p50):   {metrics['p50_edge']:.5f} ({metrics['p50_edge']*100:.3f}%)")
    print(f"  p75 edge:            {metrics['p75_edge']:.5f} ({metrics['p75_edge']*100:.3f}%)")
    print(f"  p95 edge:            {metrics['p95_edge']:.5f} ({metrics['p95_edge']*100:.3f}%)")
    print(f"  Max edge:            {metrics['max_edge']:.5f} ({metrics['max_edge']*100:.3f}%)")
    print(f"  Min edge:            {metrics['min_edge']:.5f} ({metrics['min_edge']*100:.3f}%)")
    print("=" * 60)
    
    # Interpretation
    print("\n💡 Interpretation:")
    if metrics['win_rate'] >= 0.55:
        print("  ✅ Good win rate (>= 55%)")
    elif metrics['win_rate'] >= 0.50:
        print("  ⚠️  Moderate win rate (50-55%)")
    else:
        print("  ❌ Low win rate (< 50%)")
    
    if metrics['avg_edge'] > 0.0001:
        print(f"  ✅ Positive average edge")
    else:
        print(f"  ❌ Negative or zero average edge")
    
    if metrics['signals'] < 10:
        print("  ⚠️  Very few signals - consider loosening thresholds")
    elif metrics['signals'] > 1000:
        print("  ⚠️  Many signals - consider tightening thresholds")
    else:
        print(f"  ✅ Reasonable signal frequency")
    
    print()


if __name__ == "__main__":
    main()

