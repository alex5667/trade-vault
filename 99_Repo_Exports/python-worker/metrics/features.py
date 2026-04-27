"""
Advanced Features Module - Extended feature engineering for XAUUSD.

Provides bar-based features (delta series, ATR, weak progress) complementing
the existing tick-based features in signals/featurizer.py.

Usage:
    from metrics.features import delta_series_from_ticks, atr_from_bars
"""

import numpy as np
import pandas as pd
from typing import Optional

# ✅ GPU Support: импорт GPU сервиса для ускорения вычислений
try:
    from services.gpu_compute_service import get_gpu_service
    GPU_SERVICE_AVAILABLE = True
except ImportError:
    GPU_SERVICE_AVAILABLE = False
    get_gpu_service = None


def delta_series_from_ticks(
    ticks: pd.DataFrame,
    bar_ms: int = 60_000
) -> pd.DataFrame:
    """
    Aggregate ticks into bars with delta metrics.
    
    Args:
        ticks: DataFrame with columns [ts_ms, side, qty, price]
               side: 'BUY' or 'SELL'
        bar_ms: Bar size in milliseconds (default: 1 min)
        
    Returns:
        DataFrame with columns:
            - t_open: Bar open time
            - t_close: Bar close time
            - taker_buy: Buy volume
            - taker_sell: Sell volume
            - delta: taker_buy - taker_sell
            - high, low, open, close
            - range: high - low
    """
    df = ticks.copy()
    
    # Create time buckets
    df["bucket"] = (df["ts_ms"] // bar_ms) * bar_ms
    
    # Group by bucket
    gb = df.groupby("bucket")
    
    # Aggregate volumes by side
    taker_buy = gb.apply(
        lambda g: g.loc[g["side"] == "BUY", "qty"].sum()
    ).rename("taker_buy")
    
    taker_sell = gb.apply(
        lambda g: g.loc[g["side"] == "SELL", "qty"].sum()
    ).rename("taker_sell")
    
    # Price metrics
    high = gb["price"].max().rename("high")
    low = gb["price"].min().rename("low")
    open_price = gb["price"].first().rename("open")
    close_price = gb["price"].last().rename("close")
    
    # Combine
    out = pd.concat([
        taker_buy,
        taker_sell,
        high,
        low,
        open_price,
        close_price
    ], axis=1).reset_index().rename(columns={"bucket": "t_open"})
    
    # Add derived fields
    out["t_close"] = out["t_open"] + bar_ms
    out["range"] = (out["high"] - out["low"]).abs()
    out["delta"] = out["taker_buy"] - out["taker_sell"]
    
    return out


def zscore(series: pd.Series, lookback: int = 50) -> pd.Series:
    """
    Calculate rolling Z-score with optional GPU acceleration.
    
    Args:
        series: Input series
        lookback: Rolling window size
        
    Returns:
        Z-score series
    """
    # ✅ GPU Support: используем GPU для всех серий (убрали порог для разгрузки CPU)
    if GPU_SERVICE_AVAILABLE and len(series) > 0:
        try:
            gpu_service = get_gpu_service()
            if gpu_service and gpu_service.is_gpu_available():
                values = series.values.astype(np.float32)
                z_scores = gpu_service.compute_z_scores(values, window=lookback)
                return pd.Series(z_scores, index=series.index)
        except Exception:
            pass  # Fallback to CPU
    
    # CPU fallback
    r = series.rolling(lookback, min_periods=lookback)
    return (series - r.mean()) / r.std(ddof=0)


def atr_from_bars(bars: pd.DataFrame, n: int = 14) -> pd.Series:
    """
    Calculate ATR using Wilder's smoothing with optional GPU acceleration.
    
    Args:
        bars: DataFrame with high, low, close columns
        n: ATR period
        
    Returns:
        ATR series
    """
    # ✅ GPU Support: используем GPU для всех датафреймов (убрали порог для разгрузки CPU)
    if GPU_SERVICE_AVAILABLE and len(bars) > 0:
        try:
            gpu_service = get_gpu_service()
            if gpu_service and gpu_service.is_gpu_available():
                highs = bars["high"].values.astype(np.float32)
                lows = bars["low"].values.astype(np.float32)
                closes = bars["close"].values.astype(np.float32)
                atr_values = gpu_service.compute_atr_batch(highs, lows, closes, period=n)
                return pd.Series(atr_values, index=bars.index)
        except Exception:
            pass  # Fallback to CPU
    
    # CPU fallback
    high = bars["high"]
    low = bars["low"]
    prev_close = bars["close"].shift(1)
    
    # True Range
    tr = pd.concat([
        (high - low).abs(),
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    
    # Wilder's smoothing (EMA with alpha = 1/n)
    atr = tr.ewm(alpha=1 / n, adjust=False).mean()
    
    return atr


def weak_progress(
    bars: pd.DataFrame,
    atr: pd.Series,
    threshold: float = 0.3
) -> pd.Series:
    """
    Detect weak progress (absorption).
    
    Args:
        bars: DataFrame with range column
        atr: ATR series
        threshold: Threshold for range/ATR ratio
        
    Returns:
        Boolean series (True = weak progress)
    """
    ratio = (bars["range"] / atr).fillna(0.0)
    return ratio <= threshold


def delta_spike_z(bars: pd.DataFrame, lookback: int = 50) -> pd.Series:
    """
    Calculate Delta Z-score for spike detection.
    
    Args:
        bars: DataFrame with delta column
        lookback: Rolling window
        
    Returns:
        Z-score series
    """
    return zscore(bars["delta"], lookback=lookback)


def absorption_mask(
    bars: pd.DataFrame,
    z: pd.Series,
    wp_mask: pd.Series,
    z_strong: float = 2.0,
    z_moderate: float = 1.5
) -> pd.Series:
    """
    Detect absorption (strong delta + weak progress).
    
    Args:
        bars: DataFrame
        z: Delta Z-score
        wp_mask: Weak progress mask
        z_strong: Strong threshold
        z_moderate: Moderate threshold
        
    Returns:
        Boolean series (True = absorption detected)
    """
    # Strong or moderate delta spike + weak progress
    return ((z.abs() >= z_moderate) & wp_mask)


def cvd_from_delta(delta: pd.Series) -> pd.Series:
    """
    Cumulative Volume Delta with optional GPU acceleration.
    
    Args:
        delta: Delta series
        
    Returns:
        CVD series
    """
    # ✅ GPU Support: используем GPU для всех серий (убрали порог для разгрузки CPU)
    if GPU_SERVICE_AVAILABLE and len(delta) > 0:
        try:
            gpu_service = get_gpu_service()
            if gpu_service and gpu_service.is_gpu_available():
                deltas = delta.values.astype(np.float32)
                cvd_values = gpu_service.compute_cvd(deltas)
                return pd.Series(cvd_values, index=delta.index)
        except Exception:
            pass  # Fallback to CPU
    
    # CPU fallback
    return delta.cumsum()




