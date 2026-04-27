from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


@dataclass
class RegimeQuantiles:
    """Regime quantiles for a symbol/timeframe pair."""
    symbol: str
    timeframe: str
    adx_p40: float
    adx_p60: float
    adx_p75: float
    atrp_p25: float
    atrp_p50: float
    atrp_p75: float
    sample_size: int
    updated_at_ms: int


def approx_quantile_3pt(x: float, q25: float, q50: float, q75: float) -> float:
    """
    LEGACY: Approximate quantile using 3 points (25/50/75 percentiles).
    
    This function is kept for backward compatibility with ATRP calculations.
    For ADX quantiles (40/60/75), use approx_quantile_adx() instead.
    
    Args:
        x: Value to map to quantile position
        q25: 25th percentile reference value
        q50: 50th percentile (median) reference value
        q75: 75th percentile reference value
    
    Returns:
        Approximate quantile position of x in the distribution [0.0..0.95].
    """
    if x <= 0:
        return 0.0
    if q25 <= 0 or q50 <= 0 or q75 <= 0:
        return 0.0
    
    # Below 25th percentile: map to [0.10..0.25]
    if x <= q25:
        return 0.10 + 0.15 * (x / max(q25, 1e-12))
    
    # Between 25th and 50th: map to [0.25..0.50]
    if x <= q50:
        return 0.25 + 0.25 * ((x - q25) / max((q50 - q25), 1e-12))
    
    # Between 50th and 75th: map to [0.50..0.75]
    if x <= q75:
        return 0.50 + 0.25 * ((x - q50) / max((q75 - q50), 1e-12))
    
    # Above 75th percentile: map to [0.75..0.95] (soft cap)
    return min(0.95, 0.75 + 0.20 * ((x - q75) / max(q75, 1e-12)))


def approx_quantile_adx(x: float, p40: float, p60: float, p75: float) -> float:
    """
    Approximate ADX quantile using 3 points (40/60/75 percentiles).
    Piecewise linear. Optimized for ADX strength distribution.
    
    Args:
        x: ADX value to map to quantile position
        p40: 40th percentile reference value
        p60: 60th percentile reference value
        p75: 75th percentile reference value
    
    Returns:
        Approximate quantile position of x in the distribution [0.0..0.95].
    """
    if x <= 0:
        return 0.0
    if p40 <= 0 or p60 <= 0 or p75 <= 0:
        return 0.0
    
    # Below 40th percentile: map to [0.10..0.40]
    if x <= p40:
        return 0.10 + 0.30 * (x / max(p40, 1e-12))
    
    # Between 40th and 60th: map to [0.40..0.60]
    if x <= p60:
        return 0.40 + 0.20 * ((x - p40) / max((p60 - p40), 1e-12))
    
    # Between 60th and 75th: map to [0.60..0.75]
    if x <= p75:
        return 0.60 + 0.15 * ((x - p60) / max((p75 - p60), 1e-12))
    
    # Above 75th percentile: map to [0.75..0.95] (soft cap)
    return min(0.95, 0.75 + 0.20 * ((x - p75) / max(p75, 1e-12)))


class RegimeQuantilesStore:
    """
    In-memory cache of regime_quantiles pulled from DB via a caller-provided fetcher.
    We keep it connector-agnostic to avoid wiring DB clients everywhere.
    """

    def __init__(self, *, refresh_ms: int = 300_000) -> None:
        self.refresh_ms = int(refresh_ms)
        self._last_refresh_ms: int = 0
        self._cache: Dict[Tuple[str, str], RegimeQuantiles] = {}

    def maybe_refresh(self, *, now_ms: int, rows: Optional[list[dict]] = None) -> None:
        """
        Refresh cache from DB rows if refresh interval has passed.
        
        rows: list of dicts from DB. If None -> do nothing.
        Callers decide when/how to fetch (async/sync).
        """
        if now_ms - self._last_refresh_ms < self.refresh_ms:
            return
        if rows is None:
            return
        new: Dict[Tuple[str, str], RegimeQuantiles] = {}
        for d in rows:
            try:
                sym = str(d["symbol"]).upper()
                tf = str(d["timeframe"])
                rq = RegimeQuantiles(
                    symbol=sym,
                    timeframe=tf,
                    adx_p40=float(d["adx_p40"]),
                    adx_p60=float(d["adx_p60"]),
                    adx_p75=float(d["adx_p75"]),
                    atrp_p25=float(d["atrp_p25"]),
                    atrp_p50=float(d["atrp_p50"]),
                    atrp_p75=float(d["atrp_p75"]),
                    sample_size=int(d.get("sampleSize") or d.get("sample_size") or 0),
                    updated_at_ms=int(d.get("updatedAtMs") or d.get("updated_at_ms") or now_ms),
                )
                new[(sym, tf)] = rq
            except Exception:
                continue
        self._cache = new
        self._last_refresh_ms = int(now_ms)

    def get(self, *, symbol: str, timeframe: str) -> Optional[RegimeQuantiles]:
        """Get cached quantiles for a symbol/timeframe pair."""
        return self._cache.get((str(symbol).upper(), str(timeframe)))
