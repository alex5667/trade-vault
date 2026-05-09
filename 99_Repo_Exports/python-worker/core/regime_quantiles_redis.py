from __future__ import annotations

import json
from dataclasses import dataclass


@dataclass
class RQ:
    """Regime Quantiles from Redis."""
    symbol: str
    timeframe: str
    sample_size: int
    adx_p40: float
    adx_p60: float
    adx_p75: float
    atrp_p25: float
    atrp_p50: float
    atrp_p75: float
    updated_at_ms: int


def parse_rq(raw: str) -> RQ | None:
    """
    Parse regime quantiles from Redis JSON.
    Validates monotonicity: atrp_p25 <= atrp_p50 <= atrp_p75.
    Returns None if invalid or insufficient samples.
    """
    try:
        d = json.loads(raw)
        sym = (d.get("symbol") or "").upper()
        tf = (d.get("timeframe") or "1m")
        n = int(d.get("sampleSize") or d.get("sample_size") or 0)
        if not sym or n <= 0:
            return None

        # Monotonic sanity check for ATR% quantiles
        a25 = float(d.get("atrp_p25") or 0.0)
        a50 = float(d.get("atrp_p50") or 0.0)
        a75 = float(d.get("atrp_p75") or 0.0)
        if a25 <= 0 or a50 <= 0 or a75 <= 0 or not (a25 <= a50 <= a75):
            return None

        return RQ(
            symbol=sym,
            timeframe=tf,
            sample_size=n,
            adx_p40=float(d.get("adx_p40") or 0.0),
            adx_p60=float(d.get("adx_p60") or 0.0),
            adx_p75=float(d.get("adx_p75") or 0.0),
            atrp_p25=a25,
            atrp_p50=a50,
            atrp_p75=a75,
            updated_at_ms=int(d.get("updatedAtMs") or 0),
        )
    except Exception:
        return None
