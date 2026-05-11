from __future__ import annotations

"""
CVD Reclaim (bonus-only)
-----------------------
На событии reclaim (hold_end) проверяем, что CVD за период sweep->reclaim
двигался в сторону "reversal bias" sweep-а.

Важно (по вашим вводным):
- sweep_ts_ms и reclaim_ts_ms точные (бар-close).
- return_ts_ms внутри бара нет — поэтому проверка дискретная.

Мы сохраняем last_cvd_reclaim ТОЛЬКО когда reclaim подтверждён (как вы хотите).
"""

from typing import Optional, Any
from dataclasses import dataclass


def _dir_sign(side: Any) -> int:
    s = (side or "").upper()
    if s == "LONG":
        return 1
    if s == "SHORT":
        return -1
    return 0


@dataclass(frozen=True)
class CVDReclaimEvent:
    ts_ms: int
    bias: str          # LONG/SHORT (reversal direction)
    sweep_ts_ms: int
    reclaim_ts_ms: int
    cvd_sweep: float
    cvd_reclaim: float
    delta_cvd: float
    ok: int
    score: float = 0.0
    cvd_delta: float = 0.0 # alias for backward compatibility if needed


def compute_cvd_reclaim(
    *,
    ts_ms: int,
    bias: Optional[str] = None,
    direction_bias: Optional[str] = None,
    sweep_ts_ms: int,
    reclaim_ts_ms: int,
    cvd_sweep: float,
    cvd_reclaim: float,
    min_abs_delta: float = 0.0,
    min_abs: Optional[float] = None,    # alias
    sat_abs: float = 0.0,             # saturation for scoring
    **kwargs: Any
) -> CVDReclaimEvent:
    """
    ok=1 если:
      sign(delta_cvd) == sign(bias) и |delta_cvd| >= min_abs_delta
    """
    # Resolve aliases
    effective_bias = direction_bias if direction_bias is not None else bias
    effective_min_abs = min_abs if min_abs is not None else min_abs_delta

    dc = float(cvd_reclaim) - float(cvd_sweep)
    sgn = 1 if dc > 0 else (-1 if dc < 0 else 0)
    want = _dir_sign(effective_bias)

    ok = 1 if (want != 0 and sgn == want and abs(dc) >= float(effective_min_abs)) else 0

    # Calculate score (optional)
    score = 0.0
    if want != 0 and sgn == want:
        if sat_abs > 0:
            score = min(1.0, abs(dc) / float(sat_abs))
        else:
            score = 1.0 if ok else 0.0

    return CVDReclaimEvent(
        ts_ms=int(ts_ms),
        bias=(effective_bias or "NONE").upper(),
        sweep_ts_ms=int(sweep_ts_ms),
        reclaim_ts_ms=int(reclaim_ts_ms),
        cvd_sweep=float(cvd_sweep),
        cvd_reclaim=float(cvd_reclaim),
        delta_cvd=float(dc),
        cvd_delta=float(dc),
        ok=int(ok),
        score=float(score)
    )
