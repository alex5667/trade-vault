from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PressureDecision:
    triggers_per_min_ema: float
    level: str  # "LOW"|"HI"|"EXTREME"
    burst_window_ms: int


def decide_burst_window_ms(
    *,
    triggers_per_min_ema: float,
    base_ms: int,
    min_ms: int,
    mid_ms: int,
    hi_thr_per_min: float,
    extreme_thr_per_min: float,
) -> PressureDecision:
    """
    Deterministic mapping:
      LOW      -> base_ms  (default 2500)
      HI       -> mid_ms   (default 1200)
      EXTREME  -> min_ms   (default 800)

    Rationale:
    - In extreme bursts, there are MANY candidates, so shorter window still picks "best-of-burst",
      reduces latency and downstream pressure.
    - In low pressure, keep a longer window to allow 1-2 extra candidates without overtrading.
    """
    p = float(triggers_per_min_ema or 0.0)
    if p >= float(extreme_thr_per_min):
        return PressureDecision(triggers_per_min_ema=p, level="EXTREME", burst_window_ms=int(min_ms))
    if p >= float(hi_thr_per_min):
        return PressureDecision(triggers_per_min_ema=p, level="HI", burst_window_ms=int(mid_ms))
    return PressureDecision(triggers_per_min_ema=p, level="LOW", burst_window_ms=int(base_ms))
