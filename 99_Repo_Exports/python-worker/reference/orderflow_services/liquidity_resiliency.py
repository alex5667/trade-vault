"""Liquidity resiliency / recovery-time tracker (Phase C / P2).

Measures how quickly spread/depth return to baseline after a stress episode.
This is complementary to BookResilienceTracker (depth replenishment after sweeps).

Outputs:
- liq_recovery_time_ms: elapsed time since stress start (if stress active), else 0
- liq_fragility_score: 0..1 (higher => more fragile)
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict


def _clamp01(x: float) -> float:
    if not math.isfinite(x):
        return 0.0
    return float(max(0.0, min(1.0, x)))


@dataclass
class LiquidityResiliencyTracker:
    spread_ema: float = 0.0
    depth_ema_usd: float = 0.0

    stress_active: bool = False
    stress_start_ms: int = 0
    last_recovery_ms: int = 0

    ema_alpha: float = 0.02
    stress_spread_mult: float = 1.75
    stress_depth_drop_frac: float = 0.50
    recover_spread_mult: float = 1.20
    recover_depth_drop_frac: float = 0.20
    recover_hold_ms: int = 1000

    _recover_candidate_ms: int = 0

    def update(self, *, ts_ms: int, spread_bps: float, depth_usd: float) -> Dict[str, Any]:
        ts_ms = int(ts_ms or 0)
        sb = float(spread_bps or 0.0)
        du = float(depth_usd or 0.0)

        if ts_ms <= 0 or not math.isfinite(sb) or not math.isfinite(du) or sb < 0 or du < 0:
            return {
                "liq_recovery_time_ms": int(0)
                "liq_fragility_score": float(0.0)
                "liq_stress_active": int(1 if self.stress_active else 0)
                "liq_spread_ema": float(self.spread_ema)
                "liq_depth_ema_usd": float(self.depth_ema_usd)
            }

        # EMA init/update
        if self.spread_ema <= 0:
            self.spread_ema = sb
        else:
            self.spread_ema = (1.0 - self.ema_alpha) * self.spread_ema + self.ema_alpha * sb

        if self.depth_ema_usd <= 0:
            self.depth_ema_usd = du
        else:
            self.depth_ema_usd = (1.0 - self.ema_alpha) * self.depth_ema_usd + self.ema_alpha * du

        base_spread = max(self.spread_ema, 1e-9)
        base_depth = max(self.depth_ema_usd, 1e-9)

        spread_ratio = sb / base_spread
        depth_ratio = du / base_depth

        wide = max(0.0, spread_ratio - 1.0)
        shallow = max(0.0, 1.0 - depth_ratio)
        frag = _clamp01(0.6 * (wide / 1.0) + 0.6 * (shallow / 0.5))

        is_stress = (spread_ratio >= self.stress_spread_mult) or (depth_ratio <= (1.0 - self.stress_depth_drop_frac))
        is_recover = (spread_ratio <= self.recover_spread_mult) and (depth_ratio >= (1.0 - self.recover_depth_drop_frac))

        if not self.stress_active:
            if is_stress:
                self.stress_active = True
                self.stress_start_ms = ts_ms
                self._recover_candidate_ms = 0
        else:
            if is_recover:
                if self._recover_candidate_ms <= 0:
                    self._recover_candidate_ms = ts_ms
                if (ts_ms - self._recover_candidate_ms) >= self.recover_hold_ms:
                    self.stress_active = False
                    self.last_recovery_ms = int(ts_ms - self.stress_start_ms) if self.stress_start_ms > 0 else 0
                    self.stress_start_ms = 0
                    self._recover_candidate_ms = 0
            else:
                self._recover_candidate_ms = 0

        rec_ms = 0
        if self.stress_active and self.stress_start_ms > 0:
            rec_ms = int(max(0, ts_ms - self.stress_start_ms))

        return {
            "liq_recovery_time_ms": int(rec_ms)
            "liq_fragility_score": float(frag)
            "liq_stress_active": int(1 if self.stress_active else 0)
            "liq_spread_ema": float(self.spread_ema)
            "liq_depth_ema_usd": float(self.depth_ema_usd)
            "liq_spread_ratio": float(spread_ratio)
            "liq_depth_ratio": float(depth_ratio)
        }
