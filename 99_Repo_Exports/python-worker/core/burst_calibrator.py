from __future__ import annotations

from dataclasses import dataclass


@dataclass
class BurstCalibrator:
    """
    Adaptive burst tuning:
      - uses tick gap percentiles + pressure snapshot to adjust window/max_age
      - avoids changing parameters mid-burst (only when burst inactive)

    Rationale:
      - if ticks are sparse, a large window delays emission unnecessarily;
        calibrator shrinks window so "deadline" is reached earlier.
      - if pressure is high, we also shrink window (emit quicker, reduce churn).

    Note:
      - background wall-flush (separate task) is still required to handle
        the 'no ticks after candidate' scenario.
    """
    base_window_ms: int = 2500
    min_window_ms: int = 300
    max_window_ms: int = 3000
    base_max_age_ms: int = 8000
    min_max_age_ms: int = 2000
    pressure_hi_per_min: float = 60.0
    pressure_extreme_per_min: float = 200.0
    max_age_mult: float = 3.0

    def compute(self, *, gap_p50_ms: float, cand_per_min: float) -> tuple[int, int]:
        # Start from base
        w = int(self.base_window_ms)

        # 1) Pressure-driven: the more candidates per minute, the smaller the window
        if cand_per_min >= self.pressure_extreme_per_min:
            # extreme churn: shrink aggressively (emit faster)
            w = int(max(self.min_window_ms, min(w, 300)))
        elif cand_per_min >= self.pressure_hi_per_min:
            w = int(max(self.min_window_ms, min(w, 800)))

        # 2) Tick-gap-driven: if ticks are VERY sparse, shrink window to be faster
        # Tick-gap shrink only when ticks arrive faster than min_window (very dense).
        # For sparse/normal gaps (>= min_window_ms), pressure logic is sufficient.
        if 0 < gap_p50_ms < self.min_window_ms:
             w = int(min(w, max(self.min_window_ms, int(1.2 * gap_p50_ms))))

        w = int(max(self.min_window_ms, min(self.max_window_ms, w)))

        # 3) Max Age should scale with window but have floor/ceil
        max_age = int(max(self.min_max_age_ms, int(w * self.max_age_mult)))
        if cand_per_min < self.pressure_hi_per_min:
             # For low pressure, we allow it to be at least base_max_age
             max_age = max(max_age, self.base_max_age_ms)

        return w, max_age
