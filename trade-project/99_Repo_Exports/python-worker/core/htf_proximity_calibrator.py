from __future__ import annotations

"""htf_proximity_calibrator.py

Адаптивный калибратор порогов `htf_near_mult=0.2` и `htf_far_mult=0.8`
для `RegimeConfig` (models/data_models.py:43-44).

Проблема
--------
ATR-мультипликаторы 0.2/0.8 для "близко к уровню" / "далеко от уровня"
одинаковы для всех символов. BTC с суточным ATR 300 bps и PEPE с ATR 5000 bps
имеют разные типичные расстояния до HTF-уровней.

Метод
-----
Наблюдаем нормализованное расстояние до HTF-уровня: `ratio = dist_bps / daily_atr_bps`.
- q20 этого распределения → near_mult: "в 20% случаев цена настолько близко"
- q80 → far_mult: "в 80% случаев цена настолько далеко или ближе"

Инварианты
----------
- auto_enforce=True: per-symbol автопереключение после min_samples.
- Rails: near_mult ∈ [0.05, 0.5], far_mult ∈ [0.4, 2.0].
- Монотонность: far_mult > near_mult (всегда).
- Warmup: min_samples=500 per symbol.
"""

import math
from dataclasses import dataclass
from typing import Any

from core.quantile_p2 import P2Quantile

NEAR_MULT_FLOOR: float = 0.05
NEAR_MULT_CEIL: float = 0.50
FAR_MULT_FLOOR: float = 0.40
FAR_MULT_CEIL: float = 2.00

DEFAULT_NEAR_MULT: float = 0.20
DEFAULT_FAR_MULT: float = 0.80

UPDATE_BAND: float = 0.03


@dataclass
class HtfProximityThresholds:
    near_mult: float
    far_mult: float
    n: int
    src: str

    def __post_init__(self) -> None:
        # Монотонность: far > near
        if self.far_mult <= self.near_mult:
            self.far_mult = self.near_mult + 0.1


class HtfProximityCalibrator:
    """
    Наблюдает `dist_bps / daily_atr_bps` per symbol, калибрует near/far multipliers.
    """

    def __init__(
        self,
        *,
        min_samples: int = 500,
        enforce: bool = False,
        auto_enforce: bool = True,
        update_band: float = UPDATE_BAND,
    ) -> None:
        self.min_samples = min_samples
        self.enforce = enforce
        self.auto_enforce = auto_enforce
        self.update_band = update_band

        self._q20: dict[str, P2Quantile] = {}
        self._q80: dict[str, P2Quantile] = {}
        self._n: dict[str, int] = {}
        self._committed_near: dict[str, float] = {}
        self._committed_far: dict[str, float] = {}
        self._shadow: dict[str, HtfProximityThresholds] = {}

    def observe(self, *, symbol: str, dist_bps: float, daily_atr_bps: float) -> None:
        """
        Подать наблюдение. `dist_bps` — расстояние до ближайшего HTF-уровня,
        `daily_atr_bps` — дневной ATR в б.п.

        Пропускаем если daily_atr_bps ≤ 0 или dist_bps < 0.
        """
        sym = _norm(symbol)
        if not math.isfinite(dist_bps) or dist_bps < 0:
            return
        if not math.isfinite(daily_atr_bps) or daily_atr_bps <= 0:
            return

        ratio = dist_bps / daily_atr_bps
        if not (0.0 < ratio <= 5.0):  # разумный диапазон нормализованного расстояния
            return

        self._get_q20(sym).update(ratio)
        self._get_q80(sym).update(ratio)
        self._n[sym] = self._n.get(sym, 0) + 1

    def thresholds(
        self,
        *,
        symbol: str,
        default_near: float = DEFAULT_NEAR_MULT,
        default_far: float = DEFAULT_FAR_MULT,
    ) -> HtfProximityThresholds:
        sym = _norm(symbol)
        n = self._n.get(sym, 0)

        shadow = self._compute(sym, n, default_near, default_far)
        self._shadow[sym] = shadow

        warm = n >= self.min_samples
        if not (self.enforce or (self.auto_enforce and warm)):
            return HtfProximityThresholds(near_mult=default_near, far_mult=default_far, n=n, src="static")

        prev_near = self._committed_near.get(sym, default_near)
        prev_far = self._committed_far.get(sym, default_far)

        new_near = shadow.near_mult
        new_far = shadow.far_mult

        if abs(new_near - prev_near) >= self.update_band:
            self._committed_near[sym] = new_near
        else:
            new_near = prev_near

        if abs(new_far - prev_far) >= self.update_band:
            self._committed_far[sym] = new_far
        else:
            new_far = prev_far

        return HtfProximityThresholds(near_mult=new_near, far_mult=new_far, n=n, src="calib_q20q80")

    def shadow_thresholds(self, *, symbol: str) -> HtfProximityThresholds | None:
        return self._shadow.get(_norm(symbol))

    def n(self, symbol: str) -> int:
        return self._n.get(_norm(symbol), 0)

    def dump_symbol_state(self, *, symbol: str, updated_ts_ms: int) -> dict[str, Any]:
        sym = _norm(symbol)
        return {
            "v": 1, "kind": "htf_proximity", "symbol": sym,
            "updated_ts_ms": updated_ts_ms, "min_samples": self.min_samples,
            "n": self._n.get(sym, 0),
            "committed_near": self._committed_near.get(sym),
            "committed_far": self._committed_far.get(sym),
            "q20": (self._q20[sym].to_state() if sym in self._q20 else None),
            "q80": (self._q80[sym].to_state() if sym in self._q80 else None),
        }

    def load_symbol_state(self, state: Any) -> None:
        try:
            if not isinstance(state, dict) or state.get("kind") != "htf_proximity":
                return
            sym = str(state.get("symbol") or "na").lower()
            self.min_samples = int(state.get("min_samples", self.min_samples) or self.min_samples)
            self._n[sym] = int(state.get("n", 0) or 0)
            if state.get("committed_near") is not None:
                self._committed_near[sym] = float(state["committed_near"])
            if state.get("committed_far") is not None:
                self._committed_far[sym] = float(state["committed_far"])
            if q20_raw := state.get("q20"):
                self._q20[sym] = P2Quantile.from_state(q20_raw)
            if q80_raw := state.get("q80"):
                self._q80[sym] = P2Quantile.from_state(q80_raw)
        except Exception:
            pass

    def _get_q20(self, sym: str) -> P2Quantile:
        if sym not in self._q20:
            self._q20[sym] = P2Quantile(p=0.20)
        return self._q20[sym]

    def _get_q80(self, sym: str) -> P2Quantile:
        if sym not in self._q80:
            self._q80[sym] = P2Quantile(p=0.80)
        return self._q80[sym]

    def _compute(self, sym: str, n: int, d_near: float, d_far: float) -> HtfProximityThresholds:
        if n < self.min_samples:
            return HtfProximityThresholds(near_mult=d_near, far_mult=d_far, n=n, src="static")
        raw_near = self._q20[sym].value() if sym in self._q20 else None
        raw_far = self._q80[sym].value() if sym in self._q80 else None
        near = _clamp(raw_near, d_near, NEAR_MULT_FLOOR, NEAR_MULT_CEIL)
        far = _clamp(raw_far, d_far, FAR_MULT_FLOOR, FAR_MULT_CEIL)
        return HtfProximityThresholds(near_mult=near, far_mult=far, n=n, src="calib_q20q80")


def _norm(s: str | None) -> str:
    return (s or "na").strip().lower()

def _clamp(val: float | None, default: float, lo: float, hi: float) -> float:
    if val is None or not math.isfinite(val):
        return default
    return max(lo, min(hi, val))
