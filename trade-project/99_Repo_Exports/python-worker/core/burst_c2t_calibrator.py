from __future__ import annotations

"""burst_c2t_calibrator.py

Адаптивный калибратор порогов `burst_flip_max=0.85` и `c2t_max=8.0`
для `EntryPolicyGate`.

Проблема
--------
Жёсткие пороги одинаковы для всех символов:
- На ликвидных парах (BTC/ETH) burst_flip и c2t всегда около 1.0/3.0 в норме →
  порог 0.85/8.0 не работает как аномалия.
- На экзотах нормальный c2t может быть 15+, порог 8.0 даёт постоянные false-pos.

Метод
-----
P² streaming quantile q95 на фоновых значениях burst_flip и c2t
per (symbol × session). q95 → "срабатываем только на top 5% событий".

Особенность: burst_flip и c2t наблюдаем ТОЛЬКО когда > 0 (реальные значения,
не пропуски). Нулевые/отсутствующие значения не включаются в фоновое распределение.

Инварианты
----------
- auto_enforce=True (default): per-regime автопереключение после прогрева.
- Hard rails: burst_flip ∈ [0.3, 1.0], c2t ∈ [1.0, 100.0].
- Warmup: min_samples=300 per regime.
- Гистерезис: 0.03 (burst_flip) / 0.5 (c2t).
"""

import math
from dataclasses import dataclass
from typing import Any

from core.quantile_p2 import P2Quantile

# ── hard rails ──────────────────────────────────────────────────────────────
BURST_FLIP_FLOOR: float = 0.30   # ниже: не аномалия
BURST_FLIP_CEIL: float = 1.00    # выше: технически невозможно (ratio)
C2T_FLOOR: float = 1.0           # ниже: c2t < 1 — маловероятно при нормальной торговле
C2T_CEIL: float = 100.0          # выше: extreme market anomaly

DEFAULT_BURST_FLIP_MAX: float = 0.85
DEFAULT_C2T_MAX: float = 8.0

UPDATE_BAND_BURST: float = 0.03
UPDATE_BAND_C2T: float = 0.5


@dataclass
class BurstC2TThresholds:
    """
    burst_flip_max — q95 наблюдённого burst_flip в данном режиме
    c2t_max        — q95 наблюдённого c2t в данном режиме
    n              — число наблюдений
    src            — "static" / "calib_q95"
    """
    burst_flip_max: float
    c2t_max: float
    n: int
    src: str


class BurstC2TCalibrator:
    """
    Онлайн-калибратор порогов burst_flip и c2t для EntryPolicyGate.

    auto_enforce=True (default): режим автоматически переключается на
    калиброванные пороги как только накопилось min_samples наблюдений.
    """

    def __init__(
        self,
        *,
        min_samples: int = 300,
        enforce: bool = False,
        auto_enforce: bool = True,
        update_band_burst: float = UPDATE_BAND_BURST,
        update_band_c2t: float = UPDATE_BAND_C2T,
    ) -> None:
        self.min_samples = min_samples
        self.enforce = enforce
        self.auto_enforce = auto_enforce
        self.update_band_burst = update_band_burst
        self.update_band_c2t = update_band_c2t

        self._bf95: dict[str, P2Quantile] = {}
        self._c2t95: dict[str, P2Quantile] = {}
        self._n: dict[str, int] = {}
        self._committed_bf: dict[str, float] = {}
        self._committed_c2t: dict[str, float] = {}
        self._shadow: dict[str, BurstC2TThresholds] = {}

    # ── публичный API ────────────────────────────────────────────────────────

    def observe(self, *, regime: str, burst_flip: float, c2t: float) -> None:
        """
        Подать наблюдение burst_flip и c2t.

        Значения ≤ 0 (пропуски/заглушки) и вне rails игнорируются.
        """
        r = _norm(regime)
        counted = False

        if math.isfinite(burst_flip) and burst_flip > 0 and BURST_FLIP_FLOOR <= burst_flip <= BURST_FLIP_CEIL:
            self._get_bf95(r).update(burst_flip)
            counted = True

        if math.isfinite(c2t) and c2t > 0 and C2T_FLOOR <= c2t <= C2T_CEIL:
            self._get_c2t95(r).update(c2t)
            counted = True

        if counted:
            self._n[r] = self._n.get(r, 0) + 1

    def thresholds(
        self,
        *,
        regime: str,
        default_burst_flip: float = DEFAULT_BURST_FLIP_MAX,
        default_c2t: float = DEFAULT_C2T_MAX,
    ) -> BurstC2TThresholds:
        r = _norm(regime)
        n = self._n.get(r, 0)

        shadow = self._compute(r, n, default_burst_flip, default_c2t)
        self._shadow[r] = shadow

        warm = n >= self.min_samples
        effective_enforce = self.enforce or (self.auto_enforce and warm)
        if not effective_enforce:
            return BurstC2TThresholds(
                burst_flip_max=default_burst_flip,
                c2t_max=default_c2t,
                n=n,
                src="static",
            )

        prev_bf = self._committed_bf.get(r, default_burst_flip)
        prev_c2t = self._committed_c2t.get(r, default_c2t)

        new_bf = shadow.burst_flip_max
        new_c2t = shadow.c2t_max

        if abs(new_bf - prev_bf) >= self.update_band_burst:
            self._committed_bf[r] = new_bf
        else:
            new_bf = prev_bf

        if abs(new_c2t - prev_c2t) >= self.update_band_c2t:
            self._committed_c2t[r] = new_c2t
        else:
            new_c2t = prev_c2t

        return BurstC2TThresholds(
            burst_flip_max=new_bf, c2t_max=new_c2t, n=n, src="calib_q95"
        )

    def shadow_thresholds(self, *, regime: str) -> BurstC2TThresholds | None:
        return self._shadow.get(_norm(regime))

    def n(self, regime: str) -> int:
        return self._n.get(_norm(regime), 0)

    # ── персистентность ──────────────────────────────────────────────────────

    def dump_regime_state(self, *, symbol: str, regime: str, updated_ts_ms: int) -> dict[str, Any]:
        r = _norm(regime)
        return {
            "v": 1, "kind": "burst_c2t", "symbol": symbol, "regime": r,
            "updated_ts_ms": updated_ts_ms, "min_samples": self.min_samples,
            "enforce": self.enforce, "auto_enforce": self.auto_enforce,
            "n": self._n.get(r, 0),
            "committed_bf": self._committed_bf.get(r),
            "committed_c2t": self._committed_c2t.get(r),
            "bf95": (self._bf95[r].to_state() if r in self._bf95 else None),
            "c2t95": (self._c2t95[r].to_state() if r in self._c2t95 else None),
        }

    def load_regime_state(self, state: Any) -> None:
        try:
            if not isinstance(state, dict) or state.get("kind") != "burst_c2t":
                return
            r = str(state.get("regime") or "na").lower()
            self.min_samples = int(state.get("min_samples", self.min_samples) or self.min_samples)
            self._n[r] = int(state.get("n", 0) or 0)
            if state.get("committed_bf") is not None:
                self._committed_bf[r] = float(state["committed_bf"])
            if state.get("committed_c2t") is not None:
                self._committed_c2t[r] = float(state["committed_c2t"])
            if bf_raw := state.get("bf95"):
                self._bf95[r] = P2Quantile.from_state(bf_raw)
            if c2t_raw := state.get("c2t95"):
                self._c2t95[r] = P2Quantile.from_state(c2t_raw)
        except Exception:
            pass

    # ── вспомогательные ──────────────────────────────────────────────────────

    def _get_bf95(self, r: str) -> P2Quantile:
        if r not in self._bf95:
            self._bf95[r] = P2Quantile(p=0.95)
        return self._bf95[r]

    def _get_c2t95(self, r: str) -> P2Quantile:
        if r not in self._c2t95:
            self._c2t95[r] = P2Quantile(p=0.95)
        return self._c2t95[r]

    def _compute(self, r: str, n: int, d_bf: float, d_c2t: float) -> BurstC2TThresholds:
        if n < self.min_samples:
            return BurstC2TThresholds(burst_flip_max=d_bf, c2t_max=d_c2t, n=n, src="static")
        raw_bf = self._bf95[r].value() if r in self._bf95 else None
        raw_c2t = self._c2t95[r].value() if r in self._c2t95 else None
        bf = _clamp(raw_bf, d_bf, BURST_FLIP_FLOOR, BURST_FLIP_CEIL)
        c2t = _clamp(raw_c2t, d_c2t, C2T_FLOOR, C2T_CEIL)
        return BurstC2TThresholds(burst_flip_max=bf, c2t_max=c2t, n=n, src="calib_q95")


def _norm(regime: str | None) -> str:
    return (regime or "na").strip().lower()

def _clamp(val: float | None, default: float, lo: float, hi: float) -> float:
    if val is None or not math.isfinite(val):
        return default
    return max(lo, min(hi, val))
