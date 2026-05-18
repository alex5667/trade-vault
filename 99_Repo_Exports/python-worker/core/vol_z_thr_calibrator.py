from __future__ import annotations

"""vol_z_thr_calibrator.py

Адаптивный калибратор порога vol_z_thr для детектора локальных экстремумов
(`CryptoMarketState.is_new_local_extreme` / `_is_new_local_extreme`).

Проблема
--------
Жёсткий порог `vol_z_thr = 1.5` не учитывает:
- Различие в распределении объёмов по символам (BTC vs альты).
- Внутрисуточные сессии (US-сессия даёт систематически больший объём).
- Смену рыночного режима (после новостного шока базовая µ/σ объёма меняется).

Метод (мировые практики)
------------------------
1. **P² streaming quantile** (Jain & Chlamtac, 1985):
   O(1) память (5 маркеров), пригоден для онлайн-потоков.
   Два уровня: q80 (soft) и q90 (hard).

2. **Режим (symbol, session)**: (4 сессии × N символов).
   Ключ: `"{symbol_lower}:{session}"` — соответствует project-convention.

3. **Двухуровневый вывод**:
   - `vol_z_soft` = q80 фонового vol_z → "top 20 % объёмных событий".
   - `vol_z_hard` = q90 → "top 10 % — только явные выбросы".

4. **Гистерезис** (UPDATE_BAND): порог обновляется только при изменении > band,
   что предотвращает микро-осцилляцию при нестационарности.

5. **Hard rails** [VOL_Z_FLOOR, VOL_Z_CEIL]: защита от NaN / бесконечностей.

6. **Warmup guard**: до min_samples наблюдений — fail-open, возвращаем
   статические дефолты (shadow не применяется).

7. **Персистентность**: dump_regime_state() / load_regime_state() для
   сохранения в Redis HSET или JSON-файл при перезапуске.

Инварианты
----------
- Нет IO: вся персистентность — на стороне вызывающего кода.
- Детерминизм: одна и та же последовательность observe() → те же пороги.
- Монотонность: vol_z_hard ≥ vol_z_soft (в рамках одного режима).
- vol_z_soft ≤ vol_z_hard (clamped).

Использование
-------------
    calib = VolZThrCalibrator(min_samples=300, enforce=False)

    # Раз за бар:
    vol_z = (bar.volume - mu_vol) / std_vol
    calib.observe(regime="btcusdt:us", vol_z=vol_z)

    # В детекторе:
    th = calib.thresholds(regime="btcusdt:us")
    if vol_z >= th.soft:          # soft: one of the two gates must pass
        ...
    if vol_z >= th.hard:          # hard: high-confidence extreme
        ...

    # Audit / shadow (всегда доступно вне зависимости от enforce):
    shadow = calib.shadow_thresholds(regime="btcusdt:us")
"""

import math
from dataclasses import dataclass
from typing import Any

from core.quantile_p2 import P2Quantile

# ── hard rails ──────────────────────────────────────────────────────────────
VOL_Z_FLOOR: float = 0.3   # ниже — числовой шум (std≈0) или константный объём
VOL_Z_CEIL: float = 6.0    # выше — крайний выброс; в q90 практически не попадает

# Статические дефолты — синхронизированы с сигнатурой _is_new_local_extreme.
DEFAULT_VOL_Z_SOFT: float = 1.5   # текущее жёсткое значение → soft
DEFAULT_VOL_Z_HARD: float = 2.2   # консервативный hard (≈q97.2 N(0,1))

# Минимальное изменение для применения нового порога (гистерезис).
UPDATE_BAND: float = 0.10


@dataclass
class VolZThresholds:
    """
    Калиброванные (или статические) пороги vol_z для одного (symbol × session).

    soft  — q80 фонового vol_z (менее строгий критерий)
    hard  — q90 фонового vol_z (строгий критерий)
    n     — число наблюдений в данном режиме
    src   — "static" если холодный/shadow; "calib_q80q90" если applied
    """
    soft: float
    hard: float
    n: int
    src: str

    def __post_init__(self) -> None:
        # Монотонность: hard всегда ≥ soft
        if self.hard < self.soft:
            self.hard = self.soft


class VolZThrCalibrator:
    """
    Онлайн-калибратор порогов vol_z для детектора локальных экстремумов.

    Параметры
    ----------
    min_samples : int
        Минимальное число наблюдений в режиме перед применением калибровки.
    enforce : bool
        True = принудительно применять, даже до auto_enforce.
    auto_enforce : bool
        True (default) = автоматически переключиться на калиброванные значения
        для каждого режима как только n >= min_samples. Отдельно per-regime.
        False = чистый shadow-режим, требует явного enforce=True.
    update_band : float
        Минимальное абсолютное изменение порога для его обновления (гистерезис).
    """

    def __init__(
        self,
        *,
        min_samples: int = 300,
        enforce: bool = False,
        auto_enforce: bool = True,
        update_band: float = UPDATE_BAND,
    ) -> None:
        self.min_samples = min_samples
        self.enforce = enforce
        self.auto_enforce = auto_enforce
        self.update_band = update_band

        # P² оценщики per-режим: q80 (soft) и q90 (hard)
        self._q80: dict[str, P2Quantile] = {}
        self._q90: dict[str, P2Quantile] = {}

        # Количество наблюдений per-режим
        self._n: dict[str, int] = {}

        # Зафиксированные (committed) пороги per-режим — обновляются с гистерезисом
        self._committed_soft: dict[str, float] = {}
        self._committed_hard: dict[str, float] = {}

        # Теневые предложения — всегда вычисляются для аудита
        self._shadow: dict[str, VolZThresholds] = {}

    # ── публичный API ────────────────────────────────────────────────────────

    def observe(self, *, regime: str, vol_z: float) -> None:
        """
        Подать одно наблюдение vol_z.

        Значения вне [VOL_Z_FLOOR, VOL_Z_CEIL] игнорируются (фильтр выбросов).
        NaN / Inf отбрасываются.
        """
        r = _norm_regime(regime)
        if not math.isfinite(vol_z):
            return
        if not (VOL_Z_FLOOR <= vol_z <= VOL_Z_CEIL):
            return

        self._get_q80(r).update(vol_z)
        self._get_q90(r).update(vol_z)
        self._n[r] = self._n.get(r, 0) + 1

    def thresholds(
        self,
        *,
        regime: str,
        default_soft: float = DEFAULT_VOL_Z_SOFT,
        default_hard: float = DEFAULT_VOL_Z_HARD,
    ) -> VolZThresholds:
        """
        Вернуть пороги для данного режима.

        enforce=False или холодный режим → статические дефолты (fail-open).
        Теневое предложение всегда обновляется и доступно через shadow_thresholds().
        """
        r = _norm_regime(regime)
        n = self._n.get(r, 0)

        shadow = self._compute(r, n, default_soft, default_hard)
        self._shadow[r] = shadow

        warm = n >= self.min_samples
        effective_enforce = self.enforce or (self.auto_enforce and warm)
        if not effective_enforce:
            return VolZThresholds(
                soft=default_soft,
                hard=default_hard,
                n=n,
                src="static",
            )

        # Применяем с гистерезисом
        prev_soft = self._committed_soft.get(r, default_soft)
        prev_hard = self._committed_hard.get(r, default_hard)

        new_soft = shadow.soft
        new_hard = shadow.hard

        if abs(new_soft - prev_soft) >= self.update_band:
            self._committed_soft[r] = new_soft
        else:
            new_soft = prev_soft

        if abs(new_hard - prev_hard) >= self.update_band:
            self._committed_hard[r] = new_hard
        else:
            new_hard = prev_hard

        return VolZThresholds(soft=new_soft, hard=new_hard, n=n, src="calib_q80q90")

    def shadow_thresholds(self, *, regime: str) -> VolZThresholds | None:
        """Последнее вычисленное теневое предложение (без применения)."""
        return self._shadow.get(_norm_regime(regime))

    def n(self, regime: str) -> int:
        """Число наблюдений в данном режиме."""
        return self._n.get(_norm_regime(regime), 0)

    # ── персистентность ──────────────────────────────────────────────────────

    def dump_regime_state(
        self, *, symbol: str, regime: str, updated_ts_ms: int
    ) -> dict[str, Any]:
        """JSON-сериализуемое состояние для сохранения в Redis / файл."""
        r = _norm_regime(regime)
        return {
            "v": 1,
            "kind": "vol_z_thr",
            "symbol": symbol,
            "regime": r,
            "updated_ts_ms": updated_ts_ms,
            "min_samples": self.min_samples,
            "enforce": self.enforce,
            "n": self._n.get(r, 0),
            "committed_soft": self._committed_soft.get(r),
            "committed_hard": self._committed_hard.get(r),
            "q80": (self._q80[r].to_state() if r in self._q80 else None),
            "q90": (self._q90[r].to_state() if r in self._q90 else None),
        }

    def load_regime_state(self, state: Any) -> None:
        """Восстановить состояние из dump_regime_state(). Fail-open."""
        try:
            if not isinstance(state, dict):
                return
            if state.get("kind") != "vol_z_thr":
                return
            r = str(state.get("regime") or "na").lower()
            self.min_samples = int(
                state.get("min_samples", self.min_samples) or self.min_samples
            )
            self._n[r] = int(state.get("n", 0) or 0)

            if state.get("committed_soft") is not None:
                self._committed_soft[r] = float(state["committed_soft"])
            if state.get("committed_hard") is not None:
                self._committed_hard[r] = float(state["committed_hard"])

            if q80_raw := state.get("q80"):
                self._q80[r] = P2Quantile.from_state(q80_raw)
            if q90_raw := state.get("q90"):
                self._q90[r] = P2Quantile.from_state(q90_raw)
        except Exception:
            pass  # fail-open: при ошибке — просто начинаем с нуля

    # ── вспомогательные ──────────────────────────────────────────────────────

    def _get_q80(self, r: str) -> P2Quantile:
        if r not in self._q80:
            self._q80[r] = P2Quantile(p=0.80)
        return self._q80[r]

    def _get_q90(self, r: str) -> P2Quantile:
        if r not in self._q90:
            self._q90[r] = P2Quantile(p=0.90)
        return self._q90[r]

    def _compute(
        self,
        r: str,
        n: int,
        default_soft: float,
        default_hard: float,
    ) -> VolZThresholds:
        """Вычислить теневые пороги; fall-back на дефолты при холодном режиме."""
        if n < self.min_samples:
            return VolZThresholds(
                soft=default_soft, hard=default_hard, n=n, src="static"
            )

        raw_soft = self._q80[r].value() if r in self._q80 else None
        raw_hard = self._q90[r].value() if r in self._q90 else None

        soft = _clamp(raw_soft, default_soft)
        hard = _clamp(raw_hard, default_hard)

        # Монотонность: hard ≥ soft
        if hard < soft:
            hard = soft

        return VolZThresholds(soft=soft, hard=hard, n=n, src="calib_q80q90")


# ── helpers ──────────────────────────────────────────────────────────────────

def _norm_regime(regime: str | None) -> str:
    return (regime or "na").strip().lower()


def _clamp(val: float | None, default: float) -> float:
    if val is None or not math.isfinite(val):
        return default
    return max(VOL_Z_FLOOR, min(VOL_Z_CEIL, val))
