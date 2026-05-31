from __future__ import annotations

"""smt_coherence_calibrator.py

Адаптивный калибратор порога `coh_min` для `SmtCoherenceGate`.

Проблема
--------
Жёсткий порог `coh_min = 0.65` одинаков для всех символов/сессий.
На ликвидных парах (BTC/ETH) SMT-когерентность может систематически быть выше
0.65 даже в «нейтральных» состояниях → частые ложные вето.
На экзотах когерентность редко достигает 0.65 → вето почти никогда не срабатывает.

Метод (roadmap P1 #9, "isotonic coherence → P(success), G5-style shadow → enforce")
------------------------------------------------------------------------------------
Используем **P² streaming quantile** на условном распределении `coh` — только
в ситуациях-кандидатах на вето (countertrend AND leader_confirm == 1).

Обоснование:
  q80 условного coh → "вето только если когерентность в топ-20% ситуаций-кандидатов"
  Это адаптирует к per-symbol базовому уровню когерентности без outcome-данных.

Это промежуточный шаг; полная isotonic-регрессия (P(прибыльность) | coh ≥ thr)
будет доступна при наличии достаточного histories из trades_closed (Phase 2).

Архитектура
-----------
- Нет IO: вызывающий код владеет Redis / persistence.
- Shadow-mode (enforce=False): observe + shadow_thresholds(), thresholds() = static.
- Hard rails [COH_FLOOR=0.30, COH_CEIL=0.98].
- Warmup: min_samples (кандидаты-ситуации) перед применением.
- Гистерезис UPDATE_BAND=0.05 (порог когерентности очень чувствителен).
- Персистентность: dump_regime_state() / load_regime_state().

Использование
-------------
    calib = SmtCoherenceCalibrator(min_samples=200, enforce=False)

    # Раз за оценку (ТОЛЬКО при countertrend AND leader_confirm):
    if countertrend and leader_confirm == 1:
        calib.observe(regime=regime_key, coh=coh)

    # В gate перед veto-проверкой:
    th = calib.thresholds(regime=regime_key)
    if countertrend and leader_confirm == 1 and coh >= th.coh_min:
        return DENY
"""

import math
from dataclasses import dataclass
from typing import Any

from core.quantile_p2 import P2Quantile

# ── hard rails ──────────────────────────────────────────────────────────────
COH_FLOOR: float = 0.30   # ниже: когерентность слишком слабая для любого вето
COH_CEIL: float = 0.98    # выше: технически невозможно (нарушение нормировки)

# Статический дефолт — синхронизирован с SmtCoherenceGate.from_env().
DEFAULT_COH_MIN: float = 0.65

# Гистерезис — порог когерентности очень чувствителен к малым сдвигам.
UPDATE_BAND: float = 0.05


@dataclass
class SmtCoherenceThresholds:
    """
    Калиброванный (или статический) минимальный порог когерентности.

    coh_min  — q80 условного coh (кандидаты на вето)
    n        — число наблюдений-кандидатов в данном режиме
    src      — "static" / "calib_q80"
    """
    coh_min: float
    n: int
    src: str


class SmtCoherenceCalibrator:
    """
    Онлайн-калибратор `coh_min` для SmtCoherenceGate.

    Параметры
    ----------
    min_samples : int
        Минимум наблюдений-кандидатов до применения калибровки.
    enforce : bool
        False = shadow-mode (fail-open).
    update_band : float
        Гистерезис: порог обновляется только при изменении > band.
    """

    def __init__(
        self,
        *,
        min_samples: int = 200,
        enforce: bool = False,
        auto_enforce: bool = True,
        update_band: float = UPDATE_BAND,
    ) -> None:
        self.min_samples = min_samples
        self.enforce = enforce
        self.auto_enforce = auto_enforce
        self.update_band = update_band

        # P² q80 per-режим (только на ситуациях-кандидатах)
        self._q80: dict[str, P2Quantile] = {}
        self._n: dict[str, int] = {}
        self._committed: dict[str, float] = {}
        self._shadow: dict[str, SmtCoherenceThresholds] = {}

    # ── публичный API ────────────────────────────────────────────────────────

    def observe(self, *, regime: str, coh: float) -> None:
        """
        Подать наблюдение когерентности в ситуации-кандидате на вето.

        Вызывать ТОЛЬКО когда: countertrend AND leader_confirm == 1.
        Значения вне [COH_FLOOR, COH_CEIL] и NaN/Inf отбрасываются.
        """
        r = _norm(regime)
        if not math.isfinite(coh):
            return
        if not (COH_FLOOR <= coh <= COH_CEIL):
            return
        self._get_q80(r).update(coh)
        self._n[r] = self._n.get(r, 0) + 1

    def thresholds(
        self,
        *,
        regime: str,
        default_coh_min: float = DEFAULT_COH_MIN,
    ) -> SmtCoherenceThresholds:
        """
        Вернуть порог когерентности для данного режима.

        enforce=False или холодный режим → static default (fail-open).
        """
        r = _norm(regime)
        n = self._n.get(r, 0)

        shadow = self._compute(r, n, default_coh_min)
        self._shadow[r] = shadow

        warm = n >= self.min_samples
        effective_enforce = self.enforce or (self.auto_enforce and warm)
        if not effective_enforce:
            return SmtCoherenceThresholds(
                coh_min=default_coh_min, n=n, src="static"
            )

        prev = self._committed.get(r, default_coh_min)
        new_val = shadow.coh_min

        if abs(new_val - prev) >= self.update_band:
            self._committed[r] = new_val
        else:
            new_val = prev

        return SmtCoherenceThresholds(coh_min=new_val, n=n, src="calib_q80")

    def shadow_thresholds(self, *, regime: str) -> SmtCoherenceThresholds | None:
        return self._shadow.get(_norm(regime))

    def n(self, regime: str) -> int:
        return self._n.get(_norm(regime), 0)

    # ── персистентность ──────────────────────────────────────────────────────

    def dump_regime_state(
        self, *, symbol: str, regime: str, updated_ts_ms: int
    ) -> dict[str, Any]:
        r = _norm(regime)
        return {
            "v": 1,
            "kind": "smt_coherence",
            "symbol": symbol,
            "regime": r,
            "updated_ts_ms": updated_ts_ms,
            "min_samples": self.min_samples,
            "enforce": self.enforce,
            "n": self._n.get(r, 0),
            "committed": self._committed.get(r),
            "q80": (self._q80[r].to_state() if r in self._q80 else None),
        }

    def load_regime_state(self, state: Any) -> None:
        """Fail-open."""
        try:
            if not isinstance(state, dict):
                return
            if state.get("kind") != "smt_coherence":
                return
            r = str(state.get("regime") or "na").lower()
            self.min_samples = int(
                state.get("min_samples", self.min_samples) or self.min_samples
            )
            self._n[r] = int(state.get("n", 0) or 0)
            if state.get("committed") is not None:
                self._committed[r] = float(state["committed"])
            if q80_raw := state.get("q80"):
                self._q80[r] = P2Quantile.from_state(q80_raw)
        except Exception:
            pass

    # ── вспомогательные ──────────────────────────────────────────────────────

    def _get_q80(self, r: str) -> P2Quantile:
        if r not in self._q80:
            self._q80[r] = P2Quantile(p=0.80)
        return self._q80[r]

    def _compute(
        self, r: str, n: int, default: float
    ) -> SmtCoherenceThresholds:
        if n < self.min_samples:
            return SmtCoherenceThresholds(coh_min=default, n=n, src="static")
        raw = self._q80[r].value() if r in self._q80 else None
        val = _clamp(raw, default)
        return SmtCoherenceThresholds(coh_min=val, n=n, src="calib_q80")


# ── helpers ──────────────────────────────────────────────────────────────────

def _norm(regime: str | None) -> str:
    return (regime or "na").strip().lower()


def _clamp(val: float | None, default: float) -> float:
    if val is None or not math.isfinite(val):
        return default
    return max(COH_FLOOR, min(COH_CEIL, val))
