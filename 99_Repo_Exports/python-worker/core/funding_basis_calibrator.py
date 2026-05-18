from __future__ import annotations

"""funding_basis_calibrator.py  v2

Адаптивный калибратор порогов funding_rate_z и basis_bps.

v2 добавляет режим-чувствительную калибровку: два параллельных суб-режима
на каждый символ:

  carry-режим (abs_fz < PANIC_FZ_BOUNDARY, abs_bb < PANIC_BB_BOUNDARY):
      P²-оценщик q95 — "top 5% нормального рынка".
  panic-режим (abs_fz ≥ boundary ИЛИ abs_bb ≥ boundary — squeeze/squeeze):
      P²-оценщик q99 — "top 1% экстремальных событий".

Инвариант: panic-порог ≥ carry-порог (принудительно через clamping в
_compute_panic). Это предотвращает вето-блокировку всех сделок во время
squeeze, когда funding genuinely extreme.

Метод (roadmap P1 #8 v2, "(symbol × exchange) × tag, 30d, q95/q99")
---------------------------------------------------------------------
- Carry-наблюдения обновляют объединённый estimator (fz95, bb95) — как в v1.
- Panic-наблюдения обновляют ОБА estimator-а: объединённый + отдельный q99.
  Объединённый нужен для warmup-счётчика и fallback-логики.
- thresholds(current_regime_tag="carry") → q95 (backward-совместимо).
- thresholds(current_regime_tag="panic") → q99 если ≥ min_panic_samples.

Backward-совместимость v1 → v2
-------------------------------
- Существующий вызов thresholds(regime=...) без current_regime_tag работает как
  раньше (carry-путь, q95).
- dump/load: v1 state загружается как carry-state.
- observe() теперь возвращает обнаруженный тег ("carry"/"panic").

Архитектура
-----------
- Нет IO: чистый класс, вызывающий код владеет Redis.
- Shadow-mode (enforce=False): observe + shadow_thresholds() для аудита.
- Гистерезис per-(regime × tag).
- Hard rails: clamped в физически осмысленный диапазон.
"""

import math
from dataclasses import dataclass
from typing import Any, Literal

from core.quantile_p2 import P2Quantile

# ── hard rails ───────────────────────────────────────────────────────────────
FUNDING_Z_FLOOR: float = 1.0    # ниже: скорее шум, чем аномалия
FUNDING_Z_CEIL: float  = 8.0    # выше: экстремальное событие (crash / squeeze)

BASIS_BPS_FLOOR: float = 1.0    # ниже: внутри нормального спреда
BASIS_BPS_CEIL: float  = 200.0  # выше: явная дислокация / liquidation event

# Статические дефолты — синхронизированы с evaluate_derivatives_context_v2.
DEFAULT_FUNDING_Z: float = 3.0
DEFAULT_BASIS_BPS: float = 10.0

# Граница разделения carry / panic суб-режимов.
PANIC_FZ_BOUNDARY: float = 2.5   # abs(funding_rate_z) >= 2.5 → panic
PANIC_BB_BOUNDARY: float = 15.0  # abs(basis_bps) >= 15.0 → panic

# Минимум panic-наблюдений до применения q99-порога.
MIN_PANIC_SAMPLES: int = 50

# Fallback для cold panic: умножитель дефолтного порога.
PANIC_COLD_FZ_MULT: float = 1.5   # 3.0 → 4.5 при холодном panic
PANIC_COLD_BB_MULT: float = 2.0   # 10.0 → 20.0 при холодном panic

# Минимальный сдвиг для применения нового порога (гистерезис).
UPDATE_BAND_FUNDING_Z: float = 0.20
UPDATE_BAND_BASIS_BPS: float = 1.0

RegimeTag = Literal["carry", "panic"]


@dataclass
class FundingBasisThresholds:
    """
    Калиброванные (или статические) пороги для одного символьного режима.

    funding_z   — порог funding_rate_z (q95 carry / q99 panic)
    basis_bps   — порог basis_bps (q95 carry / q99 panic)
    n           — число наблюдений для данного суб-режима
    src         — "static"/"static_panic"/"calib_q95"/"calib_q99"
    regime_tag  — "carry" / "panic" (v2)
    """
    funding_z: float
    basis_bps: float
    n: int
    src: str
    regime_tag: str = "carry"


class FundingBasisCalibrator:
    """
    Онлайн-калибратор порогов funding_rate_z и basis_bps.

    v2: per-symbol, carry (q95) / panic (q99) суб-режимы.

    Параметры
    ----------
    min_samples : int
        Минимум combined наблюдений до включения q95 для carry-пути.
    min_panic_samples : int
        Минимум panic-наблюдений до включения q99 для panic-пути.
    enforce : bool
        False (default) = shadow-режим.
    auto_enforce : bool
        True = автопереключение после warmup.
    panic_fz_boundary, panic_bb_boundary : float
        Порог для автодетекции суб-режима в observe().
    """

    def __init__(
        self,
        *,
        min_samples: int = 500,
        min_panic_samples: int = MIN_PANIC_SAMPLES,
        enforce: bool = False,
        auto_enforce: bool = True,
        update_band_fz: float = UPDATE_BAND_FUNDING_Z,
        update_band_bb: float = UPDATE_BAND_BASIS_BPS,
        panic_fz_boundary: float = PANIC_FZ_BOUNDARY,
        panic_bb_boundary: float = PANIC_BB_BOUNDARY,
    ) -> None:
        self.min_samples = min_samples
        self.min_panic_samples = min_panic_samples
        self.enforce = enforce
        self.auto_enforce = auto_enforce
        self.update_band_fz = update_band_fz
        self.update_band_bb = update_band_bb
        self.panic_fz_boundary = panic_fz_boundary
        self.panic_bb_boundary = panic_bb_boundary

        # v1-совместимые combined q95 estimators (carry + all observations).
        self._fz95: dict[str, P2Quantile] = {}
        self._bb95: dict[str, P2Quantile] = {}
        self._n: dict[str, int] = {}  # combined count

        # v2: отдельные q99 estimators только для panic наблюдений.
        self._fz99_panic: dict[str, P2Quantile] = {}
        self._bb99_panic: dict[str, P2Quantile] = {}
        self._n_panic: dict[str, int] = {}  # panic-only count

        # Committed thresholds с гистерезисом: carry.
        self._committed_fz: dict[str, float] = {}
        self._committed_bb: dict[str, float] = {}
        # Committed thresholds: panic.
        self._committed_fz_panic: dict[str, float] = {}
        self._committed_bb_panic: dict[str, float] = {}

        # Теневые предложения per (regime:tag).
        self._shadow: dict[str, FundingBasisThresholds] = {}

    # ── публичный API ────────────────────────────────────────────────────────

    def observe(
        self,
        *,
        regime: str,
        abs_funding_z: float,
        abs_basis_bps: float,
    ) -> RegimeTag:
        """
        Принять наблюдение (|funding_z|, |basis_bps|).

        Автодетектирует тег carry/panic и обновляет соответствующие estimators.
        Возвращает обнаруженный тег для метрик в caller-е.
        """
        r = _norm(regime)
        tag = self._detect_tag(abs_funding_z, abs_basis_bps)

        valid_fz = math.isfinite(abs_funding_z) and FUNDING_Z_FLOOR <= abs_funding_z <= FUNDING_Z_CEIL
        valid_bb = math.isfinite(abs_basis_bps) and BASIS_BPS_FLOOR <= abs_basis_bps <= BASIS_BPS_CEIL

        # Combined estimators (backward compat, drives warmup для carry-пути).
        if valid_fz:
            self._get_fz95(r).update(abs_funding_z)
        if valid_bb:
            self._get_bb95(r).update(abs_basis_bps)
        if valid_fz or valid_bb:
            self._n[r] = self._n.get(r, 0) + 1

        # Panic-only estimators (q99).
        if tag == "panic":
            if valid_fz:
                self._get_fz99_panic(r).update(abs_funding_z)
            if valid_bb:
                self._get_bb99_panic(r).update(abs_basis_bps)
            if valid_fz or valid_bb:
                self._n_panic[r] = self._n_panic.get(r, 0) + 1

        return tag

    def thresholds(
        self,
        *,
        regime: str,
        current_regime_tag: RegimeTag | None = None,
        default_funding_z: float = DEFAULT_FUNDING_Z,
        default_basis_bps: float = DEFAULT_BASIS_BPS,
    ) -> FundingBasisThresholds:
        """
        Вернуть пороги для данного режима.

        current_regime_tag=None/"carry" → q95 carry (backward-совместимо).
        current_regime_tag="panic"      → q99 panic (v2).
        """
        r = _norm(regime)
        tag: RegimeTag = current_regime_tag or "carry"

        if tag == "panic":
            shadow = self._compute_panic(r, default_funding_z, default_basis_bps)
        else:
            shadow = self._compute_carry(r, default_funding_z, default_basis_bps)

        self._shadow[f"{r}:{tag}"] = shadow

        n = shadow.n
        warm_combined = self._n.get(r, 0) >= self.min_samples
        effective_enforce = self.enforce or (self.auto_enforce and warm_combined)

        if not effective_enforce:
            if tag == "panic":
                # Fail-open для panic в shadow: вернуть чуть выше дефолта.
                return FundingBasisThresholds(
                    funding_z=min(default_funding_z * PANIC_COLD_FZ_MULT, FUNDING_Z_CEIL),
                    basis_bps=min(default_basis_bps * PANIC_COLD_BB_MULT, BASIS_BPS_CEIL),
                    n=n,
                    src="static_panic",
                    regime_tag=tag,
                )
            return FundingBasisThresholds(
                funding_z=default_funding_z,
                basis_bps=default_basis_bps,
                n=n,
                src="static",
                regime_tag=tag,
            )

        # Применяем с гистерезисом.
        if tag == "panic":
            # Panic нуждается в собственном warmup независимо от combined warmup.
            n_p = self._n_panic.get(r, 0)
            if n_p < self.min_panic_samples:
                return FundingBasisThresholds(
                    funding_z=min(default_funding_z * PANIC_COLD_FZ_MULT, FUNDING_Z_CEIL),
                    basis_bps=min(default_basis_bps * PANIC_COLD_BB_MULT, BASIS_BPS_CEIL),
                    n=n_p,
                    src="static_panic",
                    regime_tag="panic",
                )
            prev_fz = self._committed_fz_panic.get(r, default_funding_z)
            prev_bb = self._committed_bb_panic.get(r, default_basis_bps)
            new_fz, new_bb = shadow.funding_z, shadow.basis_bps
            if abs(new_fz - prev_fz) >= self.update_band_fz:
                self._committed_fz_panic[r] = new_fz
            else:
                new_fz = prev_fz
            if abs(new_bb - prev_bb) >= self.update_band_bb:
                self._committed_bb_panic[r] = new_bb
            else:
                new_bb = prev_bb
            return FundingBasisThresholds(
                funding_z=new_fz, basis_bps=new_bb, n=n, src="calib_q99", regime_tag="panic"
            )
        else:
            prev_fz = self._committed_fz.get(r, default_funding_z)
            prev_bb = self._committed_bb.get(r, default_basis_bps)
            new_fz, new_bb = shadow.funding_z, shadow.basis_bps
            if abs(new_fz - prev_fz) >= self.update_band_fz:
                self._committed_fz[r] = new_fz
            else:
                new_fz = prev_fz
            if abs(new_bb - prev_bb) >= self.update_band_bb:
                self._committed_bb[r] = new_bb
            else:
                new_bb = prev_bb
            return FundingBasisThresholds(
                funding_z=new_fz, basis_bps=new_bb, n=n, src="calib_q95", regime_tag="carry"
            )

    def shadow_thresholds(
        self,
        *,
        regime: str,
        regime_tag: str = "carry",
    ) -> FundingBasisThresholds | None:
        """Последнее теневое предложение для аудита."""
        return self._shadow.get(f"{_norm(regime)}:{regime_tag}")

    def n(self, regime: str) -> int:
        """Общее число наблюдений (carry + panic) для обратной совместимости."""
        return self._n.get(_norm(regime), 0)

    def n_panic(self, regime: str) -> int:
        return self._n_panic.get(_norm(regime), 0)

    def detect_tag(self, abs_fz: float, abs_bb: float) -> RegimeTag:
        """Публичный хелпер для caller-а (e.g. gates.py для метрик)."""
        return self._detect_tag(abs_fz, abs_bb)

    # ── персистентность ──────────────────────────────────────────────────────

    def dump_regime_state(
        self, *, symbol: str, regime: str, updated_ts_ms: int
    ) -> dict[str, Any]:
        r = _norm(regime)
        return {
            "v": 2,
            "kind": "funding_basis",
            "symbol": symbol,
            "regime": r,
            "updated_ts_ms": updated_ts_ms,
            "min_samples": self.min_samples,
            "min_panic_samples": self.min_panic_samples,
            "enforce": self.enforce,
            # combined
            "n": self._n.get(r, 0),
            "committed_fz": self._committed_fz.get(r),
            "committed_bb": self._committed_bb.get(r),
            "fz95": (self._fz95[r].to_state() if r in self._fz95 else None),
            "bb95": (self._bb95[r].to_state() if r in self._bb95 else None),
            # panic-only
            "n_panic": self._n_panic.get(r, 0),
            "committed_fz_panic": self._committed_fz_panic.get(r),
            "committed_bb_panic": self._committed_bb_panic.get(r),
            "fz99_panic": (self._fz99_panic[r].to_state() if r in self._fz99_panic else None),
            "bb99_panic": (self._bb99_panic[r].to_state() if r in self._bb99_panic else None),
        }

    def load_regime_state(self, state: Any) -> None:
        """Восстановить состояние из dump_regime_state(). Fail-open.

        Поддерживает v1 (только combined q95) и v2 (+ panic q99).
        """
        try:
            if not isinstance(state, dict):
                return
            if state.get("kind") != "funding_basis":
                return
            r = str(state.get("regime") or "na").lower()
            self.min_samples = int(
                state.get("min_samples", self.min_samples) or self.min_samples
            )
            if state.get("min_panic_samples") is not None:
                self.min_panic_samples = int(state["min_panic_samples"])

            # combined / carry (v1 + v2 compat)
            self._n[r] = int(state.get("n", 0) or 0)
            if state.get("committed_fz") is not None:
                self._committed_fz[r] = float(state["committed_fz"])
            if state.get("committed_bb") is not None:
                self._committed_bb[r] = float(state["committed_bb"])
            if fz_raw := state.get("fz95"):
                self._fz95[r] = P2Quantile.from_state(fz_raw)
            if bb_raw := state.get("bb95"):
                self._bb95[r] = P2Quantile.from_state(bb_raw)

            # panic-only (v2 only — missing fields → cold panic, fail-open)
            if state.get("n_panic") is not None:
                self._n_panic[r] = int(state["n_panic"] or 0)
            if state.get("committed_fz_panic") is not None:
                self._committed_fz_panic[r] = float(state["committed_fz_panic"])
            if state.get("committed_bb_panic") is not None:
                self._committed_bb_panic[r] = float(state["committed_bb_panic"])
            if fz_raw := state.get("fz99_panic"):
                self._fz99_panic[r] = P2Quantile.from_state(fz_raw)
            if bb_raw := state.get("bb99_panic"):
                self._bb99_panic[r] = P2Quantile.from_state(bb_raw)
        except Exception:
            pass  # fail-open

    # ── вспомогательные ──────────────────────────────────────────────────────

    def _detect_tag(self, abs_fz: float, abs_bb: float) -> RegimeTag:
        if math.isfinite(abs_fz) and abs_fz >= self.panic_fz_boundary:
            return "panic"
        if math.isfinite(abs_bb) and abs_bb >= self.panic_bb_boundary:
            return "panic"
        return "carry"

    def _get_fz95(self, r: str) -> P2Quantile:
        if r not in self._fz95:
            self._fz95[r] = P2Quantile(p=0.95)
        return self._fz95[r]

    def _get_bb95(self, r: str) -> P2Quantile:
        if r not in self._bb95:
            self._bb95[r] = P2Quantile(p=0.95)
        return self._bb95[r]

    def _get_fz99_panic(self, r: str) -> P2Quantile:
        if r not in self._fz99_panic:
            self._fz99_panic[r] = P2Quantile(p=0.99)
        return self._fz99_panic[r]

    def _get_bb99_panic(self, r: str) -> P2Quantile:
        if r not in self._bb99_panic:
            self._bb99_panic[r] = P2Quantile(p=0.99)
        return self._bb99_panic[r]

    def _compute_carry(
        self, r: str, default_fz: float, default_bb: float
    ) -> FundingBasisThresholds:
        n = self._n.get(r, 0)
        if n < self.min_samples:
            return FundingBasisThresholds(
                funding_z=default_fz, basis_bps=default_bb, n=n, src="static", regime_tag="carry"
            )
        raw_fz = self._fz95[r].value() if r in self._fz95 else None
        raw_bb = self._bb95[r].value() if r in self._bb95 else None
        fz = _clamp(raw_fz, default_fz, FUNDING_Z_FLOOR, FUNDING_Z_CEIL)
        bb = _clamp(raw_bb, default_bb, BASIS_BPS_FLOOR, BASIS_BPS_CEIL)
        return FundingBasisThresholds(funding_z=fz, basis_bps=bb, n=n, src="calib_q95", regime_tag="carry")

    def _compute_panic(
        self, r: str, default_fz: float, default_bb: float
    ) -> FundingBasisThresholds:
        n_p = self._n_panic.get(r, 0)

        if n_p < self.min_panic_samples:
            # cold panic fallback: выше дефолта, чтобы не блокировать squeeze-трейды.
            fallback_fz = min(default_fz * PANIC_COLD_FZ_MULT, FUNDING_Z_CEIL)
            fallback_bb = min(default_bb * PANIC_COLD_BB_MULT, BASIS_BPS_CEIL)
            return FundingBasisThresholds(
                funding_z=fallback_fz, basis_bps=fallback_bb,
                n=n_p, src="static_panic", regime_tag="panic"
            )

        raw_fz = self._fz99_panic[r].value() if r in self._fz99_panic else None
        raw_bb = self._bb99_panic[r].value() if r in self._bb99_panic else None
        fz = _clamp(raw_fz, default_fz, FUNDING_Z_FLOOR, FUNDING_Z_CEIL)
        bb = _clamp(raw_bb, default_bb, BASIS_BPS_FLOOR, BASIS_BPS_CEIL)

        # Инвариант: panic-порог ≥ carry-порог.
        carry_fz = self._committed_fz.get(r, default_fz)
        carry_bb = self._committed_bb.get(r, default_bb)
        fz = max(fz, carry_fz)
        bb = max(bb, carry_bb)

        return FundingBasisThresholds(funding_z=fz, basis_bps=bb, n=n_p, src="calib_q99", regime_tag="panic")


# ── helpers ──────────────────────────────────────────────────────────────────

def _norm(regime: str | None) -> str:
    return (regime or "na").strip().lower()


def _clamp(val: float | None, default: float, lo: float, hi: float) -> float:
    if val is None or not math.isfinite(val):
        return default
    return max(lo, min(hi, val))
