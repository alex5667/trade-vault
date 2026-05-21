from __future__ import annotations

"""flow_toxicity_calibrator.py

Rolling p95 ECDF thresholds for OFI norm-z and VPIN CDF per symbol.

Проблема
--------
`FLOW_OFI_NORM_Z_MAX=0` и `FLOW_VPIN_CDF_MAX=0` в signal_pipeline.py — оба
порога отключены. FlowToxicityGate всегда проходит (thr=0 → `thrz > 0` ложь).
VPIN / OFI — одни из наиболее эмпирически обоснованных индикаторов токсичности
потока (Easley et al. 2012), но без порогов они бесполезны.

Метод
-----
P² streaming quantile (Jain & Chlamtac 1985) — O(1) память, 5 маркеров.
  • ofi_norm_z → p95 за скользящее окно (AUTO-promoted per symbol).
  • vpin_cdf   → p95 за скользящее окно.

Стадии автоматического продвижения (per symbol):
  disabled  →  n < MIN_SAMPLES  (default 2000)  — thr = 0.0 (gate off)
  shadow    →  n >= MIN_SAMPLES, enforce=False   — shadow_thr доступен для аудита
  enforce   →  auto_enforce=True (default)       — live thr применяется в gate

Гистерезис: обновление committed-порога только при |Δ| ≥ UPDATE_BAND.

Инварианты
----------
- Нет IO: персистентность через dump_state / load_state (Redis HSET).
- Детерминизм: одна последовательность observe() → те же пороги.
- Fail-open: при n < MIN_SAMPLES порог = 0.0 → gate выключен.

Wiring (отдельные фазы)
-----------------------
  Feed:   orderflow_services/flow_toxicity_calibrator_v1.py
            RS.OF_INPUTS → observe() → HSET autocal:flow_toxicity:state {symbol} {json}
  Reader: services/flow_toxicity_runtime_overrides.py
            HGETALL autocal:flow_toxicity:state → per-symbol thr_z / thr_vpin
  Gate:   signal_pipeline.py
            _cached_flow_thr_z / _cached_flow_thr_vpin overridden per-symbol at call time
"""

import math
from dataclasses import dataclass
from typing import Any

from core.quantile_p2 import P2Quantile

# ── hard rails ──────────────────────────────────────────────────────────────────
# ofi_norm_z — robust z-score, можно отрицательным; для ECDF берём полное
# распределение, p95 даёт правый хвост (именно туда смотрит gate: z > thr).
OFI_Z_FLOOR: float = -20.0
OFI_Z_CEIL: float = 20.0

# vpin_cdf ∈ [0, 1] — CDF нормального распределения по vpin_tox_z.
VPIN_FLOOR: float = 0.0
VPIN_CEIL: float = 1.0

DEFAULT_OFI_Z_THR: float = 0.0   # отключено (gate pass-through)
DEFAULT_VPIN_THR: float = 0.0    # отключено

# Мировая практика (Easley et al.): ≥2000 сделок/сигналов для стабильного VPIN.
MIN_SAMPLES: int = 2000

UPDATE_BAND_Z: float = 0.10      # гистерезис ofi_norm_z (в единицах z-score)
UPDATE_BAND_VPIN: float = 0.005  # гистерезис vpin_cdf (в [0,1])


@dataclass
class FlowToxThresholds:
    """Калиброванные (или статические) пороги для одного символа."""
    thr_z: float     # ofi_norm_z p95 (0.0 = disabled)
    thr_vpin: float  # vpin_cdf p95   (0.0 = disabled)
    n: int
    src: str         # "static" | "shadow" | "calibrated"


class FlowToxicityCalibrator:
    """
    Онлайн-калибратор p95-порогов ofi_norm_z и vpin_cdf per symbol.

    Использование
    -------------
        cal = FlowToxicityCalibrator()

        # Раз за сигнал (из OF_INPUTS.indicators):
        cal.observe(symbol="BTCUSDT", ofi_z=1.2, vpin=0.73)

        # В gate:
        thr = cal.thresholds(symbol="BTCUSDT")
        thr.thr_z   # 0.0 пока холодный; p95 после прогрева
        thr.thr_vpin
    """

    def __init__(
        self,
        *,
        min_samples: int = MIN_SAMPLES,
        enforce: bool = False,
        auto_enforce: bool = True,
        update_band_z: float = UPDATE_BAND_Z,
        update_band_vpin: float = UPDATE_BAND_VPIN,
    ) -> None:
        self.min_samples = min_samples
        self.enforce = enforce
        self.auto_enforce = auto_enforce
        self.update_band_z = update_band_z
        self.update_band_vpin = update_band_vpin

        self._n: dict[str, int] = {}
        self._q95_z: dict[str, P2Quantile] = {}
        self._q95_vpin: dict[str, P2Quantile] = {}

        # Зафиксированные committed-пороги (применяются при enforce)
        self._committed_z: dict[str, float] = {}
        self._committed_vpin: dict[str, float] = {}

        # Теневые предложения (аудит; не зависят от enforce-флага)
        self._shadow: dict[str, FlowToxThresholds] = {}

    # ── публичный API ──────────────────────────────────────────────────────────

    def observe(self, *, symbol: str, ofi_z: float, vpin: float) -> None:
        """Подать одно наблюдение (ofi_norm_z, vpin_cdf) для символа.

        Значения вне hard-rails и нечисловые отбрасываются.
        """
        sym = _norm_sym(symbol)
        if not math.isfinite(ofi_z):
            ofi_z = 0.0
        if not math.isfinite(vpin):
            vpin = 0.0
        ofi_z = max(OFI_Z_FLOOR, min(OFI_Z_CEIL, ofi_z))
        vpin = max(VPIN_FLOOR, min(VPIN_CEIL, vpin))

        self._get_q95_z(sym).update(ofi_z)
        self._get_q95_vpin(sym).update(vpin)
        self._n[sym] = self._n.get(sym, 0) + 1

    def thresholds(self, *, symbol: str) -> FlowToxThresholds:
        """Вернуть пороги для символа.

        Пока n < min_samples → статические дефолты (0.0, gate off).
        Теневое предложение всегда вычисляется и доступно через shadow_thresholds().
        """
        sym = _norm_sym(symbol)
        n = self._n.get(sym, 0)

        shadow = self._compute_shadow(sym, n)
        self._shadow[sym] = shadow

        warm = n >= self.min_samples
        effective_enforce = self.enforce or (self.auto_enforce and warm)

        if not effective_enforce:
            return FlowToxThresholds(
                thr_z=DEFAULT_OFI_Z_THR,
                thr_vpin=DEFAULT_VPIN_THR,
                n=n,
                src="static",
            )

        # Применяем с гистерезисом
        prev_z = self._committed_z.get(sym, DEFAULT_OFI_Z_THR)
        prev_vpin = self._committed_vpin.get(sym, DEFAULT_VPIN_THR)

        new_z = shadow.thr_z
        new_vpin = shadow.thr_vpin

        if abs(new_z - prev_z) >= self.update_band_z:
            self._committed_z[sym] = new_z
        else:
            new_z = prev_z

        if abs(new_vpin - prev_vpin) >= self.update_band_vpin:
            self._committed_vpin[sym] = new_vpin
        else:
            new_vpin = prev_vpin

        return FlowToxThresholds(thr_z=new_z, thr_vpin=new_vpin, n=n, src="calibrated")

    def shadow_thresholds(self, *, symbol: str) -> FlowToxThresholds | None:
        """Последнее вычисленное теневое предложение (для аудита/promote)."""
        return self._shadow.get(_norm_sym(symbol))

    def n(self, symbol: str) -> int:
        """Число наблюдений для символа."""
        return self._n.get(_norm_sym(symbol), 0)

    def all_symbols(self) -> list[str]:
        """Список всех символов с наблюдениями."""
        return list(self._n.keys())

    # ── персистентность ────────────────────────────────────────────────────────

    def dump_state(self, *, symbol: str, updated_ts_ms: int) -> dict[str, Any]:
        """JSON-сериализуемое состояние для сохранения в Redis HSET / файл."""
        sym = _norm_sym(symbol)
        return {
            "v": 1,
            "kind": "flow_toxicity",
            "symbol": sym,
            "updated_ts_ms": updated_ts_ms,
            "min_samples": self.min_samples,
            "enforce": self.enforce,
            "auto_enforce": self.auto_enforce,
            "n": self._n.get(sym, 0),
            "committed_z": self._committed_z.get(sym),
            "committed_vpin": self._committed_vpin.get(sym),
            "q95_z": (self._q95_z[sym].to_state() if sym in self._q95_z else None),
            "q95_vpin": (self._q95_vpin[sym].to_state() if sym in self._q95_vpin else None),
        }

    def load_state(self, state: Any) -> None:
        """Восстановить состояние из dump_state(). Fail-open при ошибках."""
        try:
            if not isinstance(state, dict):
                return
            if state.get("kind") != "flow_toxicity":
                return
            sym = _norm_sym(str(state.get("symbol") or "na"))
            self.min_samples = int(state.get("min_samples", self.min_samples) or self.min_samples)
            self._n[sym] = int(state.get("n", 0) or 0)

            if state.get("committed_z") is not None:
                self._committed_z[sym] = float(state["committed_z"])
            if state.get("committed_vpin") is not None:
                self._committed_vpin[sym] = float(state["committed_vpin"])

            if q95_z_raw := state.get("q95_z"):
                self._q95_z[sym] = P2Quantile.from_state(q95_z_raw)
            if q95_vpin_raw := state.get("q95_vpin"):
                self._q95_vpin[sym] = P2Quantile.from_state(q95_vpin_raw)
        except Exception:
            pass  # fail-open

    # ── вспомогательные ───────────────────────────────────────────────────────

    def _get_q95_z(self, sym: str) -> P2Quantile:
        if sym not in self._q95_z:
            self._q95_z[sym] = P2Quantile(p=0.95)
        return self._q95_z[sym]

    def _get_q95_vpin(self, sym: str) -> P2Quantile:
        if sym not in self._q95_vpin:
            self._q95_vpin[sym] = P2Quantile(p=0.95)
        return self._q95_vpin[sym]

    def _compute_shadow(self, sym: str, n: int) -> FlowToxThresholds:
        """Вычислить теневые пороги; fall-back на 0.0 при недостаточном прогреве."""
        if n < self.min_samples:
            return FlowToxThresholds(
                thr_z=DEFAULT_OFI_Z_THR,
                thr_vpin=DEFAULT_VPIN_THR,
                n=n,
                src="static",
            )
        raw_z = self._q95_z[sym].value() if sym in self._q95_z else None
        raw_vpin = self._q95_vpin[sym].value() if sym in self._q95_vpin else None

        thr_z = _clamp_z(raw_z)
        thr_vpin = _clamp_vpin(raw_vpin)

        return FlowToxThresholds(thr_z=thr_z, thr_vpin=thr_vpin, n=n, src="shadow")


# ── helpers ────────────────────────────────────────────────────────────────────

def _norm_sym(sym: str | None) -> str:
    return (sym or "NA").strip().upper()


def _clamp_z(val: float | None) -> float:
    if val is None or not math.isfinite(val):
        return DEFAULT_OFI_Z_THR
    # floor at 0.5 — порог ниже 0.5σ бессмысленен (шум)
    return max(0.5, min(OFI_Z_CEIL, val))


def _clamp_vpin(val: float | None) -> float:
    if val is None or not math.isfinite(val):
        return DEFAULT_VPIN_THR
    # floor at 0.50 — ниже медианы VPIN бессмысленен как порог блокировки
    return max(0.50, min(VPIN_CEIL, val))
