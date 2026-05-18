from __future__ import annotations

"""liquidity_wall_calibrator.py

Адаптивный калибратор порогов `size_z_thr=1.5` и `max_dist_bps=15.0`
для `find_near_liquidity_wall` в `components/liquidity.py`.

Проблема
--------
Жёсткие пороги одинаковы для всех символов:
- `size_z_thr=1.5` — у ликвидного BTC z-score стен всегда >2, у мем-коинов
  нормальный wall может иметь z=0.8.
- `max_dist_bps=15.0` — для high-ATR символов стена в 15 bps — "рядом",
  для low-ATR инструментов — уже далеко.

Метод
-----
Наблюдаем фоновые значения:
- `near_wall_size_z` (z-score ближайшей стены) → q75: "top 25% событий"
- `wall_dist_bps` (расстояние до стены в б.п.) → q75: "75% стен ближе этого"

Инварианты
----------
- auto_enforce=True: per-symbol автопереключение после min_samples.
- Rails: size_z_thr ∈ [0.5, 4.0], max_dist_bps ∈ [3.0, 60.0].
- Warmup: min_samples=300 per symbol.
- Гистерезис: 0.10 (size_z), 1.0 bps (dist).
"""

import math
from dataclasses import dataclass
from typing import Any

from core.quantile_p2 import P2Quantile

# ── hard rails ───────────────────────────────────────────────────────────────
SIZE_Z_FLOOR: float = 0.50    # ниже: почти любой level считался бы wall
SIZE_Z_CEIL: float = 4.00     # выше: нереалистично высокая аномалия

DIST_BPS_FLOOR: float = 3.0   # ближе: нет смысла (уже bid/ask spread)
DIST_BPS_CEIL: float = 60.0   # дальше: слишком далеко для ближней стены

DEFAULT_SIZE_Z_THR: float = 1.5
DEFAULT_MAX_DIST_BPS: float = 15.0

UPDATE_BAND_SIZE_Z: float = 0.10
UPDATE_BAND_DIST_BPS: float = 1.0


@dataclass
class LiqWallThresholds:
    """
    size_z_thr    — минимальный z-score для детектирования wall
    max_dist_bps  — максимальное расстояние (б.п.) до стены
    n             — число наблюдений
    src           — "static" / "calib_q75"
    """
    size_z_thr: float
    max_dist_bps: float
    n: int
    src: str


class LiquidityWallCalibrator:
    """
    Онлайн-калибратор порогов для детектора liquidity wall.

    Вызывающий код:
    1. При обнаружении wall: `observe(symbol, size_z, dist_bps)`.
    2. При вызове find_near_liquidity_wall: `thresholds(symbol)`.

    auto_enforce=True: автопереключение после min_samples per symbol.
    """

    def __init__(
        self,
        *,
        min_samples: int = 300,
        enforce: bool = False,
        auto_enforce: bool = True,
        update_band_size_z: float = UPDATE_BAND_SIZE_Z,
        update_band_dist_bps: float = UPDATE_BAND_DIST_BPS,
    ) -> None:
        self.min_samples = min_samples
        self.enforce = enforce
        self.auto_enforce = auto_enforce
        self.update_band_size_z = update_band_size_z
        self.update_band_dist_bps = update_band_dist_bps

        self._q75_sz: dict[str, P2Quantile] = {}
        self._q75_dist: dict[str, P2Quantile] = {}
        self._n: dict[str, int] = {}
        self._committed_sz: dict[str, float] = {}
        self._committed_dist: dict[str, float] = {}
        self._shadow: dict[str, LiqWallThresholds] = {}

    # ── публичный API ────────────────────────────────────────────────────────

    def observe(self, *, symbol: str, size_z: float, dist_bps: float) -> None:
        """
        Подать наблюдение wall-уровня.

        `size_z` — z-score размера стены (из near_wall_size_z).
        `dist_bps` — расстояние до стены в б.п.

        Каждый параметр накапливается независимо; наблюдение считается
        если хотя бы один из них валиден.
        """
        sym = _norm(symbol)
        counted = False

        if math.isfinite(size_z) and SIZE_Z_FLOOR <= size_z <= SIZE_Z_CEIL:
            self._get_q75_sz(sym).update(size_z)
            counted = True

        if math.isfinite(dist_bps) and DIST_BPS_FLOOR <= dist_bps <= DIST_BPS_CEIL:
            self._get_q75_dist(sym).update(dist_bps)
            counted = True

        if counted:
            self._n[sym] = self._n.get(sym, 0) + 1

    def thresholds(
        self,
        *,
        symbol: str,
        default_size_z: float = DEFAULT_SIZE_Z_THR,
        default_dist_bps: float = DEFAULT_MAX_DIST_BPS,
    ) -> LiqWallThresholds:
        """
        Вернуть калиброванные пороги для symbol.

        Возвращает статические defaults до прогрева (min_samples).
        """
        sym = _norm(symbol)
        n = self._n.get(sym, 0)

        shadow = self._compute(sym, n, default_size_z, default_dist_bps)
        self._shadow[sym] = shadow

        warm = n >= self.min_samples
        effective_enforce = self.enforce or (self.auto_enforce and warm)
        if not effective_enforce:
            return LiqWallThresholds(
                size_z_thr=default_size_z,
                max_dist_bps=default_dist_bps,
                n=n,
                src="static",
            )

        prev_sz = self._committed_sz.get(sym, default_size_z)
        prev_dist = self._committed_dist.get(sym, default_dist_bps)

        new_sz = shadow.size_z_thr
        new_dist = shadow.max_dist_bps

        if abs(new_sz - prev_sz) >= self.update_band_size_z:
            self._committed_sz[sym] = new_sz
        else:
            new_sz = prev_sz

        if abs(new_dist - prev_dist) >= self.update_band_dist_bps:
            self._committed_dist[sym] = new_dist
        else:
            new_dist = prev_dist

        return LiqWallThresholds(size_z_thr=new_sz, max_dist_bps=new_dist, n=n, src="calib_q75")

    def shadow_thresholds(self, *, symbol: str) -> LiqWallThresholds | None:
        return self._shadow.get(_norm(symbol))

    def n(self, symbol: str) -> int:
        return self._n.get(_norm(symbol), 0)

    # ── персистентность ──────────────────────────────────────────────────────

    def dump_symbol_state(self, *, symbol: str, updated_ts_ms: int) -> dict[str, Any]:
        sym = _norm(symbol)
        return {
            "v": 1, "kind": "liq_wall", "symbol": sym,
            "updated_ts_ms": updated_ts_ms, "min_samples": self.min_samples,
            "enforce": self.enforce, "auto_enforce": self.auto_enforce,
            "n": self._n.get(sym, 0),
            "committed_sz": self._committed_sz.get(sym),
            "committed_dist": self._committed_dist.get(sym),
            "q75_sz": (self._q75_sz[sym].to_state() if sym in self._q75_sz else None),
            "q75_dist": (self._q75_dist[sym].to_state() if sym in self._q75_dist else None),
        }

    def load_symbol_state(self, state: Any) -> None:
        try:
            if not isinstance(state, dict) or state.get("kind") != "liq_wall":
                return
            sym = str(state.get("symbol") or "na").lower()
            self.min_samples = int(state.get("min_samples", self.min_samples) or self.min_samples)
            self._n[sym] = int(state.get("n", 0) or 0)
            if state.get("committed_sz") is not None:
                self._committed_sz[sym] = float(state["committed_sz"])
            if state.get("committed_dist") is not None:
                self._committed_dist[sym] = float(state["committed_dist"])
            if q75_sz_raw := state.get("q75_sz"):
                self._q75_sz[sym] = P2Quantile.from_state(q75_sz_raw)
            if q75_dist_raw := state.get("q75_dist"):
                self._q75_dist[sym] = P2Quantile.from_state(q75_dist_raw)
        except Exception:
            pass

    # ── вспомогательные ──────────────────────────────────────────────────────

    def _get_q75_sz(self, sym: str) -> P2Quantile:
        if sym not in self._q75_sz:
            self._q75_sz[sym] = P2Quantile(p=0.75)
        return self._q75_sz[sym]

    def _get_q75_dist(self, sym: str) -> P2Quantile:
        if sym not in self._q75_dist:
            self._q75_dist[sym] = P2Quantile(p=0.75)
        return self._q75_dist[sym]

    def _compute(self, sym: str, n: int, d_sz: float, d_dist: float) -> LiqWallThresholds:
        if n < self.min_samples:
            return LiqWallThresholds(size_z_thr=d_sz, max_dist_bps=d_dist, n=n, src="static")
        raw_sz = self._q75_sz[sym].value() if sym in self._q75_sz else None
        raw_dist = self._q75_dist[sym].value() if sym in self._q75_dist else None
        sz = _clamp(raw_sz, d_sz, SIZE_Z_FLOOR, SIZE_Z_CEIL)
        dist = _clamp(raw_dist, d_dist, DIST_BPS_FLOOR, DIST_BPS_CEIL)
        return LiqWallThresholds(size_z_thr=sz, max_dist_bps=dist, n=n, src="calib_q75")


def _norm(s: str | None) -> str:
    return (s or "na").strip().lower()

def _clamp(val: float | None, default: float, lo: float, hi: float) -> float:
    if val is None or not math.isfinite(val):
        return default
    return max(lo, min(hi, val))
