from __future__ import annotations

"""liquidity_signal_calibrator.py

Адаптивный калибратор порогов `min_notional_for_high_liq=250_000` и
`dense_cluster_bps=5.0` (models/data_models.py:120-121, contexts.py:444-445).

Метод
-----
Наблюдаем per-symbol:
  - `depth_usd`  — минимальный USD объём на top-5 уровнях best bid/ask
  - `spread_bps` — текущий bid-ask спред

Калибруем:
  - `notional_thr` = q50(depth_usd)  → "типичная ликвидность; ниже — thin book"
  - `cluster_bps`  = q50(spread_bps) → "типичная ширина кластера"

Инварианты
----------
- Rails: notional_thr ∈ [10_000, 5_000_000] USD, cluster_bps ∈ [0.5, 50.0].
- auto_enforce=True: per-symbol автопереключение после min_samples (default 500).
- Hysteresis 5%: пропускаем обновление если |Δ|/prev < rel_thresh.
"""

import math
from dataclasses import dataclass
from typing import Any

from core.quantile_p2 import P2Quantile

NOTIONAL_THR_FLOOR: float = 10_000.0
NOTIONAL_THR_CEIL: float = 5_000_000.0
CLUSTER_BPS_FLOOR: float = 0.5
CLUSTER_BPS_CEIL: float = 50.0

DEFAULT_NOTIONAL_THR: float = 250_000.0
DEFAULT_CLUSTER_BPS: float = 5.0
REL_THRESH: float = 0.05


@dataclass
class LiquiditySignalThresholds:
    notional_thr: float   # min USD depth → "thick book" (≡ min_notional_for_high_liq)
    cluster_bps: float    # typical cluster window (≡ dense_cluster_bps)
    n: int
    src: str              # "static" | "calib_q50"


def _norm(s: str | None) -> str:
    return (s or "na").strip().lower()


def _clamp(val: float | None, default: float, lo: float, hi: float) -> float:
    if val is None or not math.isfinite(val):
        return default
    return max(lo, min(hi, val))


class LiquiditySignalCalibrator:
    """
    Наблюдает `depth_usd` и `spread_bps` per symbol,
    калибрует `notional_thr` (thin-book порог) и `cluster_bps` (cluster width).

    Usage::

        cal = LiquiditySignalCalibrator()
        cal.observe(symbol="BTCUSDT", depth_usd=180_000.0, spread_bps=1.2)
        th = cal.thresholds(symbol="BTCUSDT")
        # th.notional_thr, th.cluster_bps
    """

    def __init__(
        self,
        *,
        min_samples: int = 500,
        enforce: bool = False,
        auto_enforce: bool = True,
        rel_thresh: float = REL_THRESH,
    ) -> None:
        self.min_samples = min_samples
        self.enforce = enforce
        self.auto_enforce = auto_enforce
        self.rel_thresh = rel_thresh

        self._q_notional: dict[str, P2Quantile] = {}
        self._q_cluster: dict[str, P2Quantile] = {}
        self._n: dict[str, int] = {}
        self._committed_notional: dict[str, float] = {}
        self._committed_cluster: dict[str, float] = {}
        self._shadow: dict[str, LiquiditySignalThresholds] = {}

    def observe(self, *, symbol: str, depth_usd: float, spread_bps: float) -> None:
        """Подать наблюдение. Невалидные значения молча игнорируются."""
        sym = _norm(symbol)
        if not math.isfinite(depth_usd) or depth_usd <= 0.0:
            return
        if not math.isfinite(spread_bps) or spread_bps <= 0.0:
            return

        self._get_q_notional(sym).update(depth_usd)
        self._get_q_cluster(sym).update(spread_bps)
        self._n[sym] = self._n.get(sym, 0) + 1

    def thresholds(
        self,
        *,
        symbol: str,
        default_notional: float = DEFAULT_NOTIONAL_THR,
        default_cluster: float = DEFAULT_CLUSTER_BPS,
    ) -> LiquiditySignalThresholds:
        sym = _norm(symbol)
        n = self._n.get(sym, 0)

        shadow = self._compute(sym, n, default_notional, default_cluster)
        self._shadow[sym] = shadow

        warm = n >= self.min_samples
        if not (self.enforce or (self.auto_enforce and warm)):
            return LiquiditySignalThresholds(
                notional_thr=default_notional, cluster_bps=default_cluster,
                n=n, src="static")

        prev_notional = self._committed_notional.get(sym, default_notional)
        prev_cluster = self._committed_cluster.get(sym, default_cluster)
        new_notional = shadow.notional_thr
        new_cluster = shadow.cluster_bps

        if abs(new_notional - prev_notional) / max(prev_notional, 1e-9) >= self.rel_thresh:
            self._committed_notional[sym] = new_notional
        else:
            new_notional = prev_notional

        if abs(new_cluster - prev_cluster) / max(prev_cluster, 1e-9) >= self.rel_thresh:
            self._committed_cluster[sym] = new_cluster
        else:
            new_cluster = prev_cluster

        return LiquiditySignalThresholds(
            notional_thr=new_notional, cluster_bps=new_cluster,
            n=n, src="calib_q50")

    def shadow_thresholds(self, *, symbol: str) -> LiquiditySignalThresholds | None:
        return self._shadow.get(_norm(symbol))

    def n(self, symbol: str) -> int:
        return self._n.get(_norm(symbol), 0)

    def dump_symbol_state(self, *, symbol: str, updated_ts_ms: int) -> dict[str, Any]:
        sym = _norm(symbol)
        return {
            "v": 1, "kind": "liquidity_signal", "symbol": sym,
            "updated_ts_ms": updated_ts_ms,
            "min_samples": self.min_samples,
            "n": self._n.get(sym, 0),
            "committed_notional": self._committed_notional.get(sym),
            "committed_cluster": self._committed_cluster.get(sym),
            "q_notional": (self._q_notional[sym].to_state() if sym in self._q_notional else None),
            "q_cluster": (self._q_cluster[sym].to_state() if sym in self._q_cluster else None),
        }

    def load_symbol_state(self, state: Any) -> None:
        try:
            if not isinstance(state, dict) or state.get("kind") != "liquidity_signal":
                return
            sym = _norm(str(state.get("symbol") or "na"))
            self._n[sym] = int(state.get("n", 0) or 0)
            if state.get("committed_notional") is not None:
                self._committed_notional[sym] = float(state["committed_notional"])
            if state.get("committed_cluster") is not None:
                self._committed_cluster[sym] = float(state["committed_cluster"])
            if q_raw := state.get("q_notional"):
                self._q_notional[sym] = P2Quantile.from_state(q_raw)
            if q_raw := state.get("q_cluster"):
                self._q_cluster[sym] = P2Quantile.from_state(q_raw)
        except Exception:
            pass

    # --- internals -------------------------------------------------------

    def _get_q_notional(self, sym: str) -> P2Quantile:
        if sym not in self._q_notional:
            self._q_notional[sym] = P2Quantile(p=0.50)
        return self._q_notional[sym]

    def _get_q_cluster(self, sym: str) -> P2Quantile:
        if sym not in self._q_cluster:
            self._q_cluster[sym] = P2Quantile(p=0.50)
        return self._q_cluster[sym]

    def _compute(
        self, sym: str, n: int,
        d_notional: float, d_cluster: float,
    ) -> LiquiditySignalThresholds:
        if n < self.min_samples:
            return LiquiditySignalThresholds(
                notional_thr=d_notional, cluster_bps=d_cluster, n=n, src="static")
        raw_notional = self._q_notional[sym].value() if sym in self._q_notional else None
        raw_cluster = self._q_cluster[sym].value() if sym in self._q_cluster else None
        notional = _clamp(raw_notional, d_notional, NOTIONAL_THR_FLOOR, NOTIONAL_THR_CEIL)
        cluster = _clamp(raw_cluster, d_cluster, CLUSTER_BPS_FLOOR, CLUSTER_BPS_CEIL)
        return LiquiditySignalThresholds(
            notional_thr=notional, cluster_bps=cluster, n=n, src="calib_q50")
