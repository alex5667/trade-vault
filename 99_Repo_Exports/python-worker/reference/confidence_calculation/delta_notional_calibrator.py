from __future__ import annotations

"""
DeltaNotionalCalibrator
======================
Автокалибровка "tiers" по агрессивной дельте в USD (delta_notional_usd) на уровне microbar_close.

Зачем:
  - tier0/tier1/tier2 должны быть самонастраивающимися per-symbol & per-regime
  - чтобы не подбирать руками пороги 3.1M/6.6M/8.7M и т.п.

Идея:
  - на каждом microbar_close считаем dn_usd = abs(bar.delta_sum) * bar.close_px
  - квантили считаем в лог-пространстве log1p(dn_usd), чтобы огромные выбросы
    (например из-за дублей воркеров/разных reset) не разрушали оценку.
  - tiers:
      tier0 = p50
      tier1 = p80
      tier2 = p95

Детерминизм:
  - update_ts_ms берём строго из bar.end_ts_ms (никакого wall time)

Persistence:
  - state хранится в Redis (аналогично EffQuoteCalibrator)
"""


import math
from dataclasses import dataclass
from datetime import UTC
from typing import Any

from core.quantile_p2 import P2Quantile


@dataclass
class DeltaNotionalTiers:
    tier0_usd: float
    tier1_usd: float
    tier2_usd: float
    n: int
    src: str  # "static" | "calib_p50/p80/p95"

    # Telemetry-only: hour-of-week liquidity scaling (not used for decisions by default)
    scale: float = 1.0
    hour_of_week: int = -1
    g_liq_ema: float = 0.0
    b_liq_ema: float = 0.0

    # Telemetry-only: hour-of-week liquidity scaling (not used for decisions by default)
    scale: float = 1.0
    hour_of_week: int = -1
    g_liq_ema: float = 0.0
    b_liq_ema: float = 0.0


class DeltaNotionalCalibrator:
    """
    Per-symbol calibrator with per-regime quantiles (log-space).
    """

    @staticmethod
    def _get_hour_of_week(ts_ms: int) -> int:
        from datetime import datetime
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=UTC)
        return int(dt.weekday() * 24 + dt.hour)

    def __init__(
        self,
        *,
        min_samples: int = 300,
        liq_alpha: float = 0.05,
        liq_scale_clamp: tuple[float, float] = (0.5, 2.0),
    ) -> None:
        self.min_samples = int(min_samples)
        self._liq_alpha = float(liq_alpha)
        self._liq_scale_lo = float(liq_scale_clamp[0])
        self._liq_scale_hi = float(liq_scale_clamp[1])

        self._q50: dict[str, P2Quantile] = {}
        self._q80: dict[str, P2Quantile] = {}
        self._q95: dict[str, P2Quantile] = {}
        self._n: dict[str, int] = {}

        # Liquidity EMAs (dn_usd) for hour-of-week scaling telemetry
        self._global_liq: dict[str, float] = {}
        self._bucket_liq: dict[str, dict[int, float]] = {}

    @staticmethod
    def _get(store: dict[str, P2Quantile], regime: str, p: float) -> P2Quantile:
        q = store.get(regime)
        if q is None:
            q = P2Quantile(p=float(p))
            store[regime] = q
        return q

    def update(self, *, regime: str, dn_usd: float, ts_ms: int = 0) -> None:
        r = (regime or "na")
        x = max(0.0, float(dn_usd))
        # log1p to stabilize tails
        lx = math.log1p(x)
        self._get(self._q50, r, 0.50).update(lx)
        self._get(self._q80, r, 0.80).update(lx)
        self._get(self._q95, r, 0.95).update(lx)
        self._n[r] = int(self._n.get(r, 0) + 1)

        # Update liquidity profiles (EMA of dn_usd) for hour-of-week scaling telemetry
        a = float(self._liq_alpha)
        try:
            g = float(self._global_liq.get(r, 0.0))
            self._global_liq[r] = (1.0 - a) * g + a * x
        except Exception:
            self._global_liq[r] = float(x)
        if int(ts_ms) > 0:
            h = self._get_hour_of_week(int(ts_ms))
            bm = self._bucket_liq.get(r)
            if bm is None:
                bm = {}
                self._bucket_liq[r] = bm
            b = float(bm.get(h, 0.0))
            bm[h] = (1.0 - a) * b + a * x


    @staticmethod
    def _clamp(x: float, lo: float, hi: float) -> float:
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x

    @staticmethod
    def _clamp(x: float, lo: float, hi: float) -> float:
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x

    def tiers(
        self,
        *,
        regime: str,
        ts_ms: int = 0,
        default_t0: float,
        default_t1: float,
        default_t2: float,
        clamp_usd: tuple[float, float] = (0.0, 1e12),
    ) -> DeltaNotionalTiers:
        r = (regime or "na")
        n = int(self._n.get(r, 0))

        # Telemetry-only: hour-of-week scaling factor
        scale = 1.0
        how = -1
        g_liq = float(self._global_liq.get(r, 0.0))
        b_liq = 0.0
        if int(ts_ms) > 0:
            how = self._get_hour_of_week(int(ts_ms))
            b_liq = float(self._bucket_liq.get(r, {}).get(how, 0.0))
            if g_liq > 0.0 and b_liq > 0.0:
                scale = float(b_liq / g_liq)
                scale = self._clamp(scale, self._liq_scale_lo, self._liq_scale_hi)

        if n < self.min_samples or r not in self._q50 or r not in self._q80 or r not in self._q95:
            # Not ready -> static bootstrap
            t0 = float(default_t0 or 0.0) * scale
            t1 = float(default_t1 or 0.0) * scale
            t2 = float(default_t2 or 0.0) * scale
            return DeltaNotionalTiers(
                tier0_usd=self._clamp(t0, *clamp_usd),
                tier1_usd=self._clamp(t1, *clamp_usd),
                tier2_usd=self._clamp(t2, *clamp_usd),
                n=n,
                src="static",
                scale=scale,
                hour_of_week=how,
                g_liq_ema=g_liq,
                b_liq_ema=b_liq,
            )

        # Ready -> calibrated percentiles (in USD space)
        t0 = float(math.expm1(self._q50[r].value())) * scale
        t1 = float(math.expm1(self._q80[r].value())) * scale
        t2 = float(math.expm1(self._q95[r].value())) * scale

        return DeltaNotionalTiers(
            tier0_usd=self._clamp(float(t0), *clamp_usd),
            tier1_usd=self._clamp(float(t1), *clamp_usd),
            tier2_usd=self._clamp(float(t2), *clamp_usd),
            n=n,
            src="calib_p50/p80/p95",
            scale=scale,
            hour_of_week=how,
            g_liq_ema=g_liq,
            b_liq_ema=b_liq,
        )

    # ---------------- Persistence ----------------
    def dump_regime_state(self, *, symbol: str, regime: str, updated_ts_ms: int) -> dict[str, Any]:
        r = (regime or "na")
        return {
            "v": 1,
            "symbol": symbol,
            "regime": r,
            "updated_ts_ms": int(updated_ts_ms),
            "n": int(self._n.get(r, 0)),
            "q50_log": (self._q50.get(r).to_state() if self._q50.get(r) else None),
            "q80_log": (self._q80.get(r).to_state() if self._q80.get(r) else None),
            "q95_log": (self._q95.get(r).to_state() if self._q95.get(r) else None),
            "liq_global": float(self._global_liq.get(r, 0.0)),
            "liq_bucket": dict(self._bucket_liq.get(r, {})),
        }

    def load_regime_state(self, state: dict[str, Any]) -> None:
        try:
            r = (state.get("regime") or "na")
            n = int(state.get("n", 0) or 0)
            q50 = state.get("q50_log")
            q80 = state.get("q80_log")
            q95 = state.get("q95_log")
            if isinstance(q50, dict):
                self._q50[r] = P2Quantile.from_state(q50)
            if isinstance(q80, dict):
                self._q80[r] = P2Quantile.from_state(q80)
            if isinstance(q95, dict):
                self._q95[r] = P2Quantile.from_state(q95)
            self._n[r] = n

            # Optional (backward compatible) hour-of-week liquidity state
            try:
                self._global_liq[r] = float(state.get("liq_global") or 0.0)
            except Exception:
                self._global_liq[r] = 0.0
            try:
                lb = state.get("liq_bucket") or {}
                if isinstance(lb, dict):
                    self._bucket_liq[r] = {int(k): float(v) for k, v in lb.items()}
            except Exception:
                self._bucket_liq[r] = {}
        except Exception:
            return
