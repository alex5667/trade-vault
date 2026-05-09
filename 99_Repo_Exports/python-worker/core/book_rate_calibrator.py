import json
import math
from dataclasses import dataclass
from typing import Any

from core.quantile_p2 import P2Quantile


@dataclass
class BookRateThresholds:
    """
    Derived thresholds for book health gate.
    - min_hz: below this, book-based evidences (OBI/Iceberg) are considered unreliable
    - warn_hz: below this, we mark WARN (for audit/telemetry)
    """
    min_hz: float
    warn_hz: float
    n: int
    src: str


class BookRateCalibrator:
    """
    Per-symbol, per-regime calibrator for orderbook update rate.

    Design rules (aligned with EffQuoteCalibrator):
    - deterministic inputs: update uses exchange ts_ms deltas (dt_ms) computed from book payload
    - robust: ignores absurd dt (downtime) via dt_max_ms filter
    - reproducible: supports dump/load per regime
    - fail-open: if not ready -> returns provided defaults

    What we estimate:
    - p10: "low activity but still normal" boundary
    - p50: typical median

    Suggested policy:
    - warn_hz = 0.5 * p50 (clamped) OR p10 (whichever is higher), so WARN is not too sensitive
    - min_hz  = p10 (clamped), so book evidence is disabled only on abnormally slow book
    """

    def __init__(self, *, min_samples: int = 300, dt_max_ms: int = 2000) -> None:
        self.min_samples = int(min_samples)
        self.dt_max_ms = int(dt_max_ms)
        self._p10: dict[str, P2Quantile] = {}
        self._p50: dict[str, P2Quantile] = {}
        self._n: dict[str, int] = {}

    def _get(self, m: dict[str, P2Quantile], regime: str, p: float) -> P2Quantile:
        q = m.get(regime)
        if q is None:
            q = P2Quantile(p=p)
            m[regime] = q
        return q

    def update(self, *, regime: str, inst_hz: float, dt_ms: int) -> None:
        """
        Update estimator with instantaneous rate (Hz) computed from dt_ms between book snapshots.
        We ignore:
        - non-finite values
        - dt_ms <= 0
        - dt_ms above dt_max_ms (treat as connectivity/downtime, not market microstructure)
        """
        r = (regime or "na")
        try:
            dt = int(dt_ms)
        except Exception:
            dt = 0
        if dt <= 0:
            return
        if self.dt_max_ms > 0 and dt > self.dt_max_ms:
            return
        if not (math.isfinite(inst_hz) and inst_hz > 0):
            return
        self._get(self._p10, r, 0.10).update(float(inst_hz))
        self._get(self._p50, r, 0.50).update(float(inst_hz))
        self._n[r] = int(self._n.get(r, 0) + 1)

    def thresholds(
        self,
        *,
        regime: str,
        default_min_hz: float,
        default_warn_hz: float,
        clamp: tuple[float, float] = (1.0, 500.0),
    ) -> BookRateThresholds:
        """
        Return calibrated thresholds if ready else defaults.
        """
        r = (regime or "na")
        n = int(self._n.get(r, 0))
        lo, hi = float(clamp[0]), float(clamp[1])

        # Not ready -> use bootstrap
        if n < int(self.min_samples):
            mn = float(default_min_hz)
            wn = float(default_warn_hz)
            mn = max(lo, min(hi, mn))
            wn = max(lo, min(hi, wn))
            return BookRateThresholds(min_hz=mn, warn_hz=wn, n=n, src="static")

        p10 = self._p10.get(r).value() if self._p10.get(r) else None
        p50 = self._p50.get(r).value() if self._p50.get(r) else None

        # Fail-open fallback (should be rare after ready)
        if not (p10 and p10 > 0 and math.isfinite(p10)):
            p10 = float(default_min_hz)
        if not (p50 and p50 > 0 and math.isfinite(p50)):
            p50 = max(float(default_warn_hz), float(default_min_hz))

        min_hz = max(lo, min(hi, float(p10)))
        # WARN: don't let it be below min_hz; also don't be too tight:
        warn_hz = max(min_hz, min(hi, max(float(p10), 0.5 * float(p50))))

        return BookRateThresholds(min_hz=min_hz, warn_hz=warn_hz, n=n, src="calib_p10")

    # ---------------- Persistence ----------------
    def dump_regime_state(self, *, symbol: str, regime: str, updated_ts_ms: int) -> dict[str, Any]:
        r = (regime or "na")
        return {
            "v": 1,
            "kind": "book_rate",
            "symbol": symbol,
            "regime": r,
            "updated_ts_ms": int(updated_ts_ms),
            "min_samples": int(self.min_samples),
            "dt_max_ms": int(self.dt_max_ms),
            "n": int(self._n.get(r, 0)),
            "p10": (self._p10.get(r).to_state() if self._p10.get(r) else None),
            "p50": (self._p50.get(r).to_state() if self._p50.get(r) else None),
        }

    def load_regime_state(self, state: dict[str, Any]) -> None:
        try:
            if not isinstance(state, dict):
                return
            if (state.get("kind") or "") not in ("book_rate", ""):
                # tolerate missing kind for backwards compatibility
                pass
            r = (state.get("regime") or "na")
            self.min_samples = int(state.get("min_samples", self.min_samples) or self.min_samples)
            self.dt_max_ms = int(state.get("dt_max_ms", self.dt_max_ms) or self.dt_max_ms)
            p10 = state.get("p10")
            p50 = state.get("p50")
            if isinstance(p10, dict):
                self._p10[r] = P2Quantile.from_state(p10)
            if isinstance(p50, dict):
                self._p50[r] = P2Quantile.from_state(p50)
            self._n[r] = int(state.get("n", 0) or 0)
        except Exception:
            return

    @staticmethod
    def loads(raw: str) -> dict[str, Any] | None:
        try:
            d = json.loads(raw)
            return d if isinstance(d, dict) else None
        except Exception:
            return None
