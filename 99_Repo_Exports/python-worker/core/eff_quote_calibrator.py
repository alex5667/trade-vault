from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from core.calib_audit_contract import CalibEffqAuditV1, stable_hash
from core.quantile_p2 import P2Quantile


@dataclass
class EffQuoteThresholds:
    eff_quote_th: float
    min_quote_delta: float
    n: int
    src: str


class EffQuoteCalibrator:
    """
    Per-symbol calibrator with per-regime quantiles:
      - eff_quote_p20: low efficiency threshold for absorption-on-level
      - quote_delta_p30: minimum notional to avoid noise
    Deterministic: updates use bar.end_ts_ms (no wall time).
    """
    def __init__(self, *, min_samples: int = 300) -> None:
        self.min_samples = int(min_samples)
        self._eff_q10: dict[str, P2Quantile] = {}
        self._eff_q20: dict[str, P2Quantile] = {}
        self._eff_q30: dict[str, P2Quantile] = {}
        self._qd_q30: dict[str, P2Quantile] = {}
        self._n: dict[str, int] = {}

    def _get(self, d: dict[str, P2Quantile], key: str, p: float) -> P2Quantile:
        q = d.get(key)
        if q is None:
            q = P2Quantile(p=p)
            d[key] = q
        return q

    def update(self, *, regime: str, eff_quote: float, quote_delta: float) -> None:
        r = (regime or "na")
        if math.isfinite(eff_quote) and eff_quote > 0:
            self._get(self._eff_q10, r, 0.10).update(float(eff_quote))
            self._get(self._eff_q20, r, 0.20).update(float(eff_quote))
            self._get(self._eff_q30, r, 0.30).update(float(eff_quote))
        if math.isfinite(quote_delta) and quote_delta > 0:
            self._get(self._qd_q30, r, 0.30).update(float(quote_delta))
        self._n[r] = int(self._n.get(r, 0) + 1)

    def ready(self, regime: str) -> bool:
        return int(self._n.get((regime or "na"), 0)) >= self.min_samples

    def thresholds(
        self,
        *,
        regime: str,
        default_eff_th: float,
        default_min_qd: float,
        tier: int = 1,
        clamp_eff: tuple[float, float] = (1e-9, 1.0),
        clamp_qd: tuple[float, float] = (0.0, 1e12),
    ) -> EffQuoteThresholds:
        r = (regime or "na")
        n = int(self._n.get(r, 0))
        q10 = self._eff_q10.get(r)
        q20 = self._eff_q20.get(r)
        q30 = self._eff_q30.get(r)
        q_qd = self._qd_q30.get(r)

        if int(tier) <= 0:
            eff = q30.value() if q30 else None
            src_eff = "calib_p30"
        elif int(tier) >= 2:
            eff = q10.value() if q10 else None
            src_eff = "calib_p10"
        else:
            eff = q20.value() if q20 else None
            src_eff = "calib_p20"

        qd = q_qd.value() if q_qd else None

        eff_th = float(default_eff_th)
        min_qd = float(default_min_qd)
        src = "static"

        if eff is not None and n >= self.min_samples:
            eff_th = float(eff)
            src = str(src_eff)
        if qd is not None and n >= self.min_samples:
            min_qd = float(qd)
            src = src + "+qd_p30" if src != "static" else "qd_p30"

        lo, hi = clamp_eff
        eff_th = max(lo, min(hi, eff_th))
        qlo, qhi = clamp_qd
        min_qd = max(qlo, min(qhi, min_qd))

        return EffQuoteThresholds(eff_quote_th=eff_th, min_quote_delta=min_qd, n=n, src=src)

    # ---------------- Persistence ----------------
    def dump_regime_state(self, *, symbol: str, regime: str, updated_ts_ms: int) -> dict[str, Any]:
        r = (regime or "na")
        n = int(self._n.get(r, 0))
        q_eff10 = self._eff_q10.get(r)
        q_eff20 = self._eff_q20.get(r)
        q_eff30 = self._eff_q30.get(r)
        q_qd = self._qd_q30.get(r)
        return {
            "v": 1,
            "symbol": symbol,
            "regime": r,
            "updated_ts_ms": int(updated_ts_ms),
            "min_samples": int(self.min_samples),
            "n": n,
            "eff_q10": (q_eff10.to_state() if q_eff10 else None),
            "eff_q20": (q_eff20.to_state() if q_eff20 else None),
            "eff_q30": (q_eff30.to_state() if q_eff30 else None),
            "qd_q30": (q_qd.to_state() if q_qd else None),
        }

    def load_regime_state(self, state: dict[str, Any]) -> None:
        """
        Load one regime state into this calibrator.
        Fail-open on partial data.
        """
        try:
            r = (state.get("regime") or "na")
            n = int(state.get("n", 0) or 0)
            eff10 = state.get("eff_q10")
            eff20 = state.get("eff_q20")
            eff30 = state.get("eff_q30")
            qd = state.get("qd_q30")
            if isinstance(eff10, dict):
                self._eff_q10[r] = P2Quantile.from_state(eff10)
            if isinstance(eff20, dict):
                self._eff_q20[r] = P2Quantile.from_state(eff20)
            if isinstance(eff30, dict):
                self._eff_q30[r] = P2Quantile.from_state(eff30)
            if isinstance(qd, dict):
                self._qd_q30[r] = P2Quantile.from_state(qd)
            self._n[r] = n
        except Exception:
            return

    def audit_event(
        self,
        *,
        symbol: str,
        regime: str,
        ts_ms: int,
        eff_quote_th: float,
        min_quote_delta: float,
        src: str,
    ) -> dict[str, Any]:
        """
        Build an audit event (v1) including deterministic hash of the persisted state.
        """
        st = self.dump_regime_state(symbol=symbol, regime=regime, updated_ts_ms=int(ts_ms))
        h = stable_hash(st)
        ev = CalibEffqAuditV1(
            v=1,
            symbol=symbol,
            regime=(regime or "na"),
            ts_ms=int(ts_ms),
            src=str(src),
            n=int(st.get("n", 0) or 0),
            eff_quote_th=float(eff_quote_th),
            min_quote_delta=float(min_quote_delta),
            state_hash=str(h),
        )
        return ev.to_dict()
