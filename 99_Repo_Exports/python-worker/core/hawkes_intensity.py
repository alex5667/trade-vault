# -*- coding: utf-8 -*-
"""Hawkes-like online intensity features.

Deterministic low-latency burst proxies:

    S <- exp(-beta*dt) * S + x
    lam <- mu + alpha * S

Where x is either:
  - 1 per event (event mode), or
  - rate_per_s * dt (rate mode).

All timestamps are epoch milliseconds.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, Optional


def _decay(beta: float, dt_s: float) -> float:
    if dt_s <= 0.0:
        return 1.0
    try:
        x = math.exp(-float(beta) * float(dt_s))
        return x if math.isfinite(x) else 0.0
    except Exception:
        return 0.0


@dataclass
class IntensitySnapshot:
    ts_ms: int
    s: float
    lam: float


class OnlineIntensity:
    """Single event-type intensity state."""

    def __init__(self, *, mu: float = 0.0, alpha: float = 1.0, beta: float = 2.0) -> None:
        self.mu = float(mu)
        self.alpha = float(alpha)
        self.beta = float(beta)
        self._s: float = 0.0
        self._last_ts_ms: int = 0

    def reset(self) -> None:
        self._s = 0.0
        self._last_ts_ms = 0

    def update_event(self, ts_ms: int, x: float = 1.0) -> IntensitySnapshot:
        t = int(ts_ms or 0)
        if t <= 0:
            return IntensitySnapshot(ts_ms=self._last_ts_ms, s=float(self._s), lam=self.lam)
        dt_s = 0.0
        if self._last_ts_ms > 0 and t >= self._last_ts_ms:
            dt_s = float(t - self._last_ts_ms) / 1000.0
        d = _decay(self.beta, dt_s)
        self._s = float(d * self._s + float(x))
        self._last_ts_ms = t
        return IntensitySnapshot(ts_ms=t, s=float(self._s), lam=self.lam)

    def update_rate(self, ts_ms: int, rate_per_s: float) -> IntensitySnapshot:
        t = int(ts_ms or 0)
        if t <= 0:
            return IntensitySnapshot(ts_ms=self._last_ts_ms, s=float(self._s), lam=self.lam)
        dt_s = 0.0
        if self._last_ts_ms > 0 and t >= self._last_ts_ms:
            dt_s = float(t - self._last_ts_ms) / 1000.0
        x = max(0.0, float(rate_per_s)) * dt_s
        d = _decay(self.beta, dt_s)
        self._s = float(d * self._s + x)
        self._last_ts_ms = t
        return IntensitySnapshot(ts_ms=t, s=float(self._s), lam=self.lam)

    @property
    def s(self) -> float:
        return float(self._s)

    @property
    def lam(self) -> float:
        return float(self.mu + self.alpha * self._s)


class MultiIntensity:
    """Multiple event types for one symbol (or global)."""

    def __init__(
        self,
        *,
        mu: float = 0.0,
        alpha: float = 1.0,
        beta: float = 2.0,
        event_types: Optional[list[str]] = None,
    ) -> None:
        self.mu = float(mu)
        self.alpha = float(alpha)
        self.beta = float(beta)
        self._m: Dict[str, OnlineIntensity] = {}
        for et in (event_types or []):
            self._m[str(et)] = OnlineIntensity(mu=mu, alpha=alpha, beta=beta)

    def _get(self, event_type: str) -> OnlineIntensity:
        k = str(event_type)
        st = self._m.get(k)
        if st is None:
            st = OnlineIntensity(mu=self.mu, alpha=self.alpha, beta=self.beta)
            self._m[k] = st
        return st

    def update_event(self, event_type: str, ts_ms: int, x: float = 1.0) -> IntensitySnapshot:
        return self._get(event_type).update_event(ts_ms, x=x)

    def update_rate(self, event_type: str, ts_ms: int, rate_per_s: float) -> IntensitySnapshot:
        return self._get(event_type).update_rate(ts_ms, rate_per_s)

    def snapshot(self) -> Dict[str, Dict[str, float]]:
        out: Dict[str, Dict[str, float]] = {}
        for k, st in self._m.items():
            out[k] = {"s": float(st.s), "lam": float(st.lam)}
        return out

