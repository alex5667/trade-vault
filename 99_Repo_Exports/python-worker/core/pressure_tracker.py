# -*- coding: utf-8 -*-
from __future__ import annotations
"""
PressureTracker
===============
Используем cooldown-спам / бурсты как signal-quality feature, а не только "режем".

Метрики:
  - raw_triggers_per_min (скользящее окно + EMA)
  - cooldown_hit_rate (EMA отношение cooldown_hits/raw_triggers)

Зачем:
  - pressure -> динамическая строгая фильтрация (need=3)
  - pressure -> динамический burst window (800..2500ms)
"""


import math
from collections import deque
from dataclasses import dataclass
from typing import Deque, Tuple


@dataclass
class PressureSnapshot:
    n_raw: int
    n_cd: int
    per_min: float
    cd_rate: float
    per_min_ema: float
    cd_rate_ema: float


class PressureTracker:
    def __init__(self, *, window_ms: int = 60_000, ema_alpha: float = 0.20) -> None:
        self.window_ms = int(max(5_000, window_ms))
        self.alpha = float(ema_alpha)
        self._raw_ts: Deque[int] = deque()
        self._cd_ts: Deque[int] = deque()
        self._emit_ts: Deque[int] = deque()
        self._per_min_ema: float = 0.0
        self._cd_rate_ema: float = 0.0

    def _gc(self, *, now_ms: int) -> None:
        lo = int(now_ms - self.window_ms)
        while self._raw_ts and self._raw_ts[0] < lo:
            self._raw_ts.popleft()
        while self._cd_ts and self._cd_ts[0] < lo:
            self._cd_ts.popleft()
        while self._emit_ts and self._emit_ts[0] < lo:
            self._emit_ts.popleft()

    def on_raw_trigger(self, *, ts_ms: int) -> None:
        t = int(ts_ms or 0)
        if t <= 0:
            return
        self._raw_ts.append(t)

    def on_cooldown_hit(self, *, ts_ms: int) -> None:
        t = int(ts_ms or 0)
        if t <= 0:
            return
        self._cd_ts.append(t)

    def record_emit(self, ts_ms: int) -> None:
        t = int(ts_ms or 0)
        if t <= 0:
            return
        self._emit_ts.append(t)

    def snapshot(self, now_ms: int) -> PressureSnapshot:
        now = int(now_ms or 0)
        if now <= 0:
            now = 0
        self._gc(now_ms=now)
        n_raw = int(len(self._raw_ts))
        n_cd = int(len(self._cd_ts))
        # normalize to per-minute
        per_min = 0.0
        if self.window_ms > 0:
            per_min = (float(n_raw) * 60_000.0) / float(self.window_ms)
        cd_rate = (float(n_cd) / float(max(1, n_raw))) if n_raw > 0 else 0.0
        # EMA smoothing
        a = float(self.alpha)
        if math.isfinite(per_min):
            self._per_min_ema = a * float(per_min) + (1.0 - a) * float(self._per_min_ema)
        if math.isfinite(cd_rate):
            self._cd_rate_ema = a * float(cd_rate) + (1.0 - a) * float(self._cd_rate_ema)
        return PressureSnapshot(
            n_raw=n_raw,
            n_cd=n_cd,
            per_min=float(per_min),
            cd_rate=float(cd_rate),
            per_min_ema=float(self._per_min_ema),
            cd_rate_ema=float(self._cd_rate_ema),
        )

    @staticmethod
    def burst_window_ms(
        *,
        base_ms: int,
        min_ms: int,
        per_min_ema: float,
        hi_per_min: float,
        extreme_per_min: float,
    ) -> int:
        """
        Pressure-aware best-of-burst window:
          - normal: base_ms (например 2500)
          - pressure_hi: ~60% base
          - pressure_extreme: min_ms (например 800)
        """
        b = int(base_ms)
        mn = int(min_ms)
        if b < mn:
            b = mn
        p = float(per_min_ema or 0.0)
        hi = float(hi_per_min or 0.0)
        ex = float(extreme_per_min or 0.0)
        if ex > 0 and p >= ex:
            return mn
        if hi > 0 and p >= hi:
            return max(mn, int(0.60 * float(b)))
        return b
