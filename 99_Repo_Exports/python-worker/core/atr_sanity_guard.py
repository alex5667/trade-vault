from __future__ import annotations

# -*- coding: utf-8 -*-
"""
ATR Sanity Guard (deterministic, unit-testable).

Goal
----
Select the best ATR source (fresh + consistent) and protect the system from
bad ATR inputs (stale keys, wrong tf, multi-writer jumps, etc.).

Core idea
---------
We maintain a deterministic volatility proxy computed from *our own* bars on the same TF as ATR:
  range_bps = 10000 * (high - low) / mid
Then ATR_bps should be in a reasonable band around this proxy.

Design constraints
------------------
* Deterministic time: uses bar.end_ts_ms and explicit now_ms (no wall time dependency).
* Fail-open: never blocks trading by itself; only influences ATR source selection.
* Minimal coupling: pure logic in core/, no Redis dependency.
"""


import math
from dataclasses import dataclass

from core.quantile_p2 import P2Quantile


def tf_to_ms(tf: str) -> int:
    """
    Parse timeframe string into milliseconds.
    Supported: 1m,5m,15m,30m,1h,4h,1d (case-insensitive).
    Fail-open: defaults to 60_000.
    """
    try:
        s = (tf or "").strip().lower()
        if s.endswith("m"):
            return int(float(s[:-1]) * 60_000)
        if s.endswith("h"):
            return int(float(s[:-1]) * 3_600_000)
        if s.endswith("d"):
            return int(float(s[:-1]) * 86_400_000)
    except Exception:
        pass
    return 60_000


@dataclass
class RangeStatsSnapshot:
    tf_ms: int
    n: int
    p50: float
    p95: float
    last_bucket: int


class RangeTfAggregator:
    """
    Roll-up microbars into TF buckets and track robust range_bps quantiles.
    """

    def __init__(self, *, tf_ms: int, min_samples: int = 30) -> None:
        self.tf_ms = int(max(1_000, tf_ms))
        self.min_samples = int(max(10, min_samples))
        self._q50 = P2Quantile(p=0.50)
        self._q95 = P2Quantile(p=0.95)
        self._n = 0

        # current bucket OHLC
        self._bucket: int | None = None
        self._o = 0.0
        self._h = 0.0
        self._l = 0.0
        self._c = 0.0

    def _finalize_bucket(self) -> None:
        try:
            if self._bucket is None:
                return
            if self._o <= 0 or self._h <= 0 or self._l <= 0 or self._c <= 0:
                return
            mid = 0.5 * (abs(self._o) + abs(self._c))
            if mid <= 0:
                return
            rng = float(self._h - self._l)
            if not math.isfinite(rng) or rng <= 0:
                return
            range_bps = 10000.0 * (rng / mid)
            if math.isfinite(range_bps) and range_bps > 0:
                self._q50.update(float(range_bps))
                self._q95.update(float(range_bps))
                self._n += 1
        except Exception:
            return

    def push_microbar(self, *, end_ts_ms: int, o: float, h: float, l: float, c: float) -> None:
        """
        Feed 1s microbar into TF roll-up.
        Deterministic bucket key: end_ts_ms // tf_ms.
        """
        try:
            ts = int(end_ts_ms or 0)
            if ts <= 0:
                return
            bucket = ts // self.tf_ms
            if self._bucket is None:
                self._bucket = int(bucket)
                self._o = float(o)
                self._h = float(h)
                self._l = float(l)
                self._c = float(c)
                return
            if int(bucket) != int(self._bucket):
                # close previous bucket
                self._finalize_bucket()
                # start new bucket
                self._bucket = int(bucket)
                self._o = float(o)
                self._h = float(h)
                self._l = float(l)
                self._c = float(c)
                return
            # same bucket: update
            self._h = float(max(self._h, float(h)))
            self._l = float(min(self._l, float(l)))
            self._c = float(c)
        except Exception:
            return

    def snapshot(self) -> RangeStatsSnapshot:
        p50 = float(self._q50.value() or 0.0)
        p95 = float(self._q95.value() or 0.0)
        return RangeStatsSnapshot(
            tf_ms=int(self.tf_ms),
            n=int(self._n),
            p50=p50,
            p95=p95 if p95 > 0 else p50,
            last_bucket=int(self._bucket or 0),
        )

    def is_ready(self) -> bool:
        return int(self._n) >= int(self.min_samples)

    def expected_bounds_bps(
        self,
        *,
        min_mult: float,
        max_mult: float,
        floor_bps: float = 0.0,
        ceil_bps: float = 1e9,
    ) -> tuple[float, float]:
        """
        Compute sanity bounds for ATR_bps based on p50/p95 range_bps.
        - lower bound anchored to p50
        - upper bound anchored to p95
        """
        snap = self.snapshot()
        base_lo = float(snap.p50)
        base_hi = float(snap.p95 if snap.p95 > 0 else snap.p50)
        lo = float(max(floor_bps, base_lo * float(min_mult)))
        hi = float(min(ceil_bps, max(lo, base_hi * float(max_mult))))
        return lo, hi


@dataclass
class AtrCandidate:
    atr: float
    key: str
    src: str
    tf: str
    ts_ms: int
    age_ms: int


@dataclass
class AtrPick:
    atr: float
    src: str
    key: str
    ts_ms: int
    age_ms: int
    sane: int
    reason: str
    exp_lo_bps: float
    exp_hi_bps: float


def pick_best_atr(
    *,
    candidates: list[AtrCandidate],
    entry_px: float,
    now_ms: int,
    range_agg: RangeTfAggregator | None,
    max_age_ms: int,
    min_mult: float,
    max_mult: float,
) -> AtrPick:
    """
    Choose ATR candidate:
      1) Prefer fresh (age <= max_age_ms)
      2) Prefer sanity vs range bounds (if range_agg ready)
      3) Prefer lower age (freshest)
    Fail-open:
      - if no "sane" candidates -> return freshest candidate but mark sane=0
      - if no candidates -> atr=0
    """
    nm = int(now_ms or 0)
    if nm <= 0:
        nm = 0
    px = float(entry_px or 0.0)
    if px <= 0:
        px = 0.0

    # prepare expected bounds
    exp_lo_bps = 0.0
    exp_hi_bps = 0.0
    ready = bool(range_agg is not None and range_agg.is_ready())
    if ready and px > 0:
        exp_lo_bps, exp_hi_bps = range_agg.expected_bounds_bps(min_mult=min_mult, max_mult=max_mult)

    # helper: candidate sanity
    def _cand_sane(c: AtrCandidate) -> tuple[int, str]:
        if c.atr <= 0 or px <= 0:
            return 0, "bad_atr_or_px"
        atr_bps = 10000.0 * (float(c.atr) / px)
        if not math.isfinite(atr_bps) or atr_bps <= 0:
            return 0, "bad_atr_bps"
        if c.age_ms > int(max_age_ms):
            return 0, "stale"
        if not ready:
            return 1, "no_range_ref_ready"
        if exp_lo_bps > 0 and atr_bps < exp_lo_bps:
            return 0, "too_low_vs_range"
        if exp_hi_bps > 0 and atr_bps > exp_hi_bps:
            return 0, "too_high_vs_range"
        return 1, "ok"

    # rank candidates: sane first, then by age
    sane_list: list[tuple[AtrCandidate, str]] = []
    fresh_list: list[tuple[AtrCandidate, str]] = []
    for c in candidates or []:
        if c is None:
            continue
        cc = AtrCandidate(
            atr=float(c.atr or 0.0),
            key=str(c.key or ""),
            src=str(c.src or "na"),
            tf=str(c.tf or ""),
            ts_ms=int(c.ts_ms or 0),
            age_ms=int(c.age_ms or 0),
        )
        ok, why = _cand_sane(cc)
        if ok == 1:
            sane_list.append((cc, why))
        # keep “freshest” fallback list even if not sane
        fresh_list.append((cc, why))

    sane_list.sort(key=lambda x: int(x[0].age_ms))
    fresh_list.sort(key=lambda x: int(x[0].age_ms))

    if sane_list:
        c0, why = sane_list[0]
        return AtrPick(
            atr=float(c0.atr),
            src=str(c0.src),
            key=str(c0.key),
            ts_ms=int(c0.ts_ms),
            age_ms=int(c0.age_ms),
            sane=1,
            reason=str(why),
            exp_lo_bps=float(exp_lo_bps),
            exp_hi_bps=float(exp_hi_bps),
        )

    if fresh_list:
        c0, why = fresh_list[0]
        return AtrPick(
            atr=float(c0.atr),
            src=str(c0.src),
            key=str(c0.key),
            ts_ms=int(c0.ts_ms),
            age_ms=int(c0.age_ms),
            sane=0,
            reason=str(why),
            exp_lo_bps=float(exp_lo_bps),
            exp_hi_bps=float(exp_hi_bps),
        )

    return AtrPick(
        atr=0.0,
        src="none",
        key="",
        ts_ms=0,
        age_ms=0,
        sane=0,
        reason="no_candidates",
        exp_lo_bps=float(exp_lo_bps),
        exp_hi_bps=float(exp_hi_bps),
    )
