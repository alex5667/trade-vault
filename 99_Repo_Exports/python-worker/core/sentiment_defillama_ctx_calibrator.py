from __future__ import annotations

"""P2 — Adaptive calibrator for Sentiment / DefiLlama context tighten caps.

Problem
-------
`_cached_sentiment_ctx_tighten_cap`  (default 2.0 bps) and
`_cached_defillama_ctx_tighten_cap` (default 4.0 bps) are static ENV values.
When these gates fire TIGHTEN they raise `expected_slippage_bps` in the cost
gate, filtering marginal trades during adverse macro regimes.  Until now there
was no feedback loop from trade outcomes to these caps.

Design
------
Each gate gets its own CtxGateTightenCalibrator that:
  1. Accepts rolling closed-trade samples: (r_multiple, tighten_bps, weight, ts_ms)
     - tighten_bps > 0  → this trade was affected by the gate
     - tighten_bps == 0 → baseline (gate not active / abstain)
  2. Computes weighted EV for each segment (tightened / baseline)
  3. Adapts the shadow cap:
     - EV(tightened) < TARGET − HYSTERESIS  → raise cap (filter more aggressively)
     - EV(tightened) > TARGET + MARGIN      → lower cap (relax slightly)
  4. Commits the shadow to the enforced cap after HOLD_MS if |Δ| ≥ ABS_THRESH
     and `enforce=True`.

Attribution
-----------
signal_pipeline._apply_decision writes `ctx_sentiment_tighten_bps` /
`ctx_defillama_tighten_bps` into indicators when TIGHTEN fires.  These flow
through signal_payload → POSITION_CLOSED event → trade_close_joiner, which
promotes them to top-level `calib_fields` in `trades:closed` stream.

Redis contract
--------------
Producer: orderflow_services/sentiment_defillama_ctx_tighten_calibrator_v1.py
Consumer: core.ctx_tighten_reader (optional, signal_pipeline)
Key:      autocal:ctx_tighten:state  (STRING, JSON)
Schema v1:
  {
    "schema_version": 1,
    "ts_ms": <epoch_ms>,
    "sentiment": {
      "cap_bps": <float>,          # committed (enforced) cap
      "shadow_cap_bps": <float>,   # latest shadow proposal
      "ev_tightened": <float>,     # weighted EV of tightened-pass trades
      "ev_baseline": <float>,      # weighted EV of baseline trades
      "n_tightened": <int>,        # effective sample count tightened
      "n_baseline": <int>,         # effective sample count baseline
      "last_commit_ms": <int>
    },
    "defillama": { ... same shape ... }
  }
"""

import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from core.weighted_stats import weighted_mean, effective_n

DEFAULT_SENTIMENT_CAP = 2.0
DEFAULT_DEFILLAMA_CAP = 4.0

SCHEMA_VERSION = 1


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class _Sample:
    r: float
    tighten_bps: float
    w: float
    ts_ms: int


@dataclass
class CtxGateTightenCalibrator:
    """Adaptive cap calibrator for a single context gate (sentiment or defillama)."""

    gate: str
    default_cap_bps: float

    target_ev_r: float = 0.08
    hysteresis: float = 0.02
    margin_up: float = 0.05
    step_bps: float = 0.25
    min_cap_bps: float = 0.25
    max_cap_bps: float = 12.0
    max_jump_bps: float = 1.0
    window_ms: int = 14 * 24 * 60 * 60 * 1000  # 14 days
    min_tightened: int = 50
    hold_ms: int = 24 * 60 * 60 * 1000  # 24 h between commits

    enforce: bool = False

    _cap_bps: float = field(init=False)
    _shadow_cap_bps: float = field(init=False)
    _buf: deque = field(init=False)
    _last_commit_ms: int = field(init=False, default=0)
    _ev_tightened: float = field(init=False, default=float("nan"))
    _ev_baseline: float = field(init=False, default=float("nan"))
    _n_tightened: int = field(init=False, default=0)
    _n_baseline: int = field(init=False, default=0)

    def __post_init__(self) -> None:
        self._cap_bps = self.default_cap_bps
        self._shadow_cap_bps = self.default_cap_bps
        self._buf: deque[_Sample] = deque()
        self._last_commit_ms = 0

    # ------------------------------------------------------------------ #

    def observe(self, *, r: float, tighten_bps: float, w: float = 1.0, ts_ms: int) -> None:
        if not math.isfinite(r) or not math.isfinite(tighten_bps):
            return
        w = max(0.0, min(1.0, float(w) if math.isfinite(w) else 1.0))
        if w <= 0.0:
            return
        self._buf.append(_Sample(r=float(r), tighten_bps=float(tighten_bps), w=w, ts_ms=int(ts_ms)))

    def recompute(self, now_ms: int | None = None) -> None:
        now_ms = now_ms or _now_ms()
        cutoff = now_ms - self.window_ms
        while self._buf and self._buf[0].ts_ms < cutoff:
            self._buf.popleft()

        tightened_rw: list[tuple[float, float]] = [
            (s.r, s.w) for s in self._buf if s.tighten_bps > 0.0
        ]
        baseline_rw: list[tuple[float, float]] = [
            (s.r, s.w) for s in self._buf if s.tighten_bps <= 0.0
        ]

        n_t = int(effective_n(tightened_rw))
        n_b = int(effective_n(baseline_rw))
        self._n_tightened = n_t
        self._n_baseline = n_b

        ev_t = weighted_mean(tightened_rw) if tightened_rw else float("nan")
        ev_b = weighted_mean(baseline_rw) if baseline_rw else float("nan")
        self._ev_tightened = ev_t
        self._ev_baseline = ev_b

        if n_t < self.min_tightened:
            return  # not enough tightened samples yet

        shadow = self._shadow_cap_bps

        if math.isfinite(ev_t) and ev_t < self.target_ev_r - self.hysteresis:
            # tightened-pass trades underperform → need higher cap (more filtering)
            shadow = min(self.max_cap_bps, shadow + self.step_bps)
        elif math.isfinite(ev_t) and ev_t > self.target_ev_r + self.margin_up:
            # well above target → cautiously lower cap
            shadow = max(self.min_cap_bps, shadow - self.step_bps * 0.5)

        shadow = round(shadow, 4)
        self._shadow_cap_bps = shadow

        delta = abs(shadow - self._cap_bps)
        elapsed = now_ms - self._last_commit_ms
        if self.enforce and delta >= self.hysteresis * 0.5 and elapsed >= self.hold_ms:
            jump = min(self.max_jump_bps, delta)
            direction = 1.0 if shadow > self._cap_bps else -1.0
            self._cap_bps = round(self._cap_bps + direction * jump, 4)
            self._last_commit_ms = now_ms

    # ------------------------------------------------------------------ #

    @property
    def cap_bps(self) -> float:
        return self._cap_bps

    @property
    def shadow_cap_bps(self) -> float:
        return self._shadow_cap_bps

    def snapshot(self) -> dict[str, Any]:
        return {
            "cap_bps": round(self._cap_bps, 4),
            "shadow_cap_bps": round(self._shadow_cap_bps, 4),
            "ev_tightened": None if math.isnan(self._ev_tightened) else round(self._ev_tightened, 6),
            "ev_baseline": None if math.isnan(self._ev_baseline) else round(self._ev_baseline, 6),
            "n_tightened": self._n_tightened,
            "n_baseline": self._n_baseline,
            "last_commit_ms": self._last_commit_ms,
        }

    def loads_gate_state(self, data: dict[str, Any]) -> None:
        """Restore committed cap from a snapshot dict (gate sub-key)."""
        try:
            cap = float(data.get("cap_bps", self.default_cap_bps) or self.default_cap_bps)
            shadow = float(data.get("shadow_cap_bps", cap) or cap)
            if math.isfinite(cap) and self.min_cap_bps <= cap <= self.max_cap_bps:
                self._cap_bps = cap
            if math.isfinite(shadow) and self.min_cap_bps <= shadow <= self.max_cap_bps:
                self._shadow_cap_bps = shadow
            self._last_commit_ms = int(data.get("last_commit_ms", 0) or 0)
        except Exception:
            pass


@dataclass
class SentimentDefiLlamaCtxCalibrator:
    """Umbrella calibrator holding both sub-calibrators."""

    enforce: bool = False

    sentiment: CtxGateTightenCalibrator = field(init=False)
    defillama: CtxGateTightenCalibrator = field(init=False)

    def __post_init__(self) -> None:
        self.sentiment = CtxGateTightenCalibrator(
            gate="sentiment",
            default_cap_bps=DEFAULT_SENTIMENT_CAP,
            enforce=self.enforce,
        )
        self.defillama = CtxGateTightenCalibrator(
            gate="defillama",
            default_cap_bps=DEFAULT_DEFILLAMA_CAP,
            enforce=self.enforce,
        )

    def observe(
        self,
        *,
        r: float,
        sentiment_tighten_bps: float,
        defillama_tighten_bps: float,
        w: float = 1.0,
        ts_ms: int,
    ) -> None:
        self.sentiment.observe(r=r, tighten_bps=sentiment_tighten_bps, w=w, ts_ms=ts_ms)
        self.defillama.observe(r=r, tighten_bps=defillama_tighten_bps, w=w, ts_ms=ts_ms)

    def recompute(self, now_ms: int | None = None) -> None:
        now_ms = now_ms or _now_ms()
        self.sentiment.recompute(now_ms)
        self.defillama.recompute(now_ms)

    def snapshot(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "ts_ms": _now_ms(),
            "sentiment": self.sentiment.snapshot(),
            "defillama": self.defillama.snapshot(),
        }

    @classmethod
    def loads(cls, data: dict[str, Any], enforce: bool = False) -> "SentimentDefiLlamaCtxCalibrator":
        obj = cls(enforce=enforce)
        if isinstance(data, dict):
            sv = int(data.get("schema_version", 1) or 1)
            if sv >= 1:
                s = data.get("sentiment") or {}
                d = data.get("defillama") or {}
                if isinstance(s, dict):
                    obj.sentiment.loads_gate_state(s)
                if isinstance(d, dict):
                    obj.defillama.loads_gate_state(d)
        return obj
