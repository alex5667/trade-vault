"""
exec_latency_timeout_calibrator.py — adaptive PROTECTION_ARM_TIMEOUT calibrator.

Calibrates PROTECTION_ARM_TIMEOUT_MS from observed execution audit latencies.
Ensures executor and router timeouts are consistently derived from p99 latency.

Algorithm:
  Collect arm_latency_ms samples from execution audit stream.
  committed_executor_ms = clip(p99 × EXECUTOR_MULT, EXEC_MIN, EXEC_MAX)
  committed_router_ms   = clip(p99 × ROUTER_MULT, ROUTER_MIN, ROUTER_MAX)
  (router_mult > executor_mult by design)

Output: autocal:exec_latency_timeout:state
"""
from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

_SCHEMA_VERSION = 1

_EXECUTOR_MULT = 2.0    # executor timeout = p99 × 2.0
_ROUTER_MULT = 4.0      # router timeout = p99 × 4.0 (extra margin for scale-in)
_EXEC_MIN_MS = 1500
_EXEC_MAX_MS = 10_000
_ROUTER_MIN_MS = 3000
_ROUTER_MAX_MS = 20_000
_UPDATE_BAND_MS = 200


@dataclass
class _Sample:
    arm_latency_ms: float
    ts_ms: int


@dataclass
class _Bin:
    buf: deque[_Sample] = field(default_factory=lambda: deque(maxlen=2_000))
    committed_executor_ms: int = 2500
    committed_router_ms: int = 5000
    shadow_executor_ms: int = 2500
    shadow_router_ms: int = 5000
    last_recompute_ms: int = 0
    n_observed: int = 0


class ExecLatencyTimeoutCalibrator:
    """Adaptive protection-arm timeout calibrator from execution audit latencies."""

    def __init__(
        self,
        *,
        enforce: bool = False,
        auto_enforce: bool = True,
        window_days: float = 3.0,
        min_samples: int = 50,
        executor_mult: float = _EXECUTOR_MULT,
        router_mult: float = _ROUTER_MULT,
        recompute_gap_ms: int = 600_000,
    ) -> None:
        self.enforce = enforce
        self.auto_enforce = auto_enforce
        self.window_ms = int(window_days * 86_400_000)
        self.min_samples = min_samples
        self.executor_mult = executor_mult
        self.router_mult = router_mult
        self.recompute_gap_ms = recompute_gap_ms
        self._global = _Bin()

    # ── Ingestion ─────────────────────────────────────────────────────────────

    def observe(self, *, arm_latency_ms: float, ts_ms: int) -> None:
        if not math.isfinite(arm_latency_ms) or arm_latency_ms <= 0:
            return
        now_ms = int(time.time() * 1000)
        self._global.buf.append(_Sample(arm_latency_ms=arm_latency_ms, ts_ms=ts_ms))
        self._global.n_observed += 1
        self._maybe_recompute(now_ms)

    # ── Read ──────────────────────────────────────────────────────────────────

    def get_executor_timeout_ms(self) -> int | None:
        b = self._global
        if not (self.enforce or (self.auto_enforce and b.n_observed >= self.min_samples)):
            return None
        return b.committed_executor_ms

    def get_router_timeout_ms(self) -> int | None:
        b = self._global
        if not (self.enforce or (self.auto_enforce and b.n_observed >= self.min_samples)):
            return None
        return b.committed_router_ms

    # ── Snapshot ──────────────────────────────────────────────────────────────

    def snapshot(self) -> dict[str, Any]:
        b = self._global
        return {
            "schema_version": _SCHEMA_VERSION,
            "ts_ms": int(time.time() * 1000),
            "enforce": self.enforce,
            "auto_enforce": self.auto_enforce,
            "committed_executor_ms": b.committed_executor_ms,
            "committed_router_ms": b.committed_router_ms,
            "shadow_executor_ms": b.shadow_executor_ms,
            "shadow_router_ms": b.shadow_router_ms,
            "n": b.n_observed,
            "n_buf": len(b.buf),
        }

    def load_state(self, state: dict[str, Any]) -> None:
        try:
            self.enforce = bool(state.get("enforce", self.enforce))
            if "auto_enforce" in state:
                self.auto_enforce = bool(state["auto_enforce"])
            b = self._global
            b.committed_executor_ms = int(state.get("committed_executor_ms", 2500))
            b.committed_router_ms = int(state.get("committed_router_ms", 5000))
            b.shadow_executor_ms = int(state.get("shadow_executor_ms", 2500))
            b.shadow_router_ms = int(state.get("shadow_router_ms", 5000))
            b.n_observed = int(state.get("n", 0))
        except Exception:
            pass

    # ── Internal ──────────────────────────────────────────────────────────────

    def _maybe_recompute(self, now_ms: int) -> None:
        b = self._global
        if (now_ms - b.last_recompute_ms) < self.recompute_gap_ms:
            return
        b.last_recompute_ms = now_ms
        self._prune_window(now_ms)
        if len(b.buf) < self.min_samples:
            return
        vals = sorted(s.arm_latency_ms for s in b.buf)
        p99 = _quantile(vals, 0.99)
        new_exec = int(max(_EXEC_MIN_MS, min(_EXEC_MAX_MS, p99 * self.executor_mult)))
        new_router = int(max(_ROUTER_MIN_MS, min(_ROUTER_MAX_MS, p99 * self.router_mult)))
        b.shadow_executor_ms = new_exec
        b.shadow_router_ms = new_router
        if abs(new_exec - b.committed_executor_ms) >= _UPDATE_BAND_MS:
            b.committed_executor_ms = new_exec
        if abs(new_router - b.committed_router_ms) >= _UPDATE_BAND_MS:
            b.committed_router_ms = new_router

    def _prune_window(self, now_ms: int) -> None:
        cutoff = now_ms - self.window_ms
        while self._global.buf and self._global.buf[0].ts_ms < cutoff:
            self._global.buf.popleft()


def _quantile(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = q * (len(sorted_vals) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] * (1 - (idx - lo)) + sorted_vals[hi] * (idx - lo)
