from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field


def _prune(q: deque[int], now_ms: int, window_ms: int) -> None:
    cutoff = now_ms - window_ms
    while q and q[0] < cutoff:
        q.popleft()


@dataclass
class SignalPressureTracker:
    """
    Tracks signal "pressure" in deterministic time (tick ts).

    We track:
      - candidates: events that passed all logic up to cooldown/burst
      - cooldown_veto: candidates blocked by cooldown (for diagnostics)
      - emits: actual emitted signals

    Window is rolling, measured in ms.
    """
    window_ms: int = 60_000
    candidates: deque[int] = field(default_factory=lambda: deque(maxlen=10_000))
    cooldown_veto: deque[int] = field(default_factory=lambda: deque(maxlen=10_000))
    emits: deque[int] = field(default_factory=lambda: deque(maxlen=10_000))

    def record_candidate(self, ts_ms: int) -> None:
        if ts_ms <= 0:
            return
        self.candidates.append(int(ts_ms))

    def record_cooldown_veto(self, ts_ms: int) -> None:
        if ts_ms <= 0:
            return
        self.cooldown_veto.append(int(ts_ms))

    def record_emit(self, ts_ms: int) -> None:
        if ts_ms <= 0:
            return
        self.emits.append(int(ts_ms))

    def snapshot(self, now_ms: int) -> dict:
        w = int(self.window_ms)
        if now_ms <= 0:
            return {"cand_per_min": 0.0, "veto_per_min": 0.0, "emit_per_min": 0.0}
        _prune(self.candidates, now_ms, w)
        _prune(self.cooldown_veto, now_ms, w)
        _prune(self.emits, now_ms, w)
        k = 60_000.0 / float(max(1, w))
        return {
            "cand_per_min": float(len(self.candidates)) * k,
            "veto_per_min": float(len(self.cooldown_veto)) * k,
            "emit_per_min": float(len(self.emits)) * k,
        }

    def is_pressure_hi(self, now_ms: int, hi_per_min: float) -> bool:
        s = self.snapshot(now_ms)
        return float(s.get("cand_per_min", 0.0)) >= float(hi_per_min)
