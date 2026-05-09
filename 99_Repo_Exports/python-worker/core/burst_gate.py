from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class BurstCandidate:
    ts_ms: int
    score: float
    payload: dict[str, Any]


@dataclass
class BurstState:
    active: bool = False
    start_ts_ms: int = 0
    deadline_ts_ms: int = 0
    best: BurstCandidate | None = None


class BurstCandidateSelector:
    """
    Pre-cooldown burst selection:
    - On first eligible candidate, start burst (do not emit yet).
    - During burst window, keep the best candidate by score.
    - Emit best when deadline is reached (on the first tick >= deadline).

    Deterministic time: tick ts only.
    """

    def __init__(self, *, window_ms: int = 2500, max_age_ms: int = 8000) -> None:
        self.window_ms = int(window_ms)
        self.max_age_ms = int(max_age_ms)
        self.st = BurstState()

    def is_active(self) -> bool:
        return bool(self.st.active)

    def snapshot(self) -> dict[str, Any]:
        return {
            "active": int(self.st.active),
            "start_ts_ms": int(self.st.start_ts_ms),
            "deadline_ts_ms": int(self.st.deadline_ts_ms),
            "best_score": float(self.st.best.score) if self.st.best else 0.0,
        }

    def reset(self) -> None:
        self.st = BurstState()

    def start(self, *, ts_ms: int, cand: BurstCandidate) -> None:
        self.st.active = True
        self.st.start_ts_ms = int(ts_ms)
        self.st.deadline_ts_ms = int(ts_ms) + int(self.window_ms)
        self.st.best = cand

    def consider(self, *, ts_ms: int, cand: BurstCandidate) -> None:
        if not self.st.active:
            self.start(ts_ms=ts_ms, cand=cand)
            return
        # too old burst => reset to new
        if ts_ms - self.st.start_ts_ms > self.max_age_ms:
            self.start(ts_ms=ts_ms, cand=cand)
            return
        # update best
        if self.st.best is None or float(cand.score) > float(self.st.best.score):
            self.st.best = cand

    def maybe_flush(self, *, now_ts_ms: int) -> dict[str, Any] | None:
        """
        If burst deadline reached (or safety max_age hit), emit best and reset.
        """
        st = self.st
        if not st.active:
            return None
        if now_ts_ms <= 0:
            return None

        # [EXPERT] safety: max_age flush/reset to avoid stuck bursts
        if self.max_age_ms > 0 and (now_ts_ms - int(st.start_ts_ms)) >= int(self.max_age_ms):
            best = st.best
            start_ts = int(st.start_ts_ms)
            deadline_ts = int(st.deadline_ts_ms)
            self.reset()
            if best is None:
                return None
            # if candidate itself is too old — drop (stale signal)
            if (now_ts_ms - int(best.ts_ms)) > int(self.max_age_ms):
                return None
            out = dict(best.payload)
            out["burst_emitted_at"] = int(now_ts_ms)
            out["burst_start_ts_ms"] = start_ts
            out["burst_deadline_ts_ms"] = deadline_ts
            out["burst_best_score"] = float(best.score)
            out["burst_max_age_flushed"] = 1
            return out

        if now_ts_ms < int(st.deadline_ts_ms):
            return None

        # Standard deadline reached
        if st.best:
            out = dict(st.best.payload)
            # keep candidate timestamp, but add emitted_at for audit/debug
            out["burst_emitted_at"] = int(now_ts_ms)
            out["burst_start_ts_ms"] = int(st.start_ts_ms)
            out["burst_deadline_ts_ms"] = int(st.deadline_ts_ms)
            out["burst_best_score"] = float(st.best.score)
            self.reset()
            return out

        self.reset()
        return None

    def force_flush(self) -> dict[str, Any] | None:
        """Force flush best candidate immediately (used by watchdog/error paths).

        Stamps burst metadata for audit trail consistency with normal deadline
        flushes.  Uses wall-clock ``time.time()`` because force_flush callers
        typically don't pass a tick timestamp.
        """
        st = self.st
        if not st.active:
            return None
        best = st.best
        start_ts = int(st.start_ts_ms)
        deadline_ts = int(st.deadline_ts_ms)
        self.reset()
        if best is None:
            return None
        now_ms = int(_get_ny_time_millis())
        out = dict(best.payload)
        out["burst_emitted_at"] = now_ms
        out["burst_start_ts_ms"] = start_ts
        out["burst_deadline_ts_ms"] = deadline_ts
        out["burst_best_score"] = float(best.score)
        out["burst_force_flushed"] = 1
        return out
