"""Bad-time quarantine state machine.

This is a lightweight, deterministic state tracker intended to be driven by
TickTimeGuard decisions (hard drops and soft events).

Design principles:
- Fail-open: this module must never throw in hot paths.
- Deterministic: given the same event sequence and timestamps, behavior is stable.
- Simple: state is local/in-memory; persistence is out of scope.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional


IncFn = Callable[[str, int], None]


@dataclass(frozen=True)
class BadTimeQuarantinePolicy:
    # Quarantine triggers
    hard_drop_streak_threshold: int = 3
    score_threshold: float = 3.0

    # Scoring
    hard_drop_score: float = 1.0
    soft_event_score: float = 0.2
    ok_decay: float = 0.1  # reduce score by this per ok tick

    # Durations
    quarantine_ttl_ms: int = 60_000
    state_freeze_ttl_ms: int = 15_000


class BadTimeQuarantine:
    """Tracks "bad time" events and decides whether to quarantine processing."""

    def __init__(self, *, policy: Optional[BadTimeQuarantinePolicy] = None, inc: Optional[IncFn] = None):
        self.policy = policy or BadTimeQuarantinePolicy()
        self._inc = inc

        self.score: float = 0.0
        self.hard_streak: int = 0
        self._quarantine_until_ms: int = 0
        self._freeze_until_ms: int = 0
        self._was_quarantined: bool = False

    def _metric(self, name: str, delta: int = 1) -> None:
        try:
            if self._inc is not None:
                self._inc(name, int(delta))
        except Exception:
            # Fail-open: metrics must not break the pipeline.
            pass

    def on_hard_drop(self, reason: str, now_ms: int) -> None:
        try:
            self.hard_streak += 1
            self.score += float(self.policy.hard_drop_score)
            self._metric(f"tick.time.hard_drop.{reason}")

            if (self.hard_streak >= int(self.policy.hard_drop_streak_threshold)) or (
                self.score >= float(self.policy.score_threshold)
            ):
                # Enable / extend quarantine + short freeze to avoid state corruption.
                prev = self.is_quarantined(now_ms)
                self._quarantine_until_ms = max(int(self._quarantine_until_ms), int(now_ms) + int(self.policy.quarantine_ttl_ms))
                self._freeze_until_ms = max(int(self._freeze_until_ms), int(now_ms) + int(self.policy.state_freeze_ttl_ms))
                if not prev:
                    self._metric("tick.time.quarantine.enabled")
                    self._metric("tick.time.state_freeze.enabled")
        except Exception:
            pass

    def on_soft_event(self, flag: str) -> None:
        try:
            self.score += float(self.policy.soft_event_score)
            self._metric(f"tick.time.soft_event.{flag}")
        except Exception:
            pass

    def on_ok_tick(self) -> None:
        try:
            # Successful tick resets streak and slowly decays score.
            self.hard_streak = 0
            if self.score > 0:
                self.score = max(0.0, float(self.score) - float(self.policy.ok_decay))
        except Exception:
            pass

    def is_quarantined(self, now_ms: int) -> bool:
        try:
            q = int(now_ms) < int(self._quarantine_until_ms)
            # Track recovery transition
            if self._was_quarantined and not q:
                self._metric("tick.time.recovery.passed")
            self._was_quarantined = bool(q)
            return bool(q)
        except Exception:
            return False

    def should_suppress_processing(self, now_ms: int) -> bool:
        try:
            return (int(now_ms) < int(self._freeze_until_ms)) or self.is_quarantined(now_ms)
        except Exception:
            return False

