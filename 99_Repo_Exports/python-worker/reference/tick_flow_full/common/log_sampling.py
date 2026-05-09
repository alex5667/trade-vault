from __future__ import annotations

from dataclasses import dataclass

from utils.time_utils import get_ny_time_millis


def _now_ms() -> int:
    return get_ny_time_millis()


@dataclass(slots=True)
class TimeSampler:
    """
    Hot-path friendly time-based sampler.

    Policy:
      - maybe(now_ms) -> True once per `every_ms` interval.
      - force() -> next maybe() returns True immediately.

    Usage:
      if self._candidate_log_sampler.maybe(now_ms) or force_reason:
          log(...)
    """
    every_ms: int
    _next_ms: int = 0
    _force: bool = False

    def __post_init__(self) -> None:
        # Avoid zero/negative intervals (fall back to "never" unless forced).
        if self.every_ms <= 0:
            self.every_ms = 0
        self._next_ms = 0

    def force(self) -> None:
        self._force = True

    def maybe(self, now_ms: int | None = None) -> bool:
        n = int(now_ms) if now_ms is not None else _now_ms()
        if self._force:
            self._force = False
            self._next_ms = n + self.every_ms if self.every_ms > 0 else n
            return True
        if self.every_ms <= 0:
            return False
        if self._next_ms == 0:
            self._next_ms = n + self.every_ms
            return True
        if n >= self._next_ms:
            self._next_ms = n + self.every_ms
            return True
        return False
