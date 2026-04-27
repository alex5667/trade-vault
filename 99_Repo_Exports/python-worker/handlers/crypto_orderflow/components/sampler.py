from __future__ import annotations


class _SampleEveryMs:
    """
    Ultra-light sampler for hot paths.
    Goal: avoid log spam while still providing observability for candidate flow.

    Usage:
      gate = _SampleEveryMs(every_ms=15000)
      if gate.should_log(now_ms): ... log ...
      if gate.should_log(now_ms, force=True): ... log regardless of interval ...
    """

    __slots__ = ("every_ms", "_last_ms")

    def __init__(self, *, every_ms: int) -> None:
        self.every_ms = max(0, int(every_ms))
        self._last_ms = 0

    def should_log(self, now_ms: int, *, force: bool = False) -> bool:
        if force:
            self._last_ms = int(now_ms)
            return True
        if self.every_ms <= 0:
            return False
        now = int(now_ms)
        if self._last_ms <= 0:
            self._last_ms = now
            return True
        if (now - self._last_ms) >= self.every_ms:
            self._last_ms = now
            return True
        return False
