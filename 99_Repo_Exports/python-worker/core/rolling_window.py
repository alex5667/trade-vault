from __future__ import annotations
"""Deterministic rolling windows by event time (ts_ms)."""

from dataclasses import dataclass
from collections import deque
from typing import Deque, Generic, Iterable, Iterator, Optional, Tuple, TypeVar

T = TypeVar("T")


@dataclass
class RollingWindow(Generic[T]):
    horizon_ms: int
    maxlen: int = 512
    _dq: Deque[Tuple[int, T]] = None  # type: ignore
    last_ts_ms: int = 0
    bad_time_total: int = 0

    def __post_init__(self) -> None:
        self.horizon_ms = int(self.horizon_ms or 0)
        self.maxlen = int(self.maxlen or 0)
        if self.maxlen <= 0:
            self.maxlen = 512
        self._dq = deque(maxlen=self.maxlen)

    def apply_config(self, *, horizon_ms: int, maxlen: int) -> None:
        horizon_ms = int(horizon_ms or 0)
        maxlen = int(maxlen or 0)
        if maxlen <= 0:
            maxlen = 512
        self.horizon_ms = horizon_ms
        if maxlen != self.maxlen:
            self.maxlen = maxlen
            old = list(self._dq)
            self._dq = deque(old[-maxlen:], maxlen=maxlen)
        if self.last_ts_ms > 0:
            self.evict(self.last_ts_ms)

    def evict(self, ts_ms: int) -> None:
        ts_ms = int(ts_ms or 0)
        if ts_ms <= 0 or self.horizon_ms <= 0:
            return
        cutoff = ts_ms - int(self.horizon_ms)
        while self._dq and int(self._dq[0][0]) < cutoff:
            self._dq.popleft()

    def push(self, ts_ms: int, value: T) -> bool:
        ts_ms = int(ts_ms or 0)
        if ts_ms <= 0:
            self.bad_time_total += 1
            return False
        if self.last_ts_ms and ts_ms < self.last_ts_ms:
            # Out-of-order ts_ms: reject (fail-open, value stays at last snapshot)
            self.bad_time_total += 1
            return False
        self.last_ts_ms = ts_ms
        self._dq.append((ts_ms, value))
        self.evict(ts_ms)
        return True

    def __len__(self) -> int:
        return len(self._dq)

    def items(self) -> Iterable[Tuple[int, T]]:
        return list(self._dq)

    def first(self) -> Optional[Tuple[int, T]]:
        return self._dq[0] if self._dq else None

    def last(self) -> Optional[Tuple[int, T]]:
        return self._dq[-1] if self._dq else None

    def __iter__(self) -> Iterator[Tuple[int, T]]:
        return iter(self._dq)


@dataclass
class WeightedRollingWindow:
    horizon_ms: int
    maxlen: int = 512
    _dq: Deque[Tuple[int, float, float]] = None  # type: ignore
    last_ts_ms: int = 0
    bad_time_total: int = 0

    def __post_init__(self) -> None:
        self.horizon_ms = int(self.horizon_ms or 0)
        self.maxlen = int(self.maxlen or 0)
        if self.maxlen <= 0:
            self.maxlen = 512
        self._dq = deque(maxlen=self.maxlen)

    def apply_config(self, *, horizon_ms: int, maxlen: int) -> None:
        horizon_ms = int(horizon_ms or 0)
        maxlen = int(maxlen or 0)
        if maxlen <= 0:
            maxlen = 512
        self.horizon_ms = horizon_ms
        if maxlen != self.maxlen:
            self.maxlen = maxlen
            old = list(self._dq)
            self._dq = deque(old[-maxlen:], maxlen=maxlen)
        if self.last_ts_ms > 0:
            self.evict(self.last_ts_ms)

    def evict(self, ts_ms: int) -> None:
        ts_ms = int(ts_ms or 0)
        if ts_ms <= 0 or self.horizon_ms <= 0:
            return
        cutoff = ts_ms - int(self.horizon_ms)
        while self._dq and int(self._dq[0][0]) < cutoff:
            self._dq.popleft()

    def push(self, ts_ms: int, value: float, weight: float) -> bool:
        ts_ms = int(ts_ms or 0)
        if ts_ms <= 0:
            self.bad_time_total += 1
            return False
        if self.last_ts_ms and ts_ms < self.last_ts_ms:
            # Out-of-order ts_ms: reject (fail-open, value stays at last snapshot)
            self.bad_time_total += 1
            return False
        self.last_ts_ms = ts_ms
        self._dq.append((ts_ms, float(value), float(weight)))
        self.evict(ts_ms)
        return True

    def __len__(self) -> int:
        return len(self._dq)

    def items(self) -> Iterable[Tuple[int, float, float]]:
        return list(self._dq)

    def __iter__(self) -> Iterator[Tuple[int, float, float]]:
        return iter(self._dq)
