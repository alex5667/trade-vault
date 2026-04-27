from __future__ import annotations

from collections import deque

from common.deque_utils import ensure_bounded_deque


def test_ensure_bounded_deque_wraps_unbounded_and_keeps_tail():
    d = deque()  # unbounded
    for i in range(100):
        d.append(i)
    assert d.maxlen is None

    d2 = ensure_bounded_deque(d, 10)
    assert d2.maxlen == 10
    assert list(d2) == list(range(90, 100))


def test_ensure_bounded_deque_noop_when_already_correct():
    d = deque([1, 2, 3], maxlen=5)
    d2 = ensure_bounded_deque(d, 5)
    assert d2 is d
