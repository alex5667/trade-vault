from __future__ import annotations

from collections import deque
from typing import Deque, Optional, TypeVar

T = TypeVar("T")


def ensure_bounded_deque(d: Optional[Deque[T]], maxlen: int) -> Deque[T]:
    """
    Ensure deque has a fixed maxlen.

    - If d is None: create bounded deque(maxlen=maxlen)
    - If d.maxlen differs: re-wrap preserving tail items
    - If maxlen <= 0: clamp to 1
    """
    ml = int(maxlen)
    if ml <= 0:
        ml = 1
    if d is None:
        return deque(maxlen=ml)
    cur = getattr(d, "maxlen", None)
    if cur != ml:
        return deque(d, maxlen=ml)
    return d
