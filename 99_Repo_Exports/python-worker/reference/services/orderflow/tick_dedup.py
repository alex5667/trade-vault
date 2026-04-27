from __future__ import annotations

import hashlib
from collections import deque
from typing import Any, Deque, Dict, Set, Tuple


def tick_uid(tick: Dict[str, Any]) -> str:
    """Stable-ish uid for a tick (dedup/logging).

    Preference order:
      1) trade_id (if present and non-zero)
      2) hash(ts_ms, price, qty, side, is_buyer_maker)
    """
    try:
        tid = tick.get("trade_id", None)
        if tid is not None:
            try:
                tid_i = int(tid)
            except Exception:
                tid_i = 0
            if tid_i > 0:
                return f"tid:{tid_i}"
    except Exception:
        pass

    ts_ms = int(tick.get("ts_ms") or 0)
    price = float(tick.get("price") or 0.0)
    qty = float(tick.get("qty") or 0.0)
    side = str(tick.get("side") or "").upper()
    bm = 1 if bool(tick.get("is_buyer_maker")) else 0
    base = f"{ts_ms}|{price:.8f}|{qty:.8f}|{side}|{bm}"
    h = hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]
    return f"h:{h}"


class TickDeduper:
    """Bounded in-memory deduper with TTL eviction (uses insertion time)."""

    def __init__(self, *, max_items: int = 20000, max_age_ms: int = 180_000) -> None:
        self.max_items = int(max_items)
        self.max_age_ms = int(max_age_ms)
        self._q: Deque[Tuple[int, str]] = deque()
        self._s: Set[str] = set()

    def _evict(self, now_ms: int) -> None:
        try:
            while self._q:
                ts0, k0 = self._q[0]
                if (now_ms - ts0) <= self.max_age_ms:
                    break
                self._q.popleft()
                self._s.discard(k0)
        except Exception:
            pass
        try:
            while self.max_items > 0 and len(self._q) > self.max_items:
                ts0, k0 = self._q.popleft()
                self._s.discard(k0)
        except Exception:
            pass

    def seen(self, key: str, now_ms: int) -> bool:
        """True if duplicate, else record and return False."""
        if not key:
            return False
        try:
            now_i = int(now_ms)
        except Exception:
            now_i = 0
        self._evict(now_i)
        if key in self._s:
            return True
        self._s.add(key)
        self._q.append((now_i, key))
        self._evict(now_i)
        return False







