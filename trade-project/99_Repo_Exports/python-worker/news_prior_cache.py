"""NewsPriorCache — in-memory TTL cache for priors.

Требования (trade):
- O(1) get() в критическом пути, без IO.
- TTL/expiry semantics:
  - Если prior.expires_ms задан → authoritative.
  - Иначе expires_ms = now + NEWS_PRIOR_CACHE_TTL_MS (дефолт 15m).
- Eviction:
  - LRU-ish по времени обновления, capped by max_symbols.

Cache хранит per-symbol prior (dict) + метаданные:
- set_ts_ms: когда обновили
- expires_ms: когда prior считается просроченным

ВАЖНО: gate работает с ctx.news_prior, поэтому внешний код должен сделать
ctx.news_prior = cache.get(symbol) перед запуском pre_publish_gates.
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


def _now_ms() -> int:
    return get_ny_time_millis()


@dataclass
class _Entry:
    prior: Dict[str, Any]
    set_ts_ms: int
    expires_ms: int


class NewsPriorCache:
    def __init__(self, *, default_ttl_ms: Optional[int] = None, max_symbols: Optional[int] = None) -> None:
        self.default_ttl_ms = int(default_ttl_ms or os.getenv("NEWS_PRIOR_CACHE_TTL_MS", "900000"))
        self.max_symbols = int(max_symbols or os.getenv("NEWS_PRIOR_CACHE_MAX_SYMBOLS", "2048"))
        self._m: Dict[str, _Entry] = {}

    def update(self, symbol: str, prior: Dict[str, Any], now_ms: Optional[int] = None) -> None:
        """Upsert prior for symbol."""
        if not symbol:
            return
        now = int(now_ms or _now_ms())
        exp = _safe_int(prior.get("expires_ms"))
        if exp <= 0:
            exp = now + self.default_ttl_ms
            prior = dict(prior)
            prior["expires_ms"] = exp

        self._m[symbol] = _Entry(prior=prior, set_ts_ms=now, expires_ms=exp)

        # Best-effort eviction
        if len(self._m) > self.max_symbols:
            self._evict(now)

    def get(self, symbol: str, now_ms: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Return prior if present and not expired, else None."""
        e = self._m.get(symbol)
        if not e:
            return None
        now = int(now_ms or _now_ms())
        if e.expires_ms and e.expires_ms < now:
            # expired → drop
            self._m.pop(symbol, None)
            return None
        return e.prior

    def get_with_meta(self, symbol: str, now_ms: Optional[int] = None) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
        """Return (prior, meta) where meta includes age/expires/stale."""
        e = self._m.get(symbol)
        now = int(now_ms or _now_ms())
        if not e:
            return None, {"present": False}

        stale = bool(e.expires_ms and e.expires_ms < now)
        age_ms = now - e.set_ts_ms
        meta = {
            "present": True,
            "stale": stale,
            "age_ms": age_ms,
            "expires_ms": e.expires_ms,
        }

        if stale:
            self._m.pop(symbol, None)
            return None, meta

        return e.prior, meta

    def size(self) -> int:
        return len(self._m)

    def _evict(self, now_ms: int) -> None:
        # Remove expired first.
        expired = [k for k, e in self._m.items() if e.expires_ms and e.expires_ms < now_ms]
        for k in expired:
            self._m.pop(k, None)
            if len(self._m) <= self.max_symbols:
                return

        # Then evict oldest set_ts_ms.
        # This is O(n log n) worst-case but only triggers when above max_symbols.
        items = sorted(self._m.items(), key=lambda kv: kv[1].set_ts_ms)
        while len(items) > 0 and len(self._m) > self.max_symbols:
            k, _ = items.pop(0)
            self._m.pop(k, None)


def _safe_int(v: Any) -> int:
    if v is None:
        return 0
    try:
        return int(v)
    except Exception:
        return 0
