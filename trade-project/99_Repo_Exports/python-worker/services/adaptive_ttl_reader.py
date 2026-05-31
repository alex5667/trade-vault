"""
adaptive_ttl_reader.py — Consumer-side cache for adaptive_ttl:state.

Used by signal_outcome_snapshot_writer (and any other consumer that
wants to override tp_r / sl_r based on the latest distribution-aware
barrier recommendations).

Design:
  * In-process TTL cache (ADAPTIVE_TTL_READER_TTL_SEC, default 300).
  * Lookup key:  (symbol, regime, side) → BarrierRec dict.
  * Fail-open: returns None if Redis unavailable, snapshot missing, or
    no matching group. Caller must keep ENV defaults as fallback.
  * Activation: caller chooses when to apply; ADAPTIVE_TTL_READ_ENABLED
    env (default 0) acts as a kill-switch (returns None even if cache hot).
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

log = logging.getLogger("adaptive_ttl_reader")

_KEY = "adaptive_ttl:state"
_DEFAULT_TTL_SEC = float(os.getenv("ADAPTIVE_TTL_READER_TTL_SEC", "300"))


class AdaptiveTTLReader:
    def __init__(
        self,
        rc: Any,
        key: str = _KEY,
        ttl_sec: float = _DEFAULT_TTL_SEC,
        enabled_env: str = "ADAPTIVE_TTL_READ_ENABLED",
    ) -> None:
        self._rc = rc
        self._key = key
        self._ttl_sec = ttl_sec
        self._enabled_env = enabled_env
        self._cache: dict[tuple[str, str, int], dict] = {}
        self._loaded_at: float = 0.0

    def _enabled(self) -> bool:
        return os.getenv(self._enabled_env, "0").strip() == "1"

    def _refresh_if_stale(self) -> None:
        now = time.monotonic()
        if now - self._loaded_at < self._ttl_sec and self._cache:
            return
        try:
            raw = self._rc.get(self._key)
        except Exception as e:
            log.debug("adaptive_ttl_reader Redis error: %s", e)
            return
        if not raw:
            self._cache = {}
            self._loaded_at = now
            return
        try:
            payload = json.loads(str(raw))
        except Exception:
            return
        new_cache: dict[tuple[str, str, int], dict] = {}
        for r in payload.get("recs", []) or []:
            try:
                k = (
                    str(r["symbol"]).upper(),
                    str(r.get("regime", "") or "na").lower(),
                    int(r["direction"]),
                )
                new_cache[k] = r
            except Exception:
                continue
        self._cache = new_cache
        self._loaded_at = now

    def lookup(
        self, symbol: str, regime: str, side: int
    ) -> dict | None:
        """Returns rec dict or None if disabled / missing."""
        if not self._enabled():
            return None
        self._refresh_if_stale()
        sym = str(symbol or "").upper()
        rg = str(regime or "na").lower()
        try:
            sd = int(side)
        except Exception:
            return None
        return self._cache.get((sym, rg, sd))

    def size(self) -> int:
        """For diagnostics: cache cardinality."""
        return len(self._cache)
