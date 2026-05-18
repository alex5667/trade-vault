from __future__ import annotations

"""cost_k_store.py

Calibrated K-multiplier store for Cost Edge Gate.

Two classes:

  CostKStore — immutable snapshot (load-once dict).
    Used in tests and one-shot loads.

  CostKReader — lazy TTL-cached reader with background-safe single-flight refresh.
    Wire this into EdgeCostGate.set_k_store() for live operation:

        gate.set_k_store(CostKReader(redis))

    Every call to get_k() triggers _maybe_refresh() which re-reads Redis when
    refresh_ms has elapsed since the last attempt. The lock prevents concurrent
    stampedes from multiple greenlets/threads.

    Fail-open: Redis errors keep the previous snapshot alive until stale_ms,
    then revert to the caller-supplied default.

Redis key: cfg:cost_edge_gate:v1:calibration
Payload:   {"groups": {"BTCUSDT:TREND": {"K_new": 3.8}, ...}, "calibrated_ms": ...}

Fallback hierarchy (both classes):
  1. (symbol, regime)  →  "BTCUSDT:TREND"
  2. (symbol, "*")     →  "BTCUSDT:*"
  3. ("*", "*")        →  "*:*"
  4. default argument  →  4.0
"""

import json
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_CAL_KEY = "cfg:cost_edge_gate:v1:calibration"
_DEFAULT_K_FALLBACK = 4.0


# ---------------------------------------------------------------------------
# Internal helper — parse Redis payload → k_map dict
# ---------------------------------------------------------------------------

def _parse_k_map(raw: Any) -> tuple[dict[str, float], int]:
    """Parse calibration payload → (k_map, calibrated_ms).

    Tolerant to missing / malformed fields.
    Returns ({}, 0) on any parse failure.
    """
    try:
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        obj: Any = json.loads(str(raw))
        if not isinstance(obj, dict):
            return {}, 0
        groups: Any = obj.get("groups")
        if not isinstance(groups, dict):
            return {}, 0
        k_map: dict[str, float] = {}
        for key_str, entry in groups.items():
            v = entry.get("K_new") or entry.get("K_fit") or entry.get("K_p50") if isinstance(entry, dict) else entry
            if v is None:
                continue
            try:
                k_val = float(v)
                if k_val > 0:
                    k_map[str(key_str).upper()] = k_val
            except (TypeError, ValueError):
                continue
        cal_ms = int(obj.get("calibrated_ms", 0) or int(time.time() * 1000))
        return k_map, cal_ms
    except Exception:
        return {}, 0


def _lookup(k_map: dict[str, float], symbol: str, regime: str | None, default: float) -> float:
    """Hierarchical lookup shared by both classes."""
    if not k_map:
        return default
    sym = (symbol or "").upper().strip()
    reg = (regime or "").upper().strip() or "NORMAL"
    v = k_map.get(f"{sym}:{reg}")
    if v and v > 0:
        return v
    v = k_map.get(f"{sym}:*")
    if v and v > 0:
        return v
    v = k_map.get("*:*")
    if v and v > 0:
        return v
    return default


# ---------------------------------------------------------------------------
# CostKStore — immutable snapshot
# ---------------------------------------------------------------------------

@dataclass
class CostKStore:
    """Immutable calibrated-K snapshot.

    Use CostKReader for live operation; use CostKStore for tests / one-shot loads.
    """

    _k_map: dict[str, float] = field(default_factory=dict)
    _loaded_ms: int = 0
    _schema_version: int = 0

    @classmethod
    def load(cls, redis_client: Any, key: str = DEFAULT_CAL_KEY) -> "CostKStore":
        """One-shot load from Redis. Returns empty store on any error."""
        try:
            raw = redis_client.get(key)
            if not raw:
                return cls.empty()
            k_map, cal_ms = _parse_k_map(raw)
            return cls(_k_map=k_map, _loaded_ms=cal_ms, _schema_version=1)
        except Exception:
            return cls.empty()

    @classmethod
    def empty(cls) -> "CostKStore":
        return cls(_k_map={}, _loaded_ms=0, _schema_version=0)

    @classmethod
    def from_dict(cls, groups: dict[str, Any]) -> "CostKStore":
        """Build from plain dict — for tests."""
        k_map = {str(k).upper(): float(v) for k, v in groups.items() if v is not None}
        return cls(_k_map=k_map, _loaded_ms=int(time.time() * 1000), _schema_version=1)

    def get_k(self, symbol: str, regime: str | None, default: float = _DEFAULT_K_FALLBACK) -> float:
        return _lookup(self._k_map, symbol, regime, default)

    @property
    def is_loaded(self) -> bool:
        return bool(self._k_map)

    @property
    def age_ms(self) -> int:
        if self._loaded_ms <= 0:
            return 0
        return max(0, int(time.time() * 1000) - self._loaded_ms)

    @property
    def n_keys(self) -> int:
        return len(self._k_map)

    def __repr__(self) -> str:
        return f"CostKStore(loaded={self.is_loaded}, n_keys={self.n_keys}, age_ms={self.age_ms})"


# ---------------------------------------------------------------------------
# CostKReader — lazy TTL-cached reader with single-flight refresh
# ---------------------------------------------------------------------------

class CostKReader:
    """Thread-safe, TTL-cached reader for calibrated K values.

    Wire into EdgeCostGate once at startup:

        gate.set_k_store(CostKReader(redis))

    Every get_k() call checks whether refresh_ms has elapsed and, if so, does
    a single Redis GET under a lock. All other concurrent callers skip the
    refresh and get the cached value — no stampede.

    Fail-open contract:
      - Redis error → keep last snapshot, log warning
      - Last snapshot older than stale_ms → revert to caller default
      - No snapshot ever loaded → return caller default
    """

    def __init__(
        self,
        redis_client: Any,
        *,
        key: str = DEFAULT_CAL_KEY,
        refresh_ms: int = 60_000,        # re-read Redis every 60 s
        stale_ms: int = 4 * 3600 * 1000, # treat snapshot as gone after 4 h without successful load
    ) -> None:
        self._redis = redis_client
        self._key = key
        self._refresh_ms = max(1_000, refresh_ms)
        self._stale_ms = max(self._refresh_ms, stale_ms)

        self._lock = threading.Lock()
        self._k_map: dict[str, float] = {}
        self._last_refresh_ms: int = 0   # last refresh *attempt* (even if failed)
        self._last_load_ok_ms: int = 0   # last successful Redis load

    # ------------------------------------------------------------------
    # Public API — same surface as CostKStore
    # ------------------------------------------------------------------

    def get_k(self, symbol: str, regime: str | None, default: float = _DEFAULT_K_FALLBACK) -> float:
        """Return calibrated K with automatic lazy refresh.

        Falls back to `default` when snapshot is absent or stale.
        """
        self._maybe_refresh(int(time.time() * 1000))
        if self._is_stale():
            return default
        return _lookup(self._k_map, symbol, regime, default)

    def force_refresh(self) -> bool:
        """Synchronous refresh — for tests / admin tooling. Returns True on success."""
        return self._refresh(int(time.time() * 1000), force=True)

    @property
    def is_loaded(self) -> bool:
        return bool(self._k_map)

    @property
    def is_healthy(self) -> bool:
        return bool(self._k_map) and not self._is_stale()

    @property
    def age_ms(self) -> int:
        """Age of the last successful load in ms. 0 if never loaded."""
        if self._last_load_ok_ms <= 0:
            return 0
        return max(0, int(time.time() * 1000) - self._last_load_ok_ms)

    @property
    def n_keys(self) -> int:
        return len(self._k_map)

    def __repr__(self) -> str:
        return (
            f"CostKReader(healthy={self.is_healthy}, n_keys={self.n_keys}, "
            f"age_ms={self.age_ms}, refresh_ms={self._refresh_ms})"
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _is_stale(self) -> bool:
        if self._last_load_ok_ms <= 0:
            return True
        return (int(time.time() * 1000) - self._last_load_ok_ms) > self._stale_ms

    def _maybe_refresh(self, now_ms: int) -> None:
        if (now_ms - self._last_refresh_ms) < self._refresh_ms:
            return
        self._refresh(now_ms, force=False)

    def _refresh(self, now_ms: int, *, force: bool) -> bool:
        """Single-flight refresh under lock. Returns True if snapshot loaded OK."""
        with self._lock:
            # Re-check after acquiring lock — another thread may have just refreshed.
            if not force and (now_ms - self._last_refresh_ms) < self._refresh_ms:
                return bool(self._k_map)

            self._last_refresh_ms = now_ms

            try:
                raw = self._redis.get(self._key)
            except Exception as exc:
                logger.warning("CostKReader: redis GET failed: %s", exc)
                return bool(self._k_map)  # keep previous snapshot

            if raw is None:
                logger.debug("CostKReader: key %s not found in Redis", self._key)
                return bool(self._k_map)

            k_map, _ = _parse_k_map(raw)
            if not k_map:
                logger.warning("CostKReader: empty/invalid payload at %s", self._key)
                return bool(self._k_map)

            self._k_map = k_map
            self._last_load_ok_ms = now_ms
            logger.debug("CostKReader: refreshed %d keys from %s", len(k_map), self._key)
            return True
