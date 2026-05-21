from __future__ import annotations

"""slippage_cal_store.py

Calibrated slippage reader for CostEdgeGate.

Two classes:

  SlippageCalStore — immutable snapshot (tests / one-shot loads).
  SlippageCalReader — lazy TTL-cached reader (live operation).

Redis key: slippage_bps_cal:v1  (shadow: slippage_bps_cal:v1:shadow)
Payload:   {"groups": {"BTCUSDT:US_MAIN": {"new_bps": 2.3}, ...}, "calibrated_ms": ...}

Fallback hierarchy:
  1. (symbol, session)  →  "BTCUSDT:US_MAIN"
  2. (symbol, "*")      →  "BTCUSDT:*"
  3. ("*", "*")         →  "*:*"
  4. caller default
"""

import json
import logging
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_CAL_KEY = "slippage_bps_cal:v1"
_DEFAULT_SLIP_FALLBACK = 4.0


# ---------------------------------------------------------------------------
# Parse helpers
# ---------------------------------------------------------------------------

def _parse_slip_map(raw: Any) -> tuple[dict[str, float], int]:
    """Parse calibration blob → (slip_map, calibrated_ms).

    slip_map keys are normalised to uppercase: "BTCUSDT:US_MAIN".
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
        slip_map: dict[str, float] = {}
        for key_str, entry in groups.items():
            if isinstance(entry, dict):
                v = entry.get("new_bps") or entry.get("q75") or entry.get("bps")
            else:
                v = entry
            if v is None:
                continue
            try:
                bps = float(v)
                if bps > 0:
                    slip_map[str(key_str).upper()] = bps
            except (TypeError, ValueError):
                continue
        cal_ms = int(obj.get("calibrated_ms", 0) or int(time.time() * 1000))
        return slip_map, cal_ms
    except Exception:
        return {}, 0


def _lookup(slip_map: dict[str, float], symbol: str, session: str | None, default: float) -> float:
    """Hierarchical lookup: (sym, sess) → (sym, *) → (*, *) → default."""
    if not slip_map:
        return default
    sym = (symbol or "").upper().strip()
    sess = (session or "").upper().strip() or "NA"
    v = slip_map.get(f"{sym}:{sess}")
    if v and v > 0:
        return v
    v = slip_map.get(f"{sym}:*")
    if v and v > 0:
        return v
    v = slip_map.get("*:*")
    if v and v > 0:
        return v
    return default


# ---------------------------------------------------------------------------
# SlippageCalStore — immutable snapshot
# ---------------------------------------------------------------------------

class SlippageCalStore:
    """Immutable calibrated slippage snapshot.

    Use SlippageCalReader for live operation; SlippageCalStore for tests.
    """

    def __init__(self, slip_map: dict[str, float], loaded_ms: int = 0) -> None:
        self._slip_map = slip_map
        self._loaded_ms = loaded_ms

    @classmethod
    def load(cls, redis_client: Any, key: str = DEFAULT_CAL_KEY) -> "SlippageCalStore":
        """One-shot load from Redis. Returns empty store on error."""
        try:
            raw = redis_client.get(key)
            if not raw:
                return cls.empty()
            slip_map, cal_ms = _parse_slip_map(raw)
            return cls(slip_map, cal_ms)
        except Exception:
            return cls.empty()

    @classmethod
    def empty(cls) -> "SlippageCalStore":
        return cls({}, 0)

    @classmethod
    def from_dict(cls, groups: dict[str, float]) -> "SlippageCalStore":
        """Build from plain dict for tests. Keys: 'BTCUSDT:us_main' → bps."""
        slip_map = {str(k).upper(): float(v) for k, v in groups.items() if v and float(v) > 0}
        return cls(slip_map, int(time.time() * 1000))

    def get_slippage(self, symbol: str, session: str | None,
                     default: float = _DEFAULT_SLIP_FALLBACK) -> float:
        return _lookup(self._slip_map, symbol, session, default)

    @property
    def is_loaded(self) -> bool:
        return bool(self._slip_map)

    @property
    def n_keys(self) -> int:
        return len(self._slip_map)

    @property
    def age_ms(self) -> int:
        if self._loaded_ms <= 0:
            return 0
        return max(0, int(time.time() * 1000) - self._loaded_ms)

    def __repr__(self) -> str:
        return f"SlippageCalStore(loaded={self.is_loaded}, n={self.n_keys}, age_ms={self.age_ms})"


# ---------------------------------------------------------------------------
# SlippageCalReader — lazy TTL-cached reader
# ---------------------------------------------------------------------------

class SlippageCalReader:
    """Thread-safe, TTL-cached reader for calibrated slippage values.

    Wire into CostEdgeGate once at startup:

        gate.set_slippage_store(SlippageCalReader(redis))

    Fail-open: Redis errors keep the previous snapshot alive until stale_ms,
    then fall back to caller default.
    """

    def __init__(
        self,
        redis_client: Any,
        *,
        key: str = DEFAULT_CAL_KEY,
        refresh_ms: int = 60_000,         # re-read Redis every 60 s
        stale_ms: int = 4 * 3600 * 1000,  # treat as gone after 4 h without success
    ) -> None:
        self._redis = redis_client
        self._key = key
        self._refresh_ms = max(1_000, refresh_ms)
        self._stale_ms = max(self._refresh_ms, stale_ms)

        self._lock = threading.Lock()
        self._slip_map: dict[str, float] = {}
        self._last_refresh_ms: int = 0
        self._last_load_ok_ms: int = 0

    def get_slippage(self, symbol: str, session: str | None,
                     default: float = _DEFAULT_SLIP_FALLBACK) -> float:
        """Return calibrated slippage bps with automatic lazy refresh."""
        self._maybe_refresh(int(time.time() * 1000))
        if self._is_stale():
            return default
        return _lookup(self._slip_map, symbol, session, default)

    def force_refresh(self) -> bool:
        return self._refresh(int(time.time() * 1000), force=True)

    @property
    def is_loaded(self) -> bool:
        return bool(self._slip_map)

    @property
    def is_healthy(self) -> bool:
        return bool(self._slip_map) and not self._is_stale()

    @property
    def age_ms(self) -> int:
        if self._last_load_ok_ms <= 0:
            return 0
        return max(0, int(time.time() * 1000) - self._last_load_ok_ms)

    @property
    def n_keys(self) -> int:
        return len(self._slip_map)

    def __repr__(self) -> str:
        return (
            f"SlippageCalReader(healthy={self.is_healthy}, n={self.n_keys}, "
            f"age_ms={self.age_ms})"
        )

    def _is_stale(self) -> bool:
        if self._last_load_ok_ms <= 0:
            return True
        return (int(time.time() * 1000) - self._last_load_ok_ms) > self._stale_ms

    def _maybe_refresh(self, now_ms: int) -> None:
        if (now_ms - self._last_refresh_ms) < self._refresh_ms:
            return
        self._refresh(now_ms, force=False)

    def _refresh(self, now_ms: int, *, force: bool) -> bool:
        with self._lock:
            if not force and (now_ms - self._last_refresh_ms) < self._refresh_ms:
                return bool(self._slip_map)
            self._last_refresh_ms = now_ms
            try:
                raw = self._redis.get(self._key)
            except Exception as exc:
                logger.warning("SlippageCalReader: redis GET failed: %s", exc)
                return bool(self._slip_map)
            if raw is None:
                logger.debug("SlippageCalReader: key %s not found", self._key)
                return bool(self._slip_map)
            slip_map, _ = _parse_slip_map(raw)
            if not slip_map:
                logger.warning("SlippageCalReader: empty/invalid payload at %s", self._key)
                return bool(self._slip_map)
            self._slip_map = slip_map
            self._last_load_ok_ms = now_ms
            logger.debug("SlippageCalReader: refreshed %d keys", len(slip_map))
            return True
