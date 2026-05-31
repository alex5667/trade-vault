from __future__ import annotations

"""
test_trade_monitor_single_active_guard_v1.py

Unit tests for EXEC_SINGLE_ACTIVE_POSITION_PER_SYMBOL guard in trade_monitor.

Tests the guard logic (_tm_check_single_active_guard) by constructing
a minimal object with the same attributes trade_monitor reads
— avoids heavyweight module import.

Scenarios:
  1. Guard OFF → not blocked
  2. Guard ON, no Redis key → not blocked
  3. Guard ON, active guard → blocked
  4. Guard ON, released guard → not blocked
  5. Guard ON, tombstone guard → not blocked
  6. Guard ON, same sid → not blocked (idempotent)
  7. Guard ON, Redis raises → fail-open (not blocked)
  8. Guard ON, empty sid in guard → not blocked
  9. Guard ON for symbol A → does not block symbol B
  10. Guard ON, malformed JSON in Redis → fail-open
"""

import json
from typing import Any

# ---------------------------------------------------------------------------
# We need to import _tm_check_single_active_guard from trade_monitor.
# Because the module has many heavy dependencies, we extract the method source
# and test it on a lightweight mock object with the same attributes.
# ---------------------------------------------------------------------------


class _FakeRedis:
    """Minimal fake Redis supporting get()."""

    def __init__(self, kv: dict[str, str] | None = None):
        self._kv: dict[str, str] = dict(kv or {})
        self._raise_on_get: bool = False

    def get(self, key: str) -> str | None:
        if self._raise_on_get:
            raise ConnectionError("redis is down")
        return self._kv.get(key)


class _FakeSig:
    """Minimal signal-like object."""

    def __init__(self, symbol: str = "BTCUSDT", sid: str = "sid-new"):
        self.symbol = symbol
        self.sid = sid
        self.payload: dict[str, Any] = {}


class _GuardHolder:
    """
    Minimal object that mirrors the two attributes trade_monitor uses
    in _tm_check_single_active_guard:
      - exec_single_active_position_per_symbol : bool
      - _active_symbol_key_prefix : str
      - redis : Redis-like
    """

    def __init__(
        self,
        redis: _FakeRedis,
        *,
        guard_on: bool = True,
        key_prefix: str = "orders:active_symbol_sid:",
    ):
        self.exec_single_active_position_per_symbol = guard_on
        self._active_symbol_key_prefix = key_prefix
        self.redis = redis

    # ---- exact copy of _tm_check_single_active_guard from trade_monitor.py ----
    def _tm_check_single_active_guard(self, sig) -> bool:
        """
        Return True if the signal should be BLOCKED by the single-active-position guard.

        Reads the same Redis guard key that binance_executor writes
        (key prefix = ORDERS_ACTIVE_SYMBOL_KEY_PREFIX, default: orders:active_symbol_sid:).

        Design constraints:
          - No exchange-truth API call (paper trades don't have Binance positions).
          - No release logic — guard is owned exclusively by binance_executor.
          - Fail-open: any exception returns False (allow), never blocks on error.
        """
        if not self.exec_single_active_position_per_symbol:
            return False
        try:
            symbol = str(getattr(sig, "symbol", "") or "").strip().upper()
            if not symbol:
                return False
            key = f"{self._active_symbol_key_prefix}{symbol}"
            raw = self.redis.get(key)
            if not raw:
                return False
            doc = json.loads(raw)
            if not isinstance(doc, dict):
                return False
            # Skip released / tombstoned guards
            guard_status = (doc.get("guard_status") or "active").lower()
            if guard_status in ("released", "tombstone"):
                return False
            blocked_sid = (doc.get("sid") or "").strip()
            if not blocked_sid:
                return False
            # Don't double-block the same sid (idempotent reprocessing)
            sig_sid = str(getattr(sig, "sid", "") or "")
            if sig_sid and blocked_sid == sig_sid:
                return False
            return True
        except Exception:
            return False  # fail-open: never block on Redis error


def _gk(symbol: str = "BTCUSDT") -> str:
    return f"orders:active_symbol_sid:{symbol}"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_guard_off_always_allows():
    """Guard OFF → never blocked."""
    r = _FakeRedis({_gk(): json.dumps({"sid": "sid-old", "guard_status": "active"})})
    holder = _GuardHolder(r, guard_on=False)
    assert holder._tm_check_single_active_guard(_FakeSig("BTCUSDT", "sid-new")) is False


def test_guard_on_no_redis_key():
    """Guard ON, key absent → not blocked."""
    r = _FakeRedis()
    holder = _GuardHolder(r, guard_on=True)
    assert holder._tm_check_single_active_guard(_FakeSig("BTCUSDT", "sid-new")) is False


def test_guard_on_active_guard_blocks():
    """Guard ON, active guard → BLOCKED."""
    r = _FakeRedis({_gk(): json.dumps({"sid": "sid-existing", "guard_status": "active"})})
    holder = _GuardHolder(r, guard_on=True)
    assert holder._tm_check_single_active_guard(_FakeSig("BTCUSDT", "sid-new")) is True


def test_guard_on_implicit_active_status_blocks():
    """Guard ON, no guard_status field (defaults to 'active') → BLOCKED."""
    r = _FakeRedis({_gk(): json.dumps({"sid": "sid-existing"})})
    holder = _GuardHolder(r, guard_on=True)
    assert holder._tm_check_single_active_guard(_FakeSig("BTCUSDT", "sid-new")) is True


def test_guard_on_released_allows():
    """Guard ON, guard_status=released → not blocked."""
    r = _FakeRedis({_gk("ETHUSDT"): json.dumps({"sid": "sid-old", "guard_status": "released"})})
    holder = _GuardHolder(r, guard_on=True)
    assert holder._tm_check_single_active_guard(_FakeSig("ETHUSDT", "sid-new")) is False


def test_guard_on_tombstone_allows():
    """Guard ON, guard_status=tombstone → not blocked."""
    r = _FakeRedis({_gk("SOLUSDT"): json.dumps({"sid": "sid-old", "guard_status": "tombstone"})})
    holder = _GuardHolder(r, guard_on=True)
    assert holder._tm_check_single_active_guard(_FakeSig("SOLUSDT", "sid-new")) is False


def test_guard_on_same_sid_allows():
    """Guard ON, signal sid == guard sid → not blocked (idempotent)."""
    r = _FakeRedis({_gk(): json.dumps({"sid": "sid-same", "guard_status": "active"})})
    holder = _GuardHolder(r, guard_on=True)
    assert holder._tm_check_single_active_guard(_FakeSig("BTCUSDT", "sid-same")) is False


def test_guard_on_redis_error_fail_open():
    """Guard ON, Redis raises → fail-open (not blocked)."""
    r = _FakeRedis()
    r._raise_on_get = True
    holder = _GuardHolder(r, guard_on=True)
    assert holder._tm_check_single_active_guard(_FakeSig("BTCUSDT", "sid-new")) is False


def test_guard_on_empty_sid_allows():
    """Guard ON, guard doc has empty sid → not blocked."""
    r = _FakeRedis({_gk(): json.dumps({"sid": "", "guard_status": "active"})})
    holder = _GuardHolder(r, guard_on=True)
    assert holder._tm_check_single_active_guard(_FakeSig("BTCUSDT", "sid-new")) is False


def test_guard_on_different_symbol_not_blocked():
    """Guard for BTCUSDT must NOT block ETHUSDT."""
    r = _FakeRedis({_gk("BTCUSDT"): json.dumps({"sid": "sid-existing", "guard_status": "active"})})
    holder = _GuardHolder(r, guard_on=True)
    assert holder._tm_check_single_active_guard(_FakeSig("ETHUSDT", "sid-new")) is False


def test_guard_on_malformed_json_fail_open():
    """Guard ON, malformed JSON → fail-open."""
    r = _FakeRedis({_gk(): "NOT VALID JSON {{{{"})
    holder = _GuardHolder(r, guard_on=True)
    assert holder._tm_check_single_active_guard(_FakeSig("BTCUSDT", "sid-new")) is False


def test_guard_on_non_dict_json_fail_open():
    """Guard ON, JSON is a list → fail-open."""
    r = _FakeRedis({_gk(): json.dumps(["not", "a", "dict"])})
    holder = _GuardHolder(r, guard_on=True)
    assert holder._tm_check_single_active_guard(_FakeSig("BTCUSDT", "sid-new")) is False


def test_guard_on_empty_symbol_not_blocked():
    """Guard ON, signal has empty symbol → not blocked."""
    r = _FakeRedis()
    holder = _GuardHolder(r, guard_on=True)
    assert holder._tm_check_single_active_guard(_FakeSig("", "sid-new")) is False


def test_custom_key_prefix():
    """Guard ON with custom key prefix correctly resolves key."""
    prefix = "custom:guard:prefix:"
    r = _FakeRedis({f"{prefix}BTCUSDT": json.dumps({"sid": "sid-existing", "guard_status": "active"})})
    holder = _GuardHolder(r, guard_on=True, key_prefix=prefix)
    assert holder._tm_check_single_active_guard(_FakeSig("BTCUSDT", "sid-new")) is True
    # default prefix should NOT find the key
    holder2 = _GuardHolder(r, guard_on=True, key_prefix="orders:active_symbol_sid:")
    assert holder2._tm_check_single_active_guard(_FakeSig("BTCUSDT", "sid-new")) is False


# ---------------------------------------------------------------------------
# In-memory per-symbol guard tests (open_by_symbol index)
#
# These test the logic added to on_signal() that checks the in-memory
# open_by_symbol dict to block signals when a symbol already has open
# positions and EXEC_SINGLE_ACTIVE_POSITION_PER_SYMBOL=1.
# ---------------------------------------------------------------------------


class _InMemoryGuardHolder:
    """
    Minimal object that mirrors the attributes used by the in-memory
    per-symbol guard check in on_signal():
      - exec_single_active_position_per_symbol : bool
      - open_by_symbol : Dict[str, Set[str]]
    """

    def __init__(self, *, guard_on: bool = True, open_by_symbol: dict[str, set] | None = None):
        self.exec_single_active_position_per_symbol = guard_on
        self.open_by_symbol: dict[str, set] = dict(open_by_symbol or {})

    def check_in_memory_guard(self, sig: _FakeSig) -> bool:
        """
        Return True if the signal should be BLOCKED by the in-memory
        per-symbol guard. Mirrors the logic in on_signal() exactly.
        """
        if not self.exec_single_active_position_per_symbol:
            return False
        sym_up = str(getattr(sig, "symbol", "") or "").upper()
        if not sym_up:
            return False
        existing = self.open_by_symbol.get(sym_up)
        if existing:
            return True
        return False


def test_inmemory_guard_off_allows():
    """In-memory guard OFF → never blocked."""
    h = _InMemoryGuardHolder(guard_on=False, open_by_symbol={"BTCUSDT": {"pid-1"}})
    assert h.check_in_memory_guard(_FakeSig("BTCUSDT", "sid-new")) is False


def test_inmemory_guard_on_no_positions():
    """In-memory guard ON, no positions → not blocked."""
    h = _InMemoryGuardHolder(guard_on=True, open_by_symbol={})
    assert h.check_in_memory_guard(_FakeSig("BTCUSDT", "sid-new")) is False


def test_inmemory_guard_on_blocks_same_symbol():
    """In-memory guard ON, symbol already open → BLOCKED."""
    h = _InMemoryGuardHolder(guard_on=True, open_by_symbol={"BTCUSDT": {"pid-existing"}})
    assert h.check_in_memory_guard(_FakeSig("BTCUSDT", "sid-new")) is True


def test_inmemory_guard_on_allows_different_symbol():
    """In-memory guard ON for BTCUSDT must NOT block ETHUSDT."""
    h = _InMemoryGuardHolder(guard_on=True, open_by_symbol={"BTCUSDT": {"pid-existing"}})
    assert h.check_in_memory_guard(_FakeSig("ETHUSDT", "sid-new")) is False


def test_inmemory_guard_on_empty_set_allows():
    """In-memory guard ON, symbol in dict but set is empty → NOT blocked."""
    h = _InMemoryGuardHolder(guard_on=True, open_by_symbol={"BTCUSDT": set()})
    assert h.check_in_memory_guard(_FakeSig("BTCUSDT", "sid-new")) is False


def test_inmemory_guard_on_empty_symbol_allows():
    """In-memory guard ON, signal has empty symbol → not blocked."""
    h = _InMemoryGuardHolder(guard_on=True, open_by_symbol={"BTCUSDT": {"pid-1"}})
    assert h.check_in_memory_guard(_FakeSig("", "sid-new")) is False


def test_inmemory_guard_on_multiple_positions_blocks():
    """In-memory guard ON, multiple open positions for symbol → BLOCKED."""
    h = _InMemoryGuardHolder(
        guard_on=True, open_by_symbol={"BTCUSDT": {"pid-1", "pid-2"}}
    )
    assert h.check_in_memory_guard(_FakeSig("BTCUSDT", "sid-new")) is True


def test_inmemory_guard_simulates_close_allows():
    """After removing position from open_by_symbol, new signal is allowed."""
    h = _InMemoryGuardHolder(guard_on=True, open_by_symbol={"BTCUSDT": {"pid-1"}})
    # First signal blocked
    assert h.check_in_memory_guard(_FakeSig("BTCUSDT", "sid-2")) is True
    # Simulate close: remove pid from index
    h.open_by_symbol["BTCUSDT"].discard("pid-1")
    # Now allowed
    assert h.check_in_memory_guard(_FakeSig("BTCUSDT", "sid-2")) is False

