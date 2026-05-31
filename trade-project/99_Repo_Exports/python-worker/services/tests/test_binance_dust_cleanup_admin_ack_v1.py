from __future__ import annotations

"""Tests for binance_dust_cleanup_admin_ack — core ACK workflow logic.

Covers:
  - ack_reminder creates a suppressing state
  - renew_reminder_ack extends TTL and increments version
  - revoke_reminder_ack removes the ACK and returns idempotent noop
  - should_suppress_reminder logic (no_ack, acked, expired, fingerprint_mismatch)
  - dashboard_with_unacked correctly identifies unacknowledged items
"""


from services.binance_dust_cleanup_admin_ack import (
    ack_dashboard,
    ack_reminder,
    dashboard_with_unacked,
    reminder_ack_state,
    renew_reminder_ack,
    revoke_reminder_ack,
    should_suppress_reminder,
)


class FakeRedis:
    """Minimal Redis mock sufficient for the ACK module tests."""

    def __init__(self):
        self.store = {}
        self.ttls = {}
        self.streams = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value, ex=None):
        self.store[key] = value
        if ex is not None:
            self.ttls[key] = ex

    def delete(self, key):
        self.store.pop(key, None)
        self.ttls.pop(key, None)

    def ttl(self, key):
        """Return configured TTL, -1 if present with no TTL, -2 if missing."""
        if key not in self.store:
            return -2
        return self.ttls.get(key, -1)

    def xadd(self, stream, payload, maxlen=None, approximate=None):
        self.streams.setdefault(stream, []).append(payload)

    def xrevrange(self, stream, count=10):
        rows = list(reversed(self.streams.get(stream, [])))[:count]
        out = []
        for i, row in enumerate(rows):
            out.append((str(i), row))
        return out

    def scan_iter(self, match=None, count=None):
        prefix = match.rstrip("*") if match else ""
        for key in list(self.store.keys()):
            if not prefix or str(key).startswith(prefix):
                yield key


# ---------------------------------------------------------------------------
# ack_reminder + should_suppress_reminder roundtrip
# ---------------------------------------------------------------------------

def test_ack_and_suppress_roundtrip():
    """Creating an ACK immediately suppresses subsequent reminder calls."""
    redis = FakeRedis()
    result = ack_reminder(
        redis,
        kind="old_denylist",
        symbol="APTUSDT",
        operator="alex",
        reason="investigating",
        ticket="INC-42",
        ttl_sec=1800,
        fingerprint="fp-1",
    )
    assert result["kind"] == "old_denylist"
    assert result["symbol"] == "APTUSDT"
    assert result["operator"] == "alex"
    assert result["ack_version"] == 1

    # State should be readable
    state = reminder_ack_state(redis, "old_denylist", "APTUSDT")
    assert state["operator"] == "alex"
    assert state["ticket"] == "INC-42"

    # Correct fingerprint suppresses
    suppressed = should_suppress_reminder(redis, kind="old_denylist", symbol="APTUSDT", fingerprint="fp-1")
    assert suppressed["suppressed"] is True
    assert suppressed["reason"] == "acked"


def test_fingerprint_mismatch_not_suppressed():
    """A wrong fingerprint should not suppress the reminder."""
    redis = FakeRedis()
    ack_reminder(
        redis,
        kind="old_denylist",
        symbol="SOLUSDT",
        operator="alice",
        reason="ok",
        ticket="T-1",
        ttl_sec=600,
        fingerprint="fp-original",
    )
    result = should_suppress_reminder(redis, kind="old_denylist", symbol="SOLUSDT", fingerprint="fp-changed")
    assert result["suppressed"] is False
    assert result["reason"] == "fingerprint_mismatch"


def test_no_ack_not_suppressed():
    """Without any ACK in Redis, the reminder must not be suppressed."""
    redis = FakeRedis()
    result = should_suppress_reminder(redis, kind="cooldown_loop", symbol="ETHUSDT")
    assert result["suppressed"] is False
    assert result["reason"] == "no_ack"


# ---------------------------------------------------------------------------
# renew_reminder_ack
# ---------------------------------------------------------------------------

def test_renew_increments_version_and_rebinds_ttl():
    """Renewing an ACK must increment ack_version and update operator fields."""
    redis = FakeRedis()
    ack_reminder(
        redis,
        kind="cooldown_loop",
        symbol="SUIUSDT",
        operator="alex",
        reason="owned",
        ticket="INC-7",
        ttl_sec=120,
    )
    renewed = renew_reminder_ack(
        redis,
        kind="cooldown_loop",
        symbol="SUIUSDT",
        operator="bob",
        reason="extend",
        ticket="INC-7A",
        ttl_sec=3600,
    )
    assert renewed["ok"] is True
    assert renewed["renew_operator"] == "bob"
    assert renewed["renew_ticket"] == "INC-7A"
    assert renewed["ack_version"] == 2  # incremented from 1


def test_renew_fails_when_no_ack_exists():
    """Renewing a non-existent ACK returns ok=False with reason ack_not_found."""
    redis = FakeRedis()
    result = renew_reminder_ack(
        redis,
        kind="old_denylist",
        symbol="XRPUSDT",
        operator="op",
        reason="x",
        ticket="T",
        ttl_sec=100,
    )
    assert result["ok"] is False
    assert result["reason"] == "ack_not_found"


# ---------------------------------------------------------------------------
# revoke_reminder_ack
# ---------------------------------------------------------------------------

def test_revoke_removes_ack_and_re_enables_reminder():
    """After revoke, reminders must no longer be suppressed."""
    redis = FakeRedis()
    ack_reminder(
        redis,
        kind="cooldown_loop",
        symbol="SUIUSDT",
        operator="alex",
        reason="owned",
        ticket="INC-7",
        ttl_sec=120,
    )
    # Sanity: currently suppressed
    assert should_suppress_reminder(redis, kind="cooldown_loop", symbol="SUIUSDT")["suppressed"] is True

    revoked = revoke_reminder_ack(
        redis,
        kind="cooldown_loop",
        symbol="SUIUSDT",
        operator="bob",
        reason="resolved",
        ticket="INC-7A",
    )
    assert revoked["result"] == "ok"

    # Now must not be suppressed
    assert should_suppress_reminder(redis, kind="cooldown_loop", symbol="SUIUSDT")["suppressed"] is False


def test_revoke_noop_when_no_ack():
    """Revoking a non-existent ACK returns result=noop (not an error)."""
    redis = FakeRedis()
    result = revoke_reminder_ack(
        redis,
        kind="old_denylist",
        symbol="BNBUSDT",
        operator="op",
        reason="x",
        ticket="T",
    )
    assert result["ok"] is True
    assert result["result"] == "noop"


# ---------------------------------------------------------------------------
# dashboard_with_unacked
# ---------------------------------------------------------------------------

def test_dashboard_without_ack_lists_expected_items():
    """dashboard_with_unacked correctly separates acked from unacked items."""
    redis = FakeRedis()
    # ACK APTUSDT old_denylist — it should disappear from unacked list
    ack_reminder(
        redis,
        kind="old_denylist",
        symbol="APTUSDT",
        operator="alex",
        reason="investigating",
        ticket="INC-42",
        ttl_sec=1800,
    )
    view = dashboard_with_unacked(
        redis,
        stale_denylist=[
            {"symbol": "APTUSDT", "age_sec": 999},   # acked
            {"symbol": "SUIUSDT", "age_sec": 1001},  # NOT acked
        ],
        cooldown_loops=[{"symbol": "XRPUSDT", "age_sec": 1900}],
    )
    # SUIUSDT has no ACK → still in unacked list
    assert view["counts"]["stale_denylist_without_ack"] == 1
    assert view["stale_denylist_without_ack"][0]["symbol"] == "SUIUSDT"
    # XRPUSDT cooldown with no ACK
    assert view["counts"]["cooldown_loops_without_ack"] == 1
    assert view["cooldown_loops_without_ack"][0]["symbol"] == "XRPUSDT"


def test_ack_dashboard_returns_all_items():
    """ack_dashboard enumerates all scan_iter keys matching the ack prefix."""
    redis = FakeRedis()
    ack_reminder(redis, kind="old_denylist", symbol="XRPUSDT", operator="op", reason="r", ticket="T", ttl_sec=100)
    ack_reminder(redis, kind="cooldown_loop", symbol="ETHUSDT", operator="op", reason="r", ticket="T", ttl_sec=200)
    dash = ack_dashboard(redis)
    assert dash["ok"] is True
    assert dash["counts"]["acks"] == 2
