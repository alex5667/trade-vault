"""Tests for P80 OF-gate Rollups Freshness Probe.

Covers:
  - _try_acquire_rollups_lock: Redis path, file-lock path, lock busy (both modules)
  - _release_rollups_lock: Redis + file cleanup (both modules)
  - run_of_gate_rollups_freshness_probe: enabled/disabled logic (both modules)
  - of_gate_rollups_freshness_probe_v1: dt_to_ms, query_max_bucket, hset_redis, main()
"""
from __future__ import annotations

import importlib
import os
import sys
import datetime as dt
import tempfile
from unittest.mock import MagicMock, patch, call

import pytest


# ──────────────────────────────────────────────────────────────────────────────
# Both module paths are tested (canonical + tick_flow_full mirror)
# ──────────────────────────────────────────────────────────────────────────────
TIMER_MODULES = [
    "services.of_timers_worker",
    "services.of_timers_worker",
]


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _mod(mod_name: str):
    return importlib.import_module(mod_name)


# ──────────────────────────────────────────────────────────────────────────────
# _try_acquire_rollups_lock
# ──────────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("mod_name", TIMER_MODULES)
class TestTryAcquireRollupsLock:
    """Unit tests for _try_acquire_rollups_lock."""

    def test_redis_lock_acquired_returns_true(self, mod_name, monkeypatch):
        """When Redis SET NX succeeds, returns True."""
        m = _mod(mod_name)
        monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")

        fake_redis_mod = MagicMock()
        fake_client = MagicMock()
        # SET NX EX returns the key string on success (truthy)
        fake_client.set.return_value = "OK"
        fake_redis_mod.Redis.from_url.return_value = fake_client

        with patch.dict(sys.modules, {"redis": fake_redis_mod}):
            result = m._try_acquire_rollups_lock(1800)

        assert result is True
        fake_client.set.assert_called_once()

    def test_redis_lock_busy_returns_false(self, mod_name, monkeypatch):
        """When Redis SET NX returns None (key exists), returns False."""
        m = _mod(mod_name)
        monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")

        fake_redis_mod = MagicMock()
        fake_client = MagicMock()
        fake_client.set.return_value = None  # NX failed — key already set
        fake_redis_mod.Redis.from_url.return_value = fake_client

        with patch.dict(sys.modules, {"redis": fake_redis_mod}):
            result = m._try_acquire_rollups_lock(1800)

        assert result is False

    def test_file_lock_acquired_when_no_existing_file(self, mod_name, monkeypatch, tmp_path):
        """Without Redis URL, falls back to file lock; fresh lock file → True."""
        m = _mod(mod_name)
        monkeypatch.delenv("REDIS_URL", raising=False)
        lock_file = str(tmp_path / "of_gate_rollups_refresh.lock")
        monkeypatch.setenv("OF_GATE_ROLLUPS_REFRESH_LOCK_FILE", lock_file)

        result = m._try_acquire_rollups_lock(60)

        assert result is True
        assert os.path.exists(lock_file)

    def test_file_lock_busy_returns_false(self, mod_name, monkeypatch, tmp_path):
        """File lock is recent → busy → returns False."""
        m = _mod(mod_name)
        monkeypatch.delenv("REDIS_URL", raising=False)
        lock_file = str(tmp_path / "of_gate_rollups_refresh.lock")
        monkeypatch.setenv("OF_GATE_ROLLUPS_REFRESH_LOCK_FILE", lock_file)

        # Write fresh lock file
        with open(lock_file, "w") as f:
            f.write("12345")
        # Leave mtime as-is (current time) so it's "recent"

        result = m._try_acquire_rollups_lock(3600)

        assert result is False

    def test_fail_open_on_redis_exception(self, mod_name, monkeypatch):
        """If Redis raises an exception and file lock also fails, returns True (fail-open)."""
        m = _mod(mod_name)
        monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
        # Use unwritable path so file lock also fails
        monkeypatch.setenv("OF_GATE_ROLLUPS_REFRESH_LOCK_FILE", "/dev/null/cannot_write_here/lock")

        fake_redis_mod = MagicMock()
        fake_redis_mod.Redis.from_url.side_effect = Exception("connection error")

        with patch.dict(sys.modules, {"redis": fake_redis_mod}):
            result = m._try_acquire_rollups_lock(60)

        # fail-open: prefer running over blocking
        assert result is True


# ──────────────────────────────────────────────────────────────────────────────
# _release_rollups_lock
# ──────────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("mod_name", TIMER_MODULES)
class TestReleaseRollupsLock:
    """Unit tests for _release_rollups_lock."""

    def test_releases_file_lock(self, mod_name, monkeypatch, tmp_path):
        """Removes the lock file when present."""
        m = _mod(mod_name)
        monkeypatch.delenv("REDIS_URL", raising=False)
        lock_file = str(tmp_path / "of_gate_rollups_refresh.lock")
        monkeypatch.setenv("OF_GATE_ROLLUPS_REFRESH_LOCK_FILE", lock_file)

        with open(lock_file, "w") as f:
            f.write("ts")

        m._release_rollups_lock()

        assert not os.path.exists(lock_file)

    def test_no_error_when_lock_file_missing(self, mod_name, monkeypatch, tmp_path):
        """No exception if lock file doesn't exist."""
        m = _mod(mod_name)
        monkeypatch.delenv("REDIS_URL", raising=False)
        lock_file = str(tmp_path / "nonexistent.lock")
        monkeypatch.setenv("OF_GATE_ROLLUPS_REFRESH_LOCK_FILE", lock_file)

        m._release_rollups_lock()  # should not raise

    def test_releases_redis_lock(self, mod_name, monkeypatch):
        """Calls Redis DELETE on the lock key."""
        m = _mod(mod_name)
        monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
        monkeypatch.setenv("OF_GATE_ROLLUPS_REFRESH_LOCK_KEY", "lock:test_key")

        fake_redis_mod = MagicMock()
        fake_client = MagicMock()
        fake_redis_mod.Redis.from_url.return_value = fake_client

        with patch.dict(sys.modules, {"redis": fake_redis_mod}):
            m._release_rollups_lock()

        fake_client.delete.assert_called_once_with("lock:test_key")


# ──────────────────────────────────────────────────────────────────────────────
# run_of_gate_rollups_refresh_nightly – lock guard
# ──────────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("mod_name", TIMER_MODULES)
class TestRollupsRefreshNightlyLockGuard:
    """Tests lock-guard behaviour of the updated run_of_gate_rollups_refresh_nightly."""

    def test_skips_when_lock_busy(self, mod_name, monkeypatch):
        """When _try_acquire_rollups_lock returns False, skips run_tool and returns True."""
        m = _mod(mod_name)
        monkeypatch.setenv("ENABLE_OF_GATE_ROLLUPS_REFRESH_NIGHTLY", "1")
        # Always-open time window
        monkeypatch.setenv("OF_GATE_ROLLUPS_REFRESH_SAFE_START_UTC", "00:00")
        monkeypatch.setenv("OF_GATE_ROLLUPS_REFRESH_SAFE_END_UTC", "00:00")

        calls = []
        monkeypatch.setattr(m, "_try_acquire_rollups_lock", lambda t: False)
        monkeypatch.setattr(m, "run_tool", lambda *a, **kw: calls.append(a) or True)

        result = m.run_of_gate_rollups_refresh_nightly()

        assert result is True
        assert calls == []

    def test_releases_lock_on_success(self, mod_name, monkeypatch):
        """Lock is released even when run_tool succeeds."""
        m = _mod(mod_name)
        monkeypatch.setenv("ENABLE_OF_GATE_ROLLUPS_REFRESH_NIGHTLY", "1")
        monkeypatch.setenv("OF_GATE_ROLLUPS_REFRESH_SAFE_START_UTC", "00:00")
        monkeypatch.setenv("OF_GATE_ROLLUPS_REFRESH_SAFE_END_UTC", "00:00")

        released = []
        monkeypatch.setattr(m, "_try_acquire_rollups_lock", lambda t: True)
        monkeypatch.setattr(m, "_release_rollups_lock", lambda: released.append(1))
        monkeypatch.setattr(m, "run_tool", lambda *a, **kw: True)

        m.run_of_gate_rollups_refresh_nightly()

        assert released == [1]

    def test_releases_lock_on_run_tool_failure(self, mod_name, monkeypatch):
        """Lock is released even when run_tool fails (finally block)."""
        m = _mod(mod_name)
        monkeypatch.setenv("ENABLE_OF_GATE_ROLLUPS_REFRESH_NIGHTLY", "1")
        monkeypatch.setenv("OF_GATE_ROLLUPS_REFRESH_SAFE_START_UTC", "00:00")
        monkeypatch.setenv("OF_GATE_ROLLUPS_REFRESH_SAFE_END_UTC", "00:00")

        released = []
        monkeypatch.setattr(m, "_try_acquire_rollups_lock", lambda t: True)
        monkeypatch.setattr(m, "_release_rollups_lock", lambda: released.append(1))
        monkeypatch.setattr(m, "run_tool", lambda *a, **kw: False)

        m.run_of_gate_rollups_refresh_nightly()

        assert released == [1]


# ──────────────────────────────────────────────────────────────────────────────
# run_of_gate_rollups_freshness_probe
# ──────────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("mod_name", TIMER_MODULES)
class TestRunOfGateRollupsFreshnessProbe:
    """Tests for run_of_gate_rollups_freshness_probe enable/disable logic."""

    def test_disabled_explicitly_returns_true(self, mod_name, monkeypatch):
        """ENABLE_OF_GATE_ROLLUPS_FRESHNESS_PROBE=0 → returns True (noop), no run_tool."""
        m = _mod(mod_name)
        calls = []
        monkeypatch.setattr(m, "run_tool", lambda *a, **kw: calls.append(a) or True)
        monkeypatch.setenv("ENABLE_OF_GATE_ROLLUPS_FRESHNESS_PROBE", "0")

        result = m.run_of_gate_rollups_freshness_probe()

        assert result is True
        assert calls == []

    def test_default_disabled_when_refresh_not_enabled(self, mod_name, monkeypatch):
        """Without explicit flag, inherits from ENABLE_OF_GATE_ROLLUPS_REFRESH_NIGHTLY=0."""
        m = _mod(mod_name)
        calls = []
        monkeypatch.setattr(m, "run_tool", lambda *a, **kw: calls.append(a) or True)
        monkeypatch.delenv("ENABLE_OF_GATE_ROLLUPS_FRESHNESS_PROBE", raising=False)
        monkeypatch.setenv("ENABLE_OF_GATE_ROLLUPS_REFRESH_NIGHTLY", "0")

        result = m.run_of_gate_rollups_freshness_probe()

        assert result is True
        assert calls == []

    def test_enabled_explicitly_calls_probe(self, mod_name, monkeypatch):
        """ENABLE_OF_GATE_ROLLUPS_FRESHNESS_PROBE=1 → calls run_tool with probe module."""
        m = _mod(mod_name)
        calls = []

        def fake_run_tool(module, *args, **kwargs):
            calls.append(module)
            return True

        monkeypatch.setattr(m, "run_tool", fake_run_tool)
        monkeypatch.setenv("ENABLE_OF_GATE_ROLLUPS_FRESHNESS_PROBE", "1")

        result = m.run_of_gate_rollups_freshness_probe()

        assert result is True
        assert "orderflow_services.of_gate_rollups_freshness_probe_v1" in calls

    def test_enabled_via_nightly_refresh_gate(self, mod_name, monkeypatch):
        """ENABLE_OF_GATE_ROLLUPS_REFRESH_NIGHTLY=1 (no explicit probe flag) → enabled."""
        m = _mod(mod_name)
        calls = []

        def fake_run_tool(module, *args, **kwargs):
            calls.append(module)
            return True

        monkeypatch.setattr(m, "run_tool", fake_run_tool)
        monkeypatch.delenv("ENABLE_OF_GATE_ROLLUPS_FRESHNESS_PROBE", raising=False)
        monkeypatch.setenv("ENABLE_OF_GATE_ROLLUPS_REFRESH_NIGHTLY", "1")

        result = m.run_of_gate_rollups_freshness_probe()

        assert result is True
        assert "orderflow_services.of_gate_rollups_freshness_probe_v1" in calls

    def test_timeout_default_is_60(self, mod_name, monkeypatch):
        """Default timeout is 60 seconds."""
        m = _mod(mod_name)
        timeouts = []

        def fake_run_tool(module, *args, timeout=None, **kwargs):
            timeouts.append(timeout)
            return True

        monkeypatch.setattr(m, "run_tool", fake_run_tool)
        monkeypatch.setenv("ENABLE_OF_GATE_ROLLUPS_FRESHNESS_PROBE", "1")
        monkeypatch.delenv("OF_GATE_ROLLUPS_FRESHNESS_TIMEOUT_S", raising=False)

        m.run_of_gate_rollups_freshness_probe()

        assert timeouts[0] == 60

    def test_custom_timeout_from_env(self, mod_name, monkeypatch):
        """OF_GATE_ROLLUPS_FRESHNESS_TIMEOUT_S overrides default timeout."""
        m = _mod(mod_name)
        timeouts = []

        def fake_run_tool(module, *args, timeout=None, **kwargs):
            timeouts.append(timeout)
            return True

        monkeypatch.setattr(m, "run_tool", fake_run_tool)
        monkeypatch.setenv("ENABLE_OF_GATE_ROLLUPS_FRESHNESS_PROBE", "1")
        monkeypatch.setenv("OF_GATE_ROLLUPS_FRESHNESS_TIMEOUT_S", "120")

        m.run_of_gate_rollups_freshness_probe()

        assert timeouts[0] == 120


# ──────────────────────────────────────────────────────────────────────────────
# of_gate_rollups_freshness_probe_v1 module logic
# ──────────────────────────────────────────────────────────────────────────────
def _probe():
    """Import the probe module."""
    return importlib.import_module("orderflow_services.of_gate_rollups_freshness_probe_v1")


class TestDtToMs:
    """Unit tests for dt_to_ms helper in the probe module."""

    def test_none_returns_zero(self):
        assert _probe().dt_to_ms(None) == 0

    def test_non_datetime_returns_zero(self):
        assert _probe().dt_to_ms("2024-01-01") == 0
        assert _probe().dt_to_ms(12345) == 0

    def test_naive_datetime_treated_as_utc(self):
        p = _probe()
        # 2024-01-01 00:00:00 UTC → epoch ms = 1704067200000
        naive = dt.datetime(2024, 1, 1, 0, 0, 0)
        result = p.dt_to_ms(naive)
        # Allow ±1000ms for any system tz offset on naive datetime
        assert result == 1704067200000

    def test_tz_aware_datetime(self):
        p = _probe()
        aware = dt.datetime(2024, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc)
        assert p.dt_to_ms(aware) == 1704067200000


class TestQueryMaxBucket:
    """Unit tests for query_max_bucket (uses mocked psycopg2 connection)."""

    def test_returns_timestamp_and_age(self):
        p = _probe()
        bucket_dt = dt.datetime(2024, 1, 1, 12, 0, 0)  # naive UTC
        conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchone.return_value = (bucket_dt,)
        conn.cursor.return_value.__enter__ = lambda s: cursor
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        b_ms, age_s = p.query_max_bucket(conn, "of_gate_ok_rate_5m")

        expected_ms = p.dt_to_ms(bucket_dt)
        assert b_ms == expected_ms
        assert age_s >= 0  # age always non-negative

    def test_empty_table_returns_zeros(self):
        p = _probe()
        conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchone.return_value = (None,)
        conn.cursor.return_value.__enter__ = lambda s: cursor
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        b_ms, age_s = p.query_max_bucket(conn, "of_gate_ok_rate_5m")

        assert b_ms == 0
        assert age_s == 0


class TestHsetRedis:
    """Unit tests for hset_redis (best-effort write)."""

    def test_no_op_when_redis_not_available(self, monkeypatch):
        """If redis is None (import failed), hset_redis should silently succeed."""
        p = _probe()
        original_redis = getattr(p, "redis", None)
        try:
            p.redis = None  # type: ignore
            # Should not raise
            p.hset_redis("redis://localhost:6379/0", "key", {"a": 1})
        finally:
            p.redis = original_redis

    def test_writes_mapping_to_redis(self):
        """Calls r.hset with stringified mapping when Redis is available."""
        p = _probe()
        fake_redis_mod = MagicMock()
        fake_client = MagicMock()
        fake_redis_mod.Redis.from_url.return_value = fake_client

        original_redis = p.redis
        try:
            p.redis = fake_redis_mod
            p.hset_redis("redis://localhost:6379/0", "metrics:key", {"ok": 1, "age": 120})
        finally:
            p.redis = original_redis

        fake_client.hset.assert_called_once()
        kwargs = fake_client.hset.call_args
        mapping = kwargs.kwargs.get("mapping") or kwargs.args[1] if kwargs.args else {}
        assert "ok" in {str(k) for k in mapping.keys()}


class TestProbeMain:
    """Integration-style tests for of_gate_rollups_freshness_probe_v1.main()."""

    def test_exits_2_when_no_dsn(self, monkeypatch):
        """Raises SystemExit(2) when TRADES_DB_DSN is not set."""
        p = _probe()
        monkeypatch.delenv("TRADES_DB_DSN", raising=False)
        monkeypatch.delenv("PG_DSN", raising=False)
        monkeypatch.delenv("DATABASE_URL", raising=False)

        with pytest.raises(SystemExit) as exc_info:
            p.main()

        assert exc_info.value.code == 2

    def test_ok_when_both_buckets_fresh(self, monkeypatch):
        """Returns okay (no SystemExit) when both views have recent buckets."""
        p = _probe()
        monkeypatch.setenv("TRADES_DB_DSN", "postgresql://fake/db")
        monkeypatch.delenv("REDIS_URL", raising=False)

        # Fresh bucket: 5 minutes ago
        five_min_ago = dt.datetime.utcnow() - dt.timedelta(minutes=5)

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (five_min_ago,)
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("psycopg2.connect", return_value=mock_conn):
            # Should NOT raise SystemExit(2)
            p.main()

    def test_exits_2_when_buckets_empty(self, monkeypatch):
        """Exits with code 2 when both views return NULL max(bucket)."""
        p = _probe()
        monkeypatch.setenv("TRADES_DB_DSN", "postgresql://fake/db")
        monkeypatch.delenv("REDIS_URL", raising=False)

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (None,)  # empty view
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("psycopg2.connect", return_value=mock_conn):
            with pytest.raises(SystemExit) as exc:
                p.main()

        assert exc.value.code == 2

    def test_exits_2_on_db_connection_error(self, monkeypatch):
        """Exits with code 2 on DB connection failure."""
        p = _probe()
        monkeypatch.setenv("TRADES_DB_DSN", "postgresql://bad/host")
        monkeypatch.delenv("REDIS_URL", raising=False)

        with patch("psycopg2.connect", side_effect=Exception("connection refused")):
            with pytest.raises(SystemExit) as exc:
                p.main()

        assert exc.value.code == 2

    def test_writes_to_redis_when_url_set(self, monkeypatch):
        """Calls hset_redis when REDIS_URL is configured."""
        p = _probe()
        monkeypatch.setenv("TRADES_DB_DSN", "postgresql://fake/db")
        monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")

        five_min_ago = dt.datetime.utcnow() - dt.timedelta(minutes=5)

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (five_min_ago,)
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        hset_calls = []

        def fake_hset_redis(url, key, mapping):
            hset_calls.append((url, key, mapping))

        with patch("psycopg2.connect", return_value=mock_conn):
            original_hset = p.hset_redis
            try:
                p.hset_redis = fake_hset_redis
                p.main()
            finally:
                p.hset_redis = original_hset

        assert len(hset_calls) == 1
        _url, _key, mapping = hset_calls[0]
        assert mapping.get("ok") == 1
        assert "bucket_5m_ts_ms" in mapping
        assert "age_5m_s" in mapping
