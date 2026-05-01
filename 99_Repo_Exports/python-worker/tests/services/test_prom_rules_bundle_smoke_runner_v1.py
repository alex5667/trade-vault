from __future__ import annotations
"""Tests for prom_rules_bundle_smoke_runner_v1 (P104).

Covers:
- success path: health_main returns 0, block is cleared, runner returns 0
- failure path: health_main returns 2, block is set with correct keys/meta, runner returns 2
- SystemExit handling from health_main
- Crash (Exception) in health_main: treated as rc=2  => block set
- _clear_block_if_owned: respects owner mismatch — does NOT delete foreign keys
- ENV overrides: PROM_RULES_BUNDLE_SMOKE_BLOCK_REASON, PROM_RULES_REPO_ROOT, AUTO_APPLY_BLOCK_HOLD_S
- Redis unavailable (redis=None): still returns correct rc without crashing
"""

import json
import os
from unittest.mock import MagicMock, patch, call

import pytest

import services.prom_rules_bundle_smoke_runner_v1 as runner_mod
from services.prom_rules_bundle_smoke_runner_v1 import (
    _block_keys,
    _clear_block_if_owned,
    _now_ms,
    _set_block,
    main,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_redis(store: dict | None = None):
    """Return a minimal fake redis client backed by a dict store."""
    store = store if store is not None else {}

    class FakePipe:
        def __init__(self):
            self._cmds = []

        def set(self, key, val, ex=None):
            self._cmds.append(("set", key, val, ex))
            return self

        def expire(self, key, seconds):
            self._cmds.append(("expire", key, seconds))
            return self

        def delete(self, *keys):
            self._cmds.append(("delete", keys))
            return self

        def execute(self):
            for cmd in self._cmds:
                if cmd[0] == "set":
                    store[cmd[1]] = cmd[2]
                elif cmd[0] == "delete":
                    for k in cmd[1]:
                        store.pop(k, None)
            return [True] * len(self._cmds)

    class FakeRedis:
        def get(self, key):
            return store.get(key)

        def delete(self, *keys):
            for k in keys:
                store.pop(k, None)

        def pipeline(self, transaction=True):
            return FakePipe()

    return FakeRedis(), store


# ---------------------------------------------------------------------------
# Unit tests: _block_keys
# ---------------------------------------------------------------------------

def test_block_keys_structure():
    k, k_ts, k_meta = _block_keys("cfg:suggestions:entry_policy:auto_apply_block", "prom_rules_bundle_smoke")
    assert k == "cfg:suggestions:entry_policy:auto_apply_block:prom_rules_bundle_smoke"
    assert k_ts == k + ":ts_ms"
    assert k_meta == k + ":meta"


# ---------------------------------------------------------------------------
# Unit tests: _set_block
# ---------------------------------------------------------------------------

def test_set_block_writes_keys(monkeypatch):
    fake_redis, store = _make_redis()
    monkeypatch.setattr(runner_mod, "_connect_redis", lambda: fake_redis)
    monkeypatch.delenv("AUTO_APPLY_BLOCK_PREFIX", raising=False)

    meta = {"owner": "prom_rules_bundle_smoke_runner_v1", "ts_ms": 12345, "kind": "prom_rules_bundle_smoke"}
    _set_block(reason="prom_rules_bundle_smoke", meta=meta, hold_s=300)

    prefix = "cfg:suggestions:entry_policy:auto_apply_block"
    assert store.get(f"{prefix}:prom_rules_bundle_smoke") == "1"
    assert store.get(f"{prefix}:prom_rules_bundle_smoke:ts_ms") is not None
    meta_back = json.loads(store[f"{prefix}:prom_rules_bundle_smoke:meta"])
    assert meta_back["owner"] == "prom_rules_bundle_smoke_runner_v1"


def test_set_block_redis_none(monkeypatch):
    """Should be a no-op when redis is unavailable."""
    monkeypatch.setattr(runner_mod, "_connect_redis", lambda: None)
    # Must not raise
    _set_block(reason="x", meta={}, hold_s=60)


# ---------------------------------------------------------------------------
# Unit tests: _clear_block_if_owned
# ---------------------------------------------------------------------------

def test_clear_block_if_owned_removes_when_owner_matches(monkeypatch):
    prefix = "cfg:suggestions:entry_policy:auto_apply_block"
    reason = "prom_rules_bundle_smoke"
    meta = {"owner": "prom_rules_bundle_smoke_runner_v1"}
    store = {
        f"{prefix}:{reason}": "1",
        f"{prefix}:{reason}:ts_ms": "12345",
        f"{prefix}:{reason}:meta": json.dumps(meta),
    }
    fake_redis, store = _make_redis(store)
    monkeypatch.setattr(runner_mod, "_connect_redis", lambda: fake_redis)
    monkeypatch.delenv("AUTO_APPLY_BLOCK_PREFIX", raising=False)

    _clear_block_if_owned(reason=reason)

    # All three keys must have been deleted
    assert f"{prefix}:{reason}" not in store
    assert f"{prefix}:{reason}:ts_ms" not in store
    assert f"{prefix}:{reason}:meta" not in store


def test_clear_block_does_not_remove_foreign_owner(monkeypatch):
    """If owner != our runner, we must NOT delete the key."""
    prefix = "cfg:suggestions:entry_policy:auto_apply_block"
    reason = "prom_rules_bundle_smoke"
    meta = {"owner": "other_runner"}
    store = {
        f"{prefix}:{reason}": "1",
        f"{prefix}:{reason}:meta": json.dumps(meta),
    }
    fake_redis, store = _make_redis(store)
    monkeypatch.setattr(runner_mod, "_connect_redis", lambda: fake_redis)
    monkeypatch.delenv("AUTO_APPLY_BLOCK_PREFIX", raising=False)

    _clear_block_if_owned(reason=reason)

    # Key must still exist
    assert store.get(f"{prefix}:{reason}") == "1"


def test_clear_block_no_meta_falls_through_to_delete(monkeypatch):
    """When there is no :meta key, the runner falls through and deletes the block.

    The conservative guard only activates when owner is explicitly *different*.
    An absent meta key (e.g. old key set before P104) is treated as owned by us.
    """
    prefix = "cfg:suggestions:entry_policy:auto_apply_block"
    reason = "prom_rules_bundle_smoke"
    store = {f"{prefix}:{reason}": "1"}
    fake_redis, store = _make_redis(store)
    monkeypatch.setattr(runner_mod, "_connect_redis", lambda: fake_redis)
    monkeypatch.delenv("AUTO_APPLY_BLOCK_PREFIX", raising=False)

    _clear_block_if_owned(reason=reason)

    # No meta → falls through to delete (no explicit foreign owner claim)
    assert store.get(f"{prefix}:{reason}") is None


# ---------------------------------------------------------------------------
# Integration tests: main()
# ---------------------------------------------------------------------------

def test_main_success_calls_clear_block(monkeypatch):
    """On rc=0, clear_block_if_owned is called and main returns 0."""
    fake_redis, store = _make_redis()
    monkeypatch.setattr(runner_mod, "_connect_redis", lambda: fake_redis)
    monkeypatch.setattr(runner_mod, "health_main", lambda argv: 0)
    monkeypatch.delenv("PROM_RULES_REPO_ROOT", raising=False)
    monkeypatch.delenv("PROM_RULES_BUNDLE_SMOKE_BLOCK_REASON", raising=False)
    monkeypatch.delenv("AUTO_APPLY_BLOCK_HOLD_S", raising=False)

    cleared = []
    monkeypatch.setattr(runner_mod, "_clear_block_if_owned", lambda reason: cleared.append(reason))

    rc = main()
    assert rc == 0
    assert "prom_rules_bundle_smoke" in cleared


def test_main_failure_sets_block(monkeypatch):
    """On rc=2, _set_block is called with the correct reason and main returns 2."""
    set_calls = []

    def fake_set_block(*, reason, meta, hold_s):
        set_calls.append({"reason": reason, "meta": meta, "hold_s": hold_s})

    monkeypatch.setattr(runner_mod, "health_main", lambda argv: 2)
    monkeypatch.setattr(runner_mod, "_set_block", fake_set_block)
    monkeypatch.delenv("PROM_RULES_BUNDLE_SMOKE_BLOCK_REASON", raising=False)
    monkeypatch.delenv("AUTO_APPLY_BLOCK_HOLD_S", raising=False)

    rc = main()
    assert rc == 2
    assert len(set_calls) == 1
    assert set_calls[0]["reason"] == "prom_rules_bundle_smoke"
    assert set_calls[0]["meta"]["owner"] == "prom_rules_bundle_smoke_runner_v1"
    assert set_calls[0]["meta"]["kind"] == "prom_rules_bundle_smoke"


def test_main_system_exit_rc2_sets_block(monkeypatch):
    """health_main raising SystemExit(2) must be treated as rc=2 → block."""
    set_calls = []

    def fake_set_block(*, reason, meta, hold_s):
        set_calls.append(reason)

    def fake_health(argv):
        raise SystemExit(2)

    monkeypatch.setattr(runner_mod, "health_main", fake_health)
    monkeypatch.setattr(runner_mod, "_set_block", fake_set_block)
    monkeypatch.delenv("PROM_RULES_BUNDLE_SMOKE_BLOCK_REASON", raising=False)
    monkeypatch.delenv("AUTO_APPLY_BLOCK_HOLD_S", raising=False)

    rc = main()
    assert rc == 2
    assert len(set_calls) == 1


def test_main_system_exit_rc0_clears_block(monkeypatch):
    """health_main raising SystemExit(0) → rc=0 → clear."""
    def fake_health(argv):
        raise SystemExit(0)

    cleared = []
    monkeypatch.setattr(runner_mod, "health_main", fake_health)
    monkeypatch.setattr(runner_mod, "_clear_block_if_owned", lambda reason: cleared.append(reason))
    monkeypatch.delenv("PROM_RULES_BUNDLE_SMOKE_BLOCK_REASON", raising=False)
    monkeypatch.delenv("AUTO_APPLY_BLOCK_HOLD_S", raising=False)

    rc = main()
    assert rc == 0
    assert cleared


def test_main_exception_sets_block(monkeypatch):
    """Unexpected exception from health_main → rc=2 → block set."""
    set_calls = []

    def fake_health(argv):
        raise RuntimeError("unexpected!")

    monkeypatch.setattr(runner_mod, "health_main", fake_health)
    monkeypatch.setattr(runner_mod, "_set_block", lambda **kw: set_calls.append(kw))
    monkeypatch.delenv("PROM_RULES_BUNDLE_SMOKE_BLOCK_REASON", raising=False)
    monkeypatch.delenv("AUTO_APPLY_BLOCK_HOLD_S", raising=False)

    rc = main()
    assert rc == 2
    assert len(set_calls) == 1


def test_main_env_overrides(monkeypatch):
    """Custom BLOCK_REASON and HOLD_S are picked up correctly."""
    set_calls = []

    monkeypatch.setattr(runner_mod, "health_main", lambda argv: 2)
    monkeypatch.setattr(runner_mod, "_set_block", lambda **kw: set_calls.append(kw))
    monkeypatch.setenv("PROM_RULES_BUNDLE_SMOKE_BLOCK_REASON", "custom_reason")
    monkeypatch.setenv("AUTO_APPLY_BLOCK_HOLD_S", "600")

    rc = main()
    assert rc == 2
    assert set_calls[0]["reason"] == "custom_reason"
    assert set_calls[0]["hold_s"] == 600


def test_main_passes_root_argv(monkeypatch):
    """PROM_RULES_REPO_ROOT is forwarded as --root in argv."""
    captured = []

    def fake_health(argv):
        captured.extend(argv)
        return 0

    monkeypatch.setattr(runner_mod, "health_main", fake_health)
    monkeypatch.setattr(runner_mod, "_clear_block_if_owned", lambda reason: None)
    monkeypatch.setenv("PROM_RULES_REPO_ROOT", "/app")

    main()
    assert "--root" in captured
    assert "/app" in captured


def test_main_no_root_when_env_empty(monkeypatch):
    """When PROM_RULES_REPO_ROOT is empty, --root is NOT forwarded."""
    captured = []

    def fake_health(argv):
        captured.extend(argv)
        return 0

    monkeypatch.setattr(runner_mod, "health_main", fake_health)
    monkeypatch.setattr(runner_mod, "_clear_block_if_owned", lambda reason: None)
    monkeypatch.delenv("PROM_RULES_REPO_ROOT", raising=False)

    main()
    assert "--root" not in captured


# ---------------------------------------------------------------------------
# Manifest YAML validation (quick sanity check)
# ---------------------------------------------------------------------------

def test_manifest_is_valid_yaml():
    import yaml, pathlib
    p = pathlib.Path(__file__).resolve().parents[2] / "orderflow_services" / "prometheus_rules_bundle_manifest_v1.yml"
    doc = yaml.safe_load(p.read_text())
    assert isinstance(doc, dict), "Manifest must be a YAML dict"
    assert isinstance(doc.get("rule_file_globs"), list), "rule_file_globs must be a list"
    assert len(doc["rule_file_globs"]) > 0, "rule_file_globs must not be empty"
