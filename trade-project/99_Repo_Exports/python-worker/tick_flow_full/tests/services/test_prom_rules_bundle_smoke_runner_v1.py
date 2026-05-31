from __future__ import annotations

"""Tests for tick_flow_full.services.prom_rules_bundle_smoke_runner_v1 (P104 mirror).

Mirrors the services/ test suite but imports from tick_flow_full.*
and validates the tick_flow_full manifest.
"""

import json

import tick_flow_full.services.prom_rules_bundle_smoke_runner_v1 as runner_mod
from tick_flow_full.services.prom_rules_bundle_smoke_runner_v1 import (
    _block_keys,
    _clear_block_if_owned,
    main,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_redis(store: dict | None = None):
    store = store if store is not None else {}

    class FakePipe:
        def __init__(self):
            self._cmds = []

        def set(self, key, val, ex=None):
            self._cmds.append(("set", key, val, ex))
            return self

        def execute(self):
            for cmd in self._cmds:
                if cmd[0] == "set":
                    store[cmd[1]] = cmd[2]
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
# Tests
# ---------------------------------------------------------------------------

def test_block_keys_structure():
    k, k_ts, k_meta = _block_keys("cfg:suggestions:entry_policy:auto_apply_block", "prom_rules_bundle_smoke")
    assert k.endswith(":prom_rules_bundle_smoke")
    assert k_ts == k + ":ts_ms"
    assert k_meta == k + ":meta"


def test_main_success_returns_0(monkeypatch):
    monkeypatch.setattr(runner_mod, "health_main", lambda argv: 0)
    monkeypatch.setattr(runner_mod, "_clear_block_if_owned", lambda reason: None)
    monkeypatch.delenv("PROM_RULES_BUNDLE_SMOKE_BLOCK_REASON", raising=False)
    monkeypatch.delenv("AUTO_APPLY_BLOCK_HOLD_S", raising=False)
    assert main() == 0


def test_main_failure_returns_2_and_sets_block(monkeypatch):
    set_calls = []
    monkeypatch.setattr(runner_mod, "health_main", lambda argv: 2)
    monkeypatch.setattr(runner_mod, "_set_block", lambda **kw: set_calls.append(kw))
    monkeypatch.delenv("PROM_RULES_BUNDLE_SMOKE_BLOCK_REASON", raising=False)
    monkeypatch.delenv("AUTO_APPLY_BLOCK_HOLD_S", raising=False)
    rc = main()
    assert rc == 2
    assert set_calls[0]["reason"] == "prom_rules_bundle_smoke"
    assert set_calls[0]["meta"]["owner"] == "prom_rules_bundle_smoke_runner_v1"


def test_main_system_exit_2_sets_block(monkeypatch):
    set_calls = []

    def fake_health(argv):
        raise SystemExit(2)

    monkeypatch.setattr(runner_mod, "health_main", fake_health)
    monkeypatch.setattr(runner_mod, "_set_block", lambda **kw: set_calls.append(kw))
    monkeypatch.delenv("PROM_RULES_BUNDLE_SMOKE_BLOCK_REASON", raising=False)
    monkeypatch.delenv("AUTO_APPLY_BLOCK_HOLD_S", raising=False)
    assert main() == 2
    assert set_calls


def test_clear_block_respects_owner(monkeypatch):
    prefix = "cfg:suggestions:entry_policy:auto_apply_block"
    reason = "prom_rules_bundle_smoke"
    meta = {"owner": "other"}
    store = {
        f"{prefix}:{reason}": "1",
        f"{prefix}:{reason}:meta": json.dumps(meta),
    }
    fake_redis, store = _make_redis(store)
    monkeypatch.setattr(runner_mod, "_connect_redis", lambda: fake_redis)
    monkeypatch.delenv("AUTO_APPLY_BLOCK_PREFIX", raising=False)
    _clear_block_if_owned(reason=reason)
    # foreign owner — key must remain
    assert store.get(f"{prefix}:{reason}") == "1"


def test_manifest_tick_flow_full_valid():
    import pathlib

    import yaml
    p = pathlib.Path(__file__).resolve().parents[3] / "tick_flow_full" / "orderflow_services" / "prometheus_rules_bundle_manifest_v1.yml"
    doc = yaml.safe_load(p.read_text())
    assert isinstance(doc, dict), "tick_flow_full manifest must be a YAML dict"
    assert isinstance(doc.get("rule_file_globs"), list)
    assert len(doc["rule_file_globs"]) > 0
