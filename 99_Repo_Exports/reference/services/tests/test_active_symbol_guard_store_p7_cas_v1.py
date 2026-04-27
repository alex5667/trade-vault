"""P7: Tests for ActiveSymbolGuardStore CAS logic.

Focuses on the core atomic state machine for the guard key:
1. Absent -> Active (version 1)
2. Active -> Active (version N+1, diff writer) -> REJECTED
3. Active -> Active (version N+1, same writer) -> OK
4. Active -> Released (with tombstone) -> OK
5. Released -> Active (SAME sid) -> REJECTED (stale writer)
6. Released -> Active (NEW sid) -> OK (takeover)
"""
import pytest
import json
from unittest.mock import MagicMock

from services.active_symbol_guard_store import ActiveSymbolGuardStore


class FakeRedis:
    def __init__(self):
        self.kv = {}

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value, ex=None):
        self.kv[key] = value

    def delete(self, key):
        self.kv.pop(key, None)


@pytest.fixture
def store():
    return ActiveSymbolGuardStore(FakeRedis())


def test_absent_to_active(store):
    res = store.acquire_or_refresh(symbol="BTCUSDT", sid="sid-1", payload_patch={"fsm_state": "OPEN"}, writer="exec")
    assert res['applied'] is True
    doc = res['doc']
    assert doc['guard_status'] == 'active'
    assert doc['guard_version'] == 1
    assert doc['sid'] == 'sid-1'


def test_active_refresh_same_sid(store):
    store.acquire_or_refresh(symbol="BTCUSDT", sid="sid-1", payload_patch={"fsm": "A"}, writer="exec")
    res = store.acquire_or_refresh(symbol="BTCUSDT", sid="sid-1", payload_patch={"fsm": "B"}, writer="exec")
    assert res['applied'] is True
    assert res['doc']['guard_version'] == 2
    assert res['doc']['fsm'] == 'B'


def test_active_rejected_different_sid(store):
    store.acquire_or_refresh(symbol="BTCUSDT", sid="sid-1", payload_patch={}, writer="exec")
    res = store.acquire_or_refresh(symbol="BTCUSDT", sid="sid-2", payload_patch={}, writer="exec")
    assert res['applied'] is False
    assert res['reason'] == 'held_by_other_sid'


def test_released_tombstone_blocks_same_sid_resurrection(store):
    store.acquire_or_refresh(symbol="BTCUSDT", sid="sid-1", payload_patch={}, writer="exec")
    store.mark_released(symbol="BTCUSDT", expected_sid="sid-1", writer="repair")
    
    # Stale writer for sid-1 wakes up and tries to persist (e.g. projection worker late event)
    res = store.acquire_or_refresh(symbol="BTCUSDT", sid="sid-1", payload_patch={}, writer="proj")
    assert res['applied'] is False
    assert res['reason'] == 'released_tombstone_same_sid'
    assert store.load_raw("BTCUSDT")['guard_status'] == 'released'  # unchanged


def test_different_sid_can_take_over_after_release_tombstone(store):
    store.acquire_or_refresh(symbol="BTCUSDT", sid="sid-1", payload_patch={}, writer="exec")
    store.mark_released(symbol="BTCUSDT", expected_sid="sid-1", writer="repair")
    
    # New trade for BTCUSDT comes in
    res = store.acquire_or_refresh(symbol="BTCUSDT", sid="sid-2", payload_patch={}, writer="exec")
    assert res['applied'] is True
    assert res['doc']['guard_status'] == 'active'
    assert res['doc']['sid'] == 'sid-2'
    assert res['doc']['guard_version'] > 1


def test_stale_release_cas_cannot_delete_newer_refresh(store):
    # Setup
    r = FakeRedis()
    s = ActiveSymbolGuardStore(r)
    s.acquire_or_refresh(symbol="SOLUSDT", sid="sid-1", payload_patch={}, writer="w1") # v1
    
    # Simulate a worker reading v1, pausing, while another updates to v2
    s.acquire_or_refresh(symbol="SOLUSDT", sid="sid-1", payload_patch={"note": "refresh"}, writer="w1") # v2
    
    # The paused worker tries to release using an old load_raw doc (v1)
    # Since we can't easily pause the internal CAS, we mock load_raw inside mark_released to return the stale doc
    stale_doc = {"symbol": "SOLUSDT", "sid": "sid-1", "guard_version": 1, "guard_lease_token": "old"}
    
    orig_load = s.load_raw
    s.load_raw = MagicMock(side_effect=[stale_doc, orig_load("SOLUSDT")])
    res = s.mark_released(symbol="SOLUSDT", expected_sid="sid-1", writer="w2", retry_once=False)
    s.load_raw = orig_load
    
    assert res['applied'] is False
    assert 'version_mismatch' in res['reason'] or 'lease_mismatch' in res['reason']
