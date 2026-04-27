from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from handlers.crypto_orderflow.utils.entry_policy_gate import EntryPolicyGate


class FakeRedisForGate:
    def __init__(self):
        self.hashes = {}
        self.streams = {}

    def hgetall(self, key):
        return self.hashes.get(key, {})

    def hset(self, key, mapping=None, **kwargs):
        if key not in self.hashes:
            self.hashes[key] = {}
        if mapping:
            self.hashes[key].update({k: str(v) for k, v in mapping.items()})
        else:
            self.hashes[key].update({k: str(v) for k, v in kwargs.items()})

    def expire(self, key, ttl):
        pass

    def xadd(self, stream, doc, **kwargs):
        if stream not in self.streams:
            self.streams[stream] = []
        self.streams[stream].append(doc)


def test_entry_policy_gate_disabled(monkeypatch):
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "0")
    g = EntryPolicyGate.from_env()
    ctx = SimpleNamespace(spread_bps=100.0)
    d = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")
    
    assert d.apply is False
    assert d.veto is False
    assert d.notes == "disabled"


def test_entry_policy_gate_default_profile_soft_tighten(monkeypatch):
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("ENTRY_SPREAD_SHOCK_BPS", "35")
    
    g = EntryPolicyGate.from_env()
    ctx = SimpleNamespace(
        spread_bps=40.0,  # exceeds 35, should trigger soft flag
        burst_flip_ratio=0.1,
        cancel_to_trade=0.1
    )
    
    d = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")
    
    # Policy says: in default/soft never veto
    assert d.apply is True
    assert d.veto is False
    assert d.notes == "audit_only"
    assert "spread_shock=40.0bps" in getattr(ctx, "entry_policy_flags", [])
    assert getattr(ctx, "entry_policy_tighten_k", 1.0) == 1.10


def test_entry_policy_gate_strict_profile_veto_spread(monkeypatch):
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "strict")
    monkeypatch.setenv("ENTRY_SPREAD_SHOCK_BPS_HARD", "60")
    
    g = EntryPolicyGate.from_env()
    ctx = SimpleNamespace(spread_bps=80.0) # > 60 => hard veto
    
    d = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")
    
    assert d.apply is True
    assert d.veto is True
    assert d.reason_code == "VETO_SPREAD_SHOCK"
    assert "spread_bps=80.0" in d.notes
    
    # if it was 50 (below hard, above soft)
    ctx2 = SimpleNamespace(spread_bps=50.0)
    d2 = g.evaluate(ctx=ctx2, symbol="BTCUSDT", kind="breakout")
    assert d2.apply is True
    assert d2.veto is False  # Strict doesn't veto on soft flags unless it's hard profile
    # Tighten K should be 1.25 for strict
    assert getattr(ctx2, "entry_policy_tighten_k", 1.0) == 1.25


def test_entry_policy_gate_hard_profile_veto_book_stale(monkeypatch):
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "hard")
    monkeypatch.setenv("ENTRY_BOOK_STALE_HARD_MS", "1200")
    
    g = EntryPolicyGate.from_env()
    ctx = SimpleNamespace(
        book_trade_consistency_stale_book_ms=1300.0,
        spread_bps=10.0
    )
    
    d = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")
    
    assert d.apply is True
    assert d.veto is True
    assert d.reason_code == "VETO_BOOK_STALE"
    assert "1300 >= hard=1200" in d.notes


def test_entry_policy_gate_hard_profile_veto_soft_flags(monkeypatch):
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "hard")
    monkeypatch.setenv("ENTRY_BURST_FLIP_MAX", "0.85")
    
    g = EntryPolicyGate.from_env()
    ctx = SimpleNamespace(
        spread_bps=10.0,
        burst_flip_ratio=0.9  # Tripps burst flip soft flag
    )
    
    d = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")
    
    assert d.apply is True
    assert d.veto is True
    assert d.reason_code == "VETO_ENTRY_POLICY"
    assert "burst_flip=0.9" in d.notes


def test_entry_policy_gate_fallback_spread_calc(monkeypatch):
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "hard")
    monkeypatch.setenv("ENTRY_SPREAD_SHOCK_BPS", "35")
    
    g = EntryPolicyGate.from_env()
    # 400 bps spread fallback
    ctx = SimpleNamespace(
        bid=100.0,
        ask=104.0,
        mid=102.0
    )
    
    d = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")
    
    assert d.veto is True
    assert d.reason_code == "VETO_SPREAD_SHOCK"


def test_feature_drift_alarm_fail_open_redis_missing(monkeypatch):
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "hard")
    monkeypatch.setenv("FEATURE_DRIFT_ENABLED", "1")
    monkeypatch.setenv("FEATURE_DRIFT_Z", "6.0")
    
    g = EntryPolicyGate.from_env()
    # Missing redis silently fails open
    ctx = SimpleNamespace(
        spread_bps=10.0,
        redis=None
    )
    
    # Should not crash, and should not veto on drift
    d = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")
    assert d.veto is False
    assert getattr(ctx, "feature_drift_alarm", 0) == 0


def test_feature_drift_alarm_triggers(monkeypatch):
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "hard")
    monkeypatch.setenv("FEATURE_DRIFT_ENABLED", "1")
    monkeypatch.setenv("FEATURE_DRIFT_Z", "3.0") # trigger easily
    
    g = EntryPolicyGate.from_env()
    r = FakeRedisForGate()
    
    # Setup some pre-existing drift state
    dims = "BTCUSDT:binance_futures:us_main:1m:breakout"
    r.hset(f"drift:spread_bps:{dims}", mapping={"n": 10, "mu": 5.0, "mad": 1.0})
    
    ctx = SimpleNamespace(
        spread_bps=25.0, # Z = (25-5)/1 = 20, > 3.0
        redis=r,
        symbol="BTCUSDT",
        venue="binance_futures",
        session="us_main",
        tf="1m",
        ts_ms=1700000000000
    )
    
    d = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")
    
    # Since profile is hard, should veto on feature drift
    assert d.veto is True
    assert d.reason_code == "VETO_FEATURE_DRIFT"


def test_entry_policy_gate_diag_stream(monkeypatch):
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("ENTRY_POLICY_DIAG_STREAM", "test_stream")
    monkeypatch.setenv("ENTRY_BURST_FLIP_MAX", "0.5")
    
    g = EntryPolicyGate.from_env()
    r = FakeRedisForGate()
    ctx = SimpleNamespace(
        spread_bps=10.0,
        burst_flip_ratio=0.8,
        redis=r
    )
    
    g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")
    
    stream_data = r.streams.get("test_stream", [])
    assert len(stream_data) == 1
    event = json.loads(stream_data[0]["data"])
    
    assert event["symbol"] == "BTCUSDT"
    assert "burst_flip=0.800" in event["soft_flags"]
    assert event["profile"] == "default"
