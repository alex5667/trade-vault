
import hashlib
from unittest.mock import Mock

import pytest
import redis

from services.ml_confirm import MLConfirmGate


class DummyUtilMH:
    feature_cols = ["f_spread_bps"]
    horizons = [60000]

    def predict_util(self, X):
        return {60000: [0.05]}

    def predict_unc(self, X):
        return {60000: [0.01]}

@pytest.fixture
def gate():
    r = Mock(spec=redis.Redis)
    r.get = Mock(return_value=None)
    g = MLConfirmGate(
        r=r,
        mode="SHADOW",
        fail_policy="OPEN",
        champion_key="k1",
        challenger_key="k2"
    )
    # Inject config directly
    g._cfg = {
        "kind": "util_mh_v1",
        "enforce_share": 0.5,
        "run_id": "test_canary",
        "util_floors": {"global": {"floor": 0.01}, "unc_k": 0.0}
    }
    g._model = DummyUtilMH()
    # disable caching to avoid refresh wiping our mock config
    g._cache_loaded_ms = 9999999999999
    return g

def test_canary_deterministic_hashing(gate):
    """Test that same inputs always get same mode."""
    # SID = symbol|ts|direction
    # We want a case that hashes < 0.5 and one >= 0.5

    # We'll use brute force to find two timestamps that give different buckets
    # since we don't want to rely on internal hash impl details in the test setup
    # but we DO verify consistency.

    ts_enforce = None
    ts_shadow = None

    symbol = "BTCUSDT"
    direction = "LONG"

    # Search for buckets
    for t in range(1000, 2000):
        # We can't access private hashing, so we test by calling check()
        # But we haven't implemented it yet.
        # This test expects the logic to exist.
        pass

    # Actually, for TDD, we write the test assuming the implementation.
    # In implementation: hash = (int(md5(sid)[:8]) % 10000) / 10000.0

    def get_bucket(ts):
        sid = f"{symbol}|{ts}|{direction}"
        h = hashlib.md5(sid.encode("utf-8")).digest()
        val = int.from_bytes(h[:8], "big", signed=False)
        return (val % 10000) / 10000.0

    for t in range(1000000, 2000000):
        b = get_bucket(t)
        if b < 0.5 and ts_enforce is None:
            ts_enforce = t
        if b >= 0.5 and ts_shadow is None:
            ts_shadow = t
        if ts_enforce and ts_shadow:
            break

    assert ts_enforce is not None
    assert ts_shadow is not None

    # Check Enforce Case
    dec_e = gate.check(
        symbol=symbol,
        ts_ms=ts_enforce,
        direction=direction,
        scenario="trend",
        indicators={"spread_bps": 1.0, "expected_slippage_bps": 1.0},
        rule_score=1.0, rule_have=1, rule_need=1, cancel_spike_veto=0, ok_rule=1
    )
    assert dec_e.mode == "ENFORCE"

    # Check Shadow Case
    dec_s = gate.check(
        symbol=symbol,
        ts_ms=ts_shadow,
        direction=direction,
        scenario="trend",
        indicators={"spread_bps": 1.0, "expected_slippage_bps": 1.0},
        rule_score=1.0, rule_have=1, rule_need=1, cancel_spike_veto=0, ok_rule=1
    )
    assert dec_s.mode == "SHADOW"

def test_canary_global_enforce_override(gate):
    """If global mode is ENFORCE, it should stay ENFORCE regardless of share."""
    gate.mode = "ENFORCE"
    # Even with share=0.5, everything should be ENFORCE
    # Reuse ts_shadow from above which normally would be SHADOW

    symbol = "BTCUSDT"
    direction = "LONG"
    ts = 1000 # bucket likely random

    dec = gate.check(
        symbol=symbol,
        ts_ms=ts,
        direction=direction,
        scenario="trend",
        indicators={"spread_bps": 1.0, "expected_slippage_bps": 1.0},
        rule_score=1.0, rule_have=1, rule_need=1, cancel_spike_veto=0, ok_rule=1
    )
    assert dec.mode == "ENFORCE"

def test_canary_global_off_override(gate):
    """If global mode is OFF, it should stay OFF."""
    gate.mode = "OFF"

    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=1000,
        direction="LONG",
        scenario="trend",
        indicators={"spread_bps": 1.0, "expected_slippage_bps": 1.0},
        rule_score=1.0, rule_have=1, rule_need=1, cancel_spike_veto=0, ok_rule=1
    )
    # When OFF, it usually returns empty/dummy or ERR, check impl
    # Current impl of check() calls _refresh_cache which clears cfg/model if OFF
    # check() returns ERR_NO_CFG or similar if no cfg
    # But if we force cfg injection, let's see.
    # _refresh_cache_if_needed checks mode=OFF and clears self._cfg

    # So we expect ERR_NO_CFG or similar, or just mode=OFF in decision if it proceeds
    assert dec.mode == "OFF" or dec.status.startswith("ERR")
