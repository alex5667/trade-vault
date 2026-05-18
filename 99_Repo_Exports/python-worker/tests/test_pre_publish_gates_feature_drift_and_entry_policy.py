from __future__ import annotations

from types import SimpleNamespace

# RegimeSessionGate lives here in your codebase
from handlers.crypto_orderflow.utils.pre_publish_gates import RegimeSessionGate
from services.feature_drift_alarm import DriftConfig, FeatureDriftAlarm


class FakeRedis:
    def __init__(self):
        self.kv = {}
        self.hash = {}
        self.stream = []

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value, px=None, ex=None, xx=False):
        # ignore ttl for unit tests
        if xx and key not in self.kv:
            return None
        self.kv[key] = str(value)
        return True

    def expire(self, key, seconds):
        return True

    def pexpire(self, key, ms):
        return True

    def hgetall(self, key):
        return self.hash.get(key, {})

    def hset(self, key, field=None, value=None, mapping=None):
        if key not in self.hash:
            self.hash[key] = {}
        if mapping is not None:
            for k, v in mapping.items():
                self.hash[key][str(k)] = str(v)
            return True
        self.hash[key][str(field)] = str(value)
        return True

    def xadd(self, stream, fields):
        self.stream.append((stream, dict(fields)))
        return "1-0"


def test_regime_session_gate_uses_depth_5_fields_only(monkeypatch):
    """
    Regression guard:
      ctx has depth_*_5 guaranteed.
      l2_depth_* fields MUST be ignored (they don't exist in real ctx).
    """
    monkeypatch.setenv("RS_GATE_ENABLED", "1")  # Enable the gate
    monkeypatch.setenv("RS_DEPTH_MIN_DEFAULT", "5")  # min(1.0, 1.0) = 1.0 < 5.0 -> veto
    monkeypatch.setenv("RS_DRIFT_TIGHTEN", "0")  # isolate base logic

    gate = RegimeSessionGate.from_env()

    # Create ctx.of with depth fields as per actual code
    of = SimpleNamespace(
        depth_bid_5=1.0,
        depth_ask_5=1.0,
        depth_bid_20=999.0,
        depth_ask_20=999.0,
    )

    ctx = SimpleNamespace(
        ts_ms=1700000000000,
        venue="binance_futures",
        tf="1m",
        of=of,
        # Real guaranteed fields:
        depth_bid_5=1.0,
        depth_ask_5=1.0,
        depth_bid_20=999.0,
        depth_ask_20=999.0,
        # Wrong/legacy fields that MUST NOT be used:
        l2_depth_bid=1e9,
        l2_depth_ask=1e9,
    )

    d = gate.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")
    assert d.apply is True
    assert d.veto is True
    assert d.reason_code == "VETO_RS_DEPTH"





def test_feature_drift_alarm_sets_active_key(monkeypatch):
    """
    Minimal test: after enough samples, sudden spread jump -> active drift factor > 1.
    """
    monkeypatch.setenv("FEATURE_DRIFT_ENABLED", "1")
    monkeypatch.setenv("FEATURE_DRIFT_INCLUDE_KIND", "0")
    monkeypatch.setenv("FEATURE_DRIFT_MIN_SAMPLES", "2")  # Very low threshold for test
    monkeypatch.setenv("FEATURE_DRIFT_Z_THRESHOLD", "0.1")  # Very low threshold
    monkeypatch.setenv("FEATURE_DRIFT_TIGHTEN_MULT", "0.5")
    monkeypatch.setenv("FEATURE_DRIFT_DIAG_STREAM", "")

    r = FakeRedis()
    cfg = DriftConfig.from_env()
    alarm = FeatureDriftAlarm(cfg=cfg)

    base_ctx = SimpleNamespace(
        ts_ms=1700000000000,
        venue="binance_futures",
        tf="1m",
        symbol="BTCUSDT",
        session="us_main",
        spread_bps=5.0,
        obi=0.1,
        z_delta=0.2,
        depth_bid_5=100.0,
        depth_ask_5=110.0,
        depth_bid_20=500.0,
        depth_ask_20=520.0,
    )

    # warm-up baseline with stable values
    for i in range(2):
        c = SimpleNamespace(**{**base_ctx.__dict__, "ts_ms": 1700000000000 + i * 1000})
        alarm.update(redis_client=r, ctx=c, symbol="BTCUSDT", kind="breakout")

    # shock - extreme deviation to trigger drift
    shock_ctx = SimpleNamespace(**{**base_ctx.__dict__, "ts_ms": 1700000003000, "spread_bps": 500.0, "z_delta": 50.0})
    alarm.update(redis_client=r, ctx=shock_ctx, symbol="BTCUSDT", kind="breakout")

    active_key = "drift:active:v1:BTCUSDT:binance_futures:us_main:1m"
    dd = r.hgetall(active_key)
    assert dd, f"active drift key must be set. Keys in redis: {list(r.hash.keys())}"
    assert float(dd.get("factor") or 1.0) > 1.0


def test_entry_policy_gate_reads_drift_active_key_from_feature_drift_alarm(monkeypatch):
    """
    Integration test:
      FeatureDriftAlarm.update() writes drift:active:v1:*
      EntryPolicyGate.evaluate() reads it via load_drift_active_factor()
      When drift is active, ctx.feature_drift_tighten_k > 1.0.
    """
    monkeypatch.setenv("FEATURE_DRIFT_ENABLED", "1")
    monkeypatch.setenv("FEATURE_DRIFT_INCLUDE_KIND", "0")
    monkeypatch.setenv("FEATURE_DRIFT_MIN_SAMPLES", "2")
    monkeypatch.setenv("FEATURE_DRIFT_Z_THRESHOLD", "0.1")
    monkeypatch.setenv("FEATURE_DRIFT_TIGHTEN_MULT", "0.5")
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")  # No veto, just annotate

    from handlers.crypto_orderflow.utils.entry_policy_gate import EntryPolicyGate

    r = FakeRedis()
    cfg = DriftConfig.from_env()
    alarm = FeatureDriftAlarm(cfg=cfg)

    base_ctx = SimpleNamespace(
        ts_ms=1700000000000,
        venue="binance_futures",
        tf="1m",
        session="us_main",
        spread_bps=5.0,
        obi=0.1,
        z_delta=0.2,
        depth_bid_5=100.0,
        depth_ask_5=110.0,
        depth_bid_20=500.0,
        depth_ask_20=520.0,
        redis=r,
    )

    # Warm up baseline
    for i in range(2):
        c = SimpleNamespace(**{**base_ctx.__dict__, "ts_ms": 1700000000000 + i * 1000})
        alarm.update(redis_client=r, ctx=c, symbol="BTCUSDT", kind="breakout")

    # Shock drift
    shock_ctx = SimpleNamespace(**{**base_ctx.__dict__, "ts_ms": 1700000003000,
                                   "spread_bps": 500.0, "z_delta": 50.0})
    alarm.update(redis_client=r, ctx=shock_ctx, symbol="BTCUSDT", kind="breakout")

    # Verify FeatureDriftAlarm wrote the active key
    active_key = "drift:active:v1:BTCUSDT:binance_futures:us_main:1m"
    dd = r.hgetall(active_key)
    assert dd and float(dd.get("factor", 1.0)) > 1.0, "FeatureDriftAlarm must set factor > 1.0"

    # Now EntryPolicyGate should read it
    gate = EntryPolicyGate.from_env()
    eval_ctx = SimpleNamespace(
        spread_bps=5.0,     # Normal spread — drift comes from Redis key
        ts_ms=1700000003000,
        venue="binance_futures",
        tf="1m",
        session="us_main",
        redis=r,
    )
    gate.evaluate(ctx=eval_ctx, symbol="BTCUSDT", kind="breakout")

    assert getattr(eval_ctx, "feature_drift_alarm", 0) == 1
    assert getattr(eval_ctx, "feature_drift_tighten_k", 1.0) > 1.0
