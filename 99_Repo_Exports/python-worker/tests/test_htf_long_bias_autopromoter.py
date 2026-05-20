from __future__ import annotations

import json
from types import SimpleNamespace

from core.htf_long_bias_autopromoter import HtfLongBiasAutoPromoter, HtfLongBiasState
from handlers.crypto_orderflow.utils.entry_policy_gate import EntryPolicyGate


HOUR_MS = 3600_000
BASE_MS = 1_700_000_000_000  # arbitrary epoch


# ── unit tests for the autopromoter itself ─────────────────────────────────────


def test_autopromoter_not_promoted_below_warmup():
    p = HtfLongBiasAutoPromoter(min_hours=1.0, min_hits=5, min_evals=20)
    # 10 evals, none hits, far below thresholds
    for i in range(10):
        p.observe(symbol="BTCUSDT", hit=False, now_ms=BASE_MS + i * 1000)
    assert p.is_promoted(symbol="BTCUSDT", now_ms=BASE_MS + 10_000) is False


def test_autopromoter_promotes_after_all_criteria_met():
    p = HtfLongBiasAutoPromoter(min_hours=1.0, min_hits=5, min_evals=20)
    # 20 evals → meets min_evals
    for i in range(20):
        p.observe(symbol=None, hit=(i % 2 == 0), now_ms=BASE_MS + i)
    # min_hits=5: 10 hits seen (even indices) → meets min_hits
    # min_hours=1.0: need to push last_eval past 1h boundary
    # Until time hurdle is crossed: still not promoted.
    assert p.is_promoted(symbol=None, now_ms=BASE_MS + 30 * 60 * 1000) is False
    # Cross the 1h boundary via another observe.
    p.observe(symbol=None, hit=False, now_ms=BASE_MS + HOUR_MS + 1000)
    assert p.is_promoted(symbol=None, now_ms=BASE_MS + HOUR_MS + 1000) is True
    snap = p.snapshot()
    assert snap is not None
    assert snap.promoted is True
    assert snap.promoted_ms > 0


def test_autopromoter_disabled_never_promotes():
    p = HtfLongBiasAutoPromoter(enabled=False, min_hours=0.0, min_hits=0, min_evals=0)
    for i in range(5):
        p.observe(symbol="ETHUSDT", hit=True, now_ms=BASE_MS + i)
    assert p.is_promoted(symbol="ETHUSDT", now_ms=BASE_MS + 999_999) is False


def test_autopromoter_per_symbol_scope_isolated():
    p = HtfLongBiasAutoPromoter(
        min_hours=0.0, min_hits=1, min_evals=1, per_symbol=True,
    )
    p.observe(symbol="BTCUSDT", hit=True, now_ms=BASE_MS + HOUR_MS)
    # BTC scope ready; ETH never observed → not promoted
    assert p.is_promoted(symbol="BTCUSDT", now_ms=BASE_MS + HOUR_MS + 1) is True
    assert p.is_promoted(symbol="ETHUSDT", now_ms=BASE_MS + HOUR_MS + 1) is False


def test_autopromoter_persistence_roundtrip():
    p1 = HtfLongBiasAutoPromoter(min_hours=0.0, min_hits=1, min_evals=1)
    p1.observe(symbol=None, hit=True, now_ms=BASE_MS)
    assert p1.is_promoted(symbol=None, now_ms=BASE_MS + 1) is True

    dumped = p1.dump_all()
    assert "global" in dumped

    p2 = HtfLongBiasAutoPromoter(min_hours=100.0, min_hits=999, min_evals=999)
    p2.load_mapping({k.encode(): v.encode() for k, v in dumped.items()})  # bytes keys ok
    snap2 = p2.snapshot()
    assert snap2 is not None
    assert snap2.promoted is True


def test_state_from_json_handles_bad_input():
    assert HtfLongBiasState.from_json(None) is None
    assert HtfLongBiasState.from_json("not json") is None
    assert HtfLongBiasState.from_json("[1,2,3]") is None
    st = HtfLongBiasState.from_json('{"n_evals": "12", "n_hits": "3", "promoted": "true"}')
    assert st is not None
    assert st.n_evals == 12 and st.n_hits == 3 and st.promoted is True


# ── integration with EntryPolicyGate ──────────────────────────────────────────


class _FakeRedis:
    def __init__(self):
        self.hashes: dict = {}
        self.strings: dict = {}
        self.streams: dict = {}

    def hgetall(self, key):
        return self.hashes.get(key, {})

    def hset(self, key, field=None, value=None, mapping=None, **kwargs):
        if key not in self.hashes:
            self.hashes[key] = {}
        if field is not None and value is not None:
            self.hashes[key][field] = value
        if mapping:
            self.hashes[key].update({k: str(v) for k, v in mapping.items()})
        for k, v in kwargs.items():
            self.hashes[key][k] = str(v)

    def get(self, key):
        return self.strings.get(key)

    def set(self, key, value):
        self.strings[key] = value

    def expire(self, key, ttl):
        pass

    def xadd(self, stream, doc, **kwargs):
        self.streams.setdefault(stream, []).append(doc)


def _bear_indicators() -> dict:
    return {
        "cg_rel_strength_btc_1h": -0.05,
        "btc_ret_1m": -0.0025,
        "symbol_rel_strength_vs_btc_1m": -0.003,
        "market_breadth_ret_5m": -0.002,
    }


def test_gate_shadow_mode_does_not_veto_pre_promotion(monkeypatch):
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("HTF_LONG_BIAS_ENABLED", "1")
    monkeypatch.setenv("HTF_LONG_BIAS_MODE", "shadow")
    monkeypatch.setenv("HTF_LONG_BIAS_REQUIRE_N", "2")
    # very high warmup → promoter cannot reach enforce in a few calls
    monkeypatch.setenv("HTF_LONG_BIAS_AUTO_PROMOTE", "1")
    monkeypatch.setenv("HTF_LONG_BIAS_AUTO_PROMOTE_MIN_HOURS", "100")
    monkeypatch.setenv("HTF_LONG_BIAS_AUTO_PROMOTE_MIN_HITS", "1000")
    monkeypatch.setenv("HTF_LONG_BIAS_AUTO_PROMOTE_MIN_EVALS", "1000")

    g = EntryPolicyGate.from_env()
    ctx = SimpleNamespace(
        spread_bps=5.0, indicators=_bear_indicators(), ts_ms=BASE_MS,
    )
    d = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert d.veto is False
    # Ctx annotated with promoted=False
    assert getattr(ctx, "htf_long_bias_alarm", 0) == 1
    assert getattr(ctx, "htf_long_bias_promoted", None) is False


def test_gate_auto_promotes_shadow_to_enforce_after_warmup(monkeypatch):
    """Once promoter criteria are met, subsequent bear-LONG calls must veto
    even though HTF_LONG_BIAS_MODE=shadow."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("HTF_LONG_BIAS_ENABLED", "1")
    monkeypatch.setenv("HTF_LONG_BIAS_MODE", "shadow")     # gate-local floor = shadow
    monkeypatch.setenv("HTF_LONG_BIAS_REQUIRE_N", "2")
    monkeypatch.setenv("HTF_LONG_BIAS_AUTO_PROMOTE", "1")
    monkeypatch.setenv("HTF_LONG_BIAS_AUTO_PROMOTE_MIN_HOURS", "0")  # time hurdle disabled
    monkeypatch.setenv("HTF_LONG_BIAS_AUTO_PROMOTE_MIN_HITS", "3")
    monkeypatch.setenv("HTF_LONG_BIAS_AUTO_PROMOTE_MIN_EVALS", "3")

    g = EntryPolicyGate.from_env()

    # 1st & 2nd call: counted, but evals=1,2 — promoter not ready yet (shadow).
    for i in range(2):
        ctx = SimpleNamespace(
            spread_bps=5.0, indicators=_bear_indicators(), ts_ms=BASE_MS + i,
        )
        d = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
        assert d.veto is False, f"unexpected veto on call {i}: {d}"

    # 3rd call: evals=3, hits=3 → promoter flips to enforce DURING this call.
    ctx3 = SimpleNamespace(
        spread_bps=5.0, indicators=_bear_indicators(), ts_ms=BASE_MS + 100,
    )
    d3 = g.evaluate(ctx=ctx3, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert d3.veto is True
    assert d3.reason_code == "VETO_HTF_LONG_BIAS_BEAR"
    assert getattr(ctx3, "htf_long_bias_promoted", None) is True


def test_gate_force_enforce_bypasses_promoter(monkeypatch):
    """HTF_LONG_BIAS_MODE=enforce should veto on first hit, autopromoter inactive."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("HTF_LONG_BIAS_ENABLED", "1")
    monkeypatch.setenv("HTF_LONG_BIAS_MODE", "enforce")
    monkeypatch.setenv("HTF_LONG_BIAS_REQUIRE_N", "2")
    monkeypatch.setenv("HTF_LONG_BIAS_AUTO_PROMOTE", "0")  # promoter disabled

    g = EntryPolicyGate.from_env()
    ctx = SimpleNamespace(
        spread_bps=5.0, indicators=_bear_indicators(), ts_ms=BASE_MS,
    )
    d = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert d.veto is True
    assert d.reason_code == "VETO_HTF_LONG_BIAS_BEAR"


def test_gate_persists_and_restores_promoter_state(monkeypatch):
    """After autopromotion, a fresh gate must restore promoted state from Redis."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("HTF_LONG_BIAS_ENABLED", "1")
    monkeypatch.setenv("HTF_LONG_BIAS_MODE", "shadow")
    monkeypatch.setenv("HTF_LONG_BIAS_REQUIRE_N", "2")
    monkeypatch.setenv("HTF_LONG_BIAS_AUTO_PROMOTE", "1")
    monkeypatch.setenv("HTF_LONG_BIAS_AUTO_PROMOTE_MIN_HOURS", "0")
    monkeypatch.setenv("HTF_LONG_BIAS_AUTO_PROMOTE_MIN_HITS", "1")
    monkeypatch.setenv("HTF_LONG_BIAS_AUTO_PROMOTE_MIN_EVALS", "1")
    # snapshot-throttle: force immediate write
    monkeypatch.setenv("ADVERSE_CROSS_CAL_SNAPSHOT_SEC", "0")

    redis = _FakeRedis()
    g1 = EntryPolicyGate.from_env()

    ctx = SimpleNamespace(
        spread_bps=5.0, indicators=_bear_indicators(), ts_ms=BASE_MS, redis=redis,
    )
    # First call: promoter flips during this call → veto + Redis HSET in snapshot path.
    d = g1.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert d.veto is True

    # Verify autopromoter state landed in Redis under AUTOCAL_HTF_LONG_BIAS.
    from core.redis_keys import RK
    raw_map = redis.hgetall(RK.AUTOCAL_HTF_LONG_BIAS)
    assert raw_map, "autopromoter state must be written to redis"
    state_json = next(iter(raw_map.values()))
    assert json.loads(state_json)["promoted"] is True

    # Fresh gate: load_from_redis must restore promoted=True.
    g2 = EntryPolicyGate.from_env()
    g2.load_from_redis(redis)
    snap = g2._htf_long_bias_promoter.snapshot()
    assert snap is not None and snap.promoted is True

    # Fresh gate's very first LONG eval (no prior in-process observe) must veto
    # because state was restored.
    ctx2 = SimpleNamespace(
        spread_bps=5.0, indicators=_bear_indicators(), ts_ms=BASE_MS + 1, redis=redis,
    )
    d2 = g2.evaluate(ctx=ctx2, symbol="BTCUSDT", kind="breakout", side="LONG")
    assert d2.veto is True
    assert d2.reason_code == "VETO_HTF_LONG_BIAS_BEAR"


def test_gate_short_traffic_does_not_advance_promoter(monkeypatch):
    """SHORT evaluations must NOT count toward promotion (gate is LONG-only)."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("HTF_LONG_BIAS_ENABLED", "1")
    monkeypatch.setenv("HTF_LONG_BIAS_MODE", "shadow")
    monkeypatch.setenv("HTF_LONG_BIAS_AUTO_PROMOTE_MIN_HOURS", "0")
    monkeypatch.setenv("HTF_LONG_BIAS_AUTO_PROMOTE_MIN_HITS", "1")
    monkeypatch.setenv("HTF_LONG_BIAS_AUTO_PROMOTE_MIN_EVALS", "1")

    g = EntryPolicyGate.from_env()
    for i in range(5):
        ctx = SimpleNamespace(
            spread_bps=5.0, indicators=_bear_indicators(), ts_ms=BASE_MS + i,
        )
        g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", side="SHORT")

    snap = g._htf_long_bias_promoter.snapshot()
    # Either no scope created at all, or zero evals — both are acceptable.
    assert snap is None or snap.n_evals == 0
