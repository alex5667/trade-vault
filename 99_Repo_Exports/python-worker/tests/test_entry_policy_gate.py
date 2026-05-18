from __future__ import annotations

import json
from types import SimpleNamespace

from handlers.crypto_orderflow.utils.entry_policy_gate import EntryPolicyGate


class FakeRedisForGate:
    def __init__(self):
        self.hashes: dict = {}
        self.streams: dict = {}
        self.strings: dict = {}  # Plain string keys (for freeze, etc)

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
        spread_bps=10.0,
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
    """
    When redis=None, load_drift_active_factor returns (1.0, nan, "").
    EntryPolicyGate must not veto and must leave feature_drift_alarm=0.
    """
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "hard")
    monkeypatch.setenv("FEATURE_DRIFT_ENABLED", "1")

    g = EntryPolicyGate.from_env()
    ctx = SimpleNamespace(
        spread_bps=10.0,
        redis=None,
        ts_ms=1700000000000,
    )

    d = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")

    # Fail-open: no veto, no alarm
    assert d.veto is False
    assert getattr(ctx, "feature_drift_alarm", 0) == 0
    assert getattr(ctx, "feature_drift_tighten_k", 1.0) == 1.0


def test_feature_drift_alarm_triggers(monkeypatch):
    """
    EntryPolicyGate reads drift:active:v1:* key written by FeatureDriftAlarm.
    When factor > 1.0 in Redis → drift_hit=True → VETO_FEATURE_DRIFT in hard profile.
    """
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "hard")
    monkeypatch.setenv("FEATURE_DRIFT_ENABLED", "1")
    monkeypatch.setenv("FEATURE_DRIFT_INCLUDE_KIND", "0")

    g = EntryPolicyGate.from_env()
    r = FakeRedisForGate()

    # Write pre-computed drift alarm result (as FeatureDriftAlarm would)
    active_key = "drift:active:v1:BTCUSDT:binance_futures:us_main:1m"
    r.hset(active_key, mapping={
        "factor": "2.5",
        "score": "4.8",
        "feature": "spread_bps",
    })

    ctx = SimpleNamespace(
        spread_bps=10.0,
        redis=r,
        venue="binance_futures",
        session="us_main",
        tf="1m",
        ts_ms=1700000000000,
    )

    d = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")

    assert d.veto is True
    assert d.reason_code == "VETO_FEATURE_DRIFT"
    assert "factor=2.500" in d.notes
    # ctx should be annotated
    assert getattr(ctx, "feature_drift_alarm", 0) == 1
    assert getattr(ctx, "feature_drift_tighten_k", 1.0) == 1.35  # hard profile


def test_feature_drift_no_alarm_when_factor_one(monkeypatch):
    """factor=1.0 → drift_hit=False, no alarm."""
    monkeypatch.setenv("FEATURE_DRIFT_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "hard")
    monkeypatch.setenv("FEATURE_DRIFT_INCLUDE_KIND", "0")

    g = EntryPolicyGate.from_env()
    r = FakeRedisForGate()
    r.hset("drift:active:v1:BTCUSDT:binance:us_main:1m",
           mapping={"factor": "1.0", "score": "0.5", "feature": "obi"})

    ctx = SimpleNamespace(
        spread_bps=10.0, redis=r, venue="binance", session="us_main", tf="1m",
        ts_ms=1700000000000,
    )
    g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")
    assert getattr(ctx, "feature_drift_alarm", 0) == 0


def test_feature_drift_reads_active_key_not_legacy(monkeypatch):
    """Gate reads drift:active:v1:* not old drift:spread_bps:* keys."""
    monkeypatch.setenv("FEATURE_DRIFT_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("FEATURE_DRIFT_INCLUDE_KIND", "0")

    g = EntryPolicyGate.from_env()
    r = FakeRedisForGate()
    # Old/wrong key — must NOT trigger
    r.hset("drift:spread_bps:BTCUSDT:binance:us_main:1m:breakout",
           mapping={"n": "10", "mu": "5.0", "mad": "1.0"})
    # Correct key — triggers drift
    r.hset("drift:active:v1:BTCUSDT:binance:us_main:1m",
           mapping={"factor": "1.8", "score": "3.2", "feature": "z_delta"})

    ctx = SimpleNamespace(
        spread_bps=10.0, redis=r, venue="binance", session="us_main", tf="1m",
        ts_ms=1700000000000,
    )
    g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")
    assert getattr(ctx, "feature_drift_alarm", 0) == 1
    assert getattr(ctx, "feature_drift_tighten_k", 1.0) == 1.15  # default profile


def test_feature_drift_diag_stream_new_fields(monkeypatch):
    """Diag stream must include drift_factor, drift_score, drift_feat."""
    monkeypatch.setenv("FEATURE_DRIFT_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")
    monkeypatch.setenv("FEATURE_DRIFT_INCLUDE_KIND", "0")
    monkeypatch.setenv("ENTRY_POLICY_DIAG_STREAM", "test_drift_diag")

    g = EntryPolicyGate.from_env()
    r = FakeRedisForGate()
    r.hset("drift:active:v1:BTCUSDT:binance:us_main:1m",
           mapping={"factor": "2.0", "score": "5.0", "feature": "obi"})

    ctx = SimpleNamespace(
        spread_bps=10.0, redis=r, venue="binance", session="us_main", tf="1m",
        ts_ms=1700000000000,
    )
    g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")

    events = r.streams.get("test_drift_diag", [])
    assert events
    ev = json.loads(events[0]["data"])
    assert ev["drift"] == 1
    assert ev["drift_factor"] == 2.0
    assert ev["drift_score"] == 5.0
    assert ev["drift_feat"] == "obi"


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


def test_entry_policy_gate_freeze_hard_veto(monkeypatch):
    """P0: When freeze is active with mode=hard, gate should VETO."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")

    g = EntryPolicyGate.from_env()
    r = FakeRedisForGate()

    # Write active hard freeze
    from core.entry_policy_freeze import EntryPolicyFreezeV1
    from utils.time_utils import get_ny_time_millis

    now = get_ny_time_millis()
    freeze = EntryPolicyFreezeV1(
        ver=1,
        symbol="BTCUSDT",
        group="default",
        scenario="reversal",
        until_ts_ms=now + 3_600_000,  # 1 hour from now
        mode="hard",
        reason_code="DATA_BAD",
        created_ts_ms=now,
    )
    freeze_key = "cfg:entry_policy:freeze:v1:BTCUSDT:default:reversal"
    r.set(freeze_key, freeze.to_json())

    ctx = SimpleNamespace(
        spread_bps=10.0,
        redis=r,
        ab_group="default",
        scenario="reversal"
    )

    d = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")

    assert d.apply is True
    assert d.veto is True
    assert d.reason_code == "VETO_FREEZE_ACTIVE"
    assert "freeze_active_since" in d.notes


def test_entry_policy_gate_freeze_shadow_no_veto(monkeypatch):
    """P0: When freeze is active with mode=shadow, gate should NOT veto."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")

    g = EntryPolicyGate.from_env()
    r = FakeRedisForGate()

    from core.entry_policy_freeze import EntryPolicyFreezeV1
    from utils.time_utils import get_ny_time_millis

    now = get_ny_time_millis()
    freeze = EntryPolicyFreezeV1(
        ver=1,
        symbol="ETHUSDT",
        group="default",
        scenario="continuation",
        until_ts_ms=now + 3_600_000,
        mode="shadow",  # shadow mode — audit only, not block
        reason_code="DATA_BAD",
        created_ts_ms=now,
    )
    freeze_key = "cfg:entry_policy:freeze:v1:ETHUSDT:default:continuation"
    r.set(freeze_key, freeze.to_json())

    ctx = SimpleNamespace(
        spread_bps=10.0,
        redis=r,
        ab_group="default",
        scenario="continuation"
    )

    d = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="breakout")

    # Shadow freeze should NOT veto
    assert d.veto is False


def test_entry_policy_gate_freeze_no_redis_fail_open(monkeypatch):
    """P0: If redis is None, gate should not veto (fail-open)."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")

    g = EntryPolicyGate.from_env()
    ctx = SimpleNamespace(
        spread_bps=10.0,
        redis=None,  # No redis
        ab_group="default",
        scenario="reversal"
    )

    d = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")

    # Fail-open: should not veto
    assert d.veto is False
    assert d.reason_code == "OK"


def test_entry_policy_gate_freeze_inactive_no_veto(monkeypatch):
    """P0: If freeze is inactive (expired), gate should not veto."""
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "default")

    g = EntryPolicyGate.from_env()
    r = FakeRedisForGate()

    from core.entry_policy_freeze import EntryPolicyFreezeV1

    freeze = EntryPolicyFreezeV1(
        ver=1,
        symbol="BTCUSDT",
        group="default",
        scenario="reversal",
        until_ts_ms=1000,  # Already expired (timestamp in past)
        mode="hard",
        reason_code="DATA_BAD",
        created_ts_ms=1,
    )
    freeze_key = "cfg:entry_policy:freeze:v1:BTCUSDT:default:reversal"
    r.set(freeze_key, freeze.to_json())

    ctx = SimpleNamespace(
        spread_bps=10.0,
        redis=r,
        ab_group="default",
        scenario="reversal"
    )

    d = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")

    # Expired freeze should not veto
    assert d.veto is False
    assert d.reason_code == "OK"


# ── AdverseCrossCalibrator wiring tests ──────────────────────────────────────

def _make_ctx(r, *, cross_bps=0.0, spread_bps=5.0, ts_ms=1_700_000_000_000, session="na"):
    return SimpleNamespace(
        redis=r,
        spread_bps=spread_bps,
        book_trade_consistency_stale_book_ms=0.0,
        book_trade_consistency_adverse_cross_bps=cross_bps,
        ts_ms=ts_ms,
        ab_group="default",
        scenario="na",
        session=session,
    )


def test_calibrator_wiring_observe_increments_n(monkeypatch):
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("ADVERSE_CROSS_CAL_ENFORCE", "0")
    monkeypatch.setenv("ADVERSE_CROSS_CAL_MIN_SAMPLES", "500")
    r = FakeRedisForGate()
    g = EntryPolicyGate.from_env()

    ctx = _make_ctx(r, cross_bps=0.8)
    g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")

    # calibrator should have recorded 1 observation
    assert g._adverse_calib._n.get("btcusdt:na", 0) == 1


def test_calibrator_wiring_ctx_annotation(monkeypatch):
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("ADVERSE_CROSS_CAL_ENFORCE", "0")
    r = FakeRedisForGate()
    g = EntryPolicyGate.from_env()

    ctx = _make_ctx(r, cross_bps=0.8)
    g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")

    # ctx should be annotated with calibrator budget and Phase-2 field
    assert hasattr(ctx, "adverse_cross_cal_soft_bps")
    assert hasattr(ctx, "adverse_cross_cal_hard_bps")
    assert hasattr(ctx, "adverse_cross_cal_src")
    assert hasattr(ctx, "adverse_cross_bps_at_entry")
    assert ctx.adverse_cross_bps_at_entry == 0.8


def test_calibrator_wiring_zero_cross_not_observed(monkeypatch):
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    r = FakeRedisForGate()
    g = EntryPolicyGate.from_env()

    # cross_bps=0 → calibrator drops it (below FLOOR)
    ctx = _make_ctx(r, cross_bps=0.0)
    g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")
    assert g._adverse_calib._n.get("btcusdt:na", 0) == 0


def test_snapshot_to_redis_roundtrip(monkeypatch):
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("ADVERSE_CROSS_CAL_ENFORCE", "1")
    monkeypatch.setenv("ADVERSE_CROSS_CAL_MIN_SAMPLES", "5")
    r = FakeRedisForGate()
    g = EntryPolicyGate.from_env()

    # Feed 10 samples so calibrator warms up
    for _ in range(10):
        ctx = _make_ctx(r, cross_bps=0.9, ts_ms=1_700_000_000_000)
        g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="continuation")

    assert g._adverse_calib._n.get("ethusdt:na", 0) == 10

    # Force snapshot
    g.snapshot_to_redis(r, now_ms=1_700_000_001_000)
    from core.redis_keys import RK
    assert RK.AUTOCAL_ADVERSE_CROSS in r.hashes
    assert "ethusdt:na" in r.hashes[RK.AUTOCAL_ADVERSE_CROSS]

    # Create new gate, load from Redis → calibrator should have n=10
    g2 = EntryPolicyGate.from_env()
    g2.load_from_redis(r)
    assert g2._adverse_calib._n.get("ethusdt:na", 0) == 10


def test_lazy_load_on_first_evaluate(monkeypatch):
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("ADVERSE_CROSS_CAL_MIN_SAMPLES", "5")
    r = FakeRedisForGate()

    # Pre-populate Redis with saved state
    g_src = EntryPolicyGate.from_env()
    for _ in range(10):
        g_src._adverse_calib.observe(regime="btcusdt:na", cross_bps=1.1)
    g_src.snapshot_to_redis(r, now_ms=1_700_000_000_000)

    # New gate should lazy-load on first evaluate()
    g2 = EntryPolicyGate.from_env()
    assert not g2._ac_loaded

    ctx = _make_ctx(r, cross_bps=0.0, ts_ms=1_700_000_000_000)
    g2.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")

    assert g2._ac_loaded
    assert g2._adverse_calib._n.get("btcusdt:na", 0) == 10


def test_throttled_snapshot_written_to_redis(monkeypatch):
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("ADVERSE_CROSS_CAL_MIN_SAMPLES", "2")
    monkeypatch.setenv("ADVERSE_CROSS_CAL_SNAPSHOT_SEC", "60")
    r = FakeRedisForGate()
    g = EntryPolicyGate.from_env()

    ts_base = 1_700_000_000_000
    # First evaluate: snapshot should be written (last_snap=0, delta > 60s)
    ctx = _make_ctx(r, cross_bps=0.8, ts_ms=ts_base)
    g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")
    from core.redis_keys import RK
    # No regime samples yet → hashes may or may not have the key (empty calib)
    # Feed more and advance time > 60s
    for _ in range(3):
        g._adverse_calib.observe(regime="btcusdt:na", cross_bps=0.8)

    ctx2 = _make_ctx(r, cross_bps=0.8, ts_ms=ts_base + 70_000)
    g.evaluate(ctx=ctx2, symbol="BTCUSDT", kind="breakout")

    assert RK.AUTOCAL_ADVERSE_CROSS in r.hashes
    assert "btcusdt:na" in r.hashes[RK.AUTOCAL_ADVERSE_CROSS]


def test_hard_veto_uses_calibrated_threshold(monkeypatch):
    monkeypatch.setenv("ENTRY_POLICY_ENABLED", "1")
    monkeypatch.setenv("GATE_PROFILE", "hard")
    monkeypatch.setenv("ADVERSE_CROSS_CAL_ENFORCE", "1")
    monkeypatch.setenv("ADVERSE_CROSS_CAL_MIN_SAMPLES", "5")
    # Override static defaults via ENV to a very high value (won't trigger veto)
    monkeypatch.setenv("ENTRY_ADVERSE_CROSS_SOFT_BPS", "50")
    monkeypatch.setenv("ENTRY_ADVERSE_CROSS_HARD_BPS", "50")
    r = FakeRedisForGate()
    g = EntryPolicyGate.from_env()

    # Warm calibrator with 10 samples at 0.5 bps → calibrated hard ≈ 0.5 bps
    for _ in range(10):
        g._adverse_calib.observe(regime="btcusdt:na", cross_bps=0.5)

    # cross_bps=2.0 should trigger veto (>> calibrated q98 ≈ 0.5)
    ctx = _make_ctx(r, cross_bps=2.0, ts_ms=1_700_000_000_000)
    d = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout")
    assert d.veto is True
    assert "ADVERSE_CROSS" in d.reason_code
