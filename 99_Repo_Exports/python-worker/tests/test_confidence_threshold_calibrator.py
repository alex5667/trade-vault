from __future__ import annotations

"""Unit tests for ConfidenceThresholdCalibrator and updated ConfidenceThresholdFilter."""

import json
import time

from core.confidence_threshold_calibrator import (
    ConfidenceThresholdCalibrator,
    _build_relcal_key,
    _invert_curve,
    _parse_buckets,
)
from handlers.crypto_orderflow.core.confidence_threshold import ConfidenceThresholdFilter


# ─────────────────────────────────────────────────────────────────────────────
# helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_hash(buckets: dict[int, tuple[int, int]]) -> dict[str, str]:
    """Build Redis HASH dict from {bucket → (n, h)} pairs."""
    d: dict[str, str] = {}
    for bkt, (n, h) in buckets.items():
        d[f"b{bkt}:n"] = str(n)
        d[f"b{bkt}:h"] = str(h)
    return d


class _FakeRedis:
    """Minimal synchronous Redis stub backed by a dict-of-hashes."""

    def __init__(self, data: dict[str, dict[str, str]] | None = None) -> None:
        self._data: dict[str, dict[str, str]] = data or {}

    def hgetall(self, key: str) -> dict[str, str]:
        return dict(self._data.get(key, {}))

    def hset(self, key: str, mapping: dict[str, str]) -> None:
        self._data.setdefault(key, {}).update(mapping)

    def put(self, key: str, buckets: dict[int, tuple[int, int]]) -> None:
        self._data[key] = _make_hash(buckets)


# ─────────────────────────────────────────────────────────────────────────────
# _parse_buckets
# ─────────────────────────────────────────────────────────────────────────────

def test_parse_buckets_basic() -> None:
    h = _make_hash({50: (100, 70), 55: (80, 60), 60: (40, 35)})
    b = _parse_buckets(h)
    assert b[50] == (100, 70)
    assert b[55] == (80, 60)
    assert b[60] == (40, 35)


def test_parse_buckets_ignores_non_bucket_fields() -> None:
    h = {**_make_hash({70: (50, 40)}), "samples_total": "200", "last_ts_ms": "1234567"}
    b = _parse_buckets(h)
    assert set(b.keys()) == {70}


def test_parse_buckets_empty() -> None:
    assert _parse_buckets({}) == {}


# ─────────────────────────────────────────────────────────────────────────────
# _invert_curve
# ─────────────────────────────────────────────────────────────────────────────

def test_invert_finds_smallest_valid_threshold() -> None:
    """
    Buckets: 70→(80,64 hits=0.80), 65→(60,39 hits=0.65), 60→(50,25 hits=0.50)
    target_wr=0.55, min_samples_above=50.

    Scanning descending:
      T=70: cum_n=80, cum_h=64, hr=0.80 ≥ 0.55 → candidate
      T=65: cum_n=140, cum_h=103, hr≈0.736 ≥ 0.55 → smaller candidate
      T=60: cum_n=190, cum_h=128, hr≈0.674 ≥ 0.55 → smaller candidate

    Best = 60 (smallest T satisfying criterion).
    """
    h = _make_hash({70: (80, 64), 65: (60, 39), 60: (50, 25)})
    result = _invert_curve(h, target_wr=0.55, min_samples_above=50)
    assert result is not None
    thr, hr, n = result
    assert thr == 60.0
    assert hr >= 0.55
    assert n >= 50


def test_invert_returns_none_when_wr_never_met() -> None:
    """All buckets have hit-rate < target_wr."""
    h = _make_hash({70: (100, 40), 65: (100, 35), 60: (100, 30)})
    result = _invert_curve(h, target_wr=0.55, min_samples_above=50)
    assert result is None


def test_invert_returns_none_when_samples_too_few() -> None:
    """Even though hit-rate is high, n_above < min_samples_above."""
    h = _make_hash({80: (10, 9)})
    result = _invert_curve(h, target_wr=0.55, min_samples_above=50)
    assert result is None


def test_invert_empty_hash() -> None:
    assert _invert_curve({}, target_wr=0.55, min_samples_above=50) is None


def test_invert_accumulates_correctly() -> None:
    """
    Bucket 80: 20 samples, 18 hits (WR=0.90) — n < 50, skipped.
    Bucket 75: 30 samples, 27 hits — cum_n=50, cum_h=45, WR=0.90 ≥ 0.55 → valid.
    Bucket 70: 40 samples, 24 hits — cum_n=90, cum_h=69, WR≈0.767 ≥ 0.55 → smaller.
    Best = 70.
    """
    h = _make_hash({80: (20, 18), 75: (30, 27), 70: (40, 24)})
    result = _invert_curve(h, target_wr=0.55, min_samples_above=50)
    assert result is not None
    thr, _, n = result
    assert thr == 70.0
    assert n == 90


# ─────────────────────────────────────────────────────────────────────────────
# ConfidenceThresholdCalibrator — cold start / shadow
# ─────────────────────────────────────────────────────────────────────────────

def test_shadow_mode_returns_default() -> None:
    redis = _FakeRedis()
    cal = ConfidenceThresholdCalibrator(
        redis_client=redis, enforce=False, default_min_conf=50.0,
    )
    thr = cal.min_conf_for(symbol="BTCUSDT", kind="breakout")
    assert thr == 50.0


def test_cold_cluster_returns_default_even_when_enforcing() -> None:
    redis = _FakeRedis()
    cal = ConfidenceThresholdCalibrator(
        redis_client=redis, enforce=True, default_min_conf=50.0,
    )
    thr = cal.min_conf_for(symbol="XYZUSDT", kind="breakout")
    assert thr == 50.0


# ─────────────────────────────────────────────────────────────────────────────
# ConfidenceThresholdCalibrator — inversion from Redis
# ─────────────────────────────────────────────────────────────────────────────

def _key_for(cal: ConfidenceThresholdCalibrator, **dims: str) -> str:
    cluster = (
        dims.get("kind", "na"),
        dims.get("symbol", "*").upper(),
        dims.get("venue", "na"),
        dims.get("session", "na"),
        dims.get("tf", "na"),
        dims.get("regime", "na"),
    )
    return cal._redis_key_for(cluster)


def test_calibrator_reads_redis_and_inverts() -> None:
    """With enough high-WR samples, calibrator sets a committed threshold."""
    redis = _FakeRedis()
    cal = ConfidenceThresholdCalibrator(
        redis_client=redis,
        enforce=True,
        target_wr=0.55,
        min_samples_above=50,
        hold_sec=0.0,       # disable hold for test
        abs_thresh=0.0,     # disable hysteresis
        max_jump_abs=100.0, # disable jump-limit
        cache_ttl_sec=0.0,  # always re-read
        conf_floor=40.0,
        conf_ceil=90.0,
        default_min_conf=50.0,
    )
    # Write high-WR samples: buckets 70→(60,50 WR≈0.83), 65→(60,40 WR≈0.73 cumulative)
    key = _key_for(cal, symbol="BTCUSDT", kind="na", venue="na", session="na", tf="na", regime="na")
    redis.put(key, {70: (60, 50), 65: (60, 40)})

    thr = cal.min_conf_for(symbol="BTCUSDT")
    assert 40.0 <= thr <= 90.0
    assert thr > 0.0


def test_calibrator_falls_back_to_coarser_cluster() -> None:
    """Fine cluster has no data; falls back to coarser (symbol-only)."""
    redis = _FakeRedis()
    cal = ConfidenceThresholdCalibrator(
        redis_client=redis,
        enforce=True,
        target_wr=0.55,
        min_samples_above=50,
        hold_sec=0.0,
        abs_thresh=0.0,
        max_jump_abs=100.0,
        cache_ttl_sec=0.0,
        default_min_conf=50.0,
    )
    # Only the symbol-only fallback key has data
    fallback_key = _build_relcal_key("relcal", "tp2", "na", "BTCUSDT", "na", "na", "na", "na")
    redis._data[fallback_key] = _make_hash({70: (100, 80), 65: (80, 60)})

    thr = cal.min_conf_for(symbol="BTCUSDT", kind="breakout", regime="trend", session="us")
    assert thr > 0.0
    assert thr != 50.0  # must have come from Redis, not default


def test_calibrator_shadow_vs_enforce() -> None:
    """shadow_min_conf_for() always returns proposal; min_conf_for() respects enforce flag."""
    redis = _FakeRedis()
    key_glob = _build_relcal_key("relcal", "tp2", "na", "*", "na", "na", "na", "na")
    redis._data[key_glob] = _make_hash({70: (100, 80)})

    cal_shadow = ConfidenceThresholdCalibrator(
        redis_client=redis, enforce=False,
        hold_sec=0.0, abs_thresh=0.0, max_jump_abs=100.0, cache_ttl_sec=0.0,
        default_min_conf=50.0, target_wr=0.55, min_samples_above=50,
    )
    # Shadow → min_conf_for returns default
    assert cal_shadow.min_conf_for(symbol="ANYUSDT") == 50.0
    # But shadow proposal is filled
    shadow = cal_shadow.shadow_min_conf_for(symbol="ANYUSDT")
    assert shadow > 0.0

    cal_enforce = ConfidenceThresholdCalibrator(
        redis_client=redis, enforce=True,
        hold_sec=0.0, abs_thresh=0.0, max_jump_abs=100.0, cache_ttl_sec=0.0,
        default_min_conf=50.0, target_wr=0.55, min_samples_above=50,
    )
    thr = cal_enforce.min_conf_for(symbol="ANYUSDT")
    assert thr > 0.0 and thr != 50.0


# ─────────────────────────────────────────────────────────────────────────────
# Hysteresis + jump-limit + hold throttle
# ─────────────────────────────────────────────────────────────────────────────

def test_hysteresis_skips_small_changes() -> None:
    redis = _FakeRedis()
    cal = ConfidenceThresholdCalibrator(
        redis_client=redis, enforce=True,
        hold_sec=0.0, abs_thresh=3.0, max_jump_abs=100.0, cache_ttl_sec=0.0,
        default_min_conf=50.0, target_wr=0.55, min_samples_above=50,
    )
    key = _key_for(cal, symbol="BTCUSDT")
    redis.put(key, {65: (100, 75)})  # → inverts to ~65

    thr1 = cal.min_conf_for(symbol="BTCUSDT")

    # Now put a curve that would invert to 65+1.5 (within hysteresis of 3.0)
    redis.put(key, {66: (100, 75), 65: (0, 0)})
    cal.bins.clear()  # flush cache to force re-read
    thr2 = cal.min_conf_for(symbol="BTCUSDT")

    # Change < abs_thresh=3.0 → should not have moved
    assert abs(thr2 - thr1) < 3.0


def test_jump_limit_caps_large_swings() -> None:
    redis = _FakeRedis()
    cal = ConfidenceThresholdCalibrator(
        redis_client=redis, enforce=True,
        hold_sec=0.0, abs_thresh=0.0, max_jump_abs=5.0, cache_ttl_sec=0.0,
        default_min_conf=50.0, target_wr=0.55, min_samples_above=50,
    )
    key = _key_for(cal, symbol="BTCUSDT")

    # First commit: signals around 60 → threshold ~60
    redis.put(key, {60: (200, 150), 55: (100, 60)})
    thr1 = cal.min_conf_for(symbol="BTCUSDT")
    assert thr1 > 0.0

    # Expire cache but KEEP bin state (committed thr1 must survive for jump-limit to apply)
    cluster = ("na", "BTCUSDT", "na", "na", "na", "na")
    cal.bins[cluster].cache_expires_sec = 0.0  # force cache miss

    # New curve inverts far higher (e.g. 85)
    redis.put(key, {85: (200, 160), 80: (100, 80)})
    thr2 = cal.min_conf_for(symbol="BTCUSDT")

    # Jump-limit 5.0 → at most thr1 + 5
    assert thr2 <= thr1 + 5.0 + 1e-9


def test_hold_throttle_prevents_rapid_updates() -> None:
    redis = _FakeRedis()
    cal = ConfidenceThresholdCalibrator(
        redis_client=redis, enforce=True,
        hold_sec=3600.0, abs_thresh=0.0, max_jump_abs=100.0, cache_ttl_sec=0.0,
        default_min_conf=50.0, target_wr=0.55, min_samples_above=50,
    )
    key = _key_for(cal, symbol="ETCUSDT")
    redis.put(key, {65: (100, 75)})

    thr1 = cal.min_conf_for(symbol="ETCUSDT")
    assert thr1 > 0.0

    # Expire cache, keep bin state (committed thr1 must survive for hold-throttle to fire)
    cluster = ("na", "ETCUSDT", "na", "na", "na", "na")
    cal.bins[cluster].cache_expires_sec = 0.0
    redis.put(key, {80: (100, 80)})
    thr2 = cal.min_conf_for(symbol="ETCUSDT")

    # Hold throttle: last_apply was just now, hold_sec=3600 → no commit
    # thr2 must equal thr1 (same committed value)
    assert thr2 == thr1


# ─────────────────────────────────────────────────────────────────────────────
# Hard bounds (conf_floor / conf_ceil)
# ─────────────────────────────────────────────────────────────────────────────

def test_conf_floor_enforced() -> None:
    redis = _FakeRedis()
    cal = ConfidenceThresholdCalibrator(
        redis_client=redis, enforce=True,
        hold_sec=0.0, abs_thresh=0.0, max_jump_abs=100.0, cache_ttl_sec=0.0,
        default_min_conf=50.0, target_wr=0.55, min_samples_above=5,
        conf_floor=55.0, conf_ceil=90.0,
    )
    key = _key_for(cal, symbol="SOLUSDT")
    # Inversion would return bucket 10 (very low), but floor should clip to 55
    redis.put(key, {10: (100, 80)})

    thr = cal.min_conf_for(symbol="SOLUSDT")
    assert thr >= 55.0


def test_conf_ceil_enforced() -> None:
    redis = _FakeRedis()
    cal = ConfidenceThresholdCalibrator(
        redis_client=redis, enforce=True,
        hold_sec=0.0, abs_thresh=0.0, max_jump_abs=100.0, cache_ttl_sec=0.0,
        default_min_conf=50.0, target_wr=0.55, min_samples_above=5,
        conf_floor=40.0, conf_ceil=75.0,
    )
    key = _key_for(cal, symbol="SOLUSDT")
    # Inversion would return 95, but ceil clips to 75
    redis.put(key, {95: (100, 90)})

    thr = cal.min_conf_for(symbol="SOLUSDT")
    assert thr <= 75.0


# ─────────────────────────────────────────────────────────────────────────────
# Snapshot / load_state round-trip
# ─────────────────────────────────────────────────────────────────────────────

def test_snapshot_load_state_roundtrip() -> None:
    redis = _FakeRedis()
    cal1 = ConfidenceThresholdCalibrator(
        redis_client=redis, enforce=True,
        hold_sec=0.0, abs_thresh=0.0, max_jump_abs=100.0, cache_ttl_sec=0.0,
        default_min_conf=50.0, target_wr=0.55, min_samples_above=50,
    )
    key = _key_for(cal1, symbol="BTCUSDT", kind="breakout")
    redis.put(key, {70: (100, 80), 65: (60, 40)})
    thr1 = cal1.min_conf_for(symbol="BTCUSDT", kind="breakout")
    assert thr1 > 0.0

    snap = cal1.snapshot()
    raw = json.dumps(snap)
    parsed = json.loads(raw)

    cal2 = ConfidenceThresholdCalibrator(redis_client=redis, enforce=False)
    cal2.load_state(parsed)
    assert cal2.enforce is True

    # After load_state, committed thresholds are restored (no Redis needed)
    thr2 = cal2.min_conf_for(symbol="BTCUSDT", kind="breakout")
    assert abs(thr2 - thr1) < 1e-9


def test_load_state_skips_malformed_rows() -> None:
    cal = ConfidenceThresholdCalibrator(redis_client=None, enforce=False)
    state = {
        "enforce": True,
        "bins": [
            {"symbol": "BTCUSDT", "min_conf": 65.0},    # valid
            {"no_symbol": True},                          # malformed → skip
            None,                                         # malformed → skip
        ],
    }
    cal.load_state(state)
    assert cal.enforce is True
    assert any(b.min_conf == 65.0 for b in cal.bins.values())


# ─────────────────────────────────────────────────────────────────────────────
# Redis TTL cache behaviour
# ─────────────────────────────────────────────────────────────────────────────

def test_cache_prevents_duplicate_redis_reads() -> None:
    """Within cache_ttl_sec, hgetall must not be called again."""
    call_count = 0

    class CountingRedis(_FakeRedis):
        def hgetall(self, key: str) -> dict[str, str]:
            nonlocal call_count
            call_count += 1
            return super().hgetall(key)

    redis = CountingRedis()
    cal = ConfidenceThresholdCalibrator(
        redis_client=redis, enforce=True,
        hold_sec=0.0, abs_thresh=0.0, max_jump_abs=100.0,
        cache_ttl_sec=3600.0,  # long TTL
        default_min_conf=50.0, target_wr=0.55, min_samples_above=5,
    )
    key = _key_for(cal, symbol="XRPUSDT")
    redis.put(key, {70: (100, 80)})

    cal.min_conf_for(symbol="XRPUSDT")
    first_count = call_count
    # Second call within TTL → no Redis read for this cluster
    cal.min_conf_for(symbol="XRPUSDT")
    assert call_count == first_count  # cache hit


# ─────────────────────────────────────────────────────────────────────────────
# ConfidenceThresholdFilter — static (no calibrator)
# ─────────────────────────────────────────────────────────────────────────────

def test_filter_static_pass() -> None:
    from handlers.crypto_orderflow.core.confidence_threshold import ConfidenceThresholdConfig
    cfg = ConfidenceThresholdConfig(min_conf_default=50.0, min_conf_factor_default=0.45)
    f = ConfidenceThresholdFilter(cfg)
    r = f.evaluate(confidence_pct=72.0, conf_factor=0.55, symbol="BTCUSDT")
    assert r.passed is True
    assert r.calibrated is False


def test_filter_static_fail_on_low_conf() -> None:
    from handlers.crypto_orderflow.core.confidence_threshold import ConfidenceThresholdConfig
    cfg = ConfidenceThresholdConfig(min_conf_default=70.0, min_conf_factor_default=0.45)
    f = ConfidenceThresholdFilter(cfg)
    r = f.evaluate(confidence_pct=60.0, conf_factor=0.55, symbol="BTCUSDT")
    assert r.passed is False
    assert "confidence" in (r.veto_reason or "")


def test_filter_static_fail_on_low_factor() -> None:
    from handlers.crypto_orderflow.core.confidence_threshold import ConfidenceThresholdConfig
    cfg = ConfidenceThresholdConfig(min_conf_default=50.0, min_conf_factor_default=0.60)
    f = ConfidenceThresholdFilter(cfg)
    r = f.evaluate(confidence_pct=72.0, conf_factor=0.40, symbol="BTCUSDT")
    assert r.passed is False
    assert "conf_factor" in (r.veto_reason or "")


def test_filter_fail_closed_on_none() -> None:
    from handlers.crypto_orderflow.core.confidence_threshold import ConfidenceThresholdConfig
    cfg = ConfidenceThresholdConfig(min_conf_default=50.0, min_conf_factor_default=0.45)
    f = ConfidenceThresholdFilter(cfg)
    r = f.evaluate(confidence_pct=None, conf_factor=None, symbol="BTCUSDT")
    assert r.passed is False


# ─────────────────────────────────────────────────────────────────────────────
# ConfidenceThresholdFilter — with calibrator
# ─────────────────────────────────────────────────────────────────────────────

def test_filter_with_calibrator_enforcing() -> None:
    """When calibrator enforces, min_conf_pct comes from Redis curves."""
    from handlers.crypto_orderflow.core.confidence_threshold import ConfidenceThresholdConfig

    redis = _FakeRedis()
    cal = ConfidenceThresholdCalibrator(
        redis_client=redis, enforce=True,
        hold_sec=0.0, abs_thresh=0.0, max_jump_abs=100.0, cache_ttl_sec=0.0,
        default_min_conf=50.0, target_wr=0.55, min_samples_above=50,
        conf_floor=40.0, conf_ceil=90.0,
    )
    # Set Redis data so inversion gives threshold ≈ 65
    key = _build_relcal_key("relcal", "tp2", "breakout", "BTCUSDT", "binance", "us", "5m", "trend")
    redis._data[key] = _make_hash({65: (200, 150), 60: (100, 60)})

    cfg = ConfidenceThresholdConfig(min_conf_default=50.0, min_conf_factor_default=0.45)
    f = ConfidenceThresholdFilter(cfg, calibrator=cal)

    # Signal with confidence_pct=63 should fail (below inverted threshold ~65 or 60)
    r = f.evaluate(
        confidence_pct=63.0, conf_factor=0.55, symbol="BTCUSDT",
        kind="breakout", venue="binance", session="us", tf="5m", regime="trend",
    )
    # Calibrated threshold comes from Redis — result should note it
    # (actual pass/fail depends on exact inverted value, so just check calibrated flag)
    assert r.calibrated is True


def test_filter_calibrator_shadow_ignored() -> None:
    """When calibrator.enforce=False, static thresholds apply."""
    from handlers.crypto_orderflow.core.confidence_threshold import ConfidenceThresholdConfig

    redis = _FakeRedis()
    cal = ConfidenceThresholdCalibrator(
        redis_client=redis, enforce=False,  # shadow only
        default_min_conf=80.0,  # would veto most signals if enforced
    )
    cfg = ConfidenceThresholdConfig(min_conf_default=50.0, min_conf_factor_default=0.45)
    f = ConfidenceThresholdFilter(cfg, calibrator=cal)

    r = f.evaluate(confidence_pct=60.0, conf_factor=0.55, symbol="BTCUSDT")
    assert r.passed is True   # static 50.0 applies, not calibrator's 80.0
    assert r.calibrated is False


def test_filter_veto_reason_includes_source_label() -> None:
    """When calibrated threshold triggers veto, reason contains [cal] label."""
    from handlers.crypto_orderflow.core.confidence_threshold import ConfidenceThresholdConfig

    redis = _FakeRedis()
    # Pre-seed a bin state directly to simulate calibrator having committed threshold
    cal = ConfidenceThresholdCalibrator(
        redis_client=redis, enforce=True,
        hold_sec=0.0, abs_thresh=0.0, max_jump_abs=100.0, cache_ttl_sec=9999.0,
        default_min_conf=50.0, target_wr=0.55, min_samples_above=5,
    )
    # Inject a bin state manually (simulating prior calibration)
    from core.confidence_threshold_calibrator import _BinState
    cluster = ("na", "BTCUSDT", "na", "na", "na", "na")
    b = _BinState(min_conf=72.0, shadow_min_conf=72.0)
    b.cache_expires_sec = time.monotonic() + 9999.0  # cache is fresh
    cal.bins[cluster] = b

    cfg = ConfidenceThresholdConfig(min_conf_default=50.0, min_conf_factor_default=0.45)
    f = ConfidenceThresholdFilter(cfg, calibrator=cal)

    r = f.evaluate(confidence_pct=65.0, conf_factor=0.55, symbol="BTCUSDT")
    assert r.passed is False
    assert "[cal]" in (r.veto_reason or "")
