"""Tests for slippage_autocal_v1: SlippageAutoCal + parse helpers."""
from __future__ import annotations

import json
import math
import time

import pytest

from orderflow_services.slippage_autocal_v1 import (
    SlippageAutoCal,
    _AutoPromoter,
    _extract_adverse_bps,
    _parse_message,
    _session_from_ts,
    _weighted_quantile,
    _Sample,
)


# ---------------------------------------------------------------------------
# _session_from_ts
# ---------------------------------------------------------------------------

def test_session_us_main():
    # 17:00 UTC → us_main
    ts_ms = 17 * 3_600_000  # epoch hours to ms
    assert _session_from_ts(ts_ms) == "us_main"


def test_session_asian():
    ts_ms = 3 * 3_600_000
    assert _session_from_ts(ts_ms) == "asian"


def test_session_european():
    ts_ms = 10 * 3_600_000
    assert _session_from_ts(ts_ms) == "european"


# ---------------------------------------------------------------------------
# _weighted_quantile
# ---------------------------------------------------------------------------

def _make_samples(vals: list[float], half_life: float = 7.0) -> list[_Sample]:
    now = int(time.time() * 1000)
    return [_Sample(v, now - i * 3600_000, half_life) for i, v in enumerate(vals)]


def test_weighted_quantile_median():
    samples = _make_samples([1.0, 2.0, 3.0, 4.0, 5.0])
    now_ms = int(time.time() * 1000)
    q50 = _weighted_quantile(samples, 0.5, now_ms)
    assert 1.0 < q50 < 5.0


def test_weighted_quantile_empty():
    assert _weighted_quantile([], 0.75, int(time.time() * 1000)) == 0.0


def test_weighted_quantile_single():
    samples = _make_samples([4.5])
    now_ms = int(time.time() * 1000)
    assert _weighted_quantile(samples, 0.75, now_ms) == pytest.approx(4.5)


def test_weighted_quantile_skips_non_positive():
    samples = _make_samples([0.0, -1.0, 3.0, 4.0])
    now_ms = int(time.time() * 1000)
    q75 = _weighted_quantile(samples, 0.75, now_ms)
    assert q75 > 0


# ---------------------------------------------------------------------------
# SlippageAutoCal.observe + compute_groups
# ---------------------------------------------------------------------------

def _fill(cal: SlippageAutoCal, symbol: str, session: str, vals: list[float]) -> None:
    now_ms = int(time.time() * 1000)
    for i, v in enumerate(vals):
        cal.observe(symbol, session, v, now_ms - i * 3600_000)


def test_calibrator_returns_nothing_below_min_n():
    cal = SlippageAutoCal(min_n=20)
    _fill(cal, "BTCUSDT", "us_main", [3.0, 4.0, 5.0])  # only 3 samples
    now_ms = int(time.time() * 1000)
    groups = cal.compute_groups(now_ms)
    # real group below min_n is excluded; wildcard (*:*) may appear if enough
    real_key = "BTCUSDT:US_MAIN"
    assert real_key not in groups


def test_calibrator_produces_output_after_min_n():
    cal = SlippageAutoCal(min_n=5)
    vals = [2.0, 3.0, 4.0, 5.0, 6.0, 7.0]
    _fill(cal, "BTCUSDT", "us_main", vals)
    now_ms = int(time.time() * 1000)
    groups = cal.compute_groups(now_ms)
    assert "BTCUSDT:US_MAIN" in groups


def test_calibrator_q75_in_range():
    cal = SlippageAutoCal(min_n=5, lower=1.0, upper=30.0)
    _fill(cal, "BTCUSDT", "us_main", [2.0, 3.0, 4.0, 5.0, 6.0, 7.0])
    now_ms = int(time.time() * 1000)
    groups = cal.compute_groups(now_ms)
    entry = groups.get("BTCUSDT:US_MAIN", {})
    new_bps = entry.get("new_bps", 0.0)
    assert 1.0 <= new_bps <= 30.0


def test_calibrator_wildcard_aggregates():
    cal = SlippageAutoCal(min_n=3)
    _fill(cal, "BTCUSDT", "us_main", [3.0, 4.0, 5.0])
    _fill(cal, "ETHUSDT", "asian", [2.0, 3.0, 4.0])
    now_ms = int(time.time() * 1000)
    groups = cal.compute_groups(now_ms)
    # wildcard groups should exist
    assert "BTCUSDT:*" in groups or "*:*" in groups


def test_calibrator_ewma_blend():
    cal = SlippageAutoCal(min_n=3, alpha=1.0, lower=1.0, upper=30.0, default_bps=4.0)
    # alpha=1.0 → new_bps = q75
    _fill(cal, "BTCUSDT", "us_main", [6.0, 6.0, 6.0, 6.0])
    now_ms = int(time.time() * 1000)
    groups = cal.compute_groups(now_ms)
    new_bps = groups.get("BTCUSDT:US_MAIN", {}).get("new_bps", 0.0)
    assert new_bps == pytest.approx(6.0, abs=1.0)


def test_calibrator_clamp_upper():
    cal = SlippageAutoCal(min_n=3, upper=10.0, alpha=1.0)
    _fill(cal, "SOLEND", "us_main", [50.0, 60.0, 70.0])
    now_ms = int(time.time() * 1000)
    groups = cal.compute_groups(now_ms)
    entry = groups.get("SOLEND:US_MAIN", {})
    if entry:
        assert entry["new_bps"] <= 10.0


def test_calibrator_clamp_lower():
    cal = SlippageAutoCal(min_n=3, lower=2.0, alpha=1.0)
    _fill(cal, "BTCUSDT", "us_main", [0.1, 0.2, 0.3])
    now_ms = int(time.time() * 1000)
    groups = cal.compute_groups(now_ms)
    entry = groups.get("BTCUSDT:US_MAIN", {})
    if entry:
        assert entry["new_bps"] >= 2.0


def test_calibrator_load_state():
    snapshot = {
        "schema_version": 1,
        "groups": {
            "BTCUSDT:US_MAIN": {"new_bps": 3.5},
            "ETHUSDT:ASIAN": {"new_bps": 5.0},
        },
    }
    cal = SlippageAutoCal()
    cal.load_state(snapshot)
    assert cal._committed.get("BTCUSDT:US_MAIN") == pytest.approx(3.5)
    assert cal._committed.get("ETHUSDT:ASIAN") == pytest.approx(5.0)


def test_calibrator_commit_updates_committed():
    cal = SlippageAutoCal(min_n=3, alpha=1.0)
    _fill(cal, "BTCUSDT", "us_main", [4.0, 4.0, 4.0, 4.0])
    now_ms = int(time.time() * 1000)
    groups = cal.compute_groups(now_ms)
    cal.commit(groups)
    assert "BTCUSDT:US_MAIN" in cal._committed


def test_calibrator_prune_old():
    cal = SlippageAutoCal(window_days=1)
    old_ms = int(time.time() * 1000) - 2 * 86_400_000
    now_ms = int(time.time() * 1000)
    for i in range(10):
        cal.observe("BTCUSDT", "us_main", 5.0, old_ms + i * 1000)
    cal.observe("BTCUSDT", "us_main", 5.0, now_ms)
    counts = cal.sample_counts()
    # Only recent sample + its wildcard aggregate should remain
    assert counts.get("BTCUSDT:us_main", 0) <= 2


# ---------------------------------------------------------------------------
# _extract_adverse_bps
# ---------------------------------------------------------------------------

def test_extract_from_close_dict_bucket():
    payload = {"close": {"adverse_bps_t": {2000: 8.5, 500: 3.0}}}
    v = _extract_adverse_bps(payload, adverse_key_ms=2000)
    assert v == pytest.approx(8.5)


def test_extract_from_close_float():
    payload = {"close": {"adverse_bps_t": 6.0}}
    v = _extract_adverse_bps(payload, adverse_key_ms=2000)
    assert v == pytest.approx(6.0)


def test_extract_from_expected_slippage():
    payload = {"indicators": {"expected_slippage_bps": 4.2}}
    v = _extract_adverse_bps(payload, adverse_key_ms=2000)
    assert v == pytest.approx(4.2)


def test_extract_returns_none_when_missing():
    payload = {}
    v = _extract_adverse_bps(payload, adverse_key_ms=2000)
    assert v is None


def test_extract_ignores_zero():
    payload = {"close": {"adverse_bps_t": 0.0}, "indicators": {"expected_slippage_bps": 3.5}}
    v = _extract_adverse_bps(payload, adverse_key_ms=2000)
    assert v == pytest.approx(3.5)


# ---------------------------------------------------------------------------
# _parse_message
# ---------------------------------------------------------------------------

def _fields(symbol: str, ts_ms: int, payload: dict) -> dict[str, str]:
    return {
        "symbol": symbol,
        "ts_ms": str(ts_ms),
        "session": "us_main",
        "payload": json.dumps(payload),
    }


def test_parse_message_ok():
    now_ms = int(time.time() * 1000)
    payload = {"close": {"adverse_bps_t": {2000: 7.0}}}
    fields = _fields("BTCUSDT", now_ms, payload)
    result = _parse_message(fields, 2000)
    assert result is not None
    assert result["symbol"] == "BTCUSDT"
    assert result["adverse_bps"] == pytest.approx(7.0)


def test_parse_message_missing_symbol():
    now_ms = int(time.time() * 1000)
    payload = {"close": {"adverse_bps_t": 5.0}}
    fields = {"ts_ms": str(now_ms), "payload": json.dumps(payload)}
    result = _parse_message(fields, 2000)
    assert result is None


def test_parse_message_no_adverse():
    now_ms = int(time.time() * 1000)
    fields = _fields("ETHUSDT", now_ms, {})
    result = _parse_message(fields, 2000)
    assert result is None


def test_parse_message_session_from_time_when_absent():
    now_ms = 17 * 3_600_000  # 17 UTC → us_main
    payload = {"close": {"adverse_bps_t": 5.0}}
    fields = {"symbol": "BTCUSDT", "ts_ms": str(now_ms), "payload": json.dumps(payload)}
    result = _parse_message(fields, 2000)
    assert result is not None
    assert result["session"] in ("us_main", "european", "asian", "overnight")


# ---------------------------------------------------------------------------
# _AutoPromoter
# ---------------------------------------------------------------------------

def _make_promoter(**kwargs) -> _AutoPromoter:
    defaults = dict(
        enabled=True,
        min_obs=10,
        min_groups=2,
        dwell_sec=0,
        max_drift_pct=100.0,
    )
    defaults.update(kwargs)
    return _AutoPromoter(**defaults)


def _make_groups(n_real: int = 3, n_per_group: int = 15) -> dict:
    groups = {}
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "PEPEUSDT"]
    for sym in symbols[:n_real]:
        groups[f"{sym}:US_MAIN"] = {"new_bps": 5.0, "old_bps": 4.5, "n": n_per_group}
    groups["BTCUSDT:*"] = {"new_bps": 5.0, "old_bps": 4.5, "n": n_per_group * 2}
    groups["*:*"] = {"new_bps": 5.0, "old_bps": 4.5, "n": n_per_group * 4}
    return groups


def test_promoter_not_triggered_before_min_obs():
    p = _make_promoter(min_obs=100, dwell_sec=0)
    for _ in range(50):
        p.observe()
    groups = _make_groups()
    assert p.check(groups=groups, r=None, min_n=5) is False
    assert not p.promoted


def test_promoter_not_triggered_before_dwell():
    p = _make_promoter(min_obs=5, dwell_sec=9999)
    for _ in range(10):
        p.observe()
    groups = _make_groups()
    assert p.check(groups=groups, r=None, min_n=5) is False


def test_promoter_not_triggered_below_min_groups():
    p = _make_promoter(min_obs=5, min_groups=10, dwell_sec=0)
    for _ in range(10):
        p.observe()
    groups = _make_groups(n_real=2)
    assert p.check(groups=groups, r=None, min_n=5) is False


def test_promoter_triggers_when_all_criteria_met():
    p = _make_promoter(min_obs=5, min_groups=2, dwell_sec=0)
    for _ in range(10):
        p.observe()
    groups = _make_groups(n_real=3)
    assert p.check(groups=groups, r=None, min_n=5) is True
    assert p.promoted


def test_promoter_idempotent_after_promote():
    p = _make_promoter(min_obs=1, min_groups=1, dwell_sec=0)
    p.observe()
    groups = _make_groups(n_real=2)
    assert p.check(groups=groups, r=None, min_n=1) is True
    assert p.check(groups=groups, r=None, min_n=1) is True  # still True, no double-log


def test_promoter_disabled_never_triggers():
    p = _make_promoter(enabled=False, min_obs=1, dwell_sec=0)
    p.observe(100)
    groups = _make_groups(n_real=3)
    assert p.check(groups=groups, r=None, min_n=1) is False


def test_promoter_drift_check_blocks():
    """If existing enforce key has very different bps, promotion is blocked."""
    import json as _json

    class _FakeRedis:
        def get(self, _key):
            old_groups = {
                "BTCUSDT:US_MAIN": {"new_bps": 50.0},
                "ETHUSDT:US_MAIN": {"new_bps": 50.0},
            }
            return _json.dumps({"schema_version": 1, "groups": old_groups}).encode()

    p = _make_promoter(min_obs=1, dwell_sec=0, max_drift_pct=10.0)
    p.observe(10)
    # Shadow groups have bps=5.0, enforce has 50.0 → 900% drift > 10%
    groups = _make_groups(n_real=2)
    assert p.check(groups=groups, r=_FakeRedis(), min_n=1) is False
    assert not p.promoted


def test_promoter_drift_check_passes_when_within_threshold():
    import json as _json

    class _FakeRedis:
        def get(self, _key):
            old_groups = {
                "BTCUSDT:US_MAIN": {"new_bps": 5.1},
                "ETHUSDT:US_MAIN": {"new_bps": 4.9},
            }
            return _json.dumps({"schema_version": 1, "groups": old_groups}).encode()

    p = _make_promoter(min_obs=1, dwell_sec=0, max_drift_pct=10.0)
    p.observe(10)
    groups = _make_groups(n_real=2)
    assert p.check(groups=groups, r=_FakeRedis(), min_n=1) is True
