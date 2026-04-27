from __future__ import annotations

from core.regime_quantiles_redis import parse_rq


def test_parse_rq_ok():
    """Verify successful parsing of valid quantiles."""
    raw = '{"symbol":"BTCUSDT","timeframe":"1m","sampleSize":1000,"adx_p40":10,"adx_p60":15,"adx_p75":22,"atrp_p25":0.001,"atrp_p50":0.002,"atrp_p75":0.003,"updatedAtMs":1}'
    rq = parse_rq(raw)
    assert rq is not None
    assert rq.symbol == "BTCUSDT"
    assert rq.sample_size == 1000
    assert rq.atrp_p25 <= rq.atrp_p50 <= rq.atrp_p75


def test_parse_rq_reject_non_monotonic():
    """Verify rejection of non-monotonic quantiles."""
    raw = '{"symbol":"BTCUSDT","timeframe":"1m","sampleSize":1000,"atrp_p25":0.003,"atrp_p50":0.002,"atrp_p75":0.001}'
    assert parse_rq(raw) is None


def test_parse_rq_reject_zero_samples():
    """Verify rejection of zero sample size."""
    raw = '{"symbol":"BTCUSDT","timeframe":"1m","sampleSize":0,"atrp_p25":0.001,"atrp_p50":0.002,"atrp_p75":0.003}'
    assert parse_rq(raw) is None


def test_parse_rq_reject_invalid_json():
    """Verify rejection of invalid JSON."""
    assert parse_rq("not json") is None
