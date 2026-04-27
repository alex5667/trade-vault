"""Tests for observability metrics registry."""

from services.observability.metrics_registry import (
    atr_bad_active,
    atr_bad_total,
    cvd_quarantine_active,
    delta_fallback_mode,
    lcb_margin,
    lcb_winner_changes_total,
    microbar_stream_xlen,
    microbar_symbols_active,
    ml_missing_critical_total,
    of_engine_build_seconds,
    redis_used_memory_mb,
)


def test_metrics_registry_imports():
    """Test that all metrics can be imported."""
    assert atr_bad_total is not None
    assert atr_bad_active is not None
    assert cvd_quarantine_active is not None
    assert delta_fallback_mode is not None
    assert microbar_stream_xlen is not None
    assert microbar_symbols_active is not None
    assert redis_used_memory_mb is not None
    assert of_engine_build_seconds is not None
    assert ml_missing_critical_total is not None
    assert lcb_winner_changes_total is not None
    assert lcb_margin is not None


def test_atr_bad_total_counter():
    """Test ATR bad counter can be incremented."""
    atr_bad_total.labels(symbol="BTCUSDT", reason="sanity_fail").inc()
    # Just verify it doesn't raise
    assert True


def test_microbar_stream_xlen_gauge():
    """Test microbar stream xlen gauge can be set."""
    microbar_stream_xlen.labels(stream="events:microbar_closed").set(100.0)
    # Just verify it doesn't raise
    assert True


def test_redis_used_memory_mb_gauge():
    """Test Redis memory gauge can be set."""
    redis_used_memory_mb.set(5000.0)
    # Just verify it doesn't raise
    assert True

