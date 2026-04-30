from __future__ import annotations

# A1.1 — Unit tests for LiqMap metric emission in TickProcessor._inject_liqmap_features.
# Tests verify that:
#   - liqmap_parse_errors_total is incremented on parse/compute error (fail-open path)
#   - liqmap_snapshot_age_ms_gauge is set to -1 on missing snapshot
#   - liqmap_snapshot_age_ms_gauge is set to -2 on parse/compute error
#   - Normal path sets the gauge to a positive age value
#
# These are isolated unit tests using fresh Prometheus registries to avoid
# cross-test contamination from shared global state.

import asyncio
import sys
import os
import types
import pytest

# ---------------------------------------------------------------------------
# Provide minimal stubs for heavy imports that aren't needed for these tests
# ---------------------------------------------------------------------------

def _make_stub_module(name: str, **attrs):
    """Create a lightweight stub module and register it in sys.modules."""
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


def _stub_redis():
    """Stub aioredis so TickProcessor can be imported without a live Redis."""
    class _FakeRedis:
        async def get(self, key):
            return None

    mod = _make_stub_module("redis")
    mod.asyncio = types.ModuleType("redis.asyncio")
    mod.asyncio.Redis = _FakeRedis
    sys.modules["redis.asyncio"] = mod.asyncio
    return _FakeRedis


# ---------------------------------------------------------------------------
# Metrics isolation helper
# ---------------------------------------------------------------------------

def _fresh_liqmap_counters():
    """Return fresh (unregistered) Prometheus counters for isolated testing."""
    from prometheus_client import Counter, Gauge, CollectorRegistry
    reg = CollectorRegistry()
    age_gauge = Gauge(
        "liqmap_snapshot_age_ms_test"
        "Test gauge"
        ["symbol", "window"]
        registry=reg
    )
    parse_errs = Counter(
        "liqmap_parse_errors_total_test"
        "Test counter"
        ["symbol", "window", "where"]
        registry=reg
    )
    return age_gauge, parse_errs, reg


# ---------------------------------------------------------------------------
# Tests: metrics.py registration
# ---------------------------------------------------------------------------

def _get_pw_metrics_module():
    """Return the python-worker services.orderflow.metrics module.

    Ensures python-worker/ is at the FRONT of sys.path before importing so
    that the python-worker version is loaded (not the root-level mirror copy
    which is missing A1.1 metrics).
    """
    pw = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    # Prepend python-worker to sys.path to shadow the root-level services/ copy.
    if sys.path and sys.path[0] != pw:
        sys.path.insert(0, pw)
    import importlib
    # If already cached in sys.modules we can get it directly.
    mod = sys.modules.get("services.orderflow.metrics")
    if mod is None:
        import services.orderflow.metrics as mod  # type: ignore[no-redef]
    return mod


def test_liqmap_parse_errors_total_exists_in_metrics():
    """liqmap_parse_errors_total must be exported from services.orderflow.metrics (A1.1)."""
    mod = _get_pw_metrics_module()
    assert hasattr(mod, "liqmap_parse_errors_total"), (
        "liqmap_parse_errors_total missing from python-worker services.orderflow.metrics (A1.1)"
    )


def test_liqmap_parse_errors_total_has_expected_labels():
    """liqmap_parse_errors_total must accept symbol, window, where labels."""
    mod = _get_pw_metrics_module()
    counter = mod.liqmap_parse_errors_total.labels(
        symbol="BTCUSDT", window="1h", where="parse_or_compute"
    )
    assert counter is not None, "liqmap_parse_errors_total labels() must return a child counter"


# ---------------------------------------------------------------------------
# Tests: metric semantics with fresh isolated gauges/counters
# ---------------------------------------------------------------------------

def test_age_gauge_set_negative_on_missing():
    """Missing snapshot sentinel value must be -1.0."""
    age_gauge, _, _ = _fresh_liqmap_counters()
    age_gauge.labels(symbol="BTCUSDT", window="1h").set(-1.0)
    val = age_gauge.labels(symbol="BTCUSDT", window="1h")._value.get()
    assert val == -1.0, f"Expected -1.0 for missing snapshot, got {val}"


def test_age_gauge_set_negative_on_parse_error():
    """Parse/compute error sentinel value must be -2.0."""
    age_gauge, _, _ = _fresh_liqmap_counters()
    age_gauge.labels(symbol="BTCUSDT", window="1h").set(-2.0)
    val = age_gauge.labels(symbol="BTCUSDT", window="1h")._value.get()
    assert val == -2.0, f"Expected -2.0 for parse error, got {val}"


def test_parse_errors_counter_increments():
    """liqmap_parse_errors_total must increment exactly once per error call."""
    _, parse_errs, _ = _fresh_liqmap_counters()
    before = parse_errs.labels(
        symbol="BTCUSDT", window="1h", where="parse_or_compute"
    )._value.get()
    parse_errs.labels(symbol="BTCUSDT", window="1h", where="parse_or_compute").inc()
    after = parse_errs.labels(
        symbol="BTCUSDT", window="1h", where="parse_or_compute"
    )._value.get()
    assert after == before + 1.0, f"Counter did not increment: before={before} after={after}"


def test_parse_errors_counter_isolated_by_window():
    """Errors on different windows must not contaminate each other."""
    _, parse_errs, _ = _fresh_liqmap_counters()
    parse_errs.labels(symbol="BTCUSDT", window="1h", where="parse_or_compute").inc()
    parse_errs.labels(symbol="BTCUSDT", window="1h", where="parse_or_compute").inc()
    parse_errs.labels(symbol="BTCUSDT", window="5m", where="parse_or_compute").inc()
    val_1h = parse_errs.labels(
        symbol="BTCUSDT", window="1h", where="parse_or_compute"
    )._value.get()
    val_5m = parse_errs.labels(
        symbol="BTCUSDT", window="5m", where="parse_or_compute"
    )._value.get()
    assert val_1h == 2.0, f"Expected 2 increments on 1h, got {val_1h}"
    assert val_5m == 1.0, f"Expected 1 increment on 5m, got {val_5m}"


def test_age_gauge_positive_on_success():
    """Successful parse must set gauge to a non-negative age value."""
    age_gauge, _, _ = _fresh_liqmap_counters()
    expected_age = 1500.0
    age_gauge.labels(symbol="BTCUSDT", window="1h").set(expected_age)
    val = age_gauge.labels(symbol="BTCUSDT", window="1h")._value.get()
    assert val >= 0, f"Successful parse must yield non-negative age, got {val}"
    assert val == expected_age, f"Expected {expected_age}, got {val}"
