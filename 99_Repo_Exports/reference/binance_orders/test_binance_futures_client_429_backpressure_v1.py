"""Tests for BinanceFuturesClient 429 backpressure and mark-price cache.

Tests:
  1. Mark price cache returns cached value within TTL window.
  2. Mark price cache expires after TTL and re-fetches.
  3. get_mark_price re-raises BinanceAPIError(429) instead of swallowing.
  4. get_mark_price swallows non-429 BinanceAPIError (fail-open).
"""
from pathlib import Path
import importlib.util
import sys
import time

mod_path = Path(__file__).parent.parent / "services" / "binance_futures_client.py"
spec = importlib.util.spec_from_file_location("binance_futures_client", mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)


BinanceFuturesClient = mod.BinanceFuturesClient
BinanceAPIError = mod.BinanceAPIError


def _make_client():
    """Create a client with short cache TTL for testing."""
    c = BinanceFuturesClient(api_key="k", api_secret="s")
    c._mark_price_cache_ttl_s = 0.2  # 200ms TTL for fast tests
    return c


def test_mark_price_cache_deduplicates_rapid_calls():
    """Two rapid calls should hit get_premium_index only once."""
    client = _make_client()
    call_count = 0

    def fake_premium_index(symbol):
        nonlocal call_count
        call_count += 1
        return {"markPrice": "50000.00"}

    client.get_premium_index = fake_premium_index

    p1 = client.get_mark_price("BTCUSDT")
    p2 = client.get_mark_price("BTCUSDT")

    assert p1 == 50000.0
    assert p2 == 50000.0
    assert call_count == 1, f"Expected 1 API call, got {call_count}"


def test_mark_price_cache_expires_after_ttl():
    """After cache TTL expires, a new API call is made."""
    client = _make_client()
    call_count = 0

    def fake_premium_index(symbol):
        nonlocal call_count
        call_count += 1
        return {"markPrice": str(50000 + call_count)}

    client.get_premium_index = fake_premium_index

    p1 = client.get_mark_price("BTCUSDT")
    assert call_count == 1

    time.sleep(0.25)  # exceed 200ms TTL

    p2 = client.get_mark_price("BTCUSDT")
    assert call_count == 2
    assert p2 != p1, "Should have fetched a new price after TTL expired"


def test_mark_price_reraises_429():
    """get_mark_price must propagate 429 BinanceAPIError to callers."""
    client = _make_client()

    def fake_premium_index(symbol):
        raise BinanceAPIError(429, {"code": -1003, "msg": "Too many requests"})

    client.get_premium_index = fake_premium_index

    raised = False
    try:
        client.get_mark_price("BTCUSDT")
    except BinanceAPIError as e:
        raised = True
        assert e.status == 429
    assert raised, "Expected BinanceAPIError(429) to propagate"


def test_mark_price_swallows_non_429_api_error():
    """get_mark_price must swallow non-429 BinanceAPIError (fail-open → 0.0)."""
    client = _make_client()

    def fake_premium_index(symbol):
        raise BinanceAPIError(500, {"code": -1001, "msg": "Internal error"})

    client.get_premium_index = fake_premium_index

    result = client.get_mark_price("BTCUSDT")
    assert result == 0.0, f"Expected 0.0 for non-429 error, got {result}"


def test_mark_price_cache_per_symbol():
    """Cache is keyed by symbol — different symbols get separate entries."""
    client = _make_client()
    calls = []

    def fake_premium_index(symbol):
        calls.append(symbol)
        prices = {"BTCUSDT": "50000.0", "ETHUSDT": "3000.0"}
        return {"markPrice": prices.get(symbol, "100.0")}

    client.get_premium_index = fake_premium_index

    btc = client.get_mark_price("BTCUSDT")
    eth = client.get_mark_price("ETHUSDT")
    btc2 = client.get_mark_price("BTCUSDT")  # cached

    assert btc == 50000.0
    assert eth == 3000.0
    assert btc2 == 50000.0
    assert len(calls) == 2, f"Expected 2 API calls (BTC+ETH), got {len(calls)}: {calls}"
