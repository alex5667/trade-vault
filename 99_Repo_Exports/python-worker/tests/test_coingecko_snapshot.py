from unittest.mock import AsyncMock, MagicMock

import pytest

from core.coingecko_snapshot import CoinGeckoSnapshotReader


@pytest.mark.asyncio
async def test_coingecko_snapshot_reader():
    # Setup mock redis
    mock_redis = AsyncMock()
    mock_redis.hgetall = AsyncMock(return_value={
        b"ts_ms": b"1000000",
        b"total_mcap_usd": b"1500000.0",
        b"btc_dom_pct": b"55.5",
        b"stable_dom_mom": b"0.12"
    })

    mock_redis.scan = AsyncMock(side_effect=[
        (b"0", [b"runtime:coingecko:market:BTCUSDT", b"runtime:coingecko:market:ETHUSDT"]),
        (b"0", []),
        (b"0", [])
    ])

    # Setup pipeline mock
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[
        {b"ts_ms": b"1000000", b"market_cap_usd": b"800000.0", b"rel_strength_btc_1h": b"0.0"},
        {b"ts_ms": b"1000000", b"market_cap_usd": b"300000.0", b"rel_strength_btc_1h": b"2.5"}
    ])
    mock_redis.pipeline = MagicMock(return_value=mock_pipe)

    reader = CoinGeckoSnapshotReader(redis_client=mock_redis, max_stale_ms=300_000)

    # Trigger refresh directly instead of polling loop
    await reader._refresh_cache()

    # Assert global populated
    assert reader._global["total_mcap_usd"] == "1500000.0"

    # Assert markets populated
    assert "BTCUSDT" in reader._markets
    assert reader._markets["ETHUSDT"]["rel_strength_btc_1h"] == "2.5"

    # Test synchronous fetch (cache hit)
    now_ms = 1000500 # 500ms later (valid)
    ind_btc = reader.get_snapshot("BTCUSDT", now_ms)

    assert ind_btc["cg_global_mcap_usd"] == 1500000.0
    assert ind_btc["cg_btc_dom_pct"] == 55.5
    assert ind_btc["cg_symbol_market_cap_usd"] == 800000.0

    # Assert new quality fields
    assert ind_btc["cg_status"] == "ok"
    assert ind_btc["cg_quality"] == 1.0

    # Test synchronous fetch (cache stale)
    now_ms_stale = 1000000 + 400000 # 400s later (stale, max 300s)
    ind_btc_stale = reader.get_snapshot("BTCUSDT", now_ms_stale)

    assert "cg_global_mcap_usd" not in ind_btc_stale
    assert "cg_symbol_market_cap_usd" not in ind_btc_stale
    
    assert ind_btc_stale["cg_status"] == "stale"
    assert ind_btc_stale["cg_quality"] == 0.5
    assert ind_btc_stale["cg_liquidity_status"] == "disabled"

@pytest.mark.asyncio
async def test_reader_uses_max_fresh_age_ms_from_redis():
    mock_redis = AsyncMock()
    mock_redis.hgetall = AsyncMock(side_effect=lambda key: {
        b"ts_ms": b"1000000",
        b"max_fresh_age_ms": b"1200000", # 1200s
        b"total_mcap_usd": b"1500000.0"
    } if "global" in key else {})
    mock_redis.scan = AsyncMock(return_value=(b"0", []))
    
    reader = CoinGeckoSnapshotReader(redis_client=mock_redis, max_stale_ms=300_000)
    await reader._refresh_cache()
    
    # 500s later (stale by default 300s, but max_fresh_age_ms is 1200s)
    ind = reader.get_snapshot("BTCUSDT", 1000000 + 500000)
    assert ind["cg_status"] == "ok"
    assert ind["cg_quality"] == 1.0

    # 1500s later (stale by max_fresh_age_ms)
    ind_stale = reader.get_snapshot("BTCUSDT", 1000000 + 1500000)
    assert ind_stale["cg_status"] == "stale"
    assert ind_stale["cg_quality"] == 0.5
    
@pytest.mark.asyncio
async def test_reader_suppresses_global_features_after_max_fresh_age_ms():
    mock_redis = AsyncMock()
    mock_redis.hgetall = AsyncMock(side_effect=lambda key: {
        b"ts_ms": b"1000000",
        b"max_fresh_age_ms": b"300000",
        b"total_mcap_usd": b"1500000.0",
        b"btc_dom_pct": b"55.5",
        b"stable_dom_mom": b"0.12",
    } if "global" in key else {})
    mock_redis.scan = AsyncMock(return_value=(b"0", []))

    reader = CoinGeckoSnapshotReader(redis_client=mock_redis, max_stale_ms=86_400_000)
    await reader._refresh_cache()

    ind = reader.get_snapshot("BTCUSDT", 1000000 + 400000)

    assert ind["cg_status"] == "stale"
    assert ind["cg_quality"] == 0.5
    assert "cg_global_mcap_usd" not in ind
    assert "cg_btc_dom_pct" not in ind
    assert "cg_stable_dom_mom" not in ind

@pytest.mark.asyncio
async def test_reader_suppresses_market_features_after_max_fresh_age_ms():
    mock_redis = AsyncMock()
    mock_redis.hgetall = AsyncMock(side_effect=lambda key: {
        b"ts_ms": b"1000000",
        b"max_fresh_age_ms": b"300000",
        b"total_mcap_usd": b"1500000.0"
    } if "global" in key else {})
    
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[
        {b"ts_ms": b"1000000", b"max_fresh_age_ms": b"300000", b"market_cap_usd": b"800000.0", b"rel_strength_btc_1h": b"2.5"}
    ])
    mock_redis.pipeline = MagicMock(return_value=mock_pipe)
    mock_redis.scan = AsyncMock(side_effect=[
        (b"0", [b"runtime:coingecko:market:BTCUSDT"]),
        (b"0", []), (b"0", [])
    ])

    reader = CoinGeckoSnapshotReader(redis_client=mock_redis, max_stale_ms=86_400_000)
    await reader._refresh_cache()

    ind = reader.get_snapshot("BTCUSDT", 1000000 + 400000)

    assert ind["cg_status"] == "stale"
    assert ind["cg_quality"] == 0.5
    assert "cg_symbol_market_cap_usd" not in ind
    assert "cg_symbol_rel_strength_btc_1h" not in ind

@pytest.mark.asyncio
async def test_429_circuit_open_sets_cg_status():
    mock_redis = AsyncMock()
    mock_redis.hgetall = AsyncMock(side_effect=lambda key: {
        b"status": b"open",
        b"reason": b"provider_429"
    } if "circuit:status" in key else {
        b"ts_ms": b"1000000",
        b"total_mcap_usd": b"1500000.0"
    })
    mock_redis.scan = AsyncMock(return_value=(b"0", []))
    
    reader = CoinGeckoSnapshotReader(redis_client=mock_redis, max_stale_ms=300_000)
    await reader._refresh_cache()
    
    ind = reader.get_snapshot("BTCUSDT", 1000500)
    assert ind["cg_status"] == "circuit_open"
    assert ind["cg_quality"] <= 0.3
    assert ind["cg_reason"] == "provider_429"
