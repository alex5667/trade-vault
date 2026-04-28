import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
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
    
    reader = CoinGeckoSnapshotReader(redis_client=mock_redis)
    
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
    
    # Test synchronous fetch (cache stale)
    now_ms_stale = 1000000 + 400000 # 400s later (stale, max 300s)
    ind_btc_stale = reader.get_snapshot("BTCUSDT", now_ms_stale)
    
    assert "cg_global_mcap_usd" not in ind_btc_stale
    assert "cg_symbol_market_cap_usd" not in ind_btc_stale
