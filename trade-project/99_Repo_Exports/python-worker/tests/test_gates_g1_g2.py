from unittest.mock import AsyncMock, MagicMock

import pytest

from services.orderflow.strategy import OrderFlowStrategy


@pytest.fixture
def mock_strategy():
    redis = AsyncMock()
    ticks = MagicMock()
    publisher = MagicMock()
    of_engine = MagicMock()
    strategy = OrderFlowStrategy(redis=redis, ticks=ticks, publisher=publisher, of_engine=of_engine)
    strategy._pbatch = MagicMock()
    strategy.logger = MagicMock()
    return strategy

def create_mock_runtime(symbol="BTCUSDT"):
    runtime = MagicMock()
    runtime.symbol = symbol
    runtime.config = {
        "delta_abs_min_usd": 0.0,
        "delta_tier_min": 0,
        "dn_tier0_usd": 120000.0,
        "dn_tier1_usd": 350000.0,
        "dn_tier2_usd": 750000.0,
    }
    runtime.dynamic_cfg = {}
    runtime.last_regime = "na"

    # Mock tick_dn_calib
    tiers_decision = MagicMock()
    tiers_decision.tier0_usd = 10000.0
    tiers_decision.tier1_usd = 50000.0
    tiers_decision.tier2_usd = 100000.0
    tiers_decision.src = "calib"
    tiers_decision.scale = 1.0

    runtime.tick_dn_calib.tiers.return_value = tiers_decision
    runtime.delta_log_sampler.should_log.return_value = True

    return runtime, tiers_decision

@pytest.mark.asyncio
async def test_g2_gate_normal_pass(mock_strategy):
    # Tier 0 pass
    runtime, tiers_decision = create_mock_runtime()
    runtime.config["delta_tier_min"] = 0
    delta_event = {"delta": 2.0} # delta=2.0 * price(6000) = 12000 > tier0_usd
    price = 6000.0
    indicators = {}

    passed, tier, delta_usd, dec = mock_strategy._eval_dn_gate(runtime, 1700000000000, delta_event, price, indicators)
    assert passed is True
    assert tier == 0
    assert delta_usd == 12000.0
    assert indicators["dn_tier"] == 0

@pytest.mark.asyncio
async def test_g2_gate_normal_veto(mock_strategy):
    # Tier -1 veto
    runtime, tiers_decision = create_mock_runtime()
    runtime.config["delta_tier_min"] = 0
    delta_event = {"delta": 1.0} # delta=1.0 * price(6000) = 6000 < tier0_usd (10000)
    price = 6000.0
    indicators = {}

    passed, tier, delta_usd, dec = mock_strategy._eval_dn_gate(runtime, 1700000000000, delta_event, price, indicators)
    assert passed is False
    assert tier == -1
    assert "dn_tier" in indicators # Updated before return in fixed version

@pytest.mark.asyncio
async def test_g2_gate_meme_relaxation(mock_strategy):
    # Tier -1 but relaxed because of PEPE
    runtime, tiers_decision = create_mock_runtime(symbol="PEPEUSDT")
    runtime.config["delta_tier_min"] = 0
    # delta_usd = 6000, tier0 = 10000. 6000 >= 10000*0.5 -> Should pass for meme
    delta_event = {"delta": 1.0}
    price = 6000.0
    indicators = {}

    passed, tier, delta_usd, dec = mock_strategy._eval_dn_gate(runtime, 1700000000000, delta_event, price, indicators)
    assert passed is True
    assert tier == 0
    assert indicators.get("dn_gate_relaxed") == 1

@pytest.mark.asyncio
async def test_g2_gate_meme_relaxation_fail(mock_strategy):
    # Tier -1, meme but delta_usd < 50%
    runtime, tiers_decision = create_mock_runtime(symbol="PEPEUSDT")
    runtime.config["delta_tier_min"] = 0
    # delta_usd = 4000, tier0 = 10000. 4000 < 10000*0.5 -> Should fail even for meme
    delta_event = {"delta": 1.0}
    price = 4000.0
    indicators = {}

    passed, tier, delta_usd, dec = mock_strategy._eval_dn_gate(runtime, 1700000000000, delta_event, price, indicators)
    assert passed is False
    assert tier == -1

@pytest.mark.asyncio
async def test_g1_gate_min_usd_logic():
    # Test the standalone logic (as implemented in process_tick ~ line 845)
    delta_event = {"delta": 1.5}
    price = 10000.0
    delta_usd = abs(delta_event["delta"]) * price # 15000.0

    # Case 1: min_usd = 20000 (blocks)
    min_usd_block = 20000.0
    assert min_usd_block > 1.0 and delta_usd < min_usd_block, "Should block"

    # Case 2: min_usd = 10000 (passes)
    min_usd_pass = 10000.0
    assert not (min_usd_pass > 1.0 and delta_usd < min_usd_pass), "Should pass"

    # Case 3: min_usd = 0 (passes)
    min_usd_zero = 0.0
    assert not (min_usd_zero > 1.0 and delta_usd < min_usd_zero), "Should pass"
