import pytest
import json
import asyncio
from unittest.mock import AsyncMock, patch
from services.ab_winner_suggester_service_v3 import ABWinnerSuggesterV3

@pytest.mark.asyncio
async def test_winner_selection_logic():
    svc = ABWinnerSuggesterV3()
    svc.r = AsyncMock()
    
    # Setup matrix
    svc.matrix = "BTCUSDT|range|default|reversal"
    
    # 1. No LCB data -> No suggestion
    svc.r.get.side_effect = lambda k: None
    await svc.tick_once()
    assert svc.r.set.call_count == 0

    # 2. Data for A and B, but margin not met
    # cur=A, lcb(A)=0.1, lcb(B)=0.12 (margin=0.05) -> No change
    svc.r.get.side_effect = lambda k: {
        "cfg:entry_policy:active_arm:BTCUSDT:range:default:reversal": "A",
        "cfg:entry_policy:active_arm:BTCUSDT:range:default": None,
        "cfg:entry_policy:active_arm:default": "A",
        "cfg:entry_policy:lcb:v1:snap:BTCUSDT:range:default:reversal:A": json.dumps({"n": 50, "lcb": 0.1}),
        "cfg:entry_policy:lcb:v1:snap:BTCUSDT:range:default:reversal:B": json.dumps({"n": 50, "lcb": 0.12}),
        "cfg:entry_policy:lcb:v1:snap:BTCUSDT:range:default:reversal:C": None,
    }.get(k)
    
    await svc.tick_once()
    assert svc.r.set.call_count == 0

    # 3. Data for A and B, margin MET (lcb(B)=0.16 > 0.1 + 0.05)
    svc.r.get.side_effect = lambda k: {
        "cfg:entry_policy:active_arm:BTCUSDT:range:default:reversal": "A",
        "cfg:entry_policy:lcb:v1:snap:BTCUSDT:range:default:reversal:A": json.dumps({"n": 50, "lcb": 0.1}),
        "cfg:entry_policy:lcb:v1:snap:BTCUSDT:range:default:reversal:B": json.dumps({"n": 50, "lcb": 0.16}),
        "cfg:entry_policy:lcb:v1:snap:BTCUSDT:range:default:reversal:C": None,
    }.get(k)
    
    n = await svc.tick_once()
    assert n == 1
    # Check that it tried to set the meta/approvals/latest
    assert svc.r.set.call_count >= 1

    # 4. Min N not met for Winner
    svc.r.get.side_effect = lambda k: {
        "cfg:entry_policy:active_arm:BTCUSDT:range:default:reversal": "A",
        "cfg:entry_policy:lcb:v1:snap:BTCUSDT:range:default:reversal:A": json.dumps({"n": 50, "lcb": 0.1}),
        "cfg:entry_policy:lcb:v1:snap:BTCUSDT:range:default:reversal:B": json.dumps({"n": 10, "lcb": 0.2}), # n < 30
    }.get(k)
    
    svc.r.set.reset_mock()
    await svc.tick_once()
    assert svc.r.set.call_count == 0

async def run_test():
    await test_winner_selection_logic()
    print("✅ test_winner_selection_logic passed")

if __name__ == "__main__":
    asyncio.run(run_test())
