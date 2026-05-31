import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.ab_winner_suggester_service_v3 import ABWinnerSuggesterV3
from core.lcb_evaluator import ArmAgg


def _make_svc():
    """Create ABWinnerSuggesterV3 bypassing real Redis init."""
    svc = ABWinnerSuggesterV3.__new__(ABWinnerSuggesterV3)
    svc.r = AsyncMock()
    svc.events_stream = "events:trades"
    svc.audit_stream = "cfg:suggestions:entry_policy:stream"
    svc.latest_prefix = "cfg:suggestions:entry_policy:latest:ab_winner"
    svc.meta_prefix = "cfg:suggestions:entry_policy:meta"
    svc.approvals_required = 2
    svc.cursor_key = "state:ab_winner_suggester_v3:cursor"
    svc._last_id = "0-0"
    svc._agg = {}
    svc._seen_keys = {}
    svc.eval_every_sec = 3600
    svc.lookback_hours = 168
    # Mock lock to always succeed (bypass Redis)
    lock = MagicMock()
    lock.acquire = AsyncMock(return_value=True)
    lock.release = AsyncMock()
    svc.lock = lock
    return svc


@pytest.mark.asyncio
async def test_winner_selection_no_data():
    """No in-memory aggregation data -> No proposals emitted."""
    svc = _make_svc()
    assert svc._agg == {}

    n = await svc.evaluate_once()
    assert n == 0
    # Only lock ops, no set for proposals
    proposal_sets = [c for c in svc.r.set.call_args_list if "suggestions" in str(c)]
    assert len(proposal_sets) == 0


@pytest.mark.asyncio
async def test_winner_selection_insufficient_samples():
    """When min_n not met, no winner proposal should be emitted."""
    svc = _make_svc()

    # Only 5 samples — well below min_n (which is 30 in default regime)
    k = ("BTCUSDT", "range", "default", "reversal")
    svc._agg[k] = {"A": ArmAgg(), "B": ArmAgg()}
    for _ in range(5):
        svc._agg[k]["A"].add(0.1)
        svc._agg[k]["B"].add(0.2)

    n = await svc.evaluate_once()
    assert n == 0


@pytest.mark.asyncio
async def test_winner_selection_winner_found():
    """When arm B clearly beats A with sufficient samples, a proposal is emitted."""
    svc = _make_svc()

    k = ("BTCUSDT", "range", "default", "reversal")
    svc._agg[k] = {"A": ArmAgg(), "B": ArmAgg()}
    for _ in range(50):
        svc._agg[k]["A"].add(0.05)  # low R
    for _ in range(50):
        svc._agg[k]["B"].add(0.35)  # high R

    # Mock _emit_proposal to return a sid (indicating emission happened)
    svc._emit_proposal = AsyncMock(return_value="sid-test-123")

    n = await svc.evaluate_once()
    assert n == 1
    svc._emit_proposal.assert_called_once()


async def run_test():
    await test_winner_selection_no_data()
    await test_winner_selection_insufficient_samples()
    await test_winner_selection_winner_found()
    print("✅ All AB winner selection tests passed")

if __name__ == "__main__":
    asyncio.run(run_test())
