import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock
from services.smt_entry_policy_service import EntryPolicyService

@pytest.fixture
def anyio_backend():
    return 'asyncio'

@pytest.mark.anyio
async def test_active_arm_gate():
    """
    Verify _check_active_arm logic:
      - Reads Redis key cfg:entry_policy:active_arm:<group>
      - Compares with candidate['ab_arm']
      - Returns True if match (Active)
      - Returns False if mismatch (Shadow)
    """
    svc = EntryPolicyService()
    svc.r = AsyncMock()
    
    # Case 1: Default (Redis returns None -> A), Cand=A -> Active
    svc.r.get.return_value = None
    cand_a = {"ab_arm": "A", "regime": "range"}
    assert await svc._check_active_arm(cand_a) is True
    
    # Case 2: Redis=B, Cand=A -> Shadow
    svc.r.get.return_value = "B"
    cand_a = {"ab_arm": "A", "regime": "range"}
    assert await svc._check_active_arm(cand_a) is False
    
    # Case 3: Redis=B, Cand=B -> Active
    svc.r.get.return_value = "B"
    cand_b = {"ab_arm": "B", "regime": "range"}
    assert await svc._check_active_arm(cand_b) is True
    
    # Case 4: Regime Thin -> Group Thin
    # Redis for thin = C
    def side_effect(key):
        if "thin" in key:
            return "C"
        return "A"
    svc.r.get.side_effect = side_effect
    
    cand_thin_c = {"ab_arm": "C", "regime": "thin"}
    assert await svc._check_active_arm(cand_thin_c) is True
    
    cand_thin_a = {"ab_arm": "A", "regime": "thin"}
    assert await svc._check_active_arm(cand_thin_a) is False
    
    # Case 5: Fail Open (Exception) -> True
    svc.r.get.side_effect = Exception("Boom")
    assert await svc._check_active_arm(cand_a) is True
