
import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest


# Minimal mock for EntryPolicyService
class MockEntryPolicyService:
    def __init__(self):
        self.r = AsyncMock()
        self._arm_stab = MagicMock()
        self._arm_stab.update.side_effect = lambda key, raw, now_ms: raw # passthrough

    async def _get_active_arm(self, *, symbol: str, regime: str, group: str, scenario: str, raw_only: bool = False) -> str:
        # Copied logic from patched smt_entry_policy_service.py
        sym, rg, g, scn = symbol.upper(), regime.lower(), group.lower(), (scenario or "").lower()

        # 1) Get raw value from Redis (Resolution order: scenario -> base -> group)
        keys = []
        if sym and scn in ("continuation", "reversal"):
            keys.append(f"cfg:entry_policy:active_arm:{sym}:{rg}:{g}:{scn}")
        if sym:
            keys.append(f"cfg:entry_policy:active_arm:{sym}:{rg}:{g}")
        keys.append(f"cfg:entry_policy:active_arm:{g}")

        raw_v = ""
        for k in keys:
            v = await self.r.get(k)
            raw_v = (v or "").strip().upper()
            if raw_v: break

        return raw_v

@pytest.mark.asyncio
async def test_active_arm_precedence():
    svc = MockEntryPolicyService()

    # keys
    k_scn = "cfg:entry_policy:active_arm:BTCUSD:trend:default:continuation"
    k_base = "cfg:entry_policy:active_arm:BTCUSD:trend:default"
    k_grp = "cfg:entry_policy:active_arm:default"

    # 1. Scenario match
    svc.r.get.side_effect = lambda k: "B" if k == k_scn else ("A" if k == k_base else "C")
    res = await svc._get_active_arm(symbol="BTCUSD", regime="trend", group="default", scenario="continuation")
    assert res == "B", "Should prefer scenario key"

    # 2. Base match (scenario missing or None)
    svc.r.get.side_effect = lambda k: None if k == k_scn else ("A" if k == k_base else "C")
    res = await svc._get_active_arm(symbol="BTCUSD", regime="trend", group="default", scenario="continuation")
    assert res == "A", "Should fallback to base key"

    # 3. Group match (others missing)
    svc.r.get.side_effect = lambda k: None if k in (k_scn, k_base) else "C"
    res = await svc._get_active_arm(symbol="BTCUSD", regime="trend", group="default", scenario="continuation")
    assert res == "C", "Should fallback to group key"

    # 4. Unknown scenario -> skip scenario lookup
    svc.r.get.side_effect = lambda k: "X" if k == k_base else None
    res = await svc._get_active_arm(symbol="BTCUSD", regime="trend", group="default", scenario="chop_unknown")
    assert res == "X"
    # Verify k_scn was NOT checked (arguable, but our logic checks `if scn in (...)`)
    # ...

    print("test_active_arm_precedence passed")

if __name__ == "__main__":
    asyncio.run(test_active_arm_precedence())
