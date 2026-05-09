
import dataclasses

import pytest


# Mock dependencies
class FakeRedis:
    def __init__(self, data: dict[str, str]):
        self.data = data
        self.calls = []

    async def get(self, key: str) -> str | None:
        self.calls.append(("get", key))
        return self.data.get(key)

@dataclasses.dataclass
class FakeCfg:
    active_arm_cache_ttl_ms: int = 1000
    # Other fields needed by EntryPolicyService.__init__?
    # The real init calls os.getenv heavily.
    # To test *logic* of _get_active_arm easily, we can subclass EntryPolicyService
    # or just monkeypatch a bare instance.

@pytest.fixture
def entry_policy_service_harness():
    # We import the genuine class but bypass __init__ to avoid side effects/redis connections
    from services.smt_entry_policy_service import EntryPolicyService

    class TestEntryPolicyService(EntryPolicyService):
        def __init__(self):
            # Bypass super().__init__ which connects to Redis
            self.r = None
            self._active_arm_cache_ts = {} # Not used in new logic directly for fallback,
                                           # but defined in __init__ usually?
                                           # Wait, the new logic REMOVED the cache usage in _get_active_arm!
                                           # It now depends on ActiveArmStabilizer.
            self.cfg = FakeCfg()
            # We need to mock ActiveArmStabilizer
            from core.active_arm_stabilizer import ActiveArmStabilizer
            self._arm_stab = ActiveArmStabilizer(hold_down_ms=0, min_switch_gap_ms=0)

    svc = TestEntryPolicyService()
    return svc


import asyncio


def test_active_arm_fallback_order_full(entry_policy_service_harness):
    svc = entry_policy_service_harness
    svc.r = FakeRedis({
        "cfg:entry_policy:active_arm:BTCUSDT:range:default:continuation": "B",
        "cfg:entry_policy:active_arm:BTCUSDT:range:default": "C",
        "cfg:entry_policy:active_arm:default": "A",
    })

    async def run():
        v = await svc._get_active_arm(
            symbol="BTCUSDT", regime="range", group="default", scenario="continuation"
        )
        assert v == "B"
    asyncio.run(run())

def test_active_arm_fallback_to_pooled(entry_policy_service_harness):
    svc = entry_policy_service_harness
    svc.r = FakeRedis({
        "cfg:entry_policy:active_arm:BTCUSDT:range:default": "C",
        "cfg:entry_policy:active_arm:default": "A",
    })

    async def run():
        v = await svc._get_active_arm(
            symbol="BTCUSDT", regime="range", group="default", scenario="reversal"
        )
        assert v == "C"
    asyncio.run(run())

def test_active_arm_fallback_to_group(entry_policy_service_harness):
    svc = entry_policy_service_harness
    svc.r = FakeRedis({
        "cfg:entry_policy:active_arm:thin": "B",
    })

    async def run():
        v = await svc._get_active_arm(
            symbol="ETHUSDT", regime="thin", group="thin", scenario="continuation"
        )
        assert v == "B"
    asyncio.run(run())

def test_active_arm_fail_open(entry_policy_service_harness):
    svc = entry_policy_service_harness
    svc.r = FakeRedis({})

    async def run():
        v = await svc._get_active_arm(
            symbol="ETHUSDT", regime="range", group="default", scenario="continuation"
        )
        assert v == ""
    asyncio.run(run())

def test_active_arm_invalid_scenario_fallback(entry_policy_service_harness):
    svc = entry_policy_service_harness
    svc.r = FakeRedis({
        "cfg:entry_policy:active_arm:BTCUSDT:range:default": "C",
    })

    async def run():
        v = await svc._get_active_arm(
            symbol="BTCUSDT", regime="range", group="default", scenario="strange_scenario"
        )
        assert v == "C"
    asyncio.run(run())


