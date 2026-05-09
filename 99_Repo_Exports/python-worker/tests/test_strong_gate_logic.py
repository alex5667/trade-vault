from __future__ import annotations

from unittest.mock import MagicMock, patch

from services.crypto_orderflow_service import CryptoOrderflowService
from services.orderflow.configuration import DEFAULT_CONFIG
from services.orderflow.runtime import SymbolRuntime
from utils.time_utils import get_ny_time_millis


# Mock class instead of importing to avoid complex dependencies
class MicroStructureSpikeDetector:
    pass

# Mock objects needed for _handle_tick
class MockDeltaDetector:
    def __init__(self):
        self.z_threshold = 2.0

    def push(self, tick):
        # Always return a mock delta spike to trigger evaluation logic
        return {"delta": 100.0, "z": 3.0}

class MockOBIDetector:
    def push(self, book):
        pass

class MockIcebergDetector:
    def push(self, book):
        pass

class MockMicroBar:
    def push_tick(self, tick, cvd):
        return []

class MockWeakProgress:
    weak_any = False
    range_atr = 0.0
    body_atr = 0.0
    eff = 0.0

# Mock evaluation function to intercept the call
def mock_eval_reversal(**kwargs):
    # Just return the args to verify what was passed
    return kwargs

def test_strong_gate_uses_computed_flags():
    # Setup service & runtime
    service = CryptoOrderflowService(redis_dsn="redis://localhost", ticks_dsn="redis://localhost")
    service.logger = None # Just to avoid errors if logger used

    runtime = SymbolRuntime(symbol="BTCUSDT", config=DEFAULT_CONFIG.copy())
    runtime.delta_detector = MockDeltaDetector()
    runtime.obi_detector = MockOBIDetector()
    runtime.iceberg_detector = MockIcebergDetector()
    runtime.microbar = MockMicroBar()
    runtime.last_wp = MockWeakProgress()

    # Configure Strong Gate
    runtime.config["require_strong_confirmation"] = True
    runtime.config["strong_gate_shadow"] = False
    runtime.config["obi_stable_min_secs"] = 1.0

    # Pre-populate LAST OBI EVENT as STABLE
    now_ms = get_ny_time_millis()
    runtime.last_obi_event = {
        "ts_ms": now_ms - 500, # 500ms old (fresh)
        "stable_secs": 2.0,    # > 1.0 (stable)
        "obi_z": 1.5,
        "direction": "LONG",
        "ts": now_ms - 500
    }

    # Pre-populate LAST SWEEP (needed to trigger eval_reversal in reversal logic)
    class MockSweep:
        ts_ms = now_ms - 1000
        kind = "EQH_SWEEP"
        pool_id = "p1"
        level = 100.0
        tol_px = 0.0
        breach_px = 100.1
        confirm_px = 99.9
        direction_bias = "SHORT"
        touches = 5
    runtime.last_sweep = MockSweep()
    runtime.sweep = MicroStructureSpikeDetector() # Needed for valid_ms check?
    runtime.sweep.valid_ms = 60000

    # IMPORTANT: The tick direction must mimic the reversal (SHORT sweep -> SHORT delta?)
    # Strong gate logic: sweep_recent=True -> eval_reversal called.



    # Inject our mock evaluation function into the global scope of the module
    # OR we can inspect the `indicators["of_evidence"]` which is modified in place.
    # The code modifies `indicators` dict. Let's inspect that.

    # We also need to mock `eval_reversal` to prevent it from crashing or doing real work,
    # but more importantly we want to check what arguments it received if possible.
    # Actually, the logic is:
    #   obi_stable = ... (computed)
    #   indicators["of_evidence"]["obi_stable"] = int(obi_stable)
    # So we can check the artifacts in `indicators`.

    # Mock imported functions if needed?
    # `from services.crypto_orderflow_service import eval_reversal` <- usually imported from handlers.
    # Given we are testing `_handle_tick`, we just want to ensure it passes correct flags.

    # Patch eval_reversal globally for the test execution?
    # It's hard to patch a function imported inside the class method without mock.patch.
    # But checking `indicators` output inside `_handle_tick` is tricky because it returns None
    # (since no signal published in this harness, likely `_publish_signal` fails or is skipped).
    # Wait, `_handle_tick` returns None if no delta spike, OR if success?
    # Actually `_handle_tick` calls `_publish_signal` at the end if confirmed.
    # We can rely on `runtime.last_ts_ms` or simply that it doesn't crash,
    # BUT we want to see variables.

    # Better approach: subclass Service and override `_publish_signal` to capture `indicators`.

    captured_indicators = {}

    class TestService(CryptoOrderflowService):
        async def _publish_signal(self, runtime, signal, indicators, **kwargs):
            captured_indicators.update(indicators)

    service = TestService(redis_dsn="redis://m", ticks_dsn="redis://t")
    # Need to mock attributes usually set in init or run
    service.main = None
    service.ticks = None

    # Create a tick that triggers delta
    tick = {
        "symbol": "BTCUSDT",
        "ts": now_ms,
        "price": 100.0,
        "qty": 5.0,
        "side": "buy"
    }

    # Execution
    # We expect `_handle_tick` to reach the Strong Gate block.
    # It will throw an error when calling `eval_reversal` or `_publish_signal` because of missing deps?
    # eval_reversal is imported. It should run.
    # `_publish_signal` is async. `_handle_tick` calls it with `asyncio.create_task`.
    # So we might not catch it easily in sync test.

    # ALTERNATIVE: Use the fact that we modified `indicators` in place?
    # No, `indicators` is local to `_handle_tick`.

    # Let's use `mock.patch` on `services.crypto_orderflow_service.eval_reversal`

    # Mock return object (not a dict, because code accesses .scenario etc.)
    class MockDecision:
        scenario = "REVERSAL_TEST"
        reason = "TEST_PASS"
        have = 3
        need = 2
        ok = True
        a = True
        b = False
        c = False

    # Mock engine
    mock_ofc = MagicMock()
    mock_ofc.ok = 1
    mock_ofc.scenario = "REVERSAL"
    mock_ofc.have = 3
    mock_ofc.need = 2
    mock_ofc.evidence = {"obi_stable": 1}
    mock_ofc.to_dict.return_value = {"ok": 1}

    with patch.object(service.of_engine, 'build', return_value=(mock_ofc, MagicMock())) as mock_build:
        # Also mock create_task to avoid scheduling coroutines
        with patch("asyncio.create_task"):
             service._handle_tick(runtime, tick)

        # Verification
        assert mock_build.called
        call_kwargs = mock_build.call_args[1]

        # DEBUG: Print what we captured
        print("\nDEBUG: Captured indicators:", captured_indicators)
        print("DEBUG: call_kwargs:", call_kwargs)

        # Check that confirmation string WAS added (for audit)
        # We can't easily check local variable `confirmations` inside the function,
        # but the `eval_reversal` might typically look at it? No, eval_reversal takes arguments.

        # But we can verify that logic worked despite confirm string NOT being in input confirm list (which was empty).
