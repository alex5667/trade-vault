import pytest
import time
from pathlib import Path
import importlib.util
import sys

mod_path = Path(__file__).parent.parent / 'binance_executor.py'
spec = importlib.util.spec_from_file_location('binance_executor_intent', mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)

mod_path2 = Path(__file__).parent.parent / 'execution_intent_validator.py'
spec2 = importlib.util.spec_from_file_location('execution_intent_validator', mod_path2)
mod2 = importlib.util.module_from_spec(spec2)
sys.modules[spec2.name] = mod2
assert spec2.loader is not None
spec2.loader.exec_module(mod2)

ExecutionIntent = mod2.ExecutionIntent
validate_execution_intent = mod2.validate_execution_intent

def test_execution_intent_valid():
    now = int(time.time() * 1000)
    intent = ExecutionIntent.from_payload({
        "sid": "123",
        "symbol": "BTCUSDT",
        "action": "open",
        "qty": 1.0,
        "ts_decision_ms": now - 10, # 10ms old
        "max_ttd_ms": 50
    })
    # Should not raise
    validate_execution_intent(intent, now)

def test_execution_intent_expired():
    now = int(time.time() * 1000)
    intent = ExecutionIntent.from_payload({
        "sid": "123",
        "symbol": "BTCUSDT",
        "action": "open",
        "qty": 1.0,
        "ts_decision_ms": now - 60, # 60ms old
        "max_ttd_ms": 50
    })
    with pytest.raises(ValueError, match="INTENT_EXPIRED"):
        validate_execution_intent(intent, now)

def test_execution_intent_fallback_max_ttd():
    now = int(time.time() * 1000)
    intent = ExecutionIntent.from_payload({
        "sid": "123",
        "symbol": "BTCUSDT",
        "action": "open",
        "qty": 1.0,
        "ts_decision_ms": now - 60,
        # max_ttd_ms missing
    })
    # Default is 50, so 60ms old should expire
    with pytest.raises(ValueError, match="INTENT_EXPIRED"):
        validate_execution_intent(intent, now)

def test_execution_intent_fallback_ts_decision():
    now = int(time.time() * 1000)
    intent = ExecutionIntent.from_payload({
        "sid": "123",
        "symbol": "BTCUSDT",
        "action": "open",
        "qty": 1.0,
        # ts_decision_ms missing
        "ts_exec_start_ms": now - 10
    })
    # Default ts_decision_ms is ts_exec_start_ms
    # Age is 10ms < 50ms default -> ok
    validate_execution_intent(intent, now)
