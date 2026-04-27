from pathlib import Path
import importlib.util
import sys

mod_path = Path(__file__).with_name("execution_policy.py")
spec = importlib.util.spec_from_file_location("execution_policy", mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)


def test_forced_safety_on_infra_degraded():
    decision = mod.resolve_execution_policy(
        payload={"infra_degraded": True},
        symbol="BTCUSDT",
        default_policy="MAKER_FIRST",
        maker_allowed_symbols={"BTCUSDT"},
        tp_market_working_type="MARK_PRICE",
        tp_limit_trigger_working_type="MARK_PRICE",
        tp_limit_time_in_force="GTX",
        watchdog_enabled=True,
        watchdog_timeout_ms=4000,
    )
    assert decision.name == mod.SAFETY_FIRST
    assert decision.tp_order_type == "TAKE_PROFIT_MARKET"
    assert decision.reason == "forced_infra_degraded"


def test_allowlisted_major_gets_maker_first_when_healthy():
    decision = mod.resolve_execution_policy(
        payload={},
        symbol="ETHUSDT",
        default_policy="SAFETY_FIRST",
        maker_allowed_symbols={"BTCUSDT", "ETHUSDT"},
        tp_market_working_type="MARK_PRICE",
        tp_limit_trigger_working_type="MARK_PRICE",
        tp_limit_time_in_force="GTX",
        watchdog_enabled=True,
        watchdog_timeout_ms=4000,
    )
    assert decision.name == mod.MAKER_FIRST
    assert decision.tp_order_type == "TAKE_PROFIT"
    assert decision.tp_limit_time_in_force == "GTX"
    assert decision.tp_watchdog_enabled is True
