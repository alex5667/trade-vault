from pathlib import Path
import importlib.util
import sys

mod_path = Path(__file__).parent.parent / "execution_intent_validator.py"
spec = importlib.util.spec_from_file_location("execution_intent_validator", mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)


def test_hedge_requires_position_side():
    res = mod.validate_exit_intent(
        position_mode="hedge"
        position_side=None
        exit_intent="close"
        reduce_only=False
        close_position=True
        quantity=None
        order_type="STOP_MARKET"
        working_type="MARK_PRICE"
        is_algo=True
    )
    assert res.is_valid_exit_contract is False
    assert res.reason == "positionSide_required_in_hedge"


def test_algo_close_position_incompatible_with_quantity():
    res = mod.validate_exit_intent(
        position_mode="hedge"
        position_side="LONG"
        exit_intent="close"
        reduce_only=False
        close_position=True
        quantity=1.0
        order_type="TAKE_PROFIT_MARKET"
        working_type="MARK_PRICE"
        is_algo=True
    )
    assert res.is_valid_exit_contract is False
    assert "quantity" in res.reason


def test_plain_oneway_reduce_only_market_close_is_valid():
    res = mod.validate_exit_intent(
        position_mode="oneway"
        position_side=None
        exit_intent="close"
        reduce_only=True
        close_position=False
        quantity=1.0
        order_type="MARKET"
        working_type=None
        is_algo=False
    )
    assert res.is_valid_exit_contract is True
    assert res.will_reduce_exposure is True
    assert res.will_open_new_exposure is False
