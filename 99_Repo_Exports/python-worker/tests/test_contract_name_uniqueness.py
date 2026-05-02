"""P0.4 — No duplicate class names with different shapes across contract modules."""
from __future__ import annotations

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import dataclasses
import pytest


def _dataclass_fields(cls) -> frozenset:
    return frozenset(f.name for f in dataclasses.fields(cls))


def _pydantic_fields(cls) -> frozenset:
    return frozenset(cls.model_fields.keys())


def test_registry_ofinputs_renamed():
    """registry.OFInputsV1 must be OFInputsFeatureMapV1 (generic feature-map), not replay contract."""
    from common.contracts import registry
    # canonical name exists
    assert hasattr(registry, "OFInputsFeatureMapV1")
    # backward-compat alias still present
    assert hasattr(registry, "OFInputsV1")
    # they must be the same class
    assert registry.OFInputsV1 is registry.OFInputsFeatureMapV1


def test_registry_ofconfirm_renamed():
    """registry.OFConfirmV3 must be OFConfirmMlScoreV1, not the core gate-bits contract."""
    from common.contracts import registry
    assert hasattr(registry, "OFConfirmMlScoreV1")
    assert hasattr(registry, "OFConfirmV3")
    assert registry.OFConfirmV3 is registry.OFConfirmMlScoreV1


def test_registry_ofinputs_shape_differs_from_core():
    """Registry OFInputsFeatureMapV1 must NOT share shape with core OFInputsV1."""
    from common.contracts.registry import OFInputsFeatureMapV1 as RegInputs
    from core.of_inputs_contract import OFInputsV1 as CoreInputs

    reg_fields = _pydantic_fields(RegInputs)
    core_fields = _dataclass_fields(CoreInputs)

    # they must differ — registry has 'features: Dict[str, float]',
    # core has 'delta_z', 'sweep_recent', etc.
    assert reg_fields != core_fields, (
        "registry.OFInputsFeatureMapV1 and core.OFInputsV1 must have different shapes"
    )
    assert "features" in reg_fields
    assert "delta_z" in core_fields


def test_execution_contracts_forbid_extra():
    """OrderIntentV1 and ExecutionEventV1 must reject unknown fields."""
    from common.contracts.registry import OrderIntentV1, ExecutionEventV1
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        OrderIntentV1(
            intent_id="i1", signal_id="s1", symbol="BTCUSDT",
            ts_ms=1000, side="BUY", price=100.0, qty=0.1,
            unknown_field="bad",
        )

    with pytest.raises(ValidationError):
        ExecutionEventV1(
            exec_id="e1", order_id="o1", symbol="BTCUSDT",
            ts_ms=1000, side="BUY", price=100.0, qty=0.1,
            unknown_field="bad",
        )
