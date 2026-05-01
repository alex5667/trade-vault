#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
"""
test_edge_stack_v1_champion_cfg_preserve.py

Test that champion cfg preserves additional fields (p_min_by_bucket, calibrate_p_edge, etc.)
after validation.
"""


import json
import pytest

try:
    from core.champion_cfg_validator import validate_champion_cfg, CfgError
    from services.ml_confirm_gate import MLConfirmGate
except ImportError as e:
    pytest.skip(f"Required modules not available: {e}", allow_module_level=True)


class MockRedis:
    """Mock Redis for testing."""
    def __init__(self):
        self.data = {}
    
    def get(self, key: str):
        return self.data.get(key)
    
    def set(self, key: str, value: str):
        self.data[key] = value


def test_champion_cfg_preserves_additional_fields():
    """Test that additional fields like p_min_by_bucket are preserved after validation."""
    # Create a champion cfg with additional fields
    cfg_with_extras = {
        "schema_version": 1,
        "kind": "edge_stack_v1",
        "run_id": "test_run_123",
        "created_ms": 1700000000000,
        "model_path": "/models/edge_stack_v1.joblib",
        "mode": "SHADOW",
        "enforce_share": 0.0,
        # Additional fields that should be preserved
        "p_min": 0.55,
        "p_min_by_bucket": {"trend": 0.55, "range": 0.60, "other": 0.52},
        "calibrate_p_edge": True,
        "hard_p_min_floor": 0.50,
        "custom_field": "should_be_preserved",
    }
    
    # Validate the cfg
    cfg_json = json.dumps(cfg_with_extras)
    validated_cfg, validation_info = validate_champion_cfg(cfg_json, default_enforce_share=None)
    
    # Check that validation succeeded
    assert validated_cfg.kind == "edge_stack_v1"
    assert validated_cfg.mode == "SHADOW"
    assert validated_cfg.enforce_share == 0.0
    
    # Now test that MLConfirmGate preserves additional fields
    r = MockRedis()
    r.set("cfg:ml_confirm:champion", cfg_json)
    
    gate = MLConfirmGate(
        r=r,
        mode="SHADOW",
        fail_policy="OPEN",
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger"
    )
    
    # Load cfg (this should preserve additional fields)
    cfg, model = gate._load_cfg_and_model()
    
    # Check that additional fields are preserved
    assert cfg.get("p_min") == 0.55
    assert cfg.get("p_min_by_bucket") == {"trend": 0.55, "range": 0.60, "other": 0.52}
    assert cfg.get("calibrate_p_edge") is True
    assert cfg.get("hard_p_min_floor") == 0.50
    assert cfg.get("custom_field") == "should_be_preserved"
    
    # Check that validated fields are also present
    assert cfg.get("kind") == "edge_stack_v1"
    assert cfg.get("mode") == "SHADOW"
    assert cfg.get("enforce_share") == 0.0


def test_champion_cfg_preserves_fields_in_canary_mode():
    """Test that additional fields are preserved in CANARY mode."""
    cfg_canary = {
        "schema_version": 1,
        "kind": "edge_stack_v1",
        "run_id": "test_run_456",
        "created_ms": 1700000000000,
        "model_path": "/models/edge_stack_v1.joblib",
        "mode": "CANARY",
        "enforce_share": 0.05,
        "p_min_by_bucket": {"trend": 0.58, "range": 0.62},
        "calibrate_p_edge": False,
    }
    
    r = MockRedis()
    r.set("cfg:ml_confirm:champion", json.dumps(cfg_canary))
    
    gate = MLConfirmGate(
        r=r,
        mode="SHADOW",
        fail_policy="OPEN",
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger"
    )
    
    cfg, model = gate._load_cfg_and_model()
    
    assert cfg.get("mode") == "CANARY"
    assert cfg.get("enforce_share") == 0.05
    assert cfg.get("p_min_by_bucket") == {"trend": 0.58, "range": 0.62}
    assert cfg.get("calibrate_p_edge") is False


if __name__ == "__main__":
    test_champion_cfg_preserves_additional_fields()
    test_champion_cfg_preserves_fields_in_canary_mode()
    print("All tests passed!")





