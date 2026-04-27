"""Tests for Edge Stack v1 timers and exporter."""

import pytest
import os
import sys

# Ensure python-worker is in path
# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def test_shadow_status_exporter_import():
    try:
        import orderflow_services.edge_stack_shadow_status_exporter_v1
        assert hasattr(orderflow_services.edge_stack_shadow_status_exporter_v1, "main")
    except ImportError as e:
        pytest.fail(f"Failed to import exporter: {e}")

def test_shadow_eval_bundle_import():
    try:
        import tools.edge_stack_shadow_eval_bundle_v1
        assert hasattr(tools.edge_stack_shadow_eval_bundle_v1, "main")
    except ImportError as e:
        pytest.fail(f"Failed to import shadow eval bundle: {e}")

def test_train_bundle_import():
    try:
        import tools.nightly_edge_stack_v1_train_bundle
        assert hasattr(tools.nightly_edge_stack_v1_train_bundle, "main")
    except ImportError as e:
        pytest.fail(f"Failed to import train bundle: {e}")

def test_feature_contract_import():
    try:
        import core.edge_stack_feature_contract_v1
        assert hasattr(core.edge_stack_feature_contract_v1, "EdgeStackFeatureContractV1")
    except ImportError as e:
        pytest.fail(f"Failed to import feature contract: {e}")
