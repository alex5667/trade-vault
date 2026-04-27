from utils.time_utils import get_ny_time_millis
import os
import json
import time
import tempfile
import pytest

from common.contextual_bundle_store_v1 import ContextualBundleStoreV1, ContextualBundleInfo
from core.ofc_bundle_loader_v1 import OFCBundleLoaderV1, OFCBundleV1
from core.ofc_contextual_gate_v1 import ContextualGateV1
from core.ofc_exec_cost_model_v1 import ExecCostModelV1
from core.ofc_rule_success_model_v1 import RuleSuccessModelV1

@pytest.fixture
def mock_bundle_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create manifest
        manifest = {
            "bundle_version": "v1.0",
            "created_ts_ms": get_ny_time_millis(),
            "sha256": "abcdef123456"
        }
        with open(os.path.join(tmpdir, "manifest.json"), "w") as f:
            json.dump(manifest, f)
            
        gate_cfg = {
            "version": "1.0",
            "p_min": 0.55
        }
        with open(os.path.join(tmpdir, "gate_cfg.json"), "w") as f:
            json.dump(gate_cfg, f)
            
        exec_cost = {
            "version": "1.0",
            "model_type": "lgbm"
        }
        with open(os.path.join(tmpdir, "exec_cost_model.json"), "w") as f:
            json.dump(exec_cost, f)
            
        rule_success = {
            "version": "1.0",
            "model_type": "lgbm"
        }
        with open(os.path.join(tmpdir, "rule_success_model.json"), "w") as f:
            json.dump(rule_success, f)
            
        yield tmpdir

def test_contextual_bundle_store(mock_bundle_dir):
    store = ContextualBundleStoreV1(mock_bundle_dir, reload_sec=1)
    store.maybe_reload() # force load
    
    assert store.get_manifest().get("bundle_version") == "v1.0"
    assert store.get_gate_cfg().get("p_min") == 0.55
    assert store.get_exec_cost_payload().get("model_type") == "lgbm"
    assert store.get_rule_success_payload().get("model_type") == "lgbm"
    
    info = store.get_info()
    assert info.version == "v1.0"
    assert info.sha256 == "abcdef123456"

def test_ofc_bundle_loader(mock_bundle_dir):
    loader = OFCBundleLoaderV1(mock_bundle_dir, reload_sec=1)
    loader.maybe_reload()
    
    bundle = loader.get()
    assert bundle is not None
    assert isinstance(bundle, OFCBundleV1)
    assert bundle.version == "v1.0"
    
    # Check that embedded objects are instantiated correctly
    assert isinstance(bundle.exec_cost_model, ExecCostModelV1)
    assert isinstance(bundle.rule_success_model, RuleSuccessModelV1)
    assert isinstance(bundle.gate, ContextualGateV1)
    
    # Evaluate gate
    assert bundle.gate.gate_cfg.get("p_min") == 0.55
    
    # Test evaluation with dummy data
    class DummyPred:
        p_rule_cal = 0.60
        score_min_ctx = 0.50
        cost_p50_bps = 5.0
        cost_p90_bps = 5.0
        fallback_level = "none"

    dummy_pred = DummyPred()
    decision = bundle.gate.evaluate(
        raw_score=0.55,
        ctx_features={},
        exec_cost_pred=dummy_pred,
        rule_pred=dummy_pred,
        tp_bps=50.0,
        sl_bps=50.0,
        mode="shadow"
    )
    assert decision.allow == True
    assert decision.reason == "allow"
