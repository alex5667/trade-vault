from core.instrument_config import OrderFlowConfig


def test_orderflowconfig_has_new_proximity_fields():
    cfg = OrderFlowConfig()
    assert hasattr(cfg, "dist_bp_threshold")
    assert hasattr(cfg, "dist_mode")
    assert cfg.dist_bp_threshold is None
    assert cfg.dist_mode == "or"
