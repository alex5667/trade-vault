import pytest
from pydantic import ValidationError

from services.ml_confirm_gate import MLConfirmConfig


def test_ml_confirm_config_valid_p_min():
    # p_min=0.55 is within [0.5, 0.95]
    cfg = MLConfirmConfig(p_min=0.55)
    assert cfg.p_min == 0.55

def test_ml_confirm_config_invalid_p_min_low():
    # p_min=0.4 is outside [0.5, 0.95]
    with pytest.raises(ValidationError) as excinfo:
        MLConfirmConfig(p_min=0.4)
    assert "p_min must be in range [0.5, 0.95]" in str(excinfo.value)

def test_ml_confirm_config_invalid_p_min_high():
    # p_min=0.96 is outside [0.5, 0.95]
    with pytest.raises(ValidationError) as excinfo:
        MLConfirmConfig(p_min=0.96)
    assert "p_min must be in range [0.5, 0.95]" in str(excinfo.value)

def test_ml_confirm_config_valid_p_min_by_bucket():
    # All values within [0.5, 0.95]
    p_min_by_bucket = {"trend": 0.6, "range": 0.7}
    cfg = MLConfirmConfig(p_min_by_bucket=p_min_by_bucket)
    assert cfg.p_min_by_bucket["trend"] == 0.6
    assert cfg.p_min_by_bucket["range"] == 0.7

def test_ml_confirm_config_invalid_p_min_by_bucket():
    # One value outside [0.5, 0.95]
    p_min_by_bucket = {"trend": 0.6, "range": 0.4}
    with pytest.raises(ValidationError) as excinfo:
        MLConfirmConfig(p_min_by_bucket=p_min_by_bucket)
    assert "p_min_by_bucket[range] must be in range [0.5, 0.95]" in str(excinfo.value)

def test_ml_confirm_config_valid_util_floors():
    util_floors = {
        "global": {"floor": 0.6},
        "by_bucket": {
            "trend": {"floor": 0.7},
            "range": {"floor": 0.8}
        }
    }
    cfg = MLConfirmConfig(util_floors=util_floors)
    assert cfg.util_floors["global"]["floor"] == 0.6
    assert cfg.util_floors["by_bucket"]["trend"]["floor"] == 0.7

def test_ml_confirm_config_invalid_util_floors_global():
    util_floors = {"global": {"floor": 0.4}}
    with pytest.raises(ValidationError) as excinfo:
        MLConfirmConfig(util_floors=util_floors)
    assert "util_floors.global.floor must be in range [0.5, 0.95]" in str(excinfo.value)

def test_ml_confirm_config_invalid_util_floors_bucket():
    util_floors = {
        "by_bucket": {
            "trend": {"floor": 1.0}
        }
    }
    with pytest.raises(ValidationError) as excinfo:
        MLConfirmConfig(util_floors=util_floors)
    assert "util_floors.by_bucket[trend].floor must be in range [0.5, 0.95]" in str(excinfo.value)

def test_ml_confirm_config_valid_edge_floors():
    edge_floors = {
        "global": {"floor": 0.55},
        "by_bucket": {
            "news": {"floor": 0.65}
        }
    }
    cfg = MLConfirmConfig(edge_floors=edge_floors)
    assert cfg.edge_floors["global"]["floor"] == 0.55
    assert cfg.edge_floors["by_bucket"]["news"]["floor"] == 0.65

def test_ml_confirm_config_invalid_edge_floors():
    edge_floors = {"global": {"floor": 0.0}}
    with pytest.raises(ValidationError) as excinfo:
        MLConfirmConfig(edge_floors=edge_floors)
    assert "edge_floors.global.floor must be in range [0.5, 0.95]" in str(excinfo.value)
