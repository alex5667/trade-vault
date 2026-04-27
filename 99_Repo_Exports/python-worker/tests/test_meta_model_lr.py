import json
import math
import tempfile

from core.meta_model_lr import MetaModelLR


def test_meta_model_lr_transforms_and_scaler():
    d = {
        "features": ["x"],
        "intercept": 0.0,
        "coef": [1.0],
        "threshold": 0.5,
        "transforms": {"x": {"type": "clip", "lo": 0.0, "hi": 10.0}},
        "robust_scaler": {"x": {"center": 5.0, "scale": 5.0}},
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(d, f)
        path = f.name

    m = MetaModelLR.load(path)
    # x=100 -> clip to 10 -> scale => (10-5)/5 = 1
    p = m.predict_proba({"x": 100.0})
    # sigmoid(1) ~ 0.731
    assert math.isclose(p, 1.0 / (1.0 + math.exp(-1.0)), rel_tol=1e-6)
