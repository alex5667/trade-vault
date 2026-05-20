from unittest.mock import MagicMock, patch

import pytest

from core.meta_model_lr import MetaModelLR
from services.ml_confirm import MLConfirmGate


class TestFixPEdgeCapping:
    @pytest.fixture
    def gate(self):
        # minimal mock logic
        with patch("services.ml_confirm.gate.redis.Redis") as MockRedis:
            r = MagicMock()
            MockRedis.return_value = r
            # Correct init signature
            gate = MLConfirmGate(
                 r=r,
                 mode="SHADOW",
                 fail_policy="OPEN",
                 champion_key="cfg:ml:champion",
                 challenger_key="cfg:ml:challenger"
            )
            # Override helpers
            gate._conf_from_margin = lambda m: 0.5 + m
            gate._p_min_hard_floor = 0.0
            gate._calibrator = None
            gate._abstain_on_missing = False
            gate._calib_type = None
            return gate

    def test_meta_lr_unscaled_capping(self, gate):
        """Verify that meta_lr (unscaled) produces low p_edge values (confirming capping issue)."""
        # 1. Setup MetaModelLR with minimal config
        # Use a model that outputs raw probability around 0.52 for given input
        # logistic(0.0 + 1.0*0.08) = sigmoid(0.08) = 0.5199...
        model = MetaModelLR(features=["f1"], intercept=0.0, coef=[1.0])
        gate._model = model
        gate._cfg = {"kind": "meta_lr"}
        # No calibrator in config!

        # 2. Invoke _load_calibrator_sync (should NOT load anything by default now)
        logger = MagicMock()
        gate._load_calibrator_sync(logger)
        assert gate._calibrator is None

        # 3. Simulate prediction
        # indicators is passed directly to predict_proba (raw feature names, no f_ prefix stripping)
        # _build_feature_row is still called for critical-feature checks / missing list only
        with patch.object(gate, "_build_feature_row", return_value=([0.08], [])):
            dec = gate._decide_meta_lr(
                symbol="BTCUSDT", ts_ms=1000, direction="buy", scenario="trend",
                indicators={"f1": 0.08},  # value must be in indicators for predict_proba
            )

        # 4. Assertions
        assert dec.kind == "meta_lr"
        # Raw probability should be sigmoid(0.08) ~= 0.52
        assert 0.51 < dec.p_edge_raw < 0.53, f"Expected 0.51-0.53, got {dec.p_edge_raw}"
        # Calibrated/Final p_edge should MATCH raw (no calibrator)
        assert dec.p_edge == dec.p_edge_raw

    def test_meta_lr_with_manual_calibration(self, gate):
        """Verify that we CAN still fix it via configuration if we want."""
        model = MetaModelLR(features=["f1"], intercept=0.0, coef=[1.0])
        gate._model = model
        # Manually provide calibrator in config
        gate._cfg = {
            "kind": "meta_lr",
            "calibrator": {"type": "platt_logit", "a": 2.5, "b": 0.0}
        }

        logger = MagicMock()
        gate._load_calibrator_sync(logger)
        assert gate._calibrator is not None
        assert gate._calibrator.a == 2.5

        with patch.object(gate, "_build_feature_row", return_value=([0.08], [])):
            dec = gate._decide_meta_lr(
                symbol="BTCUSDT", ts_ms=1000, direction="buy", scenario="trend",
                indicators={"f1": 0.08},  # value must be in indicators for predict_proba
            )

        # Should be scaled: sigmoid(0.08 * 2.5 + 0) = sigmoid(0.2) ~= 0.5498
        assert dec.p_edge > 0.54, f"Expected >0.54 with calibration, got {dec.p_edge}"
