"""
Tests for edge_stack_mh_v1, deterministic sampling, and canonical SID.
"""

import numpy as np
import pytest

from core.edge_stack_mh_v1 import EdgeStackMHModelV1
from services.ml_confirm_gate import _bucket_from_scenario, _canonical_sid, _stable_sample


def test_canonical_sid_from_indicators():
    """Test canonical SID generation from indicators."""
    indicators = {"sid": "crypto-of:BTCUSDT:1234567890"}
    sid = _canonical_sid(indicators, "BTCUSDT", 1234567890)
    assert sid == "crypto-of:BTCUSDT:1234567890"

    indicators = {"signal_id": "crypto-of:ETHUSDT:9876543210"}
    sid = _canonical_sid(indicators, "ETHUSDT", 9876543210)
    assert sid == "crypto-of:ETHUSDT:9876543210"

    indicators = {}
    sid = _canonical_sid(indicators, "BTCUSDT", 1234567890)
    assert sid == "crypto-of:BTCUSDT:1234567890"


def test_canonical_sid_deterministic():
    """Test that canonical SID is deterministic for same inputs."""
    indicators = {}
    sid1 = _canonical_sid(indicators, "BTCUSDT", 1234567890)
    sid2 = _canonical_sid(indicators, "BTCUSDT", 1234567890)
    assert sid1 == sid2

    sid3 = _canonical_sid(indicators, "ETHUSDT", 1234567890)
    assert sid1 != sid3


def test_stable_sample_deterministic():
    """Test that stable_sample is deterministic for same sid."""
    sid = "crypto-of:BTCUSDT:1234567890"
    salt = "test_salt"

    # Same sid + salt should always produce same result
    result1 = _stable_sample(sid, 0.5, salt)
    result2 = _stable_sample(sid, 0.5, salt)
    assert result1 == result2

    # Different salt should produce different result
    result3 = _stable_sample(sid, 0.5, "different_salt")
    # May be same or different, but should be deterministic
    result4 = _stable_sample(sid, 0.5, "different_salt")
    assert result3 == result4


def test_stable_sample_rate_boundaries():
    """Test stable_sample rate boundaries."""
    sid = "crypto-of:BTCUSDT:1234567890"

    # rate >= 1.0 should always return True
    assert _stable_sample(sid, 1.0) == True
    assert _stable_sample(sid, 1.5) == True

    # rate <= 0.0 should always return False
    assert _stable_sample(sid, 0.0) == False
    assert _stable_sample(sid, -0.1) == False


def test_bucket_from_scenario():
    """Test bucket extraction from scenario."""
    assert _bucket_from_scenario("range_meanrev") == "range"
    assert _bucket_from_scenario("range_chop") == "range"
    assert _bucket_from_scenario("trend_continuation") == "trend"
    assert _bucket_from_scenario("trend_reversal") == "trend"
    assert _bucket_from_scenario("unknown") == "other"
    assert _bucket_from_scenario("") == "other"


def test_edge_stack_mh_v1_predict_base():
    """Test EdgeStackMHModelV1 predict_base method."""
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.linear_model import LogisticRegression

    from core.feature_engineering import RobustScalerPack

    # Create mock models
    lr_model = LogisticRegression()
    gbdt_model = HistGradientBoostingClassifier()

    # Simple training data
    X_train = np.array([[1.0, 2.0], [2.0, 3.0], [3.0, 4.0]])
    y_train = np.array([0, 1, 1])

    lr_model.fit(X_train, y_train)
    gbdt_model.fit(X_train, y_train)

    # Create scaler
    scaler = RobustScalerPack.fit(X_train, feature_names=["f1", "f2"])

    # Create model
    model = EdgeStackMHModelV1(
        feature_cols=["f1", "f2"],
        horizons=[60000],
        unc_k=0.1,
        scaler=scaler,
        lr={60000: lr_model},
        gbdt={60000: gbdt_model},
        meta={60000: lr_model},  # Use same model for simplicity
        calibrator={60000: None},
    )

    # Test predict_base
    X_test = np.array([[1.5, 2.5]])
    base_preds = model.predict_base(X_test)

    assert 60000 in base_preds
    assert "lr" in base_preds[60000]
    assert "gbdt" in base_preds[60000]
    assert len(base_preds[60000]["lr"]) == 1
    assert len(base_preds[60000]["gbdt"]) == 1


def test_edge_stack_mh_v1_predict_unc():
    """Test EdgeStackMHModelV1 predict_unc method."""
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.linear_model import LogisticRegression

    from core.feature_engineering import RobustScalerPack

    # Create mock models
    lr_model = LogisticRegression()
    gbdt_model = HistGradientBoostingClassifier()

    # Simple training data
    X_train = np.array([[1.0, 2.0], [2.0, 3.0], [3.0, 4.0]])
    y_train = np.array([0, 1, 1])

    lr_model.fit(X_train, y_train)
    gbdt_model.fit(X_train, y_train)

    # Create scaler
    scaler = RobustScalerPack.fit(X_train, feature_names=["f1", "f2"])

    # Create model
    model = EdgeStackMHModelV1(
        feature_cols=["f1", "f2"],
        horizons=[60000],
        unc_k=0.1,
        scaler=scaler,
        lr={60000: lr_model},
        gbdt={60000: gbdt_model},
        meta={60000: lr_model},
        calibrator={60000: None},
    )

    # Test predict_unc
    X_test = np.array([[1.5, 2.5]])
    unc = model.predict_unc(X_test)

    assert 60000 in unc
    assert len(unc[60000]) == 1
    assert unc[60000][0] >= 0.0  # Uncertainty should be non-negative


def test_edge_stack_mh_v1_predict_score():
    """Test EdgeStackMHModelV1 predict_score method."""
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.linear_model import LogisticRegression

    from core.feature_engineering import RobustScalerPack

    # Create mock models
    lr_model = LogisticRegression()
    gbdt_model = HistGradientBoostingClassifier()

    # Simple training data
    X_train = np.array([[1.0, 2.0], [2.0, 3.0], [3.0, 4.0]])
    y_train = np.array([0, 1, 1])

    lr_model.fit(X_train, y_train)
    gbdt_model.fit(X_train, y_train)

    # Create scaler
    scaler = RobustScalerPack.fit(X_train, feature_names=["f1", "f2"])

    # Create model
    model = EdgeStackMHModelV1(
        feature_cols=["f1", "f2"],
        horizons=[60000],
        unc_k=0.1,
        scaler=scaler,
        lr={60000: lr_model},
        gbdt={60000: gbdt_model},
        meta={60000: lr_model},
        calibrator={60000: None},
    )

    # Test predict_score
    X_test = np.array([[1.5, 2.5]])
    scores = model.predict_score(X_test)

    assert 60000 in scores
    assert len(scores[60000]) == 1
    # Score should be p_cal - unc_k * unc
    assert np.isfinite(scores[60000][0])


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

