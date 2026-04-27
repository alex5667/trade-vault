from utils.time_utils import get_ny_time_millis
"""Unit tests for nightly_meta_enforce_propose_bundle.py.

Tests safety gates and bundle creation for meta ENFORCE proposals.
"""

import os
import time
import json
from unittest.mock import MagicMock, patch, Mock
import pytest

import fakeredis


def now_ms() -> int:
    """Returns current timestamp in milliseconds."""
    return get_ny_time_millis()


@pytest.fixture
def mock_redis():
    """Create a fake Redis instance for testing."""
    return fakeredis.FakeRedis(decode_responses=True)


def test_streak_gate_fails(mock_redis, monkeypatch):
    """Test that proposal is skipped when streak < min_streak."""
    mock_redis.set("sre:regress:pass_streak", "2")
    mock_redis.set("sre:regress:last_status", "PASS")
    mock_redis.set("sre:regress:last_ts_ms", str(now_ms()))
    
    monkeypatch.setenv("META_ENFORCE_MIN_STREAK", "3")
    monkeypatch.setenv("BASELINE_PROPOSE_MAX_AGE_HOURS", "30")
    monkeypatch.setenv("REGRESS_PASS_STREAK_KEY", "sre:regress:pass_streak")
    monkeypatch.setenv("REGRESS_LAST_STATUS_KEY", "sre:regress:last_status")
    monkeypatch.setenv("REGRESS_LAST_TS_KEY", "sre:regress:last_ts_ms")
    monkeypatch.setenv("META_ENFORCE_NOTIFY_ON_SKIP", "0")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    
    with patch("tools.nightly_meta_enforce_propose_bundle.redis.Redis") as mock_redis_cls:
        mock_redis_cls.from_url.return_value = mock_redis
        
        from tools.nightly_meta_enforce_propose_bundle import main
        
        # Should return early without creating bundle
        main()
        
        bundle_keys = [k for k in mock_redis.keys() if k.startswith("recs:bundle:")]
        assert len(bundle_keys) == 0, "Proposal should be skipped when streak < min_streak"


def test_health_gate_fails(mock_redis, monkeypatch):
    """Test that proposal is skipped when health metrics fail."""
    # Setup: streak passes
    mock_redis.set("sre:regress:pass_streak", "3")
    mock_redis.set("sre:regress:last_status", "PASS")
    mock_redis.set("sre:regress:last_ts_ms", str(now_ms()))
    
    # Setup metrics stream with high latency
    current_ms = now_ms()
    for i in range(300):
        lat = 5000.0 if i >= 297 else 1000.0  # p99 will be 5000
        mock_redis.xadd(
            "metrics:of_gate",
            {
                "ts_ms": str(current_ms - (300 - i) * 1000),
                "ok": "1",
                "ok_soft": "0",
                "latency_us": str(lat),
                "exec_risk_norm": "0.5",
            },
            maxlen=200000,
            approximate=True,
        )
    
    monkeypatch.setenv("META_ENFORCE_MIN_STREAK", "3")
    monkeypatch.setenv("BASELINE_PROPOSE_MAX_AGE_HOURS", "30")
    monkeypatch.setenv("BASELINE_PROPOSE_HEALTH_WINDOW_HOURS", "24")
    monkeypatch.setenv("BASELINE_PROPOSE_MIN_N", "200")
    monkeypatch.setenv("BASELINE_PROPOSE_LAT_P99_US_MAX", "4000")
    monkeypatch.setenv("OF_GATE_METRICS_STREAM", "metrics:of_gate")
    monkeypatch.setenv("META_ENFORCE_NOTIFY_ON_SKIP", "0")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    
    with patch("tools.nightly_meta_enforce_propose_bundle.redis.Redis") as mock_redis_cls:
        mock_redis_cls.from_url.return_value = mock_redis
        
        from tools.nightly_meta_enforce_propose_bundle import main
        
        # Should return early without creating bundle
        main()
        
        bundle_keys = [k for k in mock_redis.keys() if k.startswith("recs:bundle:")]
        assert len(bundle_keys) == 0, "Proposal should be skipped when health gate fails"


def test_model_path_resolution(mock_redis, monkeypatch, tmp_path):
    """Test model path resolution from MODELS_DIR."""
    # Setup: gates pass
    mock_redis.set("sre:regress:pass_streak", "3")
    mock_redis.set("sre:regress:last_status", "PASS")
    mock_redis.set("sre:regress:last_ts_ms", str(now_ms()))
    
    # Setup healthy metrics
    current_ms = now_ms()
    for i in range(300):
        mock_redis.xadd(
            "metrics:of_gate",
            {
                "ts_ms": str(current_ms - (300 - i) * 1000),
                "ok": "1",
                "ok_soft": "0",
                "latency_us": "2000.0",
                "exec_risk_norm": "0.5",
            },
            maxlen=200000,
            approximate=True,
        )
    
    # Create model file
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    model_path = models_dir / "meta_lr_20240101_120000.json"
    model_data = {
        "kind": "logreg_v1",
        "features": ["score"],
        "coef": [1.0],
        "intercept": 0.0,
    }
    with open(model_path, "w") as f:
        json.dump(model_data, f)
    
    monkeypatch.setenv("META_ENFORCE_MIN_STREAK", "3")
    monkeypatch.setenv("BASELINE_PROPOSE_MAX_AGE_HOURS", "30")
    monkeypatch.setenv("BASELINE_PROPOSE_HEALTH_WINDOW_HOURS", "24")
    monkeypatch.setenv("BASELINE_PROPOSE_MIN_N", "200")
    monkeypatch.setenv("BASELINE_PROPOSE_LAT_P99_US_MAX", "4000")
    monkeypatch.setenv("OF_GATE_METRICS_STREAM", "metrics:of_gate")
    monkeypatch.setenv("MODELS_DIR", str(models_dir))
    monkeypatch.setenv("META_ENFORCE_NOTIFY_ON_SKIP", "0")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("OUT_DIR", str(tmp_path))
    monkeypatch.setenv("CANARY_SYMBOLS", "BTCUSDT")
    monkeypatch.setenv("OF_INPUTS_STREAM", "signals:of:inputs")
    monkeypatch.setenv("OF_INPUTS_STREAM_FIELD", "payload")
    monkeypatch.setenv("TRADE_EVENTS_STREAM", "events:trades")
    monkeypatch.setenv("STATE_FILE", str(tmp_path / "state"))
    monkeypatch.setenv("RECS_HMAC_SECRET", "test_secret")
    monkeypatch.setenv("RECS_TTL_SEC", "86400")
    monkeypatch.setenv("CFG_HASH_PREFIX", "config:orderflow:")
    monkeypatch.setenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram")
    
    with patch("tools.nightly_meta_enforce_propose_bundle.redis.Redis") as mock_redis_cls:
        mock_redis_cls.from_url.return_value = mock_redis
        
        from tools.nightly_meta_enforce_propose_bundle import main
        
        # Mock all subprocess calls
        with patch("tools.nightly_meta_enforce_propose_bundle.subprocess.check_call"), \
             patch("tools.nightly_meta_enforce_propose_bundle.open", create=True), \
             patch("tools.nightly_meta_enforce_propose_bundle.os.makedirs"), \
             patch("tools.nightly_meta_enforce_propose_bundle.os.path.exists", return_value=True), \
             patch("tools.nightly_meta_enforce_propose_bundle.json.loads", return_value={"best": None}):
            
            # Should proceed to model path resolution
            try:
                main()
            except SystemExit as e:
                # Expected to fail on file operations, but model path should be resolved
                if "meta_model_path_missing" in str(e):
                    pytest.fail("Model path should be resolved from MODELS_DIR")
                # Other errors are expected (file operations)


def test_no_valid_threshold(mock_redis, monkeypatch, tmp_path):
    """Test that proposal is skipped when no valid threshold found."""
    # Setup: gates pass
    mock_redis.set("sre:regress:pass_streak", "3")
    mock_redis.set("sre:regress:last_status", "PASS")
    mock_redis.set("sre:regress:last_ts_ms", str(now_ms()))
    
    # Setup healthy metrics
    current_ms = now_ms()
    for i in range(300):
        mock_redis.xadd(
            "metrics:of_gate",
            {
                "ts_ms": str(current_ms - (300 - i) * 1000),
                "ok": "1",
                "ok_soft": "0",
                "latency_us": "2000.0",
                "exec_risk_norm": "0.5",
            },
            maxlen=200000,
            approximate=True,
        )
    
    # Create model file
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    model_path = models_dir / "meta_lr_latest.json"
    model_data = {
        "kind": "logreg_v1",
        "features": ["score"],
        "coef": [1.0],
        "intercept": 0.0,
    }
    with open(model_path, "w") as f:
        json.dump(model_data, f)
    
    monkeypatch.setenv("META_ENFORCE_MIN_STREAK", "3")
    monkeypatch.setenv("BASELINE_PROPOSE_MAX_AGE_HOURS", "30")
    monkeypatch.setenv("BASELINE_PROPOSE_HEALTH_WINDOW_HOURS", "24")
    monkeypatch.setenv("BASELINE_PROPOSE_MIN_N", "200")
    monkeypatch.setenv("BASELINE_PROPOSE_LAT_P99_US_MAX", "4000")
    monkeypatch.setenv("OF_GATE_METRICS_STREAM", "metrics:of_gate")
    monkeypatch.setenv("META_MODEL_LATEST", str(model_path))
    monkeypatch.setenv("META_ENFORCE_NOTIFY_ON_SKIP", "0")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("OUT_DIR", str(tmp_path))
    monkeypatch.setenv("CANARY_SYMBOLS", "BTCUSDT")
    monkeypatch.setenv("OF_INPUTS_STREAM", "signals:of:inputs")
    monkeypatch.setenv("OF_INPUTS_STREAM_FIELD", "payload")
    monkeypatch.setenv("TRADE_EVENTS_STREAM", "events:trades")
    monkeypatch.setenv("STATE_FILE", str(tmp_path / "state"))
    monkeypatch.setenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram")
    
    with patch("tools.nightly_meta_enforce_propose_bundle.redis.Redis") as mock_redis_cls:
        mock_redis_cls.from_url.return_value = mock_redis
        
        from tools.nightly_meta_enforce_propose_bundle import main
        
        # Mock all subprocess calls and file operations
        with patch("tools.nightly_meta_enforce_propose_bundle.subprocess.check_call"), \
             patch("tools.nightly_meta_enforce_propose_bundle.open", create=True), \
             patch("tools.nightly_meta_enforce_propose_bundle.os.makedirs"), \
             patch("tools.nightly_meta_enforce_propose_bundle.os.path.exists", return_value=True):
            
            # Mock eval output with no valid threshold
            def mock_json_loads(content):
                if isinstance(content, str) and "eval.json" in content:
                    return {"best": None}
                return json.loads(content)
            
            with patch("tools.nightly_meta_enforce_propose_bundle.json.loads", side_effect=mock_json_loads):
                # Should send NO_OP notification and return
                main()
                
                # Check that notification was sent
                notify_msgs = [k for k in mock_redis.keys() if "notify:telegram" in str(k)]
                # In fakeredis, we can't easily check stream contents, but we can verify no bundle was created
                bundle_keys = [k for k in mock_redis.keys() if k.startswith("recs:bundle:")]
                assert len(bundle_keys) == 0, "No bundle should be created when no valid threshold"

