import json
import os
import time
from unittest.mock import MagicMock, patch

import pytest
from prometheus_client import REGISTRY

from core.research_guard_calibrator import NightlyReport
from services.research_guard_calibrator_service import (
    BLOCKER_KEY_DEFAULT,
    SUMMARY_KEY_DEFAULT,
    _apply_blocker_mode,
    _load_nightly_report,
    run_research_guard_calibration,
)

# ---------------------------------------------------------------------------
# Test Helpers
# ---------------------------------------------------------------------------

class FakeRedis:
    def __init__(self):
        self.store = {}
        self.hashes = {}
        self.xadds = []

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value, ex=None):
        self.store[key] = value

    def hgetall(self, key):
        return self.hashes.get(key, {})

    def hset(self, key, mapping):
        if key not in self.hashes:
            self.hashes[key] = {}
        self.hashes[key].update(mapping)

    def pipeline(self):
        class FakePipeline:
            def __init__(self, parent):
                self.parent = parent
            def set(self, key, value, ex=None):
                self.parent.set(key, value, ex)
            def execute(self):
                pass
        return FakePipeline(self)

    def xadd(self, name, fields, maxlen=None, approximate=True):
        self.xadds.append((name, fields))

# ---------------------------------------------------------------------------
# 1. Stale Redis Data (Fail-open)
# ---------------------------------------------------------------------------

def test_stale_redis_data_fail_open():
    """Verify that stale Redis data causes a transition to 'hold' and a fail-open mode in report."""
    fake_redis = FakeRedis()
    
    # 2 days old
    stale_ts = int((time.time() - 2 * 86400) * 1000)
    fake_redis.hset(SUMMARY_KEY_DEFAULT, {
        "psr": "0.98",
        "dsr": "0.95",
        "pbo": "0.05",
        "ece": "0.10",
        "brier": "0.20",
        "updated_ts_ms": str(stale_ts),
    })
    
    report = _load_nightly_report(fake_redis)
    assert report.has_data is True
    assert report.report_age_sec >= 86400  # definitely stale
    
    with patch("services.research_guard_calibrator_service.redis_lib.Redis") as mock_redis_cls:
        mock_redis_cls.from_url.return_value = fake_redis
        
        # Suppose previous mode was 'enforce'
        fake_redis.set("cfg:rg_calib:state", json.dumps({"mode": "enforce", "rollback_streak": 1}))
        
        # Run calibration
        result = run_research_guard_calibration("redis://fake", send_telegram=False, telegram_interval_sec=0)
        
        # Should rollback due to stale data failing the threshold in enforce mode
        assert result is not None
        assert "stale" in result.failing_metrics[0]
        assert result.recommend == "rollback"
        assert result.effective_mode == "report"
        
        # State should be updated in Redis blocker
        blocker = json.loads(fake_redis.get(BLOCKER_KEY_DEFAULT) or "{}")
        assert blocker.get("report_only") == 1
        assert blocker.get("rg_calib_mode") == "report"

# ---------------------------------------------------------------------------
# 2. End-to-End Flow (Inject mock failing nightly report)
# ---------------------------------------------------------------------------

def test_end_to_end_failing_nightly_report_transition():
    """Verify the flow where a failing nightly report triggers a rollback transition in blocker_key."""
    fake_redis = FakeRedis()
    
    # Fresh but failing metrics
    fresh_ts = int((time.time() - 60) * 1000)
    fake_redis.hset(SUMMARY_KEY_DEFAULT, {
        "psr": "0.80", # Fails psr_min (0.95)
        "dsr": "0.95",
        "pbo": "0.05",
        "ece": "0.10",
        "brier": "0.20",
        "updated_ts_ms": str(fresh_ts),
    })
    
    with patch("services.research_guard_calibrator_service.redis_lib.Redis") as mock_redis_cls:
        mock_redis_cls.from_url.return_value = fake_redis
        
        # Set previous state to enforce and rollback streak to 1
        fake_redis.set("cfg:rg_calib:state", json.dumps({
            "mode": "enforce", 
            "proof_streak": 7, 
            "rollback_streak": 1
        }))
        
        # In enforce mode, it takes 2 failures to rollback. This is the 2nd failure.
        result = run_research_guard_calibration("redis://fake", send_telegram=False, telegram_interval_sec=0)
        
        assert result is not None
        assert result.recommend == "rollback"
        assert result.effective_mode == "report"
        
        # Validate that `_apply_blocker_mode` was effectively called
        blocker = json.loads(fake_redis.get(BLOCKER_KEY_DEFAULT) or "{}")
        assert blocker.get("report_only") == 1
        assert blocker.get("rg_calib_mode") == "report"

# ---------------------------------------------------------------------------
# 3. Observability (Prometheus Metrics)
# ---------------------------------------------------------------------------

def test_prometheus_metrics_reporting():
    """Verify that Prometheus metrics are updated properly after calibration."""
    fake_redis = FakeRedis()
    
    # Fresh and passing metrics
    fresh_ts = int((time.time() - 60) * 1000)
    fake_redis.hset(SUMMARY_KEY_DEFAULT, {
        "psr": "0.99", 
        "dsr": "0.96",
        "pbo": "0.02",
        "ece": "0.05",
        "brier": "0.15",
        "updated_ts_ms": str(fresh_ts),
    })
    
    with patch("services.research_guard_calibrator_service.redis_lib.Redis") as mock_redis_cls:
        mock_redis_cls.from_url.return_value = fake_redis
        
        # State is report mode, proof streak 0
        fake_redis.set("cfg:rg_calib:state", json.dumps({"mode": "report", "proof_streak": 0}))
        
        run_research_guard_calibration("redis://fake", send_telegram=False, telegram_interval_sec=0)
        
        # Check prometheus registry directly
        psr_val = REGISTRY.get_sample_value("rg_calib_latest_psr")
        mode_val = REGISTRY.get_sample_value("rg_calib_mode")
        proof_streak_val = REGISTRY.get_sample_value("rg_calib_proof_streak")
        
        assert psr_val == 0.99
        assert mode_val == 0.0 # 'report' mode
        assert proof_streak_val == 1.0 # streak incremented by 1
