from __future__ import annotations

"""Tests for ML rollout guard (freeze/unfreeze proposals)."""


from unittest.mock import MagicMock, patch

from tools.ml_rollout_guard import (
    mk_bundle_ops,
    now_ms,
    pctl,
    propose,
    read_metrics,
    sign,
    summarize,
)
from core.redis_keys import RedisStreams as RS


def test_sign():
    """Test HMAC signature generation."""
    bundle_id = "test123"
    secret = "secret_key"

    sig1 = sign(bundle_id, secret)
    sig2 = sign(bundle_id, secret)

    # Deterministic
    assert sig1 == sig2
    assert len(sig1) == 8  # 8 hex chars

    # Different secret -> different sig
    sig3 = sign(bundle_id, "other_secret")
    assert sig1 != sig3


def test_pctl():
    """Test percentile calculation."""
    xs = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert pctl(xs, 0.0) == 1.0  # min
    assert pctl(xs, 1.0) == 5.0  # max
    assert pctl(xs, 0.5) == 3.0  # median

    # Empty list
    assert pctl([], 0.5) == 0.0


def test_summarize():
    """Test metrics summarization."""
    rows = [
        {"p_edge": "0.6", "latency_ms": "5.0", "err": "", "missing": "0", "status": ""},
        {"p_edge": "0.7", "latency_ms": "6.0", "err": "", "missing": "0", "status": ""},
        {"p_edge": "0.8", "latency_us": "7000", "err": "some_error", "missing": "1", "status": "MISSING_FAILCLOSED"},
    ]

    sm = summarize(rows)
    assert sm["n"] == 3.0
    assert sm["p50"] > 0.0
    assert sm["err_rate"] > 0.0
    assert sm["missing_rate"] > 0.0

    # Empty rows
    sm_empty = summarize([])
    assert sm_empty["n"] == 0.0


def test_mk_bundle_ops():
    """Test bundle operations creation."""
    ops = mk_bundle_ops("cfg:ml_confirm", {"enforce_share": "0.05", "freeze_reason": "test"})
    assert len(ops) == 2
    assert ops[0]["op"] == "HSET"
    assert ops[0]["key"] == "cfg:ml_confirm"
    assert ops[0]["field"] == "enforce_share"
    assert ops[0]["value"] == "0.05"


def test_read_metrics():
    """Test reading metrics from stream."""
    mock_redis = MagicMock()

    # Mock xrevrange to return messages
    mock_redis.xrevrange.return_value = [
        ("1000-0", {"ts_ms": "1000", "p_edge": "0.6"}),
        ("2000-0", {"ts_ms": "2000", "p_edge": "0.7"}),
    ]

    rows = read_metrics(mock_redis, RS.ML_CONFIRM_METRICS, since_ms=0, max_scan=100)
    assert len(rows) == 2
    assert rows[0]["ts_ms"] == "1000"


def test_propose(mock_redis=None):
    """Test proposal creation."""
    if mock_redis is None:
        mock_redis = MagicMock()

    with patch("tools.ml_rollout_guard.notify") as mock_notify:
        propose(
            mock_redis,
            cfg_key="cfg:ml_confirm",
            updates={"enforce_share": "0.05"},
            title="Test Proposal",
            details={"cur": 0.10, "new": 0.05}
        )

        # Check bundle was created
        assert mock_redis.set.call_count >= 2  # bundle + status

        # Check notification was sent
        assert mock_notify.called


def test_guard_freeze_condition():
    """Test freeze condition (hard bad metrics)."""
    mock_redis = MagicMock()

    # Mock metrics with bad values
    mock_redis.xrevrange.return_value = [
        ("1000-0", {"ts_ms": str(now_ms() - 1000), "p_edge": "0.6", "latency_ms": "10.0", "err": "some_error", "missing": "1", "status": "MISSING_FAILCLOSED"}),
    ] * 100  # Many bad metrics

    mock_redis.hgetall.return_value = {"enforce_share": "0.10"}
    mock_redis.get.return_value = None

    with patch("tools.ml_rollout_guard.propose") as mock_propose:
        # This would normally call main(), but we'll test the logic directly
        from tools.ml_rollout_guard import read_metrics, summarize

        rows = read_metrics(mock_redis, RS.ML_CONFIRM_METRICS, since_ms=now_ms() - 60000, max_scan=1000)
        sm = summarize(rows)

        # Check hard_bad condition
        miss_max = 0.02
        err_max = 0.01
        lat_p99_max = 6.0

        hard_bad = (
            sm.get("missing_rate", 0.0) > miss_max or
            sm.get("err_rate", 0.0) > err_max or
            sm.get("lat_p99", 0.0) > lat_p99_max
        )

        # With bad metrics, should trigger freeze
        assert hard_bad is True


def test_guard_unfreeze_condition():
    """Test unfreeze condition (good streak)."""
    mock_redis = MagicMock()

    # Mock good metrics
    mock_redis.xrevrange.return_value = [
        ("1000-0", {"ts_ms": str(now_ms() - 1000), "p_edge": "0.6", "latency_ms": "1.0", "err": "", "missing": "0", "status": "ALLOW"}),
    ] * 500  # Many good metrics

    mock_redis.hgetall.return_value = {"enforce_share": "0.05"}
    mock_redis.get.side_effect = lambda k: {
        "ml:rollout:good_streak_days": "7",  # Good streak
        "ml:rollout:pre_freeze_share": "0.10",  # Previous share
    }.get(k)

    with patch("tools.ml_rollout_guard.propose") as mock_propose:
        from tools.ml_rollout_guard import read_metrics, summarize

        rows = read_metrics(mock_redis, RS.ML_CONFIRM_METRICS, since_ms=now_ms() - 60000, max_scan=1000)
        sm = summarize(rows)

        # Check day_good condition
        pedge_p50_min = 0.20
        miss_max = 0.02
        err_max = 0.01
        lat_p99_max = 6.0

        day_good = (
            sm.get("n", 0.0) >= 200 and
            sm.get("p50", 0.0) >= pedge_p50_min and
            sm.get("missing_rate", 0.0) <= miss_max and
            sm.get("err_rate", 0.0) <= err_max and
            sm.get("lat_p99", 0.0) <= lat_p99_max
        )

        # With good metrics, should be day_good
        assert day_good is True

