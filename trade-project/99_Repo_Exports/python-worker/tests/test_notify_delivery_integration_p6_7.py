import json
import os
import subprocess
import sys
import time

import pytest
import redis
import requests

from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS

# Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
METRICS_URL = "http://localhost:9125/metrics"

@pytest.fixture
def redis_client():
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)

@pytest.fixture
def clean_redis(redis_client):
    for key in redis_client.scan_iter("notify:*"):
        redis_client.delete(key)
    yield redis_client
    for key in redis_client.scan_iter("notify:*"):
        redis_client.delete(key)

def test_metrics_endpoint_exposed():
    """Verify that the metrics endpoint is reachable and exposes expected metrics."""
    # This test presumes the service is running.
    # Since we are in a test environment, we might need to rely on the service being up.
    # If not up, we skip.
    try:
        resp = requests.get(METRICS_URL, timeout=1)
        assert resp.status_code == 200
        assert "notify_send_total" in resp.text
        assert "notify_queue_lag_ms" in resp.text
    except requests.exceptions.ConnectionError:
        pytest.skip("Metrics endpoint not accessible (service might not be running)")

def test_health_check_tool(clean_redis):
    """Verify check_notify_delivery_health.py logic against Redis data."""
    r = clean_redis

    # 1. Healthy state
    # Simulate recent OK and low lag
    now_ms = get_ny_time_millis()
    r.set("notify:last_ok_ts_ms", now_ms - 1000) # 1 sec ago
    r.set("notify:last_queue_lag_ms", 100)
    r.set("notify:last_pending_n", 0)

    # Run tool
    cmd = [sys.executable, "python-worker/tools/check_notify_delivery_health.py"]

    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 0
    data = json.loads(res.stdout)
    assert data["status"] == "ok"

    # 2. Stale + Lag state (Critical)
    # 10 mins ago last ok
    r.set("notify:last_ok_ts_ms", now_ms - 600000)
    # High lag
    r.set("notify:last_queue_lag_ms", 2000000)

    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 2
    data = json.loads(res.stdout)
    assert data["status"] == "crit"
    assert any("Queue lag" in i for i in data["issues"])

    # 3. High Error Rate
    # Reset lag/ok to healthy logic for this part
    r.set("notify:last_queue_lag_ms", 0)
    r.set("notify:last_ok_ts_ms", now_ms)

    # Fill errors in last 5 buckets
    bucket = int(time.time() / 60)
    for i in range(5):
        r.hset(f"notify:win5m:{bucket-i}", "err", 10)
        r.hset(f"notify:win5m:{bucket-i}", "ok", 1) # 10 err vs 1 ok = high rate

    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 2
    data = json.loads(res.stdout)
    assert data["status"] == "crit"
    assert any("High error rate" in i for i in data["issues"])

def test_sre_monitor_alert(clean_redis):
    """Verify notify_delivery_sre_monitor.py sends alert on failure."""
    r = clean_redis

    # Simulate CRIT state
    r.set("notify:last_queue_lag_ms", 9999999)

    cmd = [sys.executable, "python-worker/tools/notify_delivery_sre_monitor.py"]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 2

    # Check alert stream
    entries = r.xread({RS.NOTIFY_TELEGRAM_CRIT: "0-0"}, count=1)
    assert len(entries) > 0
    stream, msgs = entries[0]
    msg_id, fields = msgs[0]
    payload = json.loads(fields["payload"])
    assert "Delivery Degraded" in payload["message"]

