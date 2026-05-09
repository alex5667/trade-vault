import time
import unittest.mock as mock

import orderflow_services.route_incident_rca_shadow_comparator_v3_6 as _mod
from orderflow_services.route_incident_rca_shadow_comparator_v3_6 import (
    compare_rows,
    correlation_key,
    refresh_pending_metrics,
)


def test_correlation_key_prefers_incident_id():
    row = {"incident_id": "i1", "request_id": "r1", "compact_hash": "h1"}
    assert correlation_key(row) == "i1"


def test_compare_rows_match():
    handoff = {
        "incident_id": "i1",
        "task_type": "route_incident_rca",
        "severity": "warning",
        "compact_hash": "abc",
        "payload_json": '{"summary":"x","primary_reason_codes":["ROUTE_MISMATCH"]}',
    }
    legacy = {
        "incident_id": "i1",
        "task_type": "route_incident_rca",
        "severity": "warning",
        "compact_hash": "abc",
        "payload_json": '{"summary":"x","primary_reason_codes":["ROUTE_MISMATCH"]}',
    }
    out = compare_rows(handoff, legacy)
    assert out["status"] == "MATCH"
    assert out["score"] >= 0.90


def test_compare_rows_detects_drift():
    handoff = {
        "incident_id": "i1",
        "task_type": "route_incident_rca",
        "severity": "warning",
        "payload_json": '{"summary":"x","primary_reason_codes":["A"],"extra_h":"1"}',
    }
    legacy = {
        "incident_id": "i1",
        "task_type": "route_incident_rca",
        "severity": "warning",
        "payload_json": '{"summary":"x","primary_reason_codes":["B"],"extra_l":"1"}',
    }
    out = compare_rows(handoff, legacy)
    assert out["status"] in {"DRIFT", "MISMATCH"}
    assert "PAYLOAD_KEY_DRIFT" in out["reason_codes"]


import pytest


@pytest.mark.asyncio
async def test_refresh_pending_metrics_throttled():
    """scan_iter must not be called twice within METRICS_REFRESH_INTERVAL_SEC."""
    fake_redis = mock.AsyncMock()
    fake_redis.scan_iter = mock.MagicMock(return_value=_aiter([]))

    _mod._last_metrics_refresh = 0.0
    await refresh_pending_metrics(fake_redis)
    assert fake_redis.scan_iter.called, "first call should run scan_iter"

    fake_redis.scan_iter.reset_mock()
    # second call immediately — should be throttled
    await refresh_pending_metrics(fake_redis)
    assert not fake_redis.scan_iter.called, "throttled call must not run scan_iter"


@pytest.mark.asyncio
async def test_refresh_pending_metrics_runs_after_interval():
    """scan_iter must be called again after interval expires."""
    fake_redis = mock.AsyncMock()
    fake_redis.scan_iter = mock.MagicMock(return_value=_aiter([]))

    _mod._last_metrics_refresh = time.monotonic() - _mod.METRICS_REFRESH_INTERVAL_SEC - 1
    await refresh_pending_metrics(fake_redis)
    assert fake_redis.scan_iter.called, "call after interval must run scan_iter"


def _aiter(items):
    """Sync list → async iterator helper for mocking scan_iter."""
    async def _gen():
        for item in items:
            yield item
    return _gen()
