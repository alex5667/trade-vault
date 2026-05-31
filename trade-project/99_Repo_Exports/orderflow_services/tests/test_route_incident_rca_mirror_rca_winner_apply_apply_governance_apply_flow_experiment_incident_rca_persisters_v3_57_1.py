import asyncio
import json
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_rollups_persister_v3_57_1 import (
    normalize as normalize_slo,
    main_loop as main_loop_slo,
    STREAM as SLO_STREAM,
    DLQ_STREAM as SLO_DLQ_STREAM,
)
from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_retry_results_persister_v3_57_1 import (
    normalize as normalize_retry,
)
from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_escalations_persister_v3_57_1 import (
    normalize as normalize_escalation,
)


def test_normalize_slo_from_payload_json():
    row = {
        "payload": '{"ts_ms":1712246400123,"window_min":10080,"verification_n":12,"verified_n":9,"rollback_planned_n":3,"rollback_applied_n":2,"retry_n":1,"escalation_n":1,"verify_keep_rate":0.75,"rollback_plan_rate":0.25,"rollback_applied_rate":0.6667,"rollback_mttr_p95_sec":120.5,"retry_rate":0.3333,"escalation_rate":0.3333,"mttr_slo_sec":900}'
    }
    out = normalize_slo(row)
    assert out["ts_ms"] == 1712246400123
    assert out["verification_n"] == 12
    assert out["rollback_mttr_p95_sec"] == 120.5


def test_normalize_retry_from_payload_json():
    row = {
        "payload": '{"ts_ms":1712246400222,"source_rollback_ts_ms":1712246400000,"source_verification_ts_ms":1712246399000,"rollback_mode":"AUTO","failed_target_mode":"VERTEX_ONLY","decision":"RETRY_ROLLBACK_TO_PREVIOUS_MODE","reason_code":"BRIDGE_MODE_MISMATCH_AFTER_APPLY","severity":"warning","attempts":1,"applied":0}'
    }
    out = normalize_retry(row)
    assert out["rollback_mode"] == "AUTO"
    assert out["reason_code"] == "BRIDGE_MODE_MISMATCH_AFTER_APPLY"
    assert out["attempts"] == 1


def test_normalize_escalation_from_payload_json():
    row = {
        "payload": '{"ts_ms":1712246400333,"source_rollback_ts_ms":1712246400000,"source_verification_ts_ms":1712246399000,"rollback_mode":"AUTO","failed_target_mode":"LOCAL_ONLY","decision":"ESCALATE","reason_code":"LOCAL_ONLY_UNDERPERFORMS_AFTER_APPLY","severity":"critical"}'
    }
    out = normalize_escalation(row)
    assert out["decision"] == "ESCALATE"
    assert out["severity"] == "critical"
    assert out["failed_target_mode"] == "LOCAL_ONLY"


class FakeRedis:
    def __init__(self, stream: str, entries):
        self.stream = stream
        self.entries = entries[:]
        self.acked = []
        self.adds = []
        self.xgroup_created = False

    async def xgroup_create(self, name, groupname, id, mkstream):
        self.xgroup_created = True
        return True

    async def xreadgroup(self, groupname, consumername, streams, count, block):
        if not self.entries:
            raise asyncio.CancelledError() # Breaks the loop
        batch = self.entries[:count]
        self.entries = self.entries[count:]
        return [(self.stream, batch)]

    async def xadd(self, stream, fields, maxlen=0, approximate=True):
        self.adds.append((stream, fields))
        return b"1-0"

    async def xack(self, stream, group, *ids):
        self.acked.extend(list(ids))
        return len(ids)

    async def close(self):
        pass


@pytest.mark.asyncio
@patch("orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_rollups_persister_v3_57_1.redis")
@patch("orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_rollups_persister_v3_57_1.psycopg")
async def test_integration_slo_rollups_persister_duplicate_idempotent_and_xack_after_commit(mock_psycopg, mock_redis):
    # Test valid message duplicate delivery
    valid_payload = '{"ts_ms":1712246400123,"window_min":10080,"verification_n":12}'
    entries = [
        (b"1-0", {b"payload": valid_payload.encode()}),
        (b"1-0", {b"payload": valid_payload.encode()}) # duplicate delivery
    ]
    
    fake_r = FakeRedis(SLO_STREAM, entries)
    mock_redis.from_url.return_value = fake_r
    
    mock_conn = AsyncMock()
    mock_cursor = AsyncMock()
    mock_cursor_ctx = AsyncMock()
    mock_cursor_ctx.__aenter__.return_value = mock_cursor
    mock_conn.cursor.return_value = mock_cursor_ctx
    mock_psycopg.AsyncConnection.connect.return_value = mock_conn

    # Check xack only after commit
    commit_called = False
    
    async def mock_commit():
        nonlocal commit_called
        assert len(fake_r.acked) == 0, "xack must not be called before commit"
        commit_called = True
        
    mock_conn.commit = AsyncMock(side_effect=mock_commit)

    try:
        await main_loop_slo()
    except asyncio.CancelledError:
        pass

    assert mock_conn.commit.call_count == 2
    assert len(fake_r.acked) == 2
    # Duplicate delivery attempts twice but the postgres ON CONFLICT query will safely upsert them idempotently.
    # The UPSERT query handles idempotency.
    execute_calls = mock_cursor.execute.call_args_list
    assert len(execute_calls) == 2
    for call in execute_calls:
        args, _ = call
        query: str = args[0]
        assert "ON CONFLICT (ts_ms)" in query
        assert "DO UPDATE SET" in query


@pytest.mark.asyncio
@patch("orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_rollups_persister_v3_57_1.redis")
@patch("orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_rollups_persister_v3_57_1.psycopg")
async def test_integration_slo_rollups_persister_bad_payload_to_dlq(mock_psycopg, mock_redis):
    bad_payload = '{"ts_ms":-1,"window_min":10080}'
    entries = [
        (b"1-0", {b"payload": bad_payload.encode()}),
    ]
    
    fake_r = FakeRedis(SLO_STREAM, entries)
    mock_redis.from_url.return_value = fake_r
    
    mock_conn = AsyncMock()
    mock_psycopg.AsyncConnection.connect.return_value = mock_conn

    try:
        await main_loop_slo()
    except asyncio.CancelledError:
        pass

    # No commits should have happened
    assert mock_conn.commit.call_count == 0
    # Payload must be xacked to avoid poison pill
    assert len(fake_r.acked) == 1
    # DLQ insert
    assert len(fake_r.adds) == 1
    stream, fields = fake_r.adds[0]
    assert stream == SLO_DLQ_STREAM
    assert "error" in fields
    assert fields["error"] == "ValueError"
