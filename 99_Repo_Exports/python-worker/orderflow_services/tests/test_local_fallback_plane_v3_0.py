import pytest
from unittest.mock import AsyncMock, patch

from orderflow_services.local_fallback_plane_gateway_v3_0 import (
    evaluate_request,
    build_prompt,
    ALLOWED_TASKS
)

def test_evaluate_request_unsupported_task():
    policy = {
        "enabled": 1,
        "kill_switch": 0,
        "mode": "FALLBACK_ONLY",
        "max_prompt_chars": 10000,
        "max_input_json_bytes": 10000,
        "max_schema_bytes": 10000,
        "require_vertex_degraded": 1,
        "task_allowlist": set(ALLOWED_TASKS)
    }
    row = {
        "request_id": "req-1",
        "task_type": "invalid_task",
        "prompt": "Test"
    }
    
    res = evaluate_request(row, policy)
    assert res["accepted"] == 0
    assert res["reason_code"] == "TASK_NOT_SUPPORTED"

def test_evaluate_request_long_prompt():
    policy = {
        "enabled": 1,
        "kill_switch": 0,
        "mode": "FALLBACK_ONLY",
        "max_prompt_chars": 10,
        "max_input_json_bytes": 10000,
        "max_schema_bytes": 10000,
        "require_vertex_degraded": 1,
        "task_allowlist": set(ALLOWED_TASKS)
    }
    row = {
        "request_id": "req-2",
        "task_type": "emergency_summarize",
        "prompt": "This prompt is too long for the limit"
    }
    
    res = evaluate_request(row, policy)
    assert res["accepted"] == 0
    assert res["reason_code"] == "PROMPT_TOO_LARGE"

def test_evaluate_request_vertex_not_degraded():
    policy = {
        "enabled": 1,
        "kill_switch": 0,
        "mode": "FALLBACK_ONLY",
        "max_prompt_chars": 10000,
        "max_input_json_bytes": 10000,
        "max_schema_bytes": 10000,
        "require_vertex_degraded": 1,
        "task_allowlist": set(ALLOWED_TASKS)
    }
    row = {
        "request_id": "req-3",
        "task_type": "vertex_unavailable_fallback",
        "prompt": "Test",
        "vertex_unavailable": 0
    }
    
    res = evaluate_request(row, policy)
    assert res["accepted"] == 0
    assert res["reason_code"] == "VERTEX_NOT_DEGRADED"

def test_evaluate_request_accepted():
    policy = {
        "enabled": 1,
        "kill_switch": 0,
        "mode": "FALLBACK_ONLY",
        "max_prompt_chars": 10000,
        "max_input_json_bytes": 10000,
        "max_schema_bytes": 10000,
        "require_vertex_degraded": 1,
        "task_allowlist": set(ALLOWED_TASKS)
    }
    row = {
        "request_id": "req-3",
        "task_type": "emergency_summarize",
        "prompt": "Test",
        "vertex_unavailable": 1,
        "severity": "critical"
    }
    
    res = evaluate_request(row, policy)
    assert res["accepted"] == 1
    assert res["reason_code"] == "OK"
