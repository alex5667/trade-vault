import ast
import os
import pytest

from unittest.mock import AsyncMock, patch

from orderflow_services.auto_rollback_trigger_engine_v1 import process_loop as auto_rollback_loop

def test_ast_no_nested_asyncio_run():
    files_to_check = [
        "python-worker/orderflow_services/post_commit_verifier_v1.py",
        "python-worker/orderflow_services/auto_rollback_trigger_engine_v1.py",
        "python-worker/orderflow_services/ml_recommendation_commit_executor_v1.py",
    ]
    
    for file_path in files_to_check:
        if not os.path.exists(file_path):
            continue
        with open(file_path, "r", encoding="utf-8") as f:
            tree = ast.parse(f.read(), filename=file_path)
            
        for node in ast.walk(tree):
            if isinstance(node, ast.While):
                # Check for asyncio.run inside While
                for subnode in ast.walk(node):
                    if isinstance(subnode, ast.Call) and isinstance(subnode.func, ast.Attribute):
                        if isinstance(subnode.func.value, ast.Name) and subnode.func.value.id == "asyncio" and subnode.func.attr == "run":
                            pytest.fail(f"Found nested asyncio.run inside While loop in {file_path}")


def test_ast_no_blocking_sleep_in_async():
    files_to_check = [
        "python-worker/orderflow_services/post_commit_verifier_v1.py",
        "python-worker/orderflow_services/auto_rollback_trigger_engine_v1.py",
        "python-worker/orderflow_services/ml_recommendation_commit_executor_v1.py",
    ]
    
    for file_path in files_to_check:
        if not os.path.exists(file_path):
            continue
        with open(file_path, "r", encoding="utf-8") as f:
            tree = ast.parse(f.read(), filename=file_path)
            
        for node in ast.walk(tree):
            if isinstance(node, ast.AsyncFunctionDef):
                for subnode in ast.walk(node):
                    if isinstance(subnode, ast.Call) and isinstance(subnode.func, ast.Attribute):
                        if isinstance(subnode.func.value, ast.Name) and subnode.func.value.id == "time" and subnode.func.attr == "sleep":
                            pytest.fail(f"Found blocking time.sleep inside async function '{node.name}' in {file_path}")


@pytest.mark.asyncio
async def test_idempotency_duplicate_suppression():
    """
    Test that auto_rollback_trigger_engine_v1 correctly suppresses duplicate rollback requests
    using nx=True on the redis set command.
    """
    mock_cli = AsyncMock()
    mock_helper = AsyncMock()
    
    # Use a set to track which keys have been "set" with nx=True
    _set_keys = set()
    async def mock_set(key, value, *args, **kwargs):
        if kwargs.get("nx"):
            if key in _set_keys:
                return None
            _set_keys.add(key)
            return True
        return True
        
    mock_cli.set.side_effect = mock_set
    mock_cli.exists.return_value = False
    
    # Setup 2 identical messages in the stream
    mock_helper.claim_pending.return_value = ("0-0", [])
    
    msg_fields = {
        b"event": b"POST_COMMIT_VERIFICATION",
        b"verification_status": b"ROLLBACK_REQUIRED",
        b"recommendation_id": b"rec_123",
        b"reason_codes_json": b'["ERROR_RATE_SPIKE"]'
    }
    
    class MockMsg:
        def __init__(self, msg_id, fields):
            self.msg_id = msg_id
            self.fields = fields
            
    mock_cli.xreadgroup.return_value = [
        (b"stream_in", [(b"1-0", msg_fields), (b"2-0", msg_fields)])
    ]
    
    await auto_rollback_loop(
        cli=mock_cli,
        helper=mock_helper,
        in_stream="stream_in",
        out_stream="stream_out",
        group="group",
        consumer="consumer"
    )
    
    # Ensure xadd was only called ONCE despite two messages
    assert mock_cli.xadd.call_count == 1
