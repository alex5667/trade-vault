from __future__ import annotations
from core.redis_keys import RedisStreams as RS

"""Tests for ML nightly train and report v1."""

import json
import os
import tempfile
import time
from unittest.mock import MagicMock, patch

from tools.ml_nightly_train_and_report_v1 import (
    _f,
    _i,
    _is_closed_event,
    _safe_json_dumps,
    _stream_id_ms,
    choose_best_model,
    export_of_inputs_ndjson,
    export_trades_closed_ndjson,
    format_model_summary,
    make_hset_bundle,
    notify_telegram,
    now_ms,
    recs_sign,
    run_cmd,
    write_bundle,
)


def test_now_ms():
    """Test now_ms returns milliseconds."""
    t1 = now_ms()
    time.sleep(0.01)
    t2 = now_ms()
    assert t2 > t1
    assert isinstance(t1, int)
    assert isinstance(t2, int)


def test_i():
    """Test _i converts to int."""
    assert _i("1.5") == 1
    assert _i(2.0) == 2
    assert _i(None, 5) == 5
    assert _i("invalid", 10) == 10


def test_f():
    """Test _f converts to float."""
    assert _f("1.5") == 1.5
    assert _f(2.0) == 2.0
    assert _f(None, 0.0) == 0.0
    assert _f("invalid", 1.0) == 1.0


def test_safe_json_dumps():
    """Test _safe_json_dumps produces compact JSON."""
    obj = {"a": 1, "b": "test"}
    result = _safe_json_dumps(obj)
    assert isinstance(result, str)
    parsed = json.loads(result)
    assert parsed == obj
    # Should be compact (no spaces)
    assert " " not in result or result.count(" ") < 3


def test_stream_id_ms():
    """Test _stream_id_ms extracts timestamp from Redis stream ID."""
    assert _stream_id_ms("1700000000000-0") == 1700000000000
    assert _stream_id_ms("1700000000000-123") == 1700000000000
    assert _stream_id_ms("invalid") == 0
    assert _stream_id_ms("") == 0


def test_notify_telegram():
    """Test notify_telegram sends to Telegram stream."""
    mock_redis = MagicMock()
    mock_xadd = MagicMock()
    mock_redis.xadd = mock_xadd

    notify_telegram(mock_redis, "Test message")
    assert mock_xadd.called
    call_args = mock_xadd.call_args
    assert call_args[0][0] == os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM)
    assert "text" in call_args[0][1]
    assert call_args[0][1]["text"] == "Test message"
    assert "type" in call_args[0][1]
    assert call_args[0][1]["type"] == "report"


def test_notify_telegram_with_buttons():
    """Test notify_telegram with inline buttons."""
    mock_redis = MagicMock()
    mock_xadd = MagicMock()
    mock_redis.xadd = mock_xadd

    buttons = [[{"text": "Test", "callback": "test:callback"}]]
    notify_telegram(mock_redis, "Test", buttons)
    call_args = mock_xadd.call_args
    assert "buttons" in call_args[0][1]
    buttons_json = json.loads(call_args[0][1]["buttons"])
    assert len(buttons_json) == 1
    assert buttons_json[0][0]["text"] == "Test"


def test_recs_sign():
    """Test recs_sign generates HMAC signature."""
    secret = "test_secret"
    bundle_id = "test_bundle_123"
    sig = recs_sign(bundle_id, secret)
    assert isinstance(sig, str)
    assert len(sig) == 8
    # Same input should produce same signature
    sig2 = recs_sign(bundle_id, secret)
    assert sig == sig2
    # Different secret should produce different signature
    sig3 = recs_sign(bundle_id, "different_secret")
    assert sig != sig3


def test_make_hset_bundle():
    """Test bundle creation for HSET operations."""
    with patch.dict(os.environ, {"RECS_HMAC_SECRET": "test_secret"}):
        bundle = make_hset_bundle(
            cfg_key="cfg:ml_confirm",
            changes={"field1": "value1", "field2": "value2"},
            who="test_user",
            ttl_sec=3600
        )
        assert isinstance(bundle.bundle_id, str)
        assert len(bundle.bundle_id) > 0
        assert isinstance(bundle.sig, str)
        assert len(bundle.sig) == 8
        assert bundle.bundle["id"] == bundle.bundle_id
        assert bundle.bundle["who"] == "test_user"
        assert bundle.bundle["ttl_sec"] == 3600
        assert len(bundle.bundle["ops"]) == 2
        assert bundle.bundle["ops"][0]["op"] == "HSET"
        assert bundle.bundle["ops"][0]["key"] == "cfg:ml_confirm"
        assert bundle.bundle["meta"]["kind"] == "ml_train_register_challenger_v1"


def test_write_bundle():
    """Test writing bundle to Redis."""
    mock_redis = MagicMock()
    mock_set = MagicMock()
    mock_redis.set = mock_set

    bundle = make_hset_bundle(
        cfg_key="cfg:test",
        changes={"field1": "value1"},
        who="test",
        ttl_sec=3600
    )
    write_bundle(mock_redis, bundle, 3600)
    assert mock_set.call_count == 2
    # Check bundle storage
    bundle_call = [c for c in mock_set.call_args_list if "recs:bundle:" in str(c[0][0])]
    assert len(bundle_call) == 1
    # Check status storage
    status_call = [c for c in mock_set.call_args_list if "recs:status:" in str(c[0][0])]
    assert len(status_call) == 1


def test_is_closed_event():
    """Test _is_closed_event detects closed events."""
    assert _is_closed_event({"event_type": "POSITION_CLOSED"}) is True
    assert _is_closed_event({"event_type": "CLOSE"}) is True
    assert _is_closed_event({"type": "POSITION_CLOSED"}) is True
    assert _is_closed_event({"type": "CLOSE"}) is True
    assert _is_closed_event({"event_type": "OPEN"}) is False
    assert _is_closed_event({"type": "OPEN"}) is False
    assert _is_closed_event({}) is False


def test_choose_best_model():
    """Test choose_best_model selects best model based on metrics."""
    # GBDT has lower Brier
    lr_meta = {"mean": {"brier": 0.2, "pr_auc": 0.7, "ece": 0.1}}
    gb_meta = {"mean": {"brier": 0.15, "pr_auc": 0.7, "ece": 0.1}}
    assert choose_best_model(lr_meta, gb_meta) == "gbdt"

    # LR has lower Brier
    lr_meta2 = {"mean": {"brier": 0.15, "pr_auc": 0.7, "ece": 0.1}}
    gb_meta2 = {"mean": {"brier": 0.2, "pr_auc": 0.7, "ece": 0.1}}
    assert choose_best_model(lr_meta2, gb_meta2) == "lr"

    # Tie on Brier, GBDT has higher PR-AUC
    lr_meta3 = {"mean": {"brier": 0.2, "pr_auc": 0.7, "ece": 0.1}}
    gb_meta3 = {"mean": {"brier": 0.2, "pr_auc": 0.75, "ece": 0.1}}
    assert choose_best_model(lr_meta3, gb_meta3) == "gbdt"

    # Tie on Brier and PR-AUC, GBDT has lower ECE
    lr_meta4 = {"mean": {"brier": 0.2, "pr_auc": 0.7, "ece": 0.15}}
    gb_meta4 = {"mean": {"brier": 0.2, "pr_auc": 0.7, "ece": 0.1}}
    assert choose_best_model(lr_meta4, gb_meta4) == "gbdt"


def test_format_model_summary():
    """Test format_model_summary formats model metrics."""
    meta = {
        "mean": {
            "pr_auc": 0.75,
            "logloss": 0.5,
            "brier": 0.2,
            "ece": 0.1
        }
    }
    result = format_model_summary("LR", meta)
    assert "LR:" in result
    assert "pr_auc=0.7500" in result
    assert "logloss=0.5000" in result
    assert "brier=0.2000" in result
    assert "ece=0.1000" in result


def test_run_cmd():
    """Test run_cmd executes command and returns result."""
    result = run_cmd(["echo", "test"])
    assert result.code == 0
    assert "test" in result.out
    assert isinstance(result.out, str)
    assert isinstance(result.err, str)


def test_run_cmd_failure():
    """Test run_cmd handles command failures."""
    result = run_cmd(["false"])
    assert result.code != 0


def test_export_of_inputs_ndjson_empty_stream():
    """Test export_of_inputs_ndjson handles empty stream."""
    mock_redis = MagicMock()
    mock_redis.xrevrange = MagicMock(return_value=[])

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.ndjson') as f:
        out_path = f.name

    try:
        written, scanned = export_of_inputs_ndjson(
            r=mock_redis,
            stream="test:stream",
            out_path=out_path,
            since_ms=0,
            max_scan=1000,
        )
        assert written == 0
        assert scanned == 0
        # File should exist but be empty or have header only
        with open(out_path) as f:
            content = f.read()
            assert len(content) == 0 or content.count('\n') == 0
    finally:
        if os.path.exists(out_path):
            os.unlink(out_path)


def test_export_trades_closed_ndjson_filters_events():
    """Test export_trades_closed_ndjson filters only closed events."""
    mock_redis = MagicMock()

    # Mock stream with mixed events - use side_effect to return empty on second call
    mock_redis.xrevrange = MagicMock(side_effect=[
        [
            ("1700000000000-0", {"payload": json.dumps({"event_type": "POSITION_CLOSED", "sid": "test1", "ts_ms": 1700000000000})}),
            ("1700000000001-0", {"payload": json.dumps({"event_type": "OPEN", "sid": "test2", "ts_ms": 1700000001000})}),
            ("1700000000002-0", {"payload": json.dumps({"event_type": "CLOSE", "sid": "test3", "ts_ms": 1700000002000})}),
        ],
        [],  # Second call returns empty (end of stream)
    ])

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.ndjson') as f:
        out_path = f.name

    try:
        written, scanned = export_trades_closed_ndjson(
            r=mock_redis,
            stream="test:stream",
            out_path=out_path,
            since_ms=0,
            max_scan=1000,
        )
        # Should write 2 closed events (POSITION_CLOSED and CLOSE)
        assert written == 2
        assert scanned == 3

        # Verify content
        with open(out_path) as f:
            lines = [line.strip() for line in f if line.strip()]
            assert len(lines) == 2
            for line in lines:
                obj = json.loads(line)
                assert obj["sid"] in ("test1", "test3")
                assert _is_closed_event(obj)
    finally:
        if os.path.exists(out_path):
            os.unlink(out_path)


def test_export_of_inputs_ndjson_with_data():
    """Test export_of_inputs_ndjson exports data correctly."""
    mock_redis = MagicMock()

    # Mock stream with data
    mock_redis.xrevrange = MagicMock(side_effect=[
        [
            ("1700000002000-0", {"payload": json.dumps({"ts_ms": 1700000002000, "sid": "test2"})}),
            ("1700000001000-0", {"payload": json.dumps({"ts_ms": 1700000001000, "sid": "test1"})}),
        ],
        [],  # Second call returns empty (end of stream)
    ])

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.ndjson') as f:
        out_path = f.name

    try:
        written, scanned = export_of_inputs_ndjson(
            r=mock_redis,
            stream="test:stream",
            out_path=out_path,
            since_ms=0,
            max_scan=1000,
        )
        assert written == 2
        assert scanned == 2

        # Verify content is in chronological order
        with open(out_path) as f:
            lines = [line.strip() for line in f if line.strip()]
            assert len(lines) == 2
            obj1 = json.loads(lines[0])
            obj2 = json.loads(lines[1])
            assert obj1["ts_ms"] == 1700000001000
            assert obj2["ts_ms"] == 1700000002000
    finally:
        if os.path.exists(out_path):
            os.unlink(out_path)


def test_export_trades_closed_ndjson_without_sid():
    """Test export_trades_closed_ndjson filters events without sid."""
    mock_redis = MagicMock()

    # Use side_effect to return empty on second call
    mock_redis.xrevrange = MagicMock(side_effect=[
        [
            ("1700000000000-0", {"payload": json.dumps({"event_type": "POSITION_CLOSED", "ts_ms": 1700000000000})}),
            ("1700000000001-0", {"payload": json.dumps({"event_type": "POSITION_CLOSED", "sid": "test1", "ts_ms": 1700000001000})}),
        ],
        [],  # Second call returns empty (end of stream)
    ])

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.ndjson') as f:
        out_path = f.name

    try:
        written, scanned = export_trades_closed_ndjson(
            r=mock_redis,
            stream="test:stream",
            out_path=out_path,
            since_ms=0,
            max_scan=1000,
        )
        # Should write only 1 event (with sid)
        assert written == 1
        assert scanned == 2

        with open(out_path) as f:
            lines = [line.strip() for line in f if line.strip()]
            assert len(lines) == 1
            obj = json.loads(lines[0])
            assert obj["sid"] == "test1"
    finally:
        if os.path.exists(out_path):
            os.unlink(out_path)


def test_export_trades_closed_ndjson_timestamp_from_stream_id():
    """Test export_trades_closed_ndjson uses stream ID for timestamp if missing."""
    mock_redis = MagicMock()

    # Use side_effect to return empty on second call
    mock_redis.xrevrange = MagicMock(side_effect=[
        [
            ("1700000000000-0", {"payload": json.dumps({"event_type": "POSITION_CLOSED", "sid": "test1"})}),
        ],
        [],  # Second call returns empty (end of stream)
    ])

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.ndjson') as f:
        out_path = f.name

    try:
        written, scanned = export_trades_closed_ndjson(
            r=mock_redis,
            stream="test:stream",
            out_path=out_path,
            since_ms=0,
            max_scan=1000,
        )
        assert written == 1

        with open(out_path) as f:
            lines = [line.strip() for line in f if line.strip()]
            obj = json.loads(lines[0])
            assert obj["ts_ms"] == 1700000000000
    finally:
        if os.path.exists(out_path):
            os.unlink(out_path)


def test_choose_best_model_empty_meta():
    """Test choose_best_model handles empty metadata."""
    lr_meta = {}
    gb_meta = {}
    # Should default to lr when both are empty
    result = choose_best_model(lr_meta, gb_meta)
    assert result in ("lr", "gbdt")


def test_choose_best_model_missing_fields():
    """Test choose_best_model handles missing metric fields."""
    lr_meta = {"mean": {"brier": 0.2}}
    gb_meta = {"mean": {"brier": 0.15}}
    assert choose_best_model(lr_meta, gb_meta) == "gbdt"


def test_format_model_summary_missing_fields():
    """Test format_model_summary handles missing metric fields."""
    meta = {"mean": {"pr_auc": 0.75}}
    result = format_model_summary("LR", meta)
    assert "LR:" in result
    assert "pr_auc=0.7500" in result
    # Should have defaults for missing fields
    assert "logloss=" in result
    assert "brier=" in result
    assert "ece=" in result


def test_format_model_summary_empty_mean():
    """Test format_model_summary handles empty mean dict."""
    meta = {"mean": {}}
    result = format_model_summary("GBDT", meta)
    assert "GBDT:" in result
    # Should use defaults
    assert "pr_auc=0.0000" in result

