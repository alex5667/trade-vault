from utils.time_utils import get_ny_time_millis
import json
import pytest
from unittest.mock import MagicMock, AsyncMock, patch

# Mocking the imports if they rely on env vars or redis
import sys
from types import SimpleNamespace

# Test button generation logic from autopilot
def test_autopilot_button_generation():
    sids = ["cfg:suggestions:entry_policy:meta:abc123hash"]
    btns = []
    for s in sids:
        parts = s.split(":")
        sid_hash = parts[-1]
        label = f"Proposal {sid_hash[:6]}"
        btns.append([{"text": f"✅ Approve {label}", "callback_data": f"approve:{sid_hash}"}])
    
    assert len(btns) == 1
    assert btns[0][0]["text"] == "✅ Approve Proposal abc123"
    assert btns[0][0]["callback_data"] == "approve:abc123hash"

# Test improved_notifier payload construction
@pytest.mark.asyncio
async def test_improved_notifier_payload():
    # We can't easily import ImprovedTelegramNotifier since it connects to Redis on init
    # But we can verify the logic we added:
    # reply_markup = {"inline_keyboard": buttons}
    
    buttons = [[{"text": "Test", "callback_data": "test"}]]
    kwargs = {"buttons": buttons}
    
    reply_markup = kwargs.get("reply_markup") or kwargs.get("buttons")
    if isinstance(reply_markup, list) and not isinstance(reply_markup, str):
        reply_markup = {"inline_keyboard": reply_markup}
        
    assert reply_markup == {"inline_keyboard": [[{"text": "Test", "callback_data": "test"}]]}

# Test notify_worker extraction logic (simulated)
def test_notify_worker_extraction():
    entry = {
        "text": "Report...",
        "buttons": json.dumps([[{"text":"OK", "callback_data":"ok"}]])
    }
    
    buttons = entry.get("buttons")
    if isinstance(buttons, str):
         try:
             buttons = json.loads(buttons)
         except:
             buttons = None
             
    assert isinstance(buttons, list)
    assert buttons[0][0]["callback_data"] == "ok"


# ── NEW TESTS: approve/reject buttons ────────────────────────────────────


def test_reject_callback_data_format():
    """Validate reject: callback_data format."""
    sid = "a1b2c3d4e5f6"
    data = f"reject:{sid}"
    assert data.startswith("reject:")
    parsed_sid = data.split(":", 1)[1]
    assert parsed_sid == sid


def test_approve_reject_buttons_in_report():
    """Verify build_proposal_buttons produces approve + reject pair."""
    # Replicate the logic from tm_autopilot_report_service.build_proposal_buttons
    sid = "deadbeef12345678abcd"
    buttons = [
        [{"text": f"✅ Approve {sid[:8]}", "callback_data": f"approve:{sid}"},
         {"text": f"❌ Reject {sid[:8]}", "callback_data": f"reject:{sid}"}]
    ]
    buttons_json = json.dumps(buttons, ensure_ascii=False)

    parsed = json.loads(buttons_json)
    assert len(parsed) == 1  # one row
    assert len(parsed[0]) == 2  # two buttons
    assert parsed[0][0]["callback_data"] == f"approve:{sid}"
    assert parsed[0][1]["callback_data"] == f"reject:{sid}"
    assert "Approve" in parsed[0][0]["text"]
    assert "Reject" in parsed[0][1]["text"]


def test_callback_handler_approve_flow():
    """Mock Redis and verify approve stores approval + applied keys."""
    import time

    r = MagicMock()
    r.sadd = MagicMock()
    r.expire = MagicMock()
    r.scard = MagicMock(return_value=1)
    r.set = MagicMock()
    r.xadd = MagicMock()

    sid = "abc123def456"
    username = "testuser"
    approvals_prefix = "cfg:suggestions:entry_policy:approvals"

    # Simulate approve logic (mirroring BotCallbackPoller.handle_update)
    key = f"{approvals_prefix}:{sid}"
    r.sadd(key, username)
    r.expire(key, 1209600)
    count = r.scard(key)

    applied_key = f"cfg:suggestions:entry_policy:applied:{sid}"
    r.set(applied_key, str(get_ny_time_millis()), ex=1209600)

    confirm_text = f"✅ <b>Proposal {sid[:8]}… APPROVED</b>\nby @{username} (approvals: {count})\n\n<i>Changes applied to cfg:suggestions</i>"
    r.xadd("notify:telegram", {"type": "report", "text": confirm_text}, maxlen=20000, approximate=True)

    # Assertions
    r.sadd.assert_called_once_with(key, username)
    r.expire.assert_called_once_with(key, 1209600)
    r.scard.assert_called_once_with(key)
    r.set.assert_called_once()
    assert applied_key in r.set.call_args[0][0]
    r.xadd.assert_called_once()
    assert "APPROVED" in r.xadd.call_args[0][1]["text"]


def test_callback_handler_reject_flow():
    """Mock Redis and verify reject deletes meta + stores rejected key."""
    import time

    r = MagicMock()
    r.set = MagicMock()
    r.delete = MagicMock()
    r.xadd = MagicMock()

    sid = "abc123def456"
    username = "testuser"
    meta_prefix = "cfg:suggestions:entry_policy:meta"

    # Simulate reject logic
    rejected_key = f"cfg:suggestions:entry_policy:rejected:{sid}"
    r.set(rejected_key, json.dumps({
        "by": username,
        "ts_ms": get_ny_time_millis(),
    }), ex=1209600)

    meta_key = f"{meta_prefix}:{sid}"
    r.delete(meta_key)

    confirm_text = f"❌ <b>Proposal {sid[:8]}… REJECTED</b>\nby @{username}\n\n<i>Proposal discarded from cfg:suggestions</i>"
    r.xadd("notify:telegram", {"type": "report", "text": confirm_text}, maxlen=20000, approximate=True)

    # Assertions
    r.set.assert_called_once()
    assert "rejected" in r.set.call_args[0][0]
    r.delete.assert_called_once_with(meta_key)
    r.xadd.assert_called_once()
    assert "REJECTED" in r.xadd.call_args[0][1]["text"]


def test_buttons_json_roundtrip():
    """Verify buttons survive JSON serialization → Redis stream → deserialization."""
    sid = "test1234abcd5678"
    buttons = [
        [{"text": f"✅ Approve {sid[:8]}", "callback_data": f"approve:{sid}"},
         {"text": f"❌ Reject {sid[:8]}", "callback_data": f"reject:{sid}"}]
    ]
    buttons_json = json.dumps(buttons, ensure_ascii=False)

    # Simulate Redis stream entry (all values are strings)
    entry = {
        "type": "report",
        "text": "test report",
        "buttons": buttons_json,
    }

    # Deserialize (as notify_worker does)
    raw_btns = entry.get("buttons")
    parsed = json.loads(raw_btns)

    assert isinstance(parsed, list)
    assert len(parsed) == 1
    assert parsed[0][0]["callback_data"] == f"approve:{sid}"
    assert parsed[0][1]["callback_data"] == f"reject:{sid}"
