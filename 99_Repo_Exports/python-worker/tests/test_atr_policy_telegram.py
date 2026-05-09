from unittest import mock

import pytest

from services import atr_policy_telegram_callback_worker as cb


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    monkeypatch.setenv("ATR_POLICY_TELEGRAM_ALLOWED_USER_IDS", "1001,1002")
    monkeypatch.setenv("ATR_POLICY_TELEGRAM_ALLOWED_USERNAMES", "auth_user")
    monkeypatch.setenv("ATR_POLICY_TELEGRAM_ALLOWED_CHAT_IDS", "-12345")

def test_parse_callback():
    assert cb._parse_callback("atrpol:approve:abc") == ("approve", "abc")
    assert cb._parse_callback("atrpol:reject:xyz123") == ("reject", "xyz123")
    assert cb._parse_callback("invalid:approve:abc") == ("", "")
    assert cb._parse_callback("atrpol:abc") == ("", "")

def test_is_allowed():
    # Denied by chat_id
    assert not cb._is_allowed({"user_id": "1001", "chat_id": "-9999"})

    # Allowed by user_id
    assert cb._is_allowed({"user_id": "1001", "chat_id": "-12345"})

    # Allowed by username
    assert cb._is_allowed({"username": "Auth_User", "chat_id": "-12345"})

    # Denied by identity
    assert not cb._is_allowed({"user_id": "9999", "username": "bad_user", "chat_id": "-12345"})

@mock.patch("services.atr_policy_telegram_callback_worker._redis")
@mock.patch("services.atr_policy_telegram_callback_worker.publish_policy_ack_to_telegram")
@mock.patch("services.atr_policy_telegram_callback_worker.record_decision")
def test_handle_event_approve(m_record, m_ack, m_redis):
    r_mock = mock.MagicMock()
    m_redis.return_value = r_mock
    # Mock dedup nx true
    r_mock.set.return_value = True

    m_record.return_value = True

    evt = {
        "user_id": "1001",
        "chat_id": "-12345",
        "username": "tester",
        "callback": "atrpol:approve:prop123"
    }

    res = cb.handle_event(evt)
    assert res is True

    m_record.assert_called_once_with("prop123", action="APPROVE", actor="tester", note="via_telegram")
    m_ack.assert_called_once_with(proposal_id="prop123", action="APPROVE", actor="tester", note="ok")

@mock.patch("services.atr_policy_telegram_callback_worker._redis")
def test_handle_event_duplicate(m_redis):
    r_mock = mock.MagicMock()
    m_redis.return_value = r_mock
    # Mock dedup nx false
    r_mock.set.return_value = False

    evt = {
        "user_id": "1001",
        "chat_id": "-12345",
        "callback": "atrpol:approve:prop123"
    }

    res = cb.handle_event(evt)
    # duplicate callback za 60 sec ne dubliruet problemu
    assert res is True

@mock.patch("services.atr_policy_telegram_callback_worker._redis")
@mock.patch("services.atr_policy_telegram_callback_worker.publish_policy_ack_to_telegram")
@mock.patch("services.atr_policy_telegram_callback_worker.publish_policy_proposal_to_telegram")
def test_handle_event_show(m_pub, m_ack, m_redis):
    r_mock = mock.MagicMock()
    m_redis.return_value = r_mock
    r_mock.set.return_value = True

    r_mock.get.return_value = '{"proposal_id":"prop999", "status":"SUBMITTED"}'

    evt = {
        "user_id": "1001",
        "chat_id": "-12345",
        "callback": "atrpol:show:prop999"
    }

    res = cb.handle_event(evt)
    assert res is True
    m_pub.assert_called_once_with({"proposal_id":"prop999", "status":"SUBMITTED"})
