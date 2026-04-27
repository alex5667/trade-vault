from unittest.mock import patch, MagicMock
import json

from order_push_dispatcher import post_order

@patch("order_push_dispatcher.ORDER_PUSH_DISABLED", False)
@patch("order_push_dispatcher.ORDER_PUSH_ENABLE", True)
@patch("order_push_dispatcher.requests.post")
@patch("order_push_dispatcher._publish_dlq")
def test_post_order_success(mock_dlq, mock_post):
    mock_resp = MagicMock()
    mock_resp.ok = True
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"ok": True}
    mock_post.return_value = mock_resp

    payload = {"sid": "123", "action": "open"}
    res = post_order(payload)

    assert res["ok"] is True
    assert res["status_code"] == 200
    mock_dlq.assert_not_called()

@patch("order_push_dispatcher.ORDER_PUSH_DISABLED", False)
@patch("order_push_dispatcher.ORDER_PUSH_ENABLE", True)
@patch("order_push_dispatcher.requests.post")
@patch("order_push_dispatcher._publish_dlq")
def test_post_order_gateway_rejected(mock_dlq, mock_post):
    mock_resp = MagicMock()
    mock_resp.ok = False
    mock_resp.status_code = 400
    mock_resp.text = "bad request"
    mock_post.return_value = mock_resp

    payload = {"sid": "123", "action": "open"}
    res = post_order(payload)

    assert res["ok"] is False
    assert res["status_code"] == 400
    assert res["reason"] == "bad request"
    mock_dlq.assert_called_once()
    assert mock_dlq.call_args[0][0] == payload
    assert mock_dlq.call_args[0][1] == "http_400"

@patch("order_push_dispatcher.ORDER_PUSH_ENABLE", False)
@patch("order_push_dispatcher.requests.post")
def test_post_order_disabled_by_env(mock_post):
    payload = {"sid": "123", "action": "open"}
    res = post_order(payload)
    assert res["ok"] is True
    assert res["payload"]["stub"] is True
    mock_post.assert_not_called()
