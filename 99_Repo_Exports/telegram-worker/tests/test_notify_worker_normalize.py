import json

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from notify_worker import normalize_entry as normalize_notify_entry


def test_normalize_bytes_dict_and_json_fields():
    fields = {
        b"sid": b"signal:BTC:123",
        b"signal_payload": json.dumps({"symbol": "BTCUSDT", "direction": "LONG", "entry": 100.0}).encode("utf-8"),
        b"signal_settings": json.dumps({"min_conf": 70}).encode("utf-8"),
        b"type": b"signal",
    }
    e = normalize_notify_entry(fields)
    assert e["sid"] == "signal:BTC:123"
    assert isinstance(e["signal_payload"], dict)
    assert e["signal_payload"]["symbol"] == "BTCUSDT"
    assert isinstance(e["signal_settings"], dict)
    assert e["signal_settings"]["min_conf"] == 70


def test_normalize_list_pairs():
    fields = [
        b"sid", b"signal:ETH:1",
        b"signal_payload", json.dumps({"symbol": "ETHUSDT"}).encode("utf-8"),
    ]
    e = normalize_notify_entry(fields)
    assert e["sid"] == "signal:ETH:1"
    assert e["signal_payload"]["symbol"] == "ETHUSDT"


def test_normalize_legacy_data_envelope_merge():
    env = {
        "type": "signal",
        "signal_payload": {"symbol": "BTCUSDT", "direction": "SHORT"},
        "signal_settings": {"k": 2.0},
    }
    fields = {
        b"sid": b"signal:BTC:999",
        b"data": json.dumps(env).encode("utf-8"),
    }
    e = normalize_notify_entry(fields)
    assert e["sid"] == "signal:BTC:999"
    # merged from data
    assert isinstance(e["signal_payload"], dict)
    assert e["signal_payload"]["direction"] == "SHORT"
    assert isinstance(e["signal_settings"], dict)
    assert e["signal_settings"]["k"] == 2.0
