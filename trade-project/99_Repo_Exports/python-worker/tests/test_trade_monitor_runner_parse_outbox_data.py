import json

from runners.trade_monitor_runner import _parse_signal


def test_parse_signal_reads_data_field_json():
    payload = {"symbol": "BTCUSDT", "direction": "LONG", "ts_ms": 1700000000000, "entry": 100.0}
    fields = {"data": json.dumps(payload, ensure_ascii=False)}
    out = _parse_signal(fields)
    assert isinstance(out, dict)
    assert out["symbol"] == "BTCUSDT"
    assert out["direction"] == "LONG"
