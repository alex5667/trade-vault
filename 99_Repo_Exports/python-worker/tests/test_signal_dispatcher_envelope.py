from __future__ import annotations
import json
from services.signal_dispatcher import SignalDispatcher


def _parse_envelope_fields(fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Mirror of production logic for testing.
    """
    # Compatibility: older producers used "data", outbox_writer.lua uses "payload".
    raw = fields.get("data")
    if not raw:
        raw = fields.get("payload")
    if not raw:
        return None
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="ignore")
    if isinstance(raw, str):
        return json.loads(raw)
    if isinstance(raw, dict):
        return raw
    return None


def test_parse_envelope_accepts_data():
    env = {"sid": "S1", "targets": {"notify": {"text": "x"}}, "meta": {}}
    raw = json.dumps(env).encode("utf-8")
    assert _parse_envelope_fields({"data": raw})["sid"] == "S1"


def test_parse_envelope_accepts_payload_fallback():
    env = {"sid": "S1", "targets": {"notify": {"text": "x"}}, "meta": {}}
    raw = json.dumps(env).encode("utf-8")
    assert _parse_envelope_fields({"payload": raw})["sid"] == "S1"