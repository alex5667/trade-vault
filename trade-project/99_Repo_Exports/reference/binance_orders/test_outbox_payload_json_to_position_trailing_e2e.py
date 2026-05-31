from __future__ import annotations

import json
from types import SimpleNamespace

from domain.handlers import create_position, _arm_trailing_after_tp1


def _parse_signal(fields: dict) -> dict:
    """Simplified version of _parse_signal for testing."""
    out = dict(fields)

    # Parse payload_json if present
    pj = fields.get("payload_json")
    if pj:
        try:
            p = json.loads(pj)
            if isinstance(p, dict):
                # Merge policy: fill missing keys from payload
                for k, v in p.items():
                    if k not in out or out.get(k) in (None, "", "0"):
                        out[k] = v
        except Exception:
            pass

    return out


def _normalize_signal_simple(raw_signal: dict) -> SimpleNamespace:
    """Simplified normalization for testing."""
    # Just return a signal-like object with the payload
    return SimpleNamespace(
        sid=raw_signal.get("sid", "test"),
        strategy=raw_signal.get("strategy", "test"),
        source=raw_signal.get("source", "test"),
        symbol=raw_signal.get("symbol", "BTCUSDT"),
        tf=raw_signal.get("tf", "1m"),
        direction=raw_signal.get("direction", "LONG"),
        entry_price=float(raw_signal.get("entry_price", 100.0)),
        entry_ts_ms=int(raw_signal.get("entry_ts_ms", 1000)),
        lot=float(raw_signal.get("lot", 1.0)),
        sl=float(raw_signal.get("sl", 95.0)),
        tp_levels=raw_signal.get("tp_levels", [101.0, 102.0, 103.0]),
        trail_profile=raw_signal.get("trail_profile", "rocket_v1"),
        payload=raw_signal,  # Keep original for create_position
        entry_tag="",
    )


class _SpecStub:
    trailing_profile_default = "rocket_v1"

    def risk_money(self, entry, sl, lot, direction):
        return abs(float(entry) - float(sl)) * float(lot)




def test_outbox_payload_json_parsed_and_trailing_flag_reaches_position():
    # Stream envelope fields (like OutboxEnvelope.to_stream_fields()).
    fields = {
        "schema": "1",
        "signal_id": "sig-123",
        "ts_ms": "1700000000000",
        "kind": "touch",
        "symbol": "BTCUSDT",
        "side": "LONG",
        # payload_json is what outbox writes
        "payload_json": json.dumps({
            # fields used by TradeMonitor._normalize_signal / create_position:
            "sid": "sig-123",
            "strategy": "CryptoOrderFlow",
            "source": "CryptoOrderFlow",
            "tf": "1m",  # or "timeframe": "1m"
            "direction": "LONG",
            "entry_price": 100.0,
            "entry_ts_ms": 1700000000000,
            "lot": 1.0,
            "sl": 95.0,
            "tp_levels": [101.0, 102.0, 103.0],
            "trail_profile": "rocket_v1",

            # NEW protocol fields we want end-to-end:
            "trail_after_tp1": 0,
            "trail_after_tp1_reason": "NO_MOMO",
        }, ensure_ascii=False, separators=(",", ":")),
    }

    raw = _parse_signal(fields)
    assert raw.get("trail_after_tp1") in (0, "0")  # merged from payload_json
    assert raw.get("trail_after_tp1_reason") == "NO_MOMO"

    sig = _normalize_signal_simple(raw)
    assert sig is not None
    # trail flags must be kept in payload for create_position()
    assert isinstance(sig.payload, dict)
    assert sig.payload.get("trail_after_tp1") in (0, "0")
    assert sig.payload.get("trail_after_tp1_reason") == "NO_MOMO"

    pos = create_position(sig, _SpecStub())
    assert pos.trail_after_tp1 is False
    assert pos.trail_after_tp1_reason == "NO_MOMO"


def test_arm_trailing_after_tp1_generates_event_and_sets_flags():
    # minimal PositionState-like object (works even if PositionState is a dataclass)
    pos = SimpleNamespace(
        closed=False,
        id="oid1",
        sid="sig-123",
        strategy="CryptoOrderFlow",
        source="CryptoOrderFlow",
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
        entry_price=100.0,
        sl=95.0,
        tp_levels=[101.0, 102.0, 103.0],
        tp_hits=1,
        trailing_started=False,
        trailing_active=False,
        trailing_distance=0.0,
        trailing_point=0.0,
        trail_profile="rocket_v1",
        signal_payload={"trail_after_tp1_reason": "TEST"},
        trail_after_tp1_reason="TEST",
    )

    ev = _arm_trailing_after_tp1(pos, ts_ms=12345)
    assert ev is not None
    assert ev.event_type == "TRAILING_SYNC"
    assert pos.trailing_started is True
    assert pos.trailing_active is True
    assert int(getattr(pos, "trailing_armed_ts_ms", 0)) == 12345
