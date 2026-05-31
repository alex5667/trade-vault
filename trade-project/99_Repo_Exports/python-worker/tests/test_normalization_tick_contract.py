"""Contract test: tick dict → normalize → signal payload.

Verifies the full normalization chain that every signal must pass through,
including enum inputs (P0 regression), safe wrappers, and payload field contracts.
"""
import pytest

from common.enums.trading import Direction, Side
from common.normalization import (
    NormalizedSide,
    generate_signal_id,
    normalize_side_3,
    normalize_side_3_safe,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_payload(tick: dict) -> dict:
    """Simulate the normalization chain from a raw tick dict to signal payload."""
    side_norm = normalize_side_3(tick["direction"])
    signal_id = generate_signal_id(tick["symbol"], tick["ts_ms"], side_norm.direction)
    return {
        "signal_id": signal_id,
        "direction": side_norm.direction.value,
        "side": side_norm.side.value,
        "side_int": side_norm.side_int,
        "symbol": tick["symbol"],
        "ts_ms": tick["ts_ms"],
    }


# ---------------------------------------------------------------------------
# Parametrized tick → payload contracts
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("direction_in,exp_dir,exp_side,exp_int,exp_id_char", [
    ("LONG",          "LONG",  "BUY",  1,  "L"),
    ("SHORT",         "SHORT", "SELL", -1, "S"),
    ("BUY",           "LONG",  "BUY",  1,  "L"),
    ("SELL",          "SHORT", "SELL", -1, "S"),
    (Direction.LONG,  "LONG",  "BUY",  1,  "L"),
    (Direction.SHORT, "SHORT", "SELL", -1, "S"),  # P0 regression: must not become LONG
    (Side.BUY,        "LONG",  "BUY",  1,  "L"),
    (Side.SELL,       "SHORT", "SELL", -1, "S"),
])
def test_tick_to_payload_contract(direction_in, exp_dir, exp_side, exp_int, exp_id_char):
    tick = {"direction": direction_in, "symbol": "ETHUSDT", "ts_ms": 1714000000000}
    payload = _build_payload(tick)

    assert payload["direction"] == exp_dir,  f"direction mismatch for {direction_in!r}"
    assert payload["side"]      == exp_side, f"side mismatch for {direction_in!r}"
    assert payload["side_int"]  == exp_int,  f"side_int mismatch for {direction_in!r}"
    assert f":{exp_id_char}" in payload["signal_id"], f"signal_id mismatch for {direction_in!r}"
    assert payload["symbol"]    == "ETHUSDT"
    assert payload["ts_ms"]     == 1714000000000


# ---------------------------------------------------------------------------
# Signal ID determinism
# ---------------------------------------------------------------------------

def test_signal_id_deterministic():
    id1 = generate_signal_id("BTCUSDT", 1714234567890, Direction.LONG)
    id2 = generate_signal_id("BTCUSDT", 1714234567890, "LONG")
    assert id1 == id2
    assert id1 == "crypto-of:BTCUSDT:1714234567890:L"


def test_signal_id_short():
    sid = generate_signal_id("SOLUSDT", 1714000000001, Direction.SHORT)
    assert sid.endswith(":S")


# ---------------------------------------------------------------------------
# Safe wrapper: unknown direction → None, no crash
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad", ["", "UNKNOWN", "??", None, "0"])
def test_safe_wrapper_returns_none_for_unknown(bad):
    result = normalize_side_3_safe(bad)
    assert result is None, f"expected None for {bad!r}, got {result!r}"


# ---------------------------------------------------------------------------
# NormalizedSide immutability (frozen + slots)
# ---------------------------------------------------------------------------

def test_normalized_side_frozen():
    ns = NormalizedSide(direction=Direction.LONG, side=Side.BUY, side_int=1)
    with pytest.raises((AttributeError, TypeError)):
        ns.direction = Direction.SHORT  # type: ignore[misc]


def test_normalized_side_no_dict():
    ns = NormalizedSide(direction=Direction.SHORT, side=Side.SELL, side_int=-1)
    assert not hasattr(ns, "__dict__")
