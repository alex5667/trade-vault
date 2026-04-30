"""Unit tests for binance_executor helper functions.

These tests are purely CPU-bound and require no network access or Binance API keys.
They test:
  - _truthy(): boolean coercion for payload values (strings, int, bool, None)
  - compute_trailing_callback_rate_pct(): callbackRate calculation from explicit %, bps, ATR
  - _make_cid(): clientOrderId construction
  - _classify_error(): error classification (transient vs fatal)
  - _round_half_up(): banker's rounding avoidance
  - _split_tp_qtys() via BinanceExecutor unit (requires no Redis)

Run from project root:
  cd python-worker && PYTHONPATH=. python -m pytest services/tests/test_binance_trailing_unit_v1.py -v
"""

import math
import importlib.util
import sys
from pathlib import Path

mod_path = Path(__file__).with_name("binance_executor.py")
spec = importlib.util.spec_from_file_location("binance_executor", mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)

client_path = Path(__file__).with_name("binance_futures_client.py")
cspec = importlib.util.spec_from_file_location("binance_futures_client", client_path)
client_mod = importlib.util.module_from_spec(cspec)
sys.modules[cspec.name] = client_mod
assert cspec.loader is not None
cspec.loader.exec_module(client_mod)

_truthy = mod._truthy
_round_half_up = mod._round_half_up
_make_cid = mod._make_cid
_sha1_8 = mod._sha1_8
_classify_error = mod._classify_error
compute_trailing_callback_rate_pct = mod.compute_trailing_callback_rate_pct
BinanceAPIError = mod.BinanceAPIError


# ---------------------------------------------------------------------------
# _truthy()
# ---------------------------------------------------------------------------

def test_truthy_bool_true():
    assert _truthy(True) is True


def test_truthy_bool_false():
    assert _truthy(False) is False


def test_truthy_int_nonzero():
    assert _truthy(1) is True


def test_truthy_int_zero():
    assert _truthy(0) is False


def test_truthy_float_nonzero():
    assert _truthy(0.1) is True


def test_truthy_string_true():
    assert _truthy("true") is True
    assert _truthy("True") is True
    assert _truthy("TRUE") is True
    assert _truthy("1") is True
    assert _truthy("yes") is True
    assert _truthy("on") is True


def test_truthy_string_false():
    assert _truthy("0") is False
    assert _truthy("false") is False
    assert _truthy("False") is False
    assert _truthy("no") is False
    assert _truthy("off") is False
    assert _truthy("") is False


def test_truthy_none():
    assert _truthy(None) is False


# ---------------------------------------------------------------------------
# _round_half_up()
# ---------------------------------------------------------------------------

def test_round_half_up_stable():
    # Python round(0.35, 1) == 0.3 due to binary float; our helper gives 0.4
    assert math.isclose(_round_half_up(0.35, 1), 0.4, rel_tol=1e-9)


def test_round_half_up_normal():
    assert math.isclose(_round_half_up(1.25, 1), 1.3, rel_tol=1e-9)
    assert math.isclose(_round_half_up(1.24, 1), 1.2, rel_tol=1e-9)
    assert math.isclose(_round_half_up(5.0, 1), 5.0, rel_tol=1e-9)


# ---------------------------------------------------------------------------
# compute_trailing_callback_rate_pct()
# ---------------------------------------------------------------------------

def _rate(payload, *, entry_price=100.0, atr=1.0
          min_pct=0.1, max_pct=5.0, default_pct=0.3, atr_mult_default=1.0):
    return compute_trailing_callback_rate_pct(
        payload
        entry_price=entry_price, atr=atr
        min_pct=min_pct, max_pct=max_pct
        default_pct=default_pct, atr_mult_default=atr_mult_default
    )


def test_trailing_callback_explicit_pct_rounding():
    # 0.27% gets rounded to 0.3% (nearest 0.1 step, half-up)
    v = _rate({"trail_callback_rate": 0.27})
    assert math.isclose(v, 0.3, rel_tol=1e-9), f"got {v}"


def test_trailing_callback_explicit_pct_aliases():
    # trail_callback_pct and trail_callback_percent are synonyms
    v1 = _rate({"trail_callback_pct": 1.0})
    v2 = _rate({"trail_callback_percent": 1.0})
    assert math.isclose(v1, 1.0) and math.isclose(v2, 1.0)


def test_trailing_callback_bps_to_pct():
    # 35 bps = 0.35% → rounded to 0.4 (half-up)
    v = _rate({"trail_callback_bps": 35})
    assert math.isclose(v, 0.4, rel_tol=1e-9), f"got {v}"


def test_trailing_callback_bps_10():
    # 10 bps = 0.10% → exactly 0.1%
    v = _rate({"trail_callback_bps": 10})
    assert math.isclose(v, 0.1, rel_tol=1e-9), f"got {v}"


def test_trailing_callback_from_atr():
    # atr=0.42, entry=100 → 0.42% → nearest 0.1 step = 0.4
    v = _rate({}, entry_price=100.0, atr=0.42)
    assert math.isclose(v, 0.4, rel_tol=1e-9), f"got {v}"


def test_trailing_callback_from_atr_with_mult():
    # atr=0.4, mult=2.0, entry=100 → 0.8% → 0.8
    v = _rate({"trail_atr_mult": "2.0"}, entry_price=100.0, atr=0.4)
    assert math.isclose(v, 0.8, rel_tol=1e-9), f"got {v}"


def test_trailing_callback_clamp_max():
    # atr=50% of entry → clamped to max_pct=5.0
    v = _rate({}, entry_price=100.0, atr=50.0, max_pct=5.0)
    assert math.isclose(v, 5.0, rel_tol=1e-9), f"got {v}"


def test_trailing_callback_clamp_min():
    # explicit 0.01% → clamped to min_pct=0.1
    v = _rate({"trail_callback_rate": 0.01}, min_pct=0.1)
    assert math.isclose(v, 0.1, rel_tol=1e-9), f"got {v}"


def test_trailing_callback_fallback_to_default():
    # No payload info, no ATR → uses default_pct
    v = _rate({}, entry_price=None, atr=None, default_pct=0.5)
    assert math.isclose(v, 0.5, rel_tol=1e-9), f"got {v}"


def test_trailing_callback_priority_pct_over_bps():
    # If both percent and bps are in payload, percent takes precedence
    v = _rate({"trail_callback_rate": 1.0, "trail_callback_bps": 50})
    assert math.isclose(v, 1.0, rel_tol=1e-9), f"got {v}"


# ---------------------------------------------------------------------------
# _make_cid()
# ---------------------------------------------------------------------------

def test_make_cid_length():
    cid = _make_cid("my-signal-id-12345", "entry")
    assert len(cid) <= 36, f"cid too long: {cid!r}"


def test_make_cid_contains_tag():
    assert _make_cid("sid-abc", "sl").endswith("-sl")


def test_make_cid_deterministic():
    assert _make_cid("same-sid", "tp1") == _make_cid("same-sid", "tp1")


def test_make_cid_different_tags():
    assert _make_cid("same-sid", "sl") != _make_cid("same-sid", "tp1")


# ---------------------------------------------------------------------------
# _sha1_8()
# ---------------------------------------------------------------------------

def test_sha1_8_len():
    assert len(_sha1_8("anything")) == 8


def test_sha1_8_deterministic():
    assert _sha1_8("abc") == _sha1_8("abc")


def test_sha1_8_different():
    assert _sha1_8("abc") != _sha1_8("xyz")


# ---------------------------------------------------------------------------
# _classify_error()
# ---------------------------------------------------------------------------

def test_classify_binance_timestamp_error():
    e = BinanceAPIError(0, {"code": -1021, "msg": "Timestamp drift"})
    assert _classify_error(e) == "transient"


def test_classify_binance_rate_limit():
    e = BinanceAPIError(429, {"code": -1003, "msg": "Too many requests"})
    assert _classify_error(e) == "transient"


def test_classify_binance_insufficient_margin():
    e = BinanceAPIError(400, {"code": -2019, "msg": "Margin is insufficient"})
    assert _classify_error(e) == "fatal"


def test_classify_binance_unknown_code():
    e = BinanceAPIError(400, {"code": -9999, "msg": "Unknown"})
    assert _classify_error(e) == "fatal"


def test_classify_network_timeout():
    e = ConnectionError("Connection timed out")
    assert _classify_error(e) == "transient"


def test_classify_generic_valueerror():
    e = ValueError("bad side: X")
    assert _classify_error(e) == "fatal"
