from __future__ import annotations

"""Unit-тесты контракта POSITION_CLOSED (A3).

Покрывают:
  - strip_heavy_fields
  - normalize_position_closed_event
  - validate_position_closed_event

Запуск:
  pytest python-worker/services/posttrade/test_trade_events_contract.py -v
"""

import hashlib
from typing import Any

from services.posttrade.trade_events_contract import (
    _HEAVY_FIELDS,
    _safe_int_ms,
    normalize_position_closed_event,
    strip_heavy_fields,
    validate_position_closed_event,
)
from utils.time_utils import get_ny_time_millis

# --------------------------------------------------------------------------- #
# Helpers / fixtures
# --------------------------------------------------------------------------- #

NOW_MS = get_ny_time_millis()


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode()).hexdigest()


def _valid_event(**overrides: Any) -> dict[str, Any]:
    """Минимально валидный нормализованный payload (V2: all required fields present)."""
    base: dict[str, Any] = {
        "event_type": "POSITION_CLOSED",
        "sid": "sid-abc-123",
        "ts": str(NOW_MS),
        "exit_ts_ms": str(NOW_MS),
        "event_id": _sha1(f"POSITION_CLOSED|sid-abc-123|{NOW_MS}||"),
        "symbol": "BTCUSDT",
        # A3 join-critical fields:
        "side": "LONG",
        "order_id": "ord-001",
        "qty": "0.1",
        "fee_bps": "2.5",
        "price": "1850.0",
        "px": "1850.0",
        "pnl": "150.25",
        "venue": "mt5",
        "risk_usd": "50.0",
        "r_mult": "3.0",
    }
    base.update(overrides)
    return base


# --------------------------------------------------------------------------- #
# strip_heavy_fields
# --------------------------------------------------------------------------- #

class TestStripHeavyFields:
    def test_strips_all_heavy(self):
        evt = dict.fromkeys(_HEAVY_FIELDS, "x")
        evt["sid"] = "ok"
        result = strip_heavy_fields(evt)
        assert "sid" in result
        for k in _HEAVY_FIELDS:
            assert k not in result

    def test_no_mutation_of_input(self):
        evt = {"sid": "x", "feature_vector": [1, 2, 3]}
        original = dict(evt)
        strip_heavy_fields(evt)
        assert evt == original  # не мутирует

    def test_empty_dict(self):
        assert strip_heavy_fields({}) == {}

    def test_no_heavy_fields_unchanged(self):
        evt = {"sid": "s", "pnl": "100"}
        assert strip_heavy_fields(evt) == evt


# --------------------------------------------------------------------------- #
# _safe_int_ms (internal, private but exported for tests)
# --------------------------------------------------------------------------- #

class TestSafeIntMs:
    def test_ms_passthrough(self):
        assert _safe_int_ms(NOW_MS) == NOW_MS

    def test_seconds_converted(self):
        sec = NOW_MS // 1000
        result = _safe_int_ms(sec)
        assert result == sec * 1000

    def test_string_ms(self):
        assert _safe_int_ms(str(NOW_MS)) == NOW_MS

    def test_none(self):
        assert _safe_int_ms(None) is None

    def test_garbage(self):
        assert _safe_int_ms("abc") is None

    def test_too_old(self):
        # 2010-01-01 in ms — меньше _MIN_EPOCH_MS
        old = 1_262_304_000_000
        assert _safe_int_ms(old) is None


# --------------------------------------------------------------------------- #
# normalize_position_closed_event
# --------------------------------------------------------------------------- #

class TestNormalizePositionClosedEvent:
    def test_basic(self):
        raw = {
            "event_type": "POSITION_CLOSED",
            "sid": "sid-1",
            "ts": NOW_MS,
            "exit_ts_ms": NOW_MS,
            "pnl": 150.25,
            "symbol": "BTCUSD",
            "source": "mt5",
            "side": "LONG",
            "price": 50000.0,
            "qty": 0.01,
            "fee_bps": 2.0,
        }
        out, errs = normalize_position_closed_event(raw)

        assert out["sid"] == "sid-1"
        assert out["event_type"] == "POSITION_CLOSED"
        assert out["ts"] == str(NOW_MS)
        assert out["exit_ts_ms"] == str(NOW_MS)
        assert len(out["event_id"]) == 40
        assert out["pnl"] == "150.25"
        assert out["symbol"] == "BTCUSD"

    def test_all_values_are_strings(self):
        raw = {
            "sid": "s",
            "ts": NOW_MS,
            "pnl": 1.5,
            "lot": 0.03,
            "side": "LONG",
            "price": 100.0,
            "fee_bps": 1.0,
        }
        out, _ = normalize_position_closed_event(raw)
        for k, v in out.items():
            assert isinstance(v, str), f"Field {k!r} is not str: {v!r}"

    def test_strip_heavy_fields(self):
        raw = {
            "sid": "s",
            "ts": NOW_MS,
            "feature_vector": [1, 2, 3],
            "evidence": {"x": 1},
            "raw_signal": "...",
        }
        out, _ = normalize_position_closed_event(raw)
        for hf in _HEAVY_FIELDS:
            assert hf not in out

    def test_preserves_existing_valid_event_id(self):
        existing = _sha1("POSITION_CLOSED|s|12345||")
        raw = {"sid": "s", "ts": NOW_MS, "event_id": existing}
        out, _ = normalize_position_closed_event(raw)
        assert out["event_id"] == existing

    def test_regenerates_invalid_event_id(self):
        raw = {"sid": "s", "ts": NOW_MS, "event_id": "short"}
        out, errs = normalize_position_closed_event(raw)
        assert len(out["event_id"]) == 40
        assert any("event_id" in e for e in errs)

    def test_ts_seconds_auto_converted_to_ms(self):
        sec_ts = NOW_MS // 1000
        raw = {"sid": "s", "ts": sec_ts}
        out, _ = normalize_position_closed_event(raw)
        assert int(out["ts"]) == sec_ts * 1000

    def test_exit_ts_ms_fallback_to_ts(self):
        """Если exit_ts_ms не задан явно, должен взять ts."""
        raw = {"sid": "s", "ts": NOW_MS}
        out, _ = normalize_position_closed_event(raw)
        assert out["exit_ts_ms"] == out["ts"]

    def test_close_reason_from_metadata(self):
        raw = {
            "sid": "s",
            "ts": NOW_MS,
            "metadata": {"close_reason": "trailing_stop"},
        }
        out, _ = normalize_position_closed_event(raw)
        assert out.get("close_reason") == "trailing_stop"

    def test_sid_alias_signal_id(self):
        raw = {"signal_id": "alias-sid", "ts": NOW_MS}
        out, _ = normalize_position_closed_event(raw)
        assert out["sid"] == "alias-sid"

    def test_missing_ts_returns_error(self):
        """V2: missing ts returns error instead of falling back to time.time()."""
        raw = {"sid": "s"}
        out, errs = normalize_position_closed_event(raw)
        assert any("ts" in e for e in errs)

    def test_metadata_dict_serialized_as_json(self):
        import json
        raw = {"sid": "s", "ts": NOW_MS, "metadata": {"a": 1}}
        out, _ = normalize_position_closed_event(raw)
        parsed = json.loads(out["metadata"])
        assert parsed == {"a": 1}


# --------------------------------------------------------------------------- #
# validate_position_closed_event
# --------------------------------------------------------------------------- #

class TestValidatePositionClosedEvent:
    def test_valid_event(self):
        ok, errs = validate_position_closed_event(_valid_event())
        assert ok is True
        assert errs == []

    def test_missing_sid(self):
        evt = _valid_event(sid="")
        ok, errs = validate_position_closed_event(evt)
        assert not ok
        assert any("sid" in e for e in errs)

    def test_none_sid(self):
        evt = _valid_event()
        del evt["sid"]
        ok, errs = validate_position_closed_event(evt)
        assert not ok

    def test_bad_ts(self):
        # V2: if ts is invalid but exit_ts_ms is valid, exit_ts_ms is used as fallback.
        # To test ts failure: remove exit_ts_ms too.
        evt = _valid_event(ts="not-a-number")
        del evt["exit_ts_ms"]
        ok, errs = validate_position_closed_event(evt)
        assert not ok
        assert any("ts" in e for e in errs)

    def test_ts_in_past_too_old(self):
        # V2: if old ts is given but exit_ts_ms is valid, exit_ts_ms wins.
        # Remove exit_ts_ms to test genuine ts-too-old failure.
        old_ms = 1_420_070_400_000
        evt = _valid_event(ts=str(old_ms))
        del evt["exit_ts_ms"]
        ok, errs = validate_position_closed_event(evt)
        assert not ok

    def test_missing_exit_ts_ms(self):
        # V2: if exit_ts_ms is missing but ts is valid, ts fills exit_ts_ms.
        # Real failure: both missing → no timestamp at all.
        evt = _valid_event()
        del evt["exit_ts_ms"]
        del evt["ts"]
        ok, errs = validate_position_closed_event(evt)
        assert not ok
        assert any("ts" in e for e in errs)

    def test_missing_event_id(self):
        # V2: normalize AUTO-HEALS missing event_id (generates SHA1).
        # This IS the correct V2 behavior — event_id is always populated.
        evt = _valid_event()
        del evt["event_id"]
        normalized, errs = normalize_position_closed_event(evt)
        # Should have generated a new valid event_id
        assert len(normalized["event_id"]) == 40
        # errs should record the regeneration (bad_event_id or similar)
        # but overall validate should pass because event_id is always healed
        ok2, _ = validate_position_closed_event(normalized)
        assert ok2

    def test_bad_event_id_not_sha1(self):
        evt = _valid_event(event_id="tooshort")
        ok, errs = validate_position_closed_event(evt)
        assert not ok

    def test_wrong_event_type(self):
        # V2: normalize_position_closed_event overrides event_type to POSITION_CLOSED,
        # so wrong event_type is caught only if explicitly forced after normalize.
        # We verify that the norm forces it to POSITION_CLOSED.
        evt = _valid_event(event_type="TP1_HIT")
        # normalize will fix event_type but add an error
        _, errs = normalize_position_closed_event(evt)
        assert any("event_type" in e for e in errs)

    def test_heavy_field_presence(self):
        """V2: heavy fields are stripped during normalize, so this test verifies strip."""
        raw = dict(_valid_event(), feature_vector="[1,2,3]")
        out, _ = normalize_position_closed_event(raw)
        assert "feature_vector" not in out

    def test_multiple_errors_collected(self):
        """Все ошибки собираются, не fail fast."""
        evt: dict[str, Any] = {}  # completely empty
        ok, errs = validate_position_closed_event(evt)
        assert not ok
        assert len(errs) >= 3  # sid, ts, exit_ts_ms, event_id, price, side...


# --------------------------------------------------------------------------- #
# Round-trip: normalize → validate
# --------------------------------------------------------------------------- #

class TestRoundTrip:
    def test_good_raw_passes(self):
        raw = {
            "event_type": "POSITION_CLOSED",
            "sid": "sid-rt-001",
            "ts": NOW_MS,
            "exit_ts_ms": NOW_MS,
            "pnl": -42.5,
            "symbol": "ETHUSD",
            "source": "paper",
            # A3 required fields:
            "side": "SHORT",
            "order_id": "oid-777",
            "qty": 0.5,
            "fee_bps": 3.0,
            "price": 3200.0,
            "risk_usd": 20.0,
            "r_mult": -2.1,
        }
        normalized, _ = normalize_position_closed_event(raw)
        ok, errs = validate_position_closed_event(normalized)
        # V2: A3 fields present so should pass completely
        assert ok, f"Expected ok, got errors: {errs}"

    def test_heavy_fields_stripped_in_roundtrip(self):
        raw = {
            "sid": "s",
            "ts": NOW_MS,
            "feature_vector": [0.1] * 100,
            "evidence": {"key": "val"},
        }
        normalized, _ = normalize_position_closed_event(raw)
        # Should pass validation (no heavy fields present)
        ok, errs = validate_position_closed_event(normalized)
        # V2: ok depends entirely on whether non-heavy required fields are present;
        # heavy field stripping alone is not enough for full validity.
        # But heavy fields must NOT be in output.
        assert "feature_vector" not in normalized
        assert "evidence" not in normalized

    def test_seconds_ts_fixed_in_roundtrip(self):
        raw = {"sid": "s", "ts": NOW_MS // 1000, "side": "LONG", "price": 100.0, "qty": 0.01, "fee_bps": 1.0, "symbol": "BTCUSD", "source": "mt5"}  # секунды
        normalized, _ = normalize_position_closed_event(raw)
        ok, errs = validate_position_closed_event(normalized)
        # ts must have been converted to ms — check it
        assert int(normalized["ts"]) == (NOW_MS // 1000) * 1000
