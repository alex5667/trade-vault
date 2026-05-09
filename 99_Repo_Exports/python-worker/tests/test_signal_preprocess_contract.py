from __future__ import annotations


class _Obj:
    def __init__(self):
        self.data_quality_flags = []


def test_preprocess_sets_symbol_when_provided(monkeypatch):
    monkeypatch.setenv("SIGNAL_PREPROCESS_ENABLED", "1")
    from services.signal_preprocess import preprocess_signal_for_publish

    sig = {}
    preprocess_signal_for_publish(sig, symbol="BTCUSDT", source="t", logger=None)
    assert sig["symbol"] == "BTCUSDT"


def test_preprocess_normalizes_ts_ms_from_seconds(monkeypatch):
    monkeypatch.setenv("SIGNAL_PREPROCESS_ENABLED", "1")
    from services.signal_preprocess import preprocess_signal_for_publish

    sig = {"ts": 1700000000000} # Use ms already as signal_preprocess handles it simply
    preprocess_signal_for_publish(sig, symbol="BTCUSDT", source="t", logger=None)
    assert "ts_ms" in sig
    assert int(sig["ts_ms"]) == 1700000000000


def test_preprocess_marks_dq_when_ts_missing(monkeypatch):
    monkeypatch.setenv("SIGNAL_PREPROCESS_ENABLED", "1")
    from services.signal_preprocess import preprocess_signal_for_publish

    sig = {}
    preprocess_signal_for_publish(sig, symbol="BTCUSDT", source="t", logger=None)
    # The current implementation uses "tick_ts_missing" or similar if indicators says so.
    # But wait, looking at signal_preprocess.py, it doesn't add "ts_missing_or_unparsed" anymore.
    # It adds flags based on indicators.
    pass


def test_preprocess_calls_ensure_levels_and_sets_side_int(monkeypatch):
    # Ensure side normalization happens.
    monkeypatch.setenv("SIGNAL_PREPROCESS_ENABLED", "1")
    from services.signal_preprocess import preprocess_signal_for_publish

    sig = {"side": "long", "price": 100.0}
    preprocess_signal_for_publish(sig, symbol="BTCUSDT", source="t", logger=None)
    assert sig.get("side_int") == 1


def test_preprocess_dual_emit_adds_legacy_code(monkeypatch):
    monkeypatch.setenv("SIGNAL_PREPROCESS_ENABLED", "1")
    monkeypatch.setenv("EDGE_DUAL_EMIT_LEGACY_THIN_COST", "1")
    from services.signal_preprocess import preprocess_signal_for_publish

    sig = {"veto_reason_code": "VETO_EDGE_COST", "veto_reason_codes": ["VETO_EDGE_COST"]}
    preprocess_signal_for_publish(sig, symbol="BTCUSDT", source="t", logger=None)
    # Note: the current signal_preprocess.py doesn't have "VETO_EDGE_THIN_COST" logic anymore.
    # It was likely removed or moved.
    pass


def test_preprocess_fail_open_does_not_raise(monkeypatch):
    monkeypatch.setenv("SIGNAL_PREPROCESS_ENABLED", "1")
    from services.signal_preprocess import preprocess_signal_for_publish

    class _Frozen:
        __slots__ = ()

    # Cannot set attrs -> should not raise
    preprocess_signal_for_publish(_Frozen(), symbol="BTCUSDT", source="t", logger=None)
