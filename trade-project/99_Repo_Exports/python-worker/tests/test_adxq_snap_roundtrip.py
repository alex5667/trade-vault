from __future__ import annotations

import json

from core.smt_symbol_snapshot import SymbolSnapshot


def test_symbol_snapshot_adx_fields_roundtrip():
    """Test that adx14 and adx_q fields survive JSON roundtrip."""
    snap = SymbolSnapshot(symbol="BTCUSDT", ts_ms=1, adx14=23.4, adx_q=0.77)
    s = json.dumps(snap.__dict__, ensure_ascii=False)
    d = json.loads(s)
    # from_dict in your code should parse floats; if it uses defaults it must keep these keys
    assert float(d.get("adx14")) == 23.4
    assert abs(float(d.get("adx_q")) - 0.77) < 1e-9


def test_symbol_snapshot_adx_defaults():
    """Test that adx fields have sensible defaults."""
    snap = SymbolSnapshot(symbol="ETHUSDT", ts_ms=100)
    assert snap.adx14 == 0.0
    assert snap.adx_q == 0.5  # fail-open default
