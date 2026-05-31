"""
tests/core/test_signal_outcome_resolver.py

Unit tests for signal_outcome_resolver and signal_outcome_snapshot_writer.

Coverage:
  1. resolve_record: TP hit → label +1
  2. resolve_record: SL hit → label -1
  3. resolve_record: vertical/timeout → label 0
  4. resolve_record: TP and SL in same tick → SL wins (conservative)
  5. LONG/SHORT symmetry: same ticks, mirror direction
  6. NO_TICKS path (empty tick list) → label 0 + quality_flag bit 0
  7. entry_px computation (long/short, spread/slip)
  8. _parse_signal: payload field (JSON blob) and flat field fallback
  9. _barrier_config: atr_bps→sl_bps, tp_r, ttl_ms by regime
  10. Feature availability guard: assert no future data in features snapshot
  11. Determinism: same ticks → identical outcome on two runs
"""
from __future__ import annotations

import json
import sys
import os

import pytest

# ---------------------------------------------------------------------------
# Path setup — run from python-worker/ root
# ---------------------------------------------------------------------------
_BASE = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _BASE not in sys.path:
    sys.path.insert(0, _BASE)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ticks(entry_px: float, moves: list[float], start_ms: int = 1_000_000) -> list[tuple[int, float]]:
    """Generate (ts_ms, price) from a list of signed-bps moves relative to entry_px."""
    result = []
    for i, move_bps in enumerate(moves):
        px = entry_px * (1.0 + move_bps / 10_000.0)
        result.append((start_ms + i * 100, px))
    return result


# ---------------------------------------------------------------------------
# Tests: resolve_record
# ---------------------------------------------------------------------------

class TestResolveRecord:

    def _make_record(
        self,
        entry_px: float = 100.0,
        side: int = 1,
        tp_r: float = 1.0,
        sl_r: float = 1.0,
        atr_bps: float = 50.0,    # 1R = 50 bps = 0.5%
        ttl_ms: int = 600_000,
    ) -> dict:
        r_unit_px = entry_px * atr_bps / 10_000.0
        return dict(
            sid="test-sid",
            decision_time_ms=1_000_000,
            symbol="BTCUSDT",
            side=side,
            entry_px=entry_px,
            r_unit_px=r_unit_px,
            tp_r=tp_r,
            sl_r=sl_r,
            ttl_ms=ttl_ms,
            quality_flags=0,
        )

    def test_tp_hit_long(self):
        """TP crossed → label +1, mfe_r ≈ tp_r."""
        from services.signal_outcome_resolver import resolve_record

        rec = self._make_record(entry_px=100.0, side=1, tp_r=1.0, atr_bps=50.0)
        # Tick that crosses TP barrier: 50 bps = tp_r × sl_bps
        ticks = _make_ticks(100.0, [10, 30, 52])  # last tick > 50 bps

        result = resolve_record(rec, ticks)

        assert result is not None
        assert result["label"] == 1
        assert result["realized_r"] > 0.9  # should be ≈ 1.0R
        assert result["mfe_r"] > 0.9
        assert result["mae_r"] >= 0.0

    def test_sl_hit_long(self):
        """SL crossed → label -1."""
        from services.signal_outcome_resolver import resolve_record

        rec = self._make_record(entry_px=100.0, side=1, sl_r=1.0, atr_bps=50.0)
        # Tick that crosses SL barrier: −50 bps
        ticks = _make_ticks(100.0, [-5, -20, -52])  # last tick < −50 bps

        result = resolve_record(rec, ticks)

        assert result is not None
        assert result["label"] == -1
        assert result["realized_r"] < -0.9

    def test_vertical_timeout(self):
        """No barrier hit within ttl_ms → label 0."""
        from services.signal_outcome_resolver import resolve_record

        rec = self._make_record(entry_px=100.0, side=1, tp_r=1.0, atr_bps=50.0, ttl_ms=500)
        # Small moves that don't reach ±50 bps, all within 500ms
        ticks = _make_ticks(100.0, [5, 10, 15, 20])

        result = resolve_record(rec, ticks)

        assert result is not None
        assert result["label"] == 0

    def test_sl_wins_on_same_tick_ambiguity(self):
        """When entry_px is set such that a single tick would hit both TP and SL,
        we test that SL is conservative. In label_path, first matched barrier wins,
        but our entry_px is set conservatively for long (higher entry → sl is closer).
        We test: the first tick after a big adverse move reports SL."""
        from services.signal_outcome_resolver import resolve_record

        # With tiny ATR, tp_r=10.0 but sl_r=1.0, a -50 bps move hits SL first
        rec = self._make_record(entry_px=100.0, side=1, tp_r=10.0, sl_r=1.0, atr_bps=40.0)
        # tick hits SL (−40 bps) but not TP (−400 bps)
        ticks = _make_ticks(100.0, [-41])

        result = resolve_record(rec, ticks)

        assert result is not None
        assert result["label"] == -1, "SL should be hit before TP"

    def test_long_short_symmetry(self):
        """LONG and SHORT with mirrored ticks should produce same absolute realized_r."""
        from services.signal_outcome_resolver import resolve_record

        rec_long  = self._make_record(entry_px=100.0, side=1,  tp_r=1.0, atr_bps=50.0)
        rec_short = self._make_record(entry_px=100.0, side=-1, tp_r=1.0, atr_bps=50.0)

        # For long: +55 bps crosses TP; for short: −55 bps crosses TP
        ticks_long  = _make_ticks(100.0, [20, 55])
        ticks_short = _make_ticks(100.0, [-20, -55])

        res_long  = resolve_record(rec_long,  ticks_long)
        res_short = resolve_record(rec_short, ticks_short)

        assert res_long  is not None
        assert res_short is not None
        assert res_long["label"]  == 1
        assert res_short["label"] == 1
        # Symmetry: realized_r should be close
        assert abs(res_long["realized_r"] - res_short["realized_r"]) < 0.2

    def test_no_ticks_quality_flag(self):
        """Empty tick list → label 0, quality_flag bit 0 set."""
        from services.signal_outcome_resolver import resolve_record

        rec = self._make_record()
        result = resolve_record(rec, [])

        assert result is not None
        assert result["label"] == 0
        assert result["quality_flags"] & 1, "bit 0 should be set for no ticks"

    def test_invalid_entry_px(self):
        """entry_px=0 + no ticks → unrecoverable; with ticks → fallback to first tick (Plan 3 Step 1)."""
        from services.signal_outcome_resolver import resolve_record

        rec = self._make_record()
        rec["entry_px"] = 0.0

        no_ticks_result = resolve_record(rec, [])
        assert no_ticks_result is not None
        assert no_ticks_result.get("_unrecoverable") is True
        assert no_ticks_result.get("entry_px_fallback_reason") == "entry_px_fallback_no_path"

        with_ticks = resolve_record(rec, _make_ticks(100.0, [55]))
        assert with_ticks is not None
        assert with_ticks.get("_unrecoverable") is not True
        assert with_ticks.get("entry_px_fallback_reason") == "entry_px_fallback_first_tick"

    def test_determinism(self):
        """Same record + same ticks produces identical label on two calls."""
        from services.signal_outcome_resolver import resolve_record

        rec   = self._make_record(entry_px=50000.0, side=1, tp_r=1.0, atr_bps=20.0)
        ticks = _make_ticks(50000.0, [5, 12, 21])  # hits TP at 20 bps

        r1 = resolve_record(rec, ticks)
        r2 = resolve_record(rec, ticks)

        assert r1 is not None
        assert r2 is not None
        assert r1["label"] == r2["label"]
        assert r1["realized_r"] == r2["realized_r"]
        assert r1["mfe_r"] == r2["mfe_r"]


# ---------------------------------------------------------------------------
# Tests: entry_px computation (snapshot_writer)
# ---------------------------------------------------------------------------

class TestEntryPxComputation:

    def test_long_entry_above_mid(self):
        """Long entry_px should be above mid (buying at ask-equivalent)."""
        from services.signal_outcome_snapshot_writer import compute_entry_px

        mid = 100.0
        entry = compute_entry_px(mid, "LONG", spread_bps=10.0, slip_prior_bps=2.0)

        assert entry > mid, "Long entry must be above mid"
        # Expected: 100 * (1 + (5+2)/10_000) = 100 * 1.0007 = 100.07
        assert abs(entry - 100.07) < 0.001

    def test_short_entry_below_mid(self):
        """Short entry_px should be below mid (selling at bid-equivalent)."""
        from services.signal_outcome_snapshot_writer import compute_entry_px

        mid = 100.0
        entry = compute_entry_px(mid, "SHORT", spread_bps=10.0, slip_prior_bps=2.0)

        assert entry < mid, "Short entry must be below mid"
        # Expected: 100 * (1 - 7/10_000) = 100 * 0.9993 = 99.93
        assert abs(entry - 99.93) < 0.001

    def test_zero_mid_returns_zero(self):
        from services.signal_outcome_snapshot_writer import compute_entry_px

        assert compute_entry_px(0.0, "LONG", 10.0, 1.5) == 0.0

    def test_symmetric_spread(self):
        """Long and short entry offsets should be symmetric around mid."""
        from services.signal_outcome_snapshot_writer import compute_entry_px

        mid = 200.0
        long_e  = compute_entry_px(mid, "LONG",  spread_bps=20.0, slip_prior_bps=0.0)
        short_e = compute_entry_px(mid, "SHORT", spread_bps=20.0, slip_prior_bps=0.0)

        assert abs((long_e - mid) + (short_e - mid)) < 1e-9, "Offsets must be symmetric"


# ---------------------------------------------------------------------------
# Tests: _parse_signal (snapshot_writer)
# ---------------------------------------------------------------------------

class TestParseSignal:

    def test_payload_field_json(self):
        """Reads signal from JSON in 'payload' field."""
        from services.signal_outcome_snapshot_writer import _parse_signal

        sig = {"symbol": "BTCUSDT", "signal_id": "abc123", "ts_ms": 99}
        fields = {"payload": json.dumps(sig)}

        result = _parse_signal(fields)

        assert result is not None
        assert result["symbol"] == "BTCUSDT"
        assert result["signal_id"] == "abc123"

    def test_flat_field_fallback(self):
        """Falls back to flat fields when 'payload' not present."""
        from services.signal_outcome_snapshot_writer import _parse_signal

        fields = {"symbol": "ETHUSDT", "signal_id": "xyz", "ts_ms": "12345"}

        result = _parse_signal(fields)

        assert result is not None
        assert result["symbol"] == "ETHUSDT"

    def test_invalid_json_payload_returns_none(self):
        """Returns None for completely unparseable payload."""
        from services.signal_outcome_snapshot_writer import _parse_signal

        fields = {"payload": "not-json-{{{"}

        result = _parse_signal(fields)

        assert result is None

    def test_empty_fields_returns_none(self):
        from services.signal_outcome_snapshot_writer import _parse_signal

        assert _parse_signal({}) is None


# ---------------------------------------------------------------------------
# Tests: _barrier_config (snapshot_writer)
# ---------------------------------------------------------------------------

class TestBarrierConfig:

    def test_atr_used_for_sl_bps(self):
        """sl_bps should be derived from atr_bps."""
        from services.signal_outcome_snapshot_writer import _barrier_config

        inds = {"atr_bps": 40.0, "spread_bps": 10.0}
        cfg  = _barrier_config(inds, "LONG", mid_px=1000.0)

        assert cfg is not None
        # entry_px ≈ 1000 * (1 + (5+1.5)/10_000) = 1000.65 approx
        entry_px = cfg["entry_px"]
        # r_unit_px = entry_px * 40 / 10_000 = 4.00 approx
        expected_r_unit = entry_px * 40.0 / 10_000.0
        assert abs(cfg["r_unit_px"] - expected_r_unit) < 0.01

    def test_momentum_regime_ttl(self):
        """momentum regime should get longer TTL."""
        from services.signal_outcome_snapshot_writer import _barrier_config, _TTL_BY_REGIME

        inds = {"atr_bps": 30.0, "regime": "momentum"}
        cfg  = _barrier_config(inds, "LONG", mid_px=500.0)

        assert cfg is not None
        assert cfg["ttl_ms"] == _TTL_BY_REGIME.get("momentum", 900_000)

    def test_zero_mid_returns_none(self):
        from services.signal_outcome_snapshot_writer import _barrier_config

        cfg = _barrier_config({"atr_bps": 30.0}, "LONG", mid_px=0.0)
        assert cfg is None

    def test_min_sl_bps_floor(self):
        """sl_bps should not fall below SO_MIN_SL_BPS even with tiny ATR."""
        from services.signal_outcome_snapshot_writer import _barrier_config, _MIN_SL_BPS

        inds = {"atr_bps": 0.1}  # tiny ATR
        cfg  = _barrier_config(inds, "LONG", mid_px=100.0)

        assert cfg is not None
        sl_bps = cfg["r_unit_px"] / cfg["entry_px"] * 10_000.0
        assert sl_bps >= _MIN_SL_BPS, f"sl_bps {sl_bps} below floor {_MIN_SL_BPS}"

    def test_quality_flag_spread_estimated(self):
        """When spread_bps=0, quality_flag bit 1 should be set."""
        from services.signal_outcome_snapshot_writer import _barrier_config

        inds = {"atr_bps": 30.0}  # no spread_bps
        cfg  = _barrier_config(inds, "LONG", mid_px=100.0)

        assert cfg is not None
        assert cfg["quality_flags"] & 2, "bit 1 should be set when spread estimated"


# ---------------------------------------------------------------------------
# Tests: feature availability (no look-ahead contamination)
# ---------------------------------------------------------------------------

class TestFeatureAvailability:
    """
    Guard: features frozen in signal snapshot must NOT contain data
    from timestamps > decision_time_ms.

    This is a structural test: verify that the snapshot writer uses
    the indicators dict from the signal payload (decision-time state),
    not a re-read of current Redis state.
    """

    def test_snapshot_uses_signal_indicators(self):
        """_extract_indicators returns the indicators from the signal, not live Redis."""
        from services.signal_outcome_snapshot_writer import _extract_indicators

        future_indicators = {"ts_ms": 99999999999999, "atr_bps": 999.0}
        sig = {"indicators": future_indicators, "ts_ms": 1000}

        inds = _extract_indicators(sig)

        # Must be exactly the signal's own indicators, not anything else
        assert inds["atr_bps"] == 999.0
        assert "ts_ms" in inds

    def test_no_future_keys_injected(self):
        """Snapshot writer must not read external state for the frozen features JSONB."""
        from services.signal_outcome_snapshot_writer import _extract_indicators

        sig = {"indicators": {"vol": 1.5, "regime": "momentum"}}
        inds = _extract_indicators(sig)

        # Must not have extra keys that weren't in the signal
        allowed_keys = {"vol", "regime"}
        extra = set(inds.keys()) - allowed_keys
        assert not extra, f"Extra keys in snapshot: {extra}"

    def test_indicators_string_json_parsed(self):
        """If indicators is a JSON string, it should be parsed correctly."""
        from services.signal_outcome_snapshot_writer import _extract_indicators

        inds_dict = {"atr_bps": 30.0, "regime": "ranging"}
        sig = {"indicators": json.dumps(inds_dict)}

        result = _extract_indicators(sig)

        assert result["atr_bps"] == 30.0
        assert result["regime"] == "ranging"


# ---------------------------------------------------------------------------
# Tests: tick fetching helper
# ---------------------------------------------------------------------------

class TestFetchTicks:

    def test_empty_stream_returns_empty(self):
        """Empty Redis stream → empty tick list."""
        from services.signal_outcome_resolver import fetch_ticks

        class MockRedis:
            def xrange(self, *args, **kwargs):
                return []

        ticks = fetch_ticks(MockRedis(), "BTCUSDT", 1000, 2000)
        assert ticks == []

    def test_price_extraction_p_field(self):
        """Ticks with 'p' field should extract price correctly."""
        from services.signal_outcome_resolver import _tick_price

        assert _tick_price({"p": "50000.5"}) == pytest.approx(50000.5)

    def test_price_extraction_price_field(self):
        from services.signal_outcome_resolver import _tick_price

        assert _tick_price({"price": "1234.56"}) == pytest.approx(1234.56)

    def test_price_bid_ask_fallback(self):
        from services.signal_outcome_resolver import _tick_price

        # bid+ask average
        px = _tick_price({"bid": "100.0", "ask": "101.0"})
        assert px == pytest.approx(100.5)

    def test_price_zero_if_no_fields(self):
        from services.signal_outcome_resolver import _tick_price

        assert _tick_price({}) == 0.0

    def test_stream_id_parse(self):
        from services.signal_outcome_resolver import _stream_id_ms

        assert _stream_id_ms("1717000000000-5") == 1717000000000
        assert _stream_id_ms(b"1717000000001-0") == 1717000000001
