"""Unit tests for P5 stream integrity.

Covers:
- StreamIntegrityTracker: gap detection, duplicate detection, schema drift
- StreamIntegrityGate: monitor / veto decision
"""

from __future__ import annotations

import pytest
from services.orderflow.stream_integrity import StreamIntegrityTracker, schema_hash, IntegritySnapshot
from services.orderflow.stream_integrity_gate import StreamIntegrityGate


class TestStreamIntegrityTracker:
    def _tracker(self) -> StreamIntegrityTracker:
        return StreamIntegrityTracker(tau_ms=5_000, z_window=10, max_gap_window_ms=10_000)

    def test_ok_sequence(self):
        t = self._tracker()
        for seq in range(1, 10):
            snap = t.update_seq(seq=seq, ts_ms=seq * 1000)
            assert snap.last_seq == seq
            assert snap.gap_last == 0
            assert snap.gap_rate_ema == pytest.approx(0.0, abs=0.01)

    def test_gap_increments_rate(self):
        t = self._tracker()
        t.update_seq(seq=1, ts_ms=1000)
        snap = t.update_seq(seq=5, ts_ms=2000)  # gap of 3
        assert snap.gap_last == 3
        assert snap.gap_rate_ema > 0.0
        assert snap.gap_max_window >= 3

    def test_dup_increments_rate(self):
        t = self._tracker()
        t.update_seq(seq=1, ts_ms=1000)
        t.update_seq(seq=2, ts_ms=2000)
        snap = t.update_seq(seq=2, ts_ms=2100)  # dup
        assert snap.dup_rate_ema > 0.0
        assert snap.last_seq == 2  # last_seq should not regress

    def test_max_gap_resets_after_window(self):
        t = self._tracker()
        t.update_seq(seq=1, ts_ms=1000)
        t.update_seq(seq=100, ts_ms=2000)  # gap=98
        snap = t.update_seq(seq=101, ts_ms=15000)  # crosses max_gap_window_ms=10s → resets
        assert snap.gap_max_window == 0  # window should have reset
        # next gap is 0 (ok seq), so max stays 0
        assert snap.gap_last == 0

    def test_schema_drift_detected(self):
        t = self._tracker()
        h1, c1 = t.update_schema(["a", "b", "c"])
        assert c1 == 0  # first observation, no change
        h2, c2 = t.update_schema(["a", "b", "d"])  # different key-set
        assert c2 == 1
        assert h2 != h1

    def test_schema_same_no_drift(self):
        t = self._tracker()
        t.update_schema(["a", "b"])
        _, c = t.update_schema(["b", "a"])  # same keys, different order → same hash
        assert c == 0

    def test_fail_open_on_bad_input(self):
        t = self._tracker()
        # Should not raise
        snap = t.update_seq(seq="not_an_int", ts_ms="bad")  # type: ignore
        assert isinstance(snap, IntegritySnapshot)


class TestSchemaHash:
    def test_order_independent(self):
        assert schema_hash(["a", "b", "c"]) == schema_hash(["c", "a", "b"])

    def test_different_keys_different_hash(self):
        assert schema_hash(["a", "b"]) != schema_hash(["a", "c"])

    def test_empty(self):
        h = schema_hash([])
        assert isinstance(h, str)


class TestStreamIntegrityGate:
    def _gate(self, mode: str = "veto", max_gap_rate: float = 0.1, max_gap_window: int = 20) -> StreamIntegrityGate:
        return StreamIntegrityGate(
            enabled=True,
            mode=mode,
            max_gap_rate_ema=max_gap_rate,
            max_dup_rate_ema=0.0,
            max_gap_window=max_gap_window,
            veto_on_schema_change=False,
        )

    def test_no_flags_no_action(self):
        g = self._gate()
        dec = g.evaluate(indicators={"tick_seq_gap_rate_ema": 0.0}, symbol="BTCUSDT")
        assert not dec.apply
        assert not dec.veto

    def test_monitor_mode_no_veto(self):
        g = StreamIntegrityGate(enabled=True, mode="monitor", max_gap_rate_ema=0.05, max_dup_rate_ema=0.0, max_gap_window=10, veto_on_schema_change=False)
        dec = g.evaluate(indicators={"tick_seq_gap_rate_ema": 0.9}, symbol="BTCUSDT")
        assert dec.apply
        assert not dec.veto
        assert "gap_rate_ema_high" in dec.flags

    def test_veto_mode_vetoes_on_threshold(self):
        g = self._gate(mode="veto", max_gap_rate=0.05)
        dec = g.evaluate(indicators={"tick_seq_gap_rate_ema": 0.9}, symbol="BTCUSDT")
        assert dec.veto

    def test_schema_change_veto(self):
        g = StreamIntegrityGate(enabled=True, mode="veto", max_gap_rate_ema=0.0, max_dup_rate_ema=0.0, max_gap_window=0, veto_on_schema_change=True)
        dec = g.evaluate(indicators={"tick_schema_changed": 1}, symbol="BTCUSDT")
        assert dec.veto
        assert "schema_changed" in dec.flags

    def test_disabled_gate_no_action(self):
        g = StreamIntegrityGate(enabled=False, mode="veto", max_gap_rate_ema=0.01, max_dup_rate_ema=0.0, max_gap_window=1, veto_on_schema_change=True)
        dec = g.evaluate(indicators={"tick_seq_gap_rate_ema": 1.0, "tick_schema_changed": 1}, symbol="X")
        assert not dec.apply
        assert not dec.veto

    def test_veto_on_dup_rate(self):
        g = StreamIntegrityGate(enabled=True, mode="veto", max_gap_rate_ema=0.0, max_dup_rate_ema=0.05, max_gap_window=0, veto_on_schema_change=False)
        dec = g.evaluate(indicators={"tick_seq_dup_rate_ema": 0.9}, symbol="BTCUSDT")
        assert dec.veto
        assert dec.reason_code == "VETO_DUP_RATE"
        assert "dup_rate_ema_high" in dec.flags

    def test_from_env_aliases_gap_and_dup_rate(self, monkeypatch):
        monkeypatch.setenv("DATA_MAX_SEQ_GAP_RATE", "0.15")
        monkeypatch.setenv("DATA_MAX_DUP_RATE", "0.07")
        g = StreamIntegrityGate.from_env()
        assert g.max_gap_rate_ema == pytest.approx(0.15)
        assert g.max_dup_rate_ema == pytest.approx(0.07)
