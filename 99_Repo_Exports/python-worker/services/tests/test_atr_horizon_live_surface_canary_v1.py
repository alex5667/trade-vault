from __future__ import annotations
"""Phase 2.4A unit tests: should_apply_live_surface canary router."""

import pytest

from services.atr_horizon_live_surface_canary import should_apply_live_surface


class TestLiveSurfaceCanaryModes:
    def test_shadow_mode_never_applies(self, monkeypatch):
        monkeypatch.setenv("ATR_HORIZON_LIVE_SURFACE_MODE", "shadow")
        out = should_apply_live_surface(symbol="BTCUSDT", sid="abc123")
        assert out["should_apply"] is False
        assert out["mode"] == "shadow"
        assert out["reason_code"] == "LIVE_SURFACE_SHADOW_ONLY"

    def test_off_mode_never_applies(self, monkeypatch):
        monkeypatch.setenv("ATR_HORIZON_LIVE_SURFACE_MODE", "off")
        out = should_apply_live_surface(symbol="BTCUSDT", sid="abc123")
        assert out["should_apply"] is False
        assert out["reason_code"] == "LIVE_SURFACE_OFF"

    def test_enforce_mode_always_applies(self, monkeypatch):
        monkeypatch.setenv("ATR_HORIZON_LIVE_SURFACE_MODE", "enforce")
        monkeypatch.delenv("ATR_HORIZON_LIVE_SURFACE_SYMBOLS", raising=False)
        out = should_apply_live_surface(symbol="BTCUSDT", sid="abc123")
        assert out["should_apply"] is True
        assert out["reason_code"] == "LIVE_SURFACE_ENFORCE_ALL"

    def test_enforce_mode_symbol_filtered(self, monkeypatch):
        monkeypatch.setenv("ATR_HORIZON_LIVE_SURFACE_MODE", "enforce")
        monkeypatch.setenv("ATR_HORIZON_LIVE_SURFACE_SYMBOLS", "ETHUSDT")
        out = should_apply_live_surface(symbol="BTCUSDT", sid="abc123")
        assert out["should_apply"] is False
        assert out["reason_code"] == "LIVE_SURFACE_SYMBOL_FILTERED"

    def test_unknown_mode_defaults_to_shadow(self, monkeypatch):
        monkeypatch.setenv("ATR_HORIZON_LIVE_SURFACE_MODE", "bogus_mode")
        out = should_apply_live_surface(symbol="BTCUSDT", sid="abc123")
        assert out["should_apply"] is False
        assert out["mode"] == "shadow"


class TestLiveSurfaceCanaryRollout:
    def test_zero_share_never_selects(self, monkeypatch):
        monkeypatch.setenv("ATR_HORIZON_LIVE_SURFACE_MODE", "canary")
        monkeypatch.setenv("ATR_HORIZON_LIVE_SURFACE_CANARY_SHARE", "0.0")
        monkeypatch.delenv("ATR_HORIZON_LIVE_SURFACE_SYMBOLS", raising=False)
        # With share=0 no signal should be selected
        for sid in ["s1", "s2", "s3", "s4", "s5"]:
            out = should_apply_live_surface(symbol="BTCUSDT", sid=sid)
            assert out["should_apply"] is False

    def test_full_share_always_selects(self, monkeypatch):
        monkeypatch.setenv("ATR_HORIZON_LIVE_SURFACE_MODE", "canary")
        monkeypatch.setenv("ATR_HORIZON_LIVE_SURFACE_CANARY_SHARE", "1.0")
        monkeypatch.delenv("ATR_HORIZON_LIVE_SURFACE_SYMBOLS", raising=False)
        for sid in ["s1", "s2", "s3", "s4", "s5"]:
            out = should_apply_live_surface(symbol="BTCUSDT", sid=sid)
            assert out["should_apply"] is True

    def test_partial_share_is_deterministic(self, monkeypatch):
        """Same sticky_key must always produce the same decision."""
        monkeypatch.setenv("ATR_HORIZON_LIVE_SURFACE_MODE", "canary")
        monkeypatch.setenv("ATR_HORIZON_LIVE_SURFACE_CANARY_SHARE", "0.5")
        monkeypatch.delenv("ATR_HORIZON_LIVE_SURFACE_SYMBOLS", raising=False)
        out1 = should_apply_live_surface(symbol="BTCUSDT", sid="fixed_sid", regime="trend_up", scenario="breakout")
        out2 = should_apply_live_surface(symbol="BTCUSDT", sid="fixed_sid", regime="trend_up", scenario="breakout")
        assert out1["should_apply"] == out2["should_apply"]

    def test_canary_symbol_filter_works(self, monkeypatch):
        monkeypatch.setenv("ATR_HORIZON_LIVE_SURFACE_MODE", "canary")
        monkeypatch.setenv("ATR_HORIZON_LIVE_SURFACE_CANARY_SHARE", "1.0")
        monkeypatch.setenv("ATR_HORIZON_LIVE_SURFACE_SYMBOLS", "ETHUSDT")
        out = should_apply_live_surface(symbol="BTCUSDT", sid="abc123")
        assert out["should_apply"] is False
        assert out["reason_code"] == "LIVE_SURFACE_SYMBOL_FILTERED"

    def test_canary_symbol_case_insensitive(self, monkeypatch):
        monkeypatch.setenv("ATR_HORIZON_LIVE_SURFACE_MODE", "canary")
        monkeypatch.setenv("ATR_HORIZON_LIVE_SURFACE_CANARY_SHARE", "1.0")
        monkeypatch.setenv("ATR_HORIZON_LIVE_SURFACE_SYMBOLS", "btcusdt")
        out = should_apply_live_surface(symbol="BTCUSDT", sid="abc123")
        assert out["should_apply"] is True

    def test_partial_share_roughly_correct(self, monkeypatch):
        """~50% share should select roughly 40-60% of signals."""
        monkeypatch.setenv("ATR_HORIZON_LIVE_SURFACE_MODE", "canary")
        monkeypatch.setenv("ATR_HORIZON_LIVE_SURFACE_CANARY_SHARE", "0.5")
        monkeypatch.delenv("ATR_HORIZON_LIVE_SURFACE_SYMBOLS", raising=False)
        selected = sum(
            1 for i in range(1000)
            if should_apply_live_surface(symbol="BTCUSDT", sid=f"sid_{i}")["should_apply"]
        )
        assert 350 < selected < 650, f"Expected ~50% selected, got {selected}/1000"


class TestLiveSurfaceCanaryContract:
    def test_returns_dict_always(self, monkeypatch):
        monkeypatch.setenv("ATR_HORIZON_LIVE_SURFACE_MODE", "canary")
        monkeypatch.setenv("ATR_HORIZON_LIVE_SURFACE_CANARY_SHARE", "0.5")
        out = should_apply_live_surface(symbol="BTCUSDT", sid="abc123", regime="trend_up", scenario="breakout")
        assert isinstance(out, dict)
        for key in ("mode", "should_apply", "share_used", "sticky_key", "reason_code"):
            assert key in out, f"Missing key: {key}"

    def test_mode_is_valid_value(self, monkeypatch):
        for m in ("off", "shadow", "canary", "enforce", "bogus"):
            monkeypatch.setenv("ATR_HORIZON_LIVE_SURFACE_MODE", m)
            out = should_apply_live_surface(symbol="BTCUSDT", sid="x")
            assert out["mode"] in {"off", "shadow", "canary", "enforce"}

    def test_sticky_key_contains_symbol(self, monkeypatch):
        monkeypatch.setenv("ATR_HORIZON_LIVE_SURFACE_MODE", "shadow")
        out = should_apply_live_surface(symbol="solusdt", sid="s1", regime="range", scenario="bounce")
        assert "SOLUSDT" in out["sticky_key"]

    def test_different_regimes_different_sticky_keys(self, monkeypatch):
        monkeypatch.setenv("ATR_HORIZON_LIVE_SURFACE_MODE", "shadow")
        out1 = should_apply_live_surface(symbol="BTCUSDT", sid="s1", regime="trend_up")
        out2 = should_apply_live_surface(symbol="BTCUSDT", sid="s1", regime="range")
        assert out1["sticky_key"] != out2["sticky_key"]
