# tests/test_trailing_profiles_registry_in_tm_v1.py
from __future__ import annotations

"""
Regression tests: trade_monitor reads trailing atr_mult from TrailingProfilesRegistry
(same source as binance_executor), not from ENV alone.

Priority chain tested:
  1) SymbolSpec.trailing_tp1_offset_atr  (calibrator output — HIGHEST)
  1b) TrailingProfilesRegistry[profile].atr_mult  ← new unified source
  2+) ENV per-symbol / per-source / global
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

from domain.models import PositionState


def _make_pos(trail_profile: str = "rocket_v1", signal_payload: dict | None = None) -> PositionState:
    pos = PositionState(
        id="t1", sid="s1", strategy="CryptoOrderFlow", source="CryptoOrderFlow",
        symbol="BTCUSDT", tf="1m", direction="LONG",
        entry_price=50000.0, entry_ts_ms=1, lot=0.1, remaining_qty=0.1,
        sl=49000.0, tp_levels=[51000.0],
        signal_payload=signal_payload or {"atr": 500.0},
    )
    pos.trail_profile = trail_profile
    return pos


def _make_spec(**kwargs) -> SimpleNamespace:
    return SimpleNamespace(**kwargs)


def _make_registry(atr_mult: float, profile_name: str = "rocket_v1") -> MagicMock:
    profile = MagicMock()
    profile.atr_mult = atr_mult
    reg = MagicMock()
    reg.get.return_value = profile
    reg.list_names.return_value = [profile_name]
    return reg


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestResolveTrailingTp1OffsetAtr:
    """Unit tests for _resolve_trailing_tp1_offset_atr priority chain."""

    def _call(self, svc, pos, spec):
        return svc._resolve_trailing_tp1_offset_atr(pos, spec)

    def _make_svc(self, registry=None, env_default: str = "0.9"):
        """Build a minimal TradeMonitorService-like stub (no Redis/DB required)."""
        from services.trade_monitor import TradeMonitorService
        svc = object.__new__(TradeMonitorService)
        svc.trailing_tp1_offset_default = float(env_default)
        svc._trailing_profiles = registry
        return svc

    # ------------------------------------------------------------------
    # 1) SymbolSpec wins over everything
    # ------------------------------------------------------------------
    def test_spec_wins_over_registry(self, monkeypatch):
        monkeypatch.setenv("TRAILING_TP1_OFFSET_ATR", "0.9")
        reg = _make_registry(0.7)
        svc = self._make_svc(registry=reg, env_default="0.9")
        pos = _make_pos()
        spec = _make_spec(trailing_tp1_offset_atr=0.33)

        result = self._call(svc, pos, spec)
        assert result == 0.33, f"expected 0.33 from SymbolSpec, got {result}"

    # ------------------------------------------------------------------
    # 1b) Registry wins over ENV when SymbolSpec is absent
    # ------------------------------------------------------------------
    def test_registry_wins_over_env_when_spec_absent(self, monkeypatch):
        monkeypatch.setenv("TRAILING_TP1_OFFSET_ATR", "0.9")
        reg = _make_registry(0.42)
        svc = self._make_svc(registry=reg, env_default="0.9")
        pos = _make_pos("rocket_v1")
        spec = _make_spec()  # no trailing_tp1_offset_atr

        result = self._call(svc, pos, spec)
        assert result == 0.42, f"expected 0.42 from registry, got {result}"

    # ------------------------------------------------------------------
    # 1b) Profile name resolved from signal_payload fallback
    # ------------------------------------------------------------------
    def test_registry_resolves_profile_from_signal_payload(self, monkeypatch):
        monkeypatch.setenv("TRAILING_TP1_OFFSET_ATR", "0.9")
        reg = _make_registry(0.55, "wide_swing")
        svc = self._make_svc(registry=reg, env_default="0.9")
        pos = _make_pos(trail_profile="", signal_payload={"trail_profile": "wide_swing", "atr": 500.0})
        pos.trail_profile = ""
        spec = _make_spec()

        result = self._call(svc, pos, spec)
        assert result == 0.55, f"expected 0.55 from registry wide_swing, got {result}"

    # ------------------------------------------------------------------
    # 2) ENV fallback when registry is None (registry unavailable)
    # ------------------------------------------------------------------
    def test_env_global_fallback_when_registry_none(self, monkeypatch):
        monkeypatch.setenv("TRAILING_TP1_OFFSET_ATR", "0.77")
        svc = self._make_svc(registry=None, env_default="0.77")
        pos = _make_pos()
        spec = _make_spec()

        result = self._call(svc, pos, spec)
        assert result == 0.77, f"expected 0.77 from ENV fallback, got {result}"

    # ------------------------------------------------------------------
    # 2) ENV per-symbol wins over global when registry is None
    # ------------------------------------------------------------------
    def test_env_per_symbol_wins_over_global(self, monkeypatch):
        monkeypatch.setenv("TRAILING_TP1_OFFSET_ATR", "0.9")
        monkeypatch.setenv("TRAILING_TP1_OFFSET_ATR_BTCUSDT", "0.55")
        svc = self._make_svc(registry=None, env_default="0.9")
        pos = _make_pos()
        spec = _make_spec()

        result = self._call(svc, pos, spec)
        assert result == 0.55, f"expected 0.55 from ENV per-symbol, got {result}"
        monkeypatch.delenv("TRAILING_TP1_OFFSET_ATR_BTCUSDT", raising=False)

    # ------------------------------------------------------------------
    # Fail-open: registry raises → falls through to ENV
    # ------------------------------------------------------------------
    def test_registry_exception_falls_through_to_env(self, monkeypatch):
        monkeypatch.setenv("TRAILING_TP1_OFFSET_ATR", "0.6")
        reg = MagicMock()
        reg.get.side_effect = RuntimeError("redis is down")
        reg.list_names.return_value = []
        svc = self._make_svc(registry=reg, env_default="0.6")
        pos = _make_pos()
        spec = _make_spec()

        result = self._call(svc, pos, spec)
        assert result == 0.6, f"expected 0.6 from ENV fallback after registry error, got {result}"

    # ------------------------------------------------------------------
    # Profile not found → fallback to rocket_v1 (same as binance_executor)
    # ------------------------------------------------------------------
    def test_registry_missing_profile_fallback_to_rocket_v1(self, monkeypatch):
        monkeypatch.setenv("TRAILING_TP1_OFFSET_ATR", "0.9")
        rocket_profile = MagicMock()
        rocket_profile.atr_mult = 0.6

        reg = MagicMock()
        # first call (unknown profile) returns None, second (rocket_v1) returns profile
        reg.get.side_effect = lambda name: None if name != "rocket_v1" else rocket_profile
        svc = self._make_svc(registry=reg, env_default="0.9")
        pos = _make_pos("nonexistent_profile")
        spec = _make_spec()

        result = self._call(svc, pos, spec)
        assert result == 0.6, f"expected 0.6 from rocket_v1 fallback, got {result}"
