from __future__ import annotations

"""Tests for liqmap_gate_calibrator_v1."""

import time

import pytest

from orderflow_services.liqmap_gate_calibrator_v1 import (
    CalConfig,
    CalStats,
    check_qualify,
    compute_stats,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cfg(
    min_shadow_hours: float = 48.0,
    min_veto_n: int = 10,
    min_precision: float = 0.55,
    min_r_delta: float = 0.25,
) -> CalConfig:
    return CalConfig(
        redis_url="redis://localhost:6379/0",
        redis_main_url="redis://localhost:6379/0",
        since_hours=168.0,
        interval_sec=3600,
        min_shadow_hours=min_shadow_hours,
        min_veto_n=min_veto_n,
        min_precision=min_precision,
        min_r_delta=min_r_delta,
        cooldown_sec=86400,
        state_path="/tmp/test_liqmap_cal_state.json",
        apply=False,
        enable=True,
        notify_stream="notify:telegram",
        recs_secret="test",
    )


def _decision(sid: str, shadow_veto: int, reason: str = "adverse_peak_in_sl",
               symbol: str = "BTCUSDT") -> tuple[str, dict]:
    return sid, {
        "shadow_veto": shadow_veto,
        "veto": 0,
        "rr": 1.5,
        "risk_bps": 20.0,
        "reward_bps": 30.0,
        "reason": reason,
        "mode": "shadow",
        "symbol": symbol,
        "direction": "LONG",
        "ts_ms": int(time.time() * 1000),
    }


def _trade(sid: str, r_mult: float, symbol: str = "BTCUSDT") -> tuple[str, dict]:
    return sid, {"r_mult": r_mult, "symbol": symbol, "direction": "LONG"}


# ---------------------------------------------------------------------------
# compute_stats
# ---------------------------------------------------------------------------

class TestComputeStats:
    def test_empty_decisions(self):
        s = compute_stats({}, {"t1": {"r_mult": 1.0, "symbol": "BTCUSDT", "direction": "LONG"}})
        assert s.total_decisions == 0
        assert s.total_joined == 0
        assert s.veto_n == 0

    def test_empty_trades(self):
        decs = dict([_decision("d1", 1)])
        s = compute_stats(decs, {})
        assert s.total_joined == 0

    def test_basic_split(self):
        # 3 veto (2 negative R), 2 pass (both positive R)
        decs = dict([
            _decision("v1", 1), _decision("v2", 1), _decision("v3", 1),
            _decision("p1", 0), _decision("p2", 0),
        ])
        trades = dict([
            _trade("v1", -1.0), _trade("v2", -0.5), _trade("v3", 0.5),
            _trade("p1", 1.0),  _trade("p2", 0.8),
        ])
        s = compute_stats(decs, trades)

        assert s.veto_n == 3
        assert s.pass_n == 2
        assert s.total_joined == 5
        # 2 out of 3 vetoed trades have negative R
        assert s.veto_precision == pytest.approx(2/3, abs=0.001)
        # pass_r_mean = (1.0+0.8)/2 = 0.9, veto_r_mean = (-1.0-0.5+0.5)/3 ≈ -0.333
        assert s.r_delta == pytest.approx(0.9 - (-1.0-0.5+0.5)/3, abs=0.01)

    def test_no_join(self):
        decs = dict([_decision("a", 1)])
        trades = dict([_trade("b", -1.0)])  # different sid
        s = compute_stats(decs, trades)
        assert s.total_joined == 0
        assert s.veto_precision == 0.0

    def test_reason_counter(self):
        decs = dict([
            _decision("v1", 1, reason="adverse_peak_in_sl"),
            _decision("v2", 1, reason="rr_low"),
            _decision("v3", 1, reason="adverse_peak_in_sl"),
            _decision("p1", 0, reason="ok"),
        ])
        trades = dict([_trade(k, -1.0) for k in ("v1", "v2", "v3", "p1")])
        s = compute_stats(decs, trades)
        assert s.veto_reasons["adverse_peak_in_sl"] == 2
        assert s.veto_reasons["rr_low"] == 1
        assert "ok" not in s.veto_reasons  # pass decisions don't record reason

    def test_by_symbol(self):
        decs = dict([
            _decision("v1", 1, symbol="BTCUSDT"),
            _decision("v2", 1, symbol="ETHUSDT"),
            _decision("p1", 0, symbol="BTCUSDT"),
        ])
        trades = dict([
            _trade("v1", -1.0, "BTCUSDT"),
            _trade("v2", -0.5, "ETHUSDT"),
            _trade("p1",  1.0, "BTCUSDT"),
        ])
        s = compute_stats(decs, trades)
        assert "BTCUSDT" in s.by_symbol
        assert "ETHUSDT" in s.by_symbol
        assert s.by_symbol["BTCUSDT"]["veto_n"] == 1
        assert s.by_symbol["BTCUSDT"]["pass_n"] == 1

    def test_nan_r_mult_excluded(self):
        # Trades with non-finite r_mult should have been filtered before compute_stats
        # (filtered in _read_trades); compute_stats itself gets clean data
        decs = dict([_decision("v1", 1)])
        trades = dict([_trade("v1", 0.0)])  # r=0 is fine, not negative
        s = compute_stats(decs, trades)
        assert s.veto_n == 1
        assert s.veto_precision == 0.0  # 0.0 is not < 0


# ---------------------------------------------------------------------------
# check_qualify
# ---------------------------------------------------------------------------

class TestCheckQualify:
    def _stats(self, veto_n=15, precision=0.65, r_delta=0.40) -> CalStats:
        return CalStats(
            total_decisions=30, total_joined=25,
            veto_n=veto_n, pass_n=25-veto_n,
            veto_precision=precision,
            veto_r_mean=-0.4, veto_r_median=-0.35,
            pass_r_mean=-0.4+r_delta, pass_r_median=0.0,
            r_delta=r_delta,
            veto_reasons={"adverse_peak_in_sl": veto_n},
            by_symbol={},
        )

    def test_all_criteria_met(self):
        cfg = _make_cfg(min_shadow_hours=48, min_veto_n=10, min_precision=0.55, min_r_delta=0.25)
        shadow_start_ms = int(time.time() * 1000) - 50 * 3_600_000  # 50h ago
        q = check_qualify(self._stats(), cfg, shadow_start_ms)
        assert q.qualified is True
        assert all(c["ok"] for c in q.checks.values())

    def test_shadow_hours_not_met(self):
        cfg = _make_cfg(min_shadow_hours=48)
        shadow_start_ms = int(time.time() * 1000) - 10 * 3_600_000  # only 10h
        q = check_qualify(self._stats(), cfg, shadow_start_ms)
        assert q.qualified is False
        assert not q.checks["shadow_hours"]["ok"]

    def test_veto_n_not_met(self):
        cfg = _make_cfg(min_veto_n=10)
        shadow_start_ms = int(time.time() * 1000) - 50 * 3_600_000
        q = check_qualify(self._stats(veto_n=5), cfg, shadow_start_ms)
        assert q.qualified is False
        assert not q.checks["veto_n"]["ok"]

    def test_precision_not_met(self):
        cfg = _make_cfg(min_precision=0.55)
        shadow_start_ms = int(time.time() * 1000) - 50 * 3_600_000
        q = check_qualify(self._stats(precision=0.40), cfg, shadow_start_ms)
        assert q.qualified is False
        assert not q.checks["veto_precision"]["ok"]

    def test_r_delta_not_met(self):
        cfg = _make_cfg(min_r_delta=0.25)
        shadow_start_ms = int(time.time() * 1000) - 50 * 3_600_000
        q = check_qualify(self._stats(r_delta=0.10), cfg, shadow_start_ms)
        assert q.qualified is False
        assert not q.checks["r_delta"]["ok"]

    def test_shadow_start_zero(self):
        cfg = _make_cfg(min_shadow_hours=48)
        q = check_qualify(self._stats(), cfg, shadow_start_ms=0)
        assert q.shadow_hours == pytest.approx(0.0, abs=0.1)
        assert not q.checks["shadow_hours"]["ok"]

    def test_shadow_hours_value(self):
        cfg = _make_cfg(min_shadow_hours=48)
        ago_ms = int(time.time() * 1000) - 72 * 3_600_000
        q = check_qualify(self._stats(), cfg, shadow_start_ms=ago_ms)
        assert q.shadow_hours == pytest.approx(72.0, abs=0.1)

    def test_exactly_at_threshold(self):
        cfg = _make_cfg(min_veto_n=10, min_precision=0.55, min_r_delta=0.25, min_shadow_hours=48)
        shadow_start_ms = int(time.time() * 1000) - 48 * 3_600_000 - 1000
        q = check_qualify(self._stats(veto_n=10, precision=0.55, r_delta=0.25), cfg, shadow_start_ms)
        assert q.qualified is True
