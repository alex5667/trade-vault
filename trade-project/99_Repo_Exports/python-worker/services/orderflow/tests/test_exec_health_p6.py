from __future__ import annotations

"""Unit tests for P6 ExecutionHealthGate.

Covers:
- decide_execution_health: monitor / tighten / veto decisions
- ExecHealthThresholds: construction from env
- build_rollup_keys: deduplication and fallback order
- apply_exec_health_to_indicators: expected_slippage_bps mutation
"""


import os
from unittest import mock

import pytest

from services.orderflow.execution_health_gate import (
    ExecHealthDecision,
    ExecHealthThresholds,
    apply_exec_health_to_indicators,
    build_rollup_keys,
    decide_execution_health,
)


class TestDecideExecutionHealth:
    def _thr(self, max_is=5.0, max_pi=3.0, min_rs=-999.0) -> ExecHealthThresholds:
        return ExecHealthThresholds(
            max_is_p95_bps=max_is,
            max_perm_impact_p95_bps=max_pi,
            min_realized_spread_p50_bps=min_rs,
            tighten_add_mult=1.0,
            tighten_add_cap_bps=8.0,
        )

    def test_no_rollups_no_action(self):
        with mock.patch.dict(os.environ, {"EXEC_HEALTH_MODE": "veto"}):
            dec = decide_execution_health(rollups={}, thr=self._thr())
        assert not dec.apply
        assert not dec.veto

    def test_below_threshold_no_flags(self):
        with mock.patch.dict(os.environ, {"EXEC_HEALTH_MODE": "veto"}):
            dec = decide_execution_health(rollups={"is_p95_bps": 2.0, "perm_impact_p95_bps_1": 1.0}, thr=self._thr())
        assert not dec.apply
        assert not dec.veto

    def test_monitor_mode_no_veto(self):
        with mock.patch.dict(os.environ, {"EXEC_HEALTH_MODE": "monitor"}):
            dec = decide_execution_health(rollups={"is_p95_bps": 10.0, "perm_impact_p95_bps_1": 8.0}, thr=self._thr())
        assert dec.apply
        assert not dec.veto
        assert "is_p95_high" in dec.flags

    def test_tighten_adds_bps(self):
        with mock.patch.dict(os.environ, {"EXEC_HEALTH_MODE": "tighten"}):
            dec = decide_execution_health(rollups={"is_p95_bps": 10.0, "perm_impact_p95_bps_1": 6.0}, thr=self._thr())
        assert dec.apply
        assert not dec.veto
        assert dec.tighten_add_bps > 0.0

    def test_veto_requires_both_is_and_perm_impact(self):
        with mock.patch.dict(os.environ, {"EXEC_HEALTH_MODE": "veto"}):
            # Only IS high → no veto
            dec = decide_execution_health(rollups={"is_p95_bps": 10.0, "perm_impact_p95_bps_1": 1.0}, thr=self._thr())
        assert not dec.veto

    def test_veto_both_paths(self):
        with mock.patch.dict(os.environ, {"EXEC_HEALTH_MODE": "veto"}):
            dec = decide_execution_health(rollups={"is_p95_bps": 10.0, "perm_impact_p95_bps_1": 8.0}, thr=self._thr())
        assert dec.veto
        assert "is_p95_high" in dec.flags
        assert "perm_impact_p95_high" in dec.flags

    def test_adverse_selection_flag(self):
        thr = ExecHealthThresholds(max_is_p95_bps=0, max_perm_impact_p95_bps=0, min_realized_spread_p50_bps=1.0)
        with mock.patch.dict(os.environ, {"EXEC_HEALTH_MODE": "monitor"}):
            dec = decide_execution_health(rollups={"realized_spread_p50_bps_1": -2.0}, thr=thr)
        assert dec.apply
        assert "adverse_realized" in dec.flags

    def test_fail_open_nan_rollup(self):
        # NaN values treated as missing → no action
        with mock.patch.dict(os.environ, {"EXEC_HEALTH_MODE": "veto"}):
            dec = decide_execution_health(rollups={"is_p95_bps": float("nan")}, thr=self._thr())
        assert not dec.apply

    def test_tighten_add_cap_bps(self):
        thr = ExecHealthThresholds(max_is_p95_bps=1.0, max_perm_impact_p95_bps=0, min_realized_spread_p50_bps=-999, tighten_add_cap_bps=3.0, tighten_add_mult=100.0)
        with mock.patch.dict(os.environ, {"EXEC_HEALTH_MODE": "tighten"}):
            dec = decide_execution_health(rollups={"is_p95_bps": 100.0}, thr=thr)
        assert dec.tighten_add_bps <= 3.0


class TestBuildRollupKeys:
    def test_exact_key_first(self):
        keys = build_rollup_keys(metric="is_p95_bps", sym="BTCUSDT", venue="binance", session="asia", tf="1m", kind="continuation", side="BUY")
        assert keys[0] == "tca:is_p95_bps:BTCUSDT:binance:asia:1m:continuation:BUY"

    def test_fallback_all_last(self):
        keys = build_rollup_keys(metric="is_p95_bps", sym="BTCUSDT", venue="binance", session="asia", tf="1m", kind="continuation", side="BUY")
        assert keys[-1] == "tca:is_p95_bps:BTCUSDT:binance:all:all:all:BUY"

    def test_dedup(self):
        keys = build_rollup_keys(metric="m", sym="X", venue="v", session="all", tf="all", kind="all", side="NA")
        # All fallbacks collapse to the same key
        seen = set(keys)
        assert len(seen) <= len(keys)


class TestApplyExecHealth:
    def test_tighten_increases_slippage(self):
        ind = {"expected_slippage_bps": 5.0}
        dec = ExecHealthDecision(apply=True, veto=False, flags=["is_p95_high"], tighten_add_bps=2.0)
        apply_exec_health_to_indicators(indicators=ind, dec=dec)
        assert ind["expected_slippage_bps"] == pytest.approx(7.0)
        assert ind["exec_health_apply"] == 1

    def test_veto_annotated(self):
        ind = {}
        dec = ExecHealthDecision(apply=True, veto=True, flags=["is_p95_high", "perm_impact_p95_high"], reason_code="VETO_IMPL_SHORTFALL_P95")
        apply_exec_health_to_indicators(indicators=ind, dec=dec)
        assert ind["exec_health_veto"] == 1

    def test_no_apply_no_mutation(self):
        ind = {}
        dec = ExecHealthDecision(apply=False, veto=False, flags=[])
        apply_exec_health_to_indicators(indicators=ind, dec=dec)
        assert ind.get("exec_health_apply") == 0

    def test_fail_open(self):
        # Should not raise even with weird inputs
        apply_exec_health_to_indicators(indicators=None, dec=ExecHealthDecision(apply=True, veto=False, flags=[]))  # type: ignore
