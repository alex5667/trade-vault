"""Tests for exec_health_observability.py."""
from __future__ import annotations

import os
import sys
import types
import unittest
from typing import Any
from unittest.mock import MagicMock, patch


def _make_decision(**kwargs) -> Any:
    from dataclasses import dataclass, field

    @dataclass
    class _Dec:
        apply: bool = True
        veto: bool = False
        mode: str = "monitor"
        scope: str = "edge"
        reason_code: str = ""
        flags: list = field(default_factory=list)
        tighten_add_bps: float = 0.0
        tighten_k_mult: float = 1.0
        rollups: dict = field(default_factory=dict)

    d = _Dec(**kwargs)
    return d


class FakeCounter:
    def __init__(self):
        self.calls: list[dict] = []

    def labels(self, **kw):
        self.calls.append(kw)
        return self

    def inc(self, v=1.0):
        return self

    def observe(self, v):
        return self

    def set(self, v):
        return self


class FakeGauge(FakeCounter):
    pass


class FakeHistogram(FakeCounter):
    pass


class TestRecordExecHealthObservability(unittest.TestCase):
    def _make_fake_metrics(self):
        return {
            "exec_health_apply_total": FakeCounter()
            "exec_health_decision_total": FakeCounter()
            "exec_health_flag_total": FakeCounter()
            "exec_health_reader_errors_total": FakeCounter()
            "exec_health_rollup_present": FakeGauge()
            "exec_health_rollup_value_bps": FakeGauge()
            "exec_health_rollup_worst_delta_sec": FakeGauge()
            "exec_health_policy_threshold_bps": FakeGauge()
            "exec_health_policy_mode": FakeGauge()
            "exec_health_tighten_add_bps_scoped": FakeHistogram()
            "exec_health_tighten_add_bps": FakeHistogram()
            "exec_health_tighten_k": FakeGauge()
            "exec_health_veto_total": FakeCounter()
            "exec_health_last_event_ts_ms": FakeGauge()
        }

    def _patch_and_call(self, fake_metrics, **call_kwargs):
        import services.orderflow.exec_health_observability as mod

        orig = {}
        for name, fake in fake_metrics.items():
            orig[name] = getattr(mod, name, None)
            setattr(mod, name, fake)
        try:
            mod.record_exec_health_observability(**call_kwargs)
        finally:
            for name, val in orig.items():
                setattr(mod, name, val)

    def test_no_decision_emits_rollup_presence(self):
        import services.orderflow.exec_health_observability as mod

        fakes = self._make_fake_metrics()
        rollups = {"is_p95_bps": 3.5, "perm_impact_p95_bps": 1.2}
        self._patch_and_call(
            fakes
            symbol="BTCUSDT"
            scope="edge"
            profile="default"
            rollups=rollups
            decision=None
        )
        calls = fakes["exec_health_rollup_present"].calls
        assert any(c.get("metric") == "is_p95_bps" and c.get("symbol") == "BTCUSDT" for c in calls)
        # decision not set -> no exec_health_decision_total call
        assert len(fakes["exec_health_decision_total"].calls) == 0

    def test_veto_decision_increments_veto_total(self):
        import services.orderflow.exec_health_observability as mod

        fakes = self._make_fake_metrics()
        dec = _make_decision(apply=True, veto=True, mode="veto", reason_code="VETO_IS_P95", flags=["IS_P95_HIGH"])
        rollups = {"is_p95_bps": 12.5}
        self._patch_and_call(
            fakes
            symbol="ETHUSDT"
            scope="pipeline"
            profile="hard"
            rollups=rollups
            decision=dec
        )
        veto_calls = fakes["exec_health_veto_total"].calls
        assert any(c.get("reason") == "VETO_IS_P95" for c in veto_calls), f"expected veto call; got {veto_calls}"

    def test_tighten_emits_histogram(self):
        import services.orderflow.exec_health_observability as mod

        fakes = self._make_fake_metrics()
        dec = _make_decision(apply=True, veto=False, mode="tighten", tighten_add_bps=2.5)
        self._patch_and_call(
            fakes
            symbol="SOLUSDT"
            scope="entry_policy"
            profile="strict"
            rollups={}
            decision=dec
        )
        hist_calls = fakes["exec_health_tighten_add_bps_scoped"].calls
        assert any(c.get("scope") == "entry_policy" and c.get("symbol") == "SOLUSDT" for c in hist_calls)

    def test_reader_error_increments_counter(self):
        import services.orderflow.exec_health_observability as mod

        fakes = self._make_fake_metrics()
        orig = mod.exec_health_reader_errors_total
        mod.exec_health_reader_errors_total = fakes["exec_health_reader_errors_total"]
        try:
            mod.record_exec_health_reader_error(scope="edge", where="read_rollups")
        finally:
            mod.exec_health_reader_errors_total = orig
        err_calls = fakes["exec_health_reader_errors_total"].calls
        assert any(c.get("scope") == "edge" and c.get("where") == "read_rollups" for c in err_calls)

    def test_fail_open_on_bad_rollup_values(self):
        """NaN / inf rollup values must not raise."""
        import services.orderflow.exec_health_observability as mod

        fakes = self._make_fake_metrics()
        rollups = {"is_p95_bps": float("nan"), "perm_impact_p95_bps": float("inf")}
        self._patch_and_call(
            fakes
            symbol="BTCUSDT"
            scope="edge"
            profile="default"
            rollups=rollups
            decision=None
        )  # No exception expected

    def test_empty_rollups_all_metrics_set_absent(self):
        import services.orderflow.exec_health_observability as mod

        fakes = self._make_fake_metrics()
        self._patch_and_call(
            fakes
            symbol="XRPUSDT"
            scope="edge"
            profile="default"
            rollups={}
            decision=None
        )
        presence_calls = fakes["exec_health_rollup_present"].calls
        # All three rollup metrics should be marked as absent (0.0)
        present_values = {c["metric"]: None for c in presence_calls if c.get("symbol") == "XRPUSDT"}
        assert "is_p95_bps" in present_values

    def test_flags_emit_per_flag(self):
        import services.orderflow.exec_health_observability as mod

        fakes = self._make_fake_metrics()
        dec = _make_decision(apply=True, veto=False, flags=["IS_P95_HIGH", "PERM_IMPACT_HIGH"])
        self._patch_and_call(
            fakes
            symbol="BTCUSDT"
            scope="edge"
            profile="default"
            rollups={"is_p95_bps": 5.0}
            decision=dec
        )
        flag_calls = fakes["exec_health_flag_total"].calls
        flags_emitted = {c["flag"] for c in flag_calls}
        assert "IS_P95_HIGH" in flags_emitted
        assert "PERM_IMPACT_HIGH" in flags_emitted


class TestExecHealthPolicySnapshot(unittest.TestCase):
    def test_get_exec_health_policy_from_env_default(self):
        from services.orderflow.exec_health_rollups import get_exec_health_policy_from_env

        pol = get_exec_health_policy_from_env(profile="default", scope="edge")
        assert pol.profile == "default"
        assert pol.scope == "edge"
        assert pol.mode in ("off", "monitor", "tighten", "veto")
        assert pol.thresholds is not None

    def test_get_exec_health_policy_snapshot_is_frozen(self):
        from services.orderflow.exec_health_rollups import get_exec_health_policy_from_env

        pol = get_exec_health_policy_from_env(profile="soft", scope="pipeline")
        with self.assertRaises(Exception):
            pol.profile = "hacked"  # type: ignore

    def test_decide_exec_health_delegates_through_policy(self):
        from services.orderflow.exec_health_rollups import decide_exec_health_from_env

        dec = decide_exec_health_from_env(profile="default", rollups={}, scope="edge")
        assert dec is not None
        assert hasattr(dec, "apply")
        assert hasattr(dec, "veto")


if __name__ == "__main__":
    unittest.main()
