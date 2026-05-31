from __future__ import annotations

"""
Tests for the dual-emit feature in MLConfirmGate.

Background: prior to this feature, MLConfirmGate loaded challenger cfg only
as a fallback to champion absence; when champion was present (the prod case)
the challenger model was never scored, so `metrics:ml_confirm` had no rows
for the challenger `kind`. That made `ml_outcome_*{kind="edge_stack_v1"}`
permanently empty in Prometheus and blinded the v14_of_auto_promote live PR-AUC
gate.

Dual-emit: when ML_DUAL_EMIT_CHALLENGER=1, the gate independently loads
challenger cfg and scores it in SHADOW alongside champion, emitting a second
`metrics:ml_confirm` row tagged with the challenger's kind. Champion path
is unaffected; challenger failures are silent.
"""

import json
import os
from unittest.mock import MagicMock

import pytest

from services.ml_confirm import MLConfirmGate


def _make_redis(champion_payload, challenger_payload):
    r = MagicMock()

    def _get(key):
        if key == "cfg:ml_confirm:champion":
            return champion_payload
        if key == "cfg:ml_confirm:challenger":
            return challenger_payload
        return None

    r.get.side_effect = _get
    r.hgetall.return_value = {}
    r.xadd.return_value = "12345-0"
    r.set.return_value = True
    return r


@pytest.fixture
def champion_cfg_json():
    return json.dumps({
        "schema_version": 1,
        "mode": "SHADOW",
        "fail_policy": "OPEN",
        "enforce_share": 0.0,
        "kind": "meta_lr_blend",
        "model_path": "/nonexistent/champion.json",
        "run_id": "champion_test",
        "p_min": 0.5,
    })


@pytest.fixture
def challenger_cfg_json():
    return json.dumps({
        "schema_version": 1,
        "mode": "SHADOW",
        "fail_policy": "OPEN",
        "enforce_share": 0.0,
        "kind": "edge_stack_v1",
        "model_path": "/nonexistent/challenger.joblib",
        "run_id": "challenger_test",
        "p_min": 0.5,
        "feature_schema_ver": "v14_of",
    })


def _make_gate(redis_mock):
    return MLConfirmGate(
        r=redis_mock,
        mode="SHADOW",
        fail_policy="OPEN",
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger",
    )


def test_dual_emit_disabled_by_default(monkeypatch, champion_cfg_json, challenger_cfg_json):
    """ML_DUAL_EMIT_CHALLENGER=0 by default → loader and helper are no-ops."""
    monkeypatch.delenv("ML_DUAL_EMIT_CHALLENGER", raising=False)
    r = _make_redis(champion_cfg_json, challenger_cfg_json)
    gate = _make_gate(r)

    assert gate._dual_emit_enabled is False

    gate._load_challenger_only_sync()
    assert gate._chal_cfg == {}
    assert gate._chal_model is None

    gate._score_challenger_shadow(
        symbol="BTCUSDT", ts_ms=1, direction="LONG", scenario="range",
        indicators={}, rule_score=0.0, rule_have=0, rule_need=0,
        cancel_spike_veto=0, ok_rule=1, sid="crypto-of:BTCUSDT:1",
    )


def test_dual_emit_enabled_loads_challenger_cfg(monkeypatch, champion_cfg_json, challenger_cfg_json):
    """When flag=1, challenger cfg is loaded independently of champion."""
    monkeypatch.setenv("ML_DUAL_EMIT_CHALLENGER", "1")
    r = _make_redis(champion_cfg_json, challenger_cfg_json)
    gate = _make_gate(r)

    assert gate._dual_emit_enabled is True

    gate._load_challenger_only_sync()
    assert gate._chal_cfg.get("kind") == "edge_stack_v1"
    assert gate._chal_cfg.get("run_id") == "challenger_test"
    assert gate._cfg_source != "challenger", \
        "champion-side state must be preserved after challenger load"


def test_dual_emit_skips_when_cfg_source_is_challenger(
    monkeypatch, challenger_cfg_json
):
    """When champion-cfg is absent and the gate is already running on
    challenger (legacy fallback path), dual-emit must NOT redundantly load
    the same challenger again — otherwise we'd score it twice."""
    monkeypatch.setenv("ML_DUAL_EMIT_CHALLENGER", "1")
    r = _make_redis(None, challenger_cfg_json)
    gate = _make_gate(r)
    gate._cfg_source = "challenger"

    gate._load_challenger_only_sync()
    assert gate._chal_cfg == {}
    assert gate._chal_model is None


def test_dual_emit_skips_when_keys_identical(monkeypatch, champion_cfg_json):
    """challenger_key == champion_key → loading challenger would just
    duplicate champion → must skip."""
    monkeypatch.setenv("ML_DUAL_EMIT_CHALLENGER", "1")
    r = _make_redis(champion_cfg_json, champion_cfg_json)
    gate = MLConfirmGate(
        r=r,
        mode="SHADOW",
        fail_policy="OPEN",
        champion_key="cfg:ml_confirm:same",
        challenger_key="cfg:ml_confirm:same",
    )
    gate._load_challenger_only_sync()
    assert gate._chal_cfg == {}


def test_score_challenger_shadow_swap_and_restore(
    monkeypatch, champion_cfg_json, challenger_cfg_json
):
    """The helper must restore self._cfg/_model/_cfg_source after scoring,
    so the champion decision returned by check() is unaffected."""
    monkeypatch.setenv("ML_DUAL_EMIT_CHALLENGER", "1")
    r = _make_redis(champion_cfg_json, challenger_cfg_json)
    gate = _make_gate(r)

    # Prime champion state.
    gate._cfg = json.loads(champion_cfg_json)
    gate._model = object()  # opaque non-None champion model placeholder
    gate._cfg_source = "champion"
    gate._cfg_key_used = "cfg:ml_confirm:champion"

    # Prime challenger state directly (skip Redis path).
    gate._chal_cfg = json.loads(challenger_cfg_json)
    gate._chal_model = {"kind": "edge_stack_v1"}  # opaque placeholder

    saved_cfg = gate._cfg
    saved_model = gate._model
    saved_source = gate._cfg_source

    # _decide_* will fail because model isn't a real edge_stack pack — but
    # the helper must still restore state in `finally`.
    gate._score_challenger_shadow(
        symbol="BTCUSDT", ts_ms=1, direction="LONG", scenario="range",
        indicators={"spread_bps": 1.0}, rule_score=0.5,
        rule_have=1, rule_need=1, cancel_spike_veto=0, ok_rule=1,
        sid="crypto-of:BTCUSDT:1",
    )
    assert gate._cfg is saved_cfg
    assert gate._model is saved_model
    assert gate._cfg_source == saved_source


def test_score_challenger_shadow_no_op_without_model(
    monkeypatch, challenger_cfg_json
):
    """Helper must early-return when challenger model is missing — no swap,
    no emit."""
    monkeypatch.setenv("ML_DUAL_EMIT_CHALLENGER", "1")
    r = _make_redis(None, challenger_cfg_json)
    gate = _make_gate(r)
    gate._chal_cfg = json.loads(challenger_cfg_json)
    gate._chal_model = None

    pre_source = gate._cfg_source
    gate._score_challenger_shadow(
        symbol="BTCUSDT", ts_ms=1, direction="LONG", scenario="range",
        indicators={}, rule_score=0.0, rule_have=0, rule_need=0,
        cancel_spike_veto=0, ok_rule=1, sid="crypto-of:BTCUSDT:1",
    )
    assert gate._cfg_source == pre_source


def test_score_challenger_shadow_zero_sample_skips(
    monkeypatch, challenger_cfg_json
):
    """ML_DUAL_EMIT_CHALLENGER_SAMPLE=0.0 → never sample → helper bails out."""
    monkeypatch.setenv("ML_DUAL_EMIT_CHALLENGER", "1")
    monkeypatch.setenv("ML_DUAL_EMIT_CHALLENGER_SAMPLE", "0.0")
    r = _make_redis(None, challenger_cfg_json)
    gate = _make_gate(r)
    gate._chal_cfg = json.loads(challenger_cfg_json)
    gate._chal_model = {"kind": "edge_stack_v1"}

    pre_source = gate._cfg_source
    gate._score_challenger_shadow(
        symbol="BTCUSDT", ts_ms=1, direction="LONG", scenario="range",
        indicators={}, rule_score=0.0, rule_have=0, rule_need=0,
        cancel_spike_veto=0, ok_rule=1, sid="crypto-of:BTCUSDT:1",
    )
    assert gate._cfg_source == pre_source
