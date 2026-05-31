"""Tests for orderflow_services.gate_value_rollout_governor_v1.

All LLM calls are mocked. fakeredis simulates the streams/state keys.
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict

import fakeredis
import pytest

from orderflow_services import gate_value_rollout_governor_v1 as gvr


@pytest.fixture
def r() -> fakeredis.FakeRedis:
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def cfg(monkeypatch) -> gvr.Cfg:
    # Tighten gates so tests don't need to simulate days.
    monkeypatch.setenv("GVR_STAGE3_MIN_GROWTH_GATED_OUT", "100")
    monkeypatch.setenv("GVR_STAGE3_MIN_GROWTH_LABELS_TB", "100")
    monkeypatch.setenv("GVR_STAGE3_MIN_DWELL_H", "0.0")
    monkeypatch.setenv("GVR_STAGE5_MIN_DURATION_H", "0.0")
    monkeypatch.setenv("GVR_STAGE5_MIN_GROUPS", "1")
    monkeypatch.setenv("GVR_STAGE5_MAX_ROLLBACK_TOTAL", "5")
    monkeypatch.setenv("GVR_STAGE5_MIN_STABLE_FRAC", "0.50")
    monkeypatch.setenv("GVR_STAGE6_CANARY_H", "0.0")
    monkeypatch.setenv("GVR_NOTIFY_TELEGRAM", "1")
    monkeypatch.setenv("GVR_LLM_ENABLED", "1")
    return gvr.load_cfg()


def _populate_streams(
    r,
    *,
    gated_out_n: int = 200,
    labels_n: int = 200,
    ml_n: int = 200,
    stream_go: str = "stream:signals:gated_out_outcomes",
    stream_lt: str = "labels:tb",
    stream_ml: str = "metrics:ml_confirm",
) -> None:
    for i in range(gated_out_n):
        r.xadd(stream_go, {"sid": f"sid_go_{i}", "y": "0"})
    for i in range(labels_n):
        r.xadd(stream_lt, {"payload": json.dumps({"sid": f"sid_lt_{i}", "primary": 1})})
    for i in range(ml_n):
        r.xadd(stream_ml, {"sid": f"sid_ml_{i}", "p_edge": "0.7"})


def _populate_autocal(
    r,
    *,
    state_key: str = "autocal:gate_value:state",
    groups: dict[str, dict] | None = None,
) -> None:
    payload = {
        "schema_version": 1,
        "ts_ms": int(time.time() * 1000),
        "groups": groups or {
            "edge_stack_v1|BTCUSDT|1800000": {
                "phase": "RELAX_APPLIED",
                "rollback_count": 0,
            },
            "edge_stack_v1|ETHUSDT|1800000": {
                "phase": "KEEP_CONFIRMED",
                "rollback_count": 0,
            },
        },
    }
    r.set(state_key, json.dumps(payload))


def _allow_advisory():
    return {
        "valid": True,
        "errors": [],
        "guarded_recommendations": [
            {"action": "propose_threshold_canary", "risk": "low", "reason": "stable"}
        ],
        "blocked_recommendations": [],
    }


def _veto_advisory():
    return {
        "valid": True,
        "errors": [],
        "guarded_recommendations": [
            {"action": "freeze_candidate", "risk": "high", "reason": "churn detected"}
        ],
        "blocked_recommendations": [],
    }


# ── numerical gates ────────────────────────────────────────────────────────


def test_numerical_gates_stage3_pass(cfg):
    state = gvr.RolloutState(stage=gvr.STAGE_3, stage_entry_ms=1)
    snap = gvr.Snapshot(
        xlen_gated_out_outcomes=500, xlen_labels_tb=500, xlen_ml_confirm=300,
        growth_gated_out_window=200, growth_labels_tb_window=200,
        autocal_groups=2, autocal_rollback_total=0,
        autocal_phase_distribution={"RELAX_APPLIED": 2},
        autocal_stable_frac=1.0, llm_veto_rate=0.0,
        days_since_start=1.0, stage_dwell_h=6.0,
    )
    ok, fails = gvr._numerical_gates_advance(state, snap, cfg)
    assert ok and fails == []


def test_numerical_gates_stage3_fail_growth(cfg):
    state = gvr.RolloutState(stage=gvr.STAGE_3, stage_entry_ms=1)
    snap = gvr.Snapshot(
        xlen_gated_out_outcomes=500, xlen_labels_tb=500, xlen_ml_confirm=300,
        growth_gated_out_window=10, growth_labels_tb_window=10,
        autocal_groups=2, autocal_rollback_total=0,
        autocal_phase_distribution={}, autocal_stable_frac=0.0,
        llm_veto_rate=0.0, days_since_start=0.0, stage_dwell_h=6.0,
    )
    ok, fails = gvr._numerical_gates_advance(state, snap, cfg)
    assert not ok
    assert any("gated_out_growth" in f for f in fails)


def test_numerical_gates_stage5_fail_unstable(cfg):
    state = gvr.RolloutState(stage=gvr.STAGE_5, stage_entry_ms=1)
    snap = gvr.Snapshot(
        xlen_gated_out_outcomes=1000, xlen_labels_tb=1000, xlen_ml_confirm=500,
        growth_gated_out_window=400, growth_labels_tb_window=400,
        autocal_groups=2, autocal_rollback_total=10,  # too many rollbacks
        autocal_phase_distribution={"OBSERVE": 2},
        autocal_stable_frac=0.0,
        llm_veto_rate=0.0, days_since_start=10.0, stage_dwell_h=200.0,
    )
    ok, fails = gvr._numerical_gates_advance(state, snap, cfg)
    assert not ok
    assert any("rollback_total" in f for f in fails) or any("stable_frac" in f for f in fails)


# ── _advisory_blocks ───────────────────────────────────────────────────────


def test_advisory_blocks_on_freeze():
    assert gvr._advisory_blocks(_veto_advisory()) is True


def test_advisory_does_not_block_on_allow():
    assert gvr._advisory_blocks(_allow_advisory()) is False


def test_advisory_does_not_block_on_empty():
    assert gvr._advisory_blocks({}) is False


# ── _phase_counts ──────────────────────────────────────────────────────────


def test_phase_counts_basic():
    autocal = {
        "groups": {
            "a": {"phase": "RELAX_APPLIED", "rollback_count": 1},
            "b": {"phase": "KEEP_CONFIRMED", "rollback_count": 0},
            "c": {"phase": "OBSERVE", "rollback_count": 2},
        }
    }
    g, rb, st, dist = gvr._phase_counts(autocal)
    assert g == 3
    assert rb == 3
    assert st == 2  # RELAX_APPLIED + KEEP_CONFIRMED
    assert dist == {"RELAX_APPLIED": 1, "KEEP_CONFIRMED": 1, "OBSERVE": 1}


def test_phase_counts_empty():
    g, rb, st, dist = gvr._phase_counts({})
    assert g == 0 and rb == 0 and st == 0 and dist == {}


# ── run_once end-to-end ───────────────────────────────────────────────────


def test_run_once_initialises_state_on_first_cycle(r, cfg, monkeypatch):
    monkeypatch.setattr(
        "orderflow_services.gate_value_rollout_llm_advisor.advise_stage_transition",
        lambda **_kw: _allow_advisory(),
    )
    _populate_streams(r, gated_out_n=50, labels_n=50)  # NOT enough to advance
    _populate_autocal(r)
    state = gvr.run_once(r, cfg)
    # First cycle: stage_entry_ms set, xlen snapshot stored
    assert state.stage == gvr.STAGE_3
    assert state.stage_entry_ms > 0
    assert state.xlen_gated_out_at_entry == 50


def test_run_once_stage3_to_stage5_when_growth_sufficient(r, cfg, monkeypatch):
    monkeypatch.setattr(
        "orderflow_services.gate_value_rollout_llm_advisor.advise_stage_transition",
        lambda **_kw: _allow_advisory(),
    )
    _populate_autocal(r)
    # First cycle initialises with xlen=0 at entry.
    state0 = gvr.run_once(r, cfg, now_ms=1_000_000_000_000)
    assert state0.stage == gvr.STAGE_3

    # Second cycle: streams have grown by 200 each — should advance.
    _populate_streams(r, gated_out_n=200, labels_n=200)
    state1 = gvr.run_once(r, cfg, now_ms=1_000_000_000_000 + 24 * 3_600_000)
    assert state1.stage == gvr.STAGE_5

    # Telegram should contain a stage_transition event
    notify = r.xrange("notify:telegram")
    events = [fields.get("event") for _id, fields in notify]
    assert "stage_transition" in events


def test_run_once_llm_veto_blocks_advance(r, cfg, monkeypatch):
    monkeypatch.setattr(
        "orderflow_services.gate_value_rollout_llm_advisor.advise_stage_transition",
        lambda **_kw: _veto_advisory(),
    )
    _populate_autocal(r)
    gvr.run_once(r, cfg, now_ms=1_000_000_000_000)
    _populate_streams(r, gated_out_n=200, labels_n=200)
    state = gvr.run_once(r, cfg, now_ms=1_000_000_000_000 + 3_600_000)
    assert state.stage == gvr.STAGE_3  # held
    notify = r.xrange("notify:telegram")
    events = [fields.get("event") for _id, fields in notify]
    assert "stage_held" in events


def test_run_once_full_promotion_chain_flips_enforce(r, cfg, monkeypatch):
    monkeypatch.setattr(
        "orderflow_services.gate_value_rollout_llm_advisor.advise_stage_transition",
        lambda **_kw: _allow_advisory(),
    )
    _populate_autocal(r)
    # Start: empty stream baseline
    gvr.run_once(r, cfg, now_ms=1_000_000_000_000)
    # Cycle 2: stage3 → stage5
    _populate_streams(r, gated_out_n=200, labels_n=200)
    s1 = gvr.run_once(r, cfg, now_ms=1_000_000_000_000 + 3_600_000)
    assert s1.stage == gvr.STAGE_5
    # Cycle 3: stage5 → stage6c
    s2 = gvr.run_once(r, cfg, now_ms=1_000_000_000_000 + 2 * 3_600_000)
    assert s2.stage == gvr.STAGE_6C
    # Cycle 4: stage6c → stage6 ENFORCED (flips cfg:gva:enforce)
    s3 = gvr.run_once(r, cfg, now_ms=1_000_000_000_000 + 3 * 3_600_000)
    assert s3.stage == gvr.STAGE_6
    assert s3.enforce_flipped_ms > 0
    # Redis flag
    assert r.get(cfg.enforce_override_key) == "1"
    # Telegram
    notify = r.xrange("notify:telegram")
    events = [fields.get("event") for _id, fields in notify]
    assert "enforce_flipped" in events


def test_run_once_stage6c_llm_veto_rolls_back_to_stage5(r, cfg, monkeypatch):
    # Pre-seed state at STAGE_6C
    state = gvr.RolloutState(
        stage=gvr.STAGE_6C,
        stage_entry_ms=1_000_000_000_000,
        xlen_gated_out_at_entry=200,
        xlen_labels_tb_at_entry=200,
    )
    r.set(cfg.rollout_state_key, json.dumps(asdict(state)))
    _populate_autocal(r)
    _populate_streams(r, gated_out_n=500, labels_n=500)

    # LLM vetoes during 6c — should roll back to stage 5.
    monkeypatch.setattr(
        "orderflow_services.gate_value_rollout_llm_advisor.advise_stage_transition",
        lambda **_kw: _veto_advisory(),
    )
    s = gvr.run_once(r, cfg, now_ms=1_000_000_000_000 + 24 * 3_600_000)
    assert s.stage == gvr.STAGE_5
    notify = r.xrange("notify:telegram")
    events = [fields.get("event") for _id, fields in notify]
    assert "rollback" in events


def test_run_once_stage6_terminal_sends_daily_summary(r, cfg, monkeypatch):
    state = gvr.RolloutState(
        stage=gvr.STAGE_6,
        stage_entry_ms=1_000_000_000_000,
        enforce_flipped_ms=1_000_000_000_000,
        last_daily_summary_ms=0,
    )
    r.set(cfg.rollout_state_key, json.dumps(asdict(state)))
    _populate_autocal(r)
    _populate_streams(r, gated_out_n=500, labels_n=500)

    s = gvr.run_once(r, cfg, now_ms=1_000_000_000_000 + 25 * 3_600_000)
    assert s.stage == gvr.STAGE_6
    notify = r.xrange("notify:telegram")
    events = [fields.get("event") for _id, fields in notify]
    assert "daily_summary" in events


def test_flip_enforce_idempotent(r, cfg):
    assert gvr._flip_enforce(r, cfg) is True
    assert r.get(cfg.enforce_override_key) == "1"
    # Second call: no-op
    assert gvr._flip_enforce(r, cfg) is False


# ── autocal Redis override (integration with autocal) ──────────────────────


def test_autocal_resolve_enforce_honours_redis_override(r, monkeypatch):
    from orderflow_services import gate_value_autocalibrator_v1 as gva

    # ENV ENFORCE=0
    monkeypatch.setenv("GVA_ENFORCE", "0")
    cfg = gva.load_cfg()
    assert gva._resolve_enforce(r, cfg) is False
    r.set(cfg.enforce_override_key, "1")
    assert gva._resolve_enforce(r, cfg) is True
    # Setting "0" in Redis does NOT downgrade env ENFORCE=1
    r.set(cfg.enforce_override_key, "0")
    assert gva._resolve_enforce(r, cfg) is False
    monkeypatch.setenv("GVA_ENFORCE", "1")
    cfg2 = gva.load_cfg()
    r.set(cfg2.enforce_override_key, "0")
    assert gva._resolve_enforce(r, cfg2) is True  # env wins
