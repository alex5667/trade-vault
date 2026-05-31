"""Tests for orderflow_services.gate_value_autocalibrator_v1.

All LLM calls are mocked so we never touch a real network. Uses fakeredis
to assert side effects (state key, applied key, notify:telegram XADDs).
"""

from __future__ import annotations

import json
import time

import fakeredis
import pytest

from orderflow_services import gate_value_autocalibrator_v1 as gva


@pytest.fixture
def cfg(monkeypatch) -> gva.Cfg:
    monkeypatch.setenv("GVA_ENFORCE", "1")
    monkeypatch.setenv("GVA_LLM_ENABLED", "1")
    monkeypatch.setenv("GVA_MIN_N_PASSED", "100")
    monkeypatch.setenv("GVA_MIN_N_GATED_OUT", "100")
    monkeypatch.setenv("GVA_MIN_DWELL_H", "0.0")
    monkeypatch.setenv("GVA_NOTIFY_TELEGRAM", "1")
    monkeypatch.setenv("GVA_RELAX_STEP", "0.03")
    return gva.load_cfg()


@pytest.fixture
def r() -> fakeredis.FakeRedis:
    return fakeredis.FakeRedis(decode_responses=True)


def _allow_advisory():
    return {
        "valid": True,
        "errors": [],
        "guarded_recommendations": [
            {"action": "propose_threshold_canary", "risk": "low", "reason": "stats consistent"}
        ],
        "blocked_recommendations": [],
    }


def _veto_advisory():
    return {
        "valid": True,
        "errors": [],
        "guarded_recommendations": [
            {"action": "freeze_candidate", "risk": "high", "reason": "regime breakdown"}
        ],
        "blocked_recommendations": [],
    }


def _blocked_advisory():
    return {
        "valid": False,
        "errors": ["payload_not_dict"],
        "guarded_recommendations": [],
        "blocked_recommendations": [{"reason": "blocked_action", "action": "enable_enforce"}],
    }


def _report(
    *,
    action: str,
    passed_n: int = 800,
    passed_avg_r: float = 0.10,
    gated_out_n: int = 800,
    gated_out_avg_r: float = -0.08,
    ci_low: float = 0.10,
    ci_high: float = 0.22,
    fn_rate: float = 0.18,
    symbol: str = "BTCUSDT",
    kind: str = "edge_stack_v1",
    horizon_ms: int = 1_800_000,
    passed_pf: float = 1.2,
    gated_out_pf: float = 0.6,
) -> dict:
    return {
        "ts_ms": int(time.time() * 1000),
        "lookback_hours": 24,
        "n_groups": 1,
        "groups": [
            {
                "group": {
                    "symbol": symbol,
                    "kind": kind,
                    "horizon_ms": horizon_ms,
                    "tp_bps_bucket": 15,
                    "sl_bps_bucket": 10,
                },
                "passed": {
                    "n": passed_n,
                    "win_rate": 0.5,
                    "avg_r": passed_avg_r,
                    "median_r": passed_avg_r,
                    "p25_r": passed_avg_r,
                    "p75_r": passed_avg_r,
                    "profit_factor": passed_pf,
                    "tp_hit_rate": 0.4,
                    "sl_hit_rate": 0.3,
                    "timeout_rate": 0.3,
                    "avg_ret_bps": passed_avg_r * 15,
                },
                "gated_out": {
                    "n": gated_out_n,
                    "win_rate": fn_rate,
                    "avg_r": gated_out_avg_r,
                    "median_r": gated_out_avg_r,
                    "p25_r": gated_out_avg_r,
                    "p75_r": gated_out_avg_r,
                    "profit_factor": gated_out_pf,
                    "tp_hit_rate": fn_rate,
                    "sl_hit_rate": 0.5,
                    "timeout_rate": 0.3,
                    "avg_ret_bps": gated_out_avg_r * 15,
                },
                "lift": {
                    "avg_r_lift": passed_avg_r - gated_out_avg_r,
                    "win_rate_lift": 0.3,
                    "profit_factor_lift": passed_pf - gated_out_pf,
                    "sl_hit_rate_reduction": 0.2,
                    "false_negative_rate": fn_rate,
                },
                "ci": {
                    "avg_r_lift_p05": ci_low,
                    "avg_r_lift_p50": (ci_low + ci_high) / 2.0,
                    "avg_r_lift_p95": ci_high,
                },
                "decision": {
                    "action": action,
                    "severity": "info",
                    "confidence": 0.8,
                    "reason_codes": ["test"],
                },
            }
        ],
    }


# ── _propose_phase logic ─────────────────────────────────────────────────────


def test_propose_keep_confirmed_when_reporter_keep(cfg):
    proposed, _ = gva._propose_phase(
        "KEEP_GATE", "OBSERVE",
        ci_low=0.10, ci_high=0.20, fn_rate=0.20, cfg=cfg,
    )
    assert proposed == "KEEP_CONFIRMED"


def test_propose_relax_canary_when_ci_high_negative(cfg):
    proposed, _ = gva._propose_phase(
        "RELAX_GATE", "OBSERVE",
        ci_low=-0.20, ci_high=-0.02, fn_rate=0.10, cfg=cfg,
    )
    assert proposed == "RELAX_CANARY"


def test_propose_relax_applied_after_canary(cfg):
    proposed, _ = gva._propose_phase(
        "RELAX_GATE", "RELAX_CANARY",
        ci_low=-0.20, ci_high=-0.02, fn_rate=0.10, cfg=cfg,
    )
    assert proposed == "RELAX_APPLIED"


def test_propose_disable_candidate(cfg):
    proposed, _ = gva._propose_phase(
        "DISABLE_GATE", "OBSERVE",
        ci_low=-0.30, ci_high=-0.10, fn_rate=0.20, cfg=cfg,
    )
    assert proposed == "DISABLE_CANDIDATE"


def test_propose_holds_phase_on_insufficient_data(cfg):
    proposed, reason = gva._propose_phase(
        "INSUFFICIENT_DATA", "RELAX_CANARY",
        ci_low=0.0, ci_high=0.0, fn_rate=0.0, cfg=cfg,
    )
    assert proposed == "RELAX_CANARY"
    assert "insufficient_data" in reason


def test_propose_inconclusive_drops_to_observe(cfg):
    proposed, _ = gva._propose_phase(
        "INCONCLUSIVE", "RELAX_CANARY",
        ci_low=-0.05, ci_high=0.10, fn_rate=0.1, cfg=cfg,
    )
    assert proposed == "OBSERVE"


def test_propose_relax_canary_when_fn_rate_high(cfg):
    proposed, reason = gva._propose_phase(
        "RELAX_GATE", "OBSERVE",
        ci_low=-0.05, ci_high=0.20, fn_rate=0.40, cfg=cfg,
    )
    assert proposed == "RELAX_CANARY"
    assert "fn_rate" in reason


# ── numerical gates ─────────────────────────────────────────────────────────


def test_numerical_gates_pass_happy(cfg):
    ok, fails = gva._numerical_gates_pass(
        passed_n=800, gated_out_n=800, dwell_h=24.0, cfg=cfg,
    )
    assert ok and fails == []


def test_numerical_gates_fail_when_passed_low(cfg):
    ok, fails = gva._numerical_gates_pass(
        passed_n=10, gated_out_n=800, dwell_h=24.0, cfg=cfg,
    )
    assert not ok and any("passed_n" in f for f in fails)


def test_numerical_gates_fail_when_dwell_too_short(monkeypatch):
    monkeypatch.setenv("GVA_MIN_DWELL_H", "12.0")
    monkeypatch.setenv("GVA_MIN_N_PASSED", "100")
    monkeypatch.setenv("GVA_MIN_N_GATED_OUT", "100")
    cfg = gva.load_cfg()
    ok, fails = gva._numerical_gates_pass(
        passed_n=800, gated_out_n=800, dwell_h=2.0, cfg=cfg,
    )
    assert not ok and any("dwell_h" in f for f in fails)


# ── advisory veto ───────────────────────────────────────────────────────────


def test_advisory_blocks_when_freeze_candidate():
    adv = _veto_advisory()
    assert gva._advisory_blocks_transition(adv) is True


def test_advisory_blocks_when_guard_blocked():
    adv = _blocked_advisory()
    assert gva._advisory_blocks_transition(adv) is True


def test_advisory_does_not_block_when_propose_canary():
    adv = _allow_advisory()
    assert gva._advisory_blocks_transition(adv) is False


def test_advisory_does_not_block_when_empty():
    assert gva._advisory_blocks_transition({}) is False


# ── _phase_to_min_conf_delta ────────────────────────────────────────────────


def test_phase_delta_only_for_relax_applied(cfg):
    assert gva._phase_to_min_conf_delta("OBSERVE", cfg) == 0.0
    assert gva._phase_to_min_conf_delta("KEEP_CONFIRMED", cfg) == 0.0
    assert gva._phase_to_min_conf_delta("RELAX_CANARY", cfg) == 0.0
    assert gva._phase_to_min_conf_delta("RELAX_APPLIED", cfg) == -0.03
    assert gva._phase_to_min_conf_delta("DISABLE_CANDIDATE", cfg) == 0.0


# ── run_once end-to-end ─────────────────────────────────────────────────────


def test_run_once_no_report_skips(r, cfg):
    decisions = gva.run_once(r, cfg)
    assert decisions == {}


def test_run_once_keep_path_does_not_apply(r, cfg, monkeypatch):
    r.set(cfg.report_key, json.dumps(_report(action="KEEP_GATE")))

    # LLM mocked to allow
    monkeypatch.setattr(
        "orderflow_services.gate_value_llm_advisor.advise_gate_transition",
        lambda **_kw: _allow_advisory(),
    )

    decisions = gva.run_once(r, cfg)
    assert len(decisions) == 1
    d = next(iter(decisions.values()))
    assert d.phase == "KEEP_CONFIRMED"
    assert d.applied_min_conf_delta == 0.0
    # No applied config key
    applied_keys = list(r.scan_iter(match=f"{cfg.applied_key_prefix}:*"))
    assert applied_keys == []
    # State key set
    state_raw = r.get(cfg.state_key)
    assert state_raw is not None
    state = json.loads(state_raw)
    assert d.group_key in state["groups"]
    # Telegram XADD for phase change
    notify = r.xrange(cfg.notify_stream)
    assert any(
        (fields.get("event") == "phase_transition" for _id, fields in notify)
    )


def test_run_once_relax_two_step_progression(r, cfg, monkeypatch):
    monkeypatch.setattr(
        "orderflow_services.gate_value_llm_advisor.advise_gate_transition",
        lambda **_kw: _allow_advisory(),
    )
    report = _report(
        action="RELAX_GATE",
        ci_low=-0.20, ci_high=-0.02,
        passed_avg_r=-0.05, gated_out_avg_r=0.10,
        passed_pf=0.9, gated_out_pf=1.3,
        fn_rate=0.4,
    )
    r.set(cfg.report_key, json.dumps(report))

    # Cycle 1: OBSERVE → RELAX_CANARY (no applied write yet)
    decisions1 = gva.run_once(r, cfg, now_ms=1_000_000_000_000)
    d1 = next(iter(decisions1.values()))
    assert d1.phase == "RELAX_CANARY"
    assert d1.applied_min_conf_delta == 0.0
    applied = list(r.scan_iter(match=f"{cfg.applied_key_prefix}:*"))
    assert applied == []

    # Cycle 2: RELAX_CANARY → RELAX_APPLIED (writes applied key when ENFORCE=1)
    decisions2 = gva.run_once(r, cfg, now_ms=1_000_000_000_000 + 3_600_000)
    d2 = next(iter(decisions2.values()))
    assert d2.phase == "RELAX_APPLIED"
    assert d2.applied_min_conf_delta == -0.03
    applied = list(r.scan_iter(match=f"{cfg.applied_key_prefix}:*"))
    assert len(applied) == 1


def test_run_once_llm_veto_blocks_transition(r, cfg, monkeypatch):
    monkeypatch.setattr(
        "orderflow_services.gate_value_llm_advisor.advise_gate_transition",
        lambda **_kw: _veto_advisory(),
    )
    r.set(
        cfg.report_key,
        json.dumps(
            _report(
                action="RELAX_GATE",
                ci_low=-0.20, ci_high=-0.02,
                passed_pf=0.9, gated_out_pf=1.3,
            )
        ),
    )
    decisions = gva.run_once(r, cfg)
    d = next(iter(decisions.values()))
    assert d.phase == "OBSERVE"  # transition vetoed
    assert "llm_veto" in d.reason


def test_run_once_disable_candidate_telegram(r, cfg, monkeypatch):
    monkeypatch.setattr(
        "orderflow_services.gate_value_llm_advisor.advise_gate_transition",
        lambda **_kw: _allow_advisory(),
    )
    r.set(
        cfg.report_key,
        json.dumps(_report(action="DISABLE_GATE", passed_avg_r=-0.20, passed_pf=0.5)),
    )
    decisions = gva.run_once(r, cfg)
    d = next(iter(decisions.values()))
    assert d.phase == "DISABLE_CANDIDATE"
    # Never auto-applied — applied_min_conf_delta == 0
    assert d.applied_min_conf_delta == 0.0
    notify = r.xrange(cfg.notify_stream)
    events = {fields.get("event") for _id, fields in notify}
    assert "disable_candidate" in events


def test_run_once_insufficient_data_holds_phase(r, cfg, monkeypatch):
    monkeypatch.setattr(
        "orderflow_services.gate_value_llm_advisor.advise_gate_transition",
        lambda **_kw: _allow_advisory(),
    )
    # Seed prev state at RELAX_CANARY
    r.set(
        cfg.state_key,
        json.dumps(
            {
                "schema_version": 1,
                "ts_ms": 1,
                "groups": {
                    "edge_stack_v1|BTCUSDT|1800000": {
                        "phase": "RELAX_CANARY",
                        "last_phase_change_ms": 1,
                        "rollback_count": 0,
                    }
                },
            }
        ),
    )
    r.set(cfg.report_key, json.dumps(_report(action="INSUFFICIENT_DATA")))
    decisions = gva.run_once(r, cfg)
    d = next(iter(decisions.values()))
    assert d.phase == "RELAX_CANARY"
    assert "insufficient_data" in d.reason


def test_run_once_numerical_gates_block_when_low_n(r, cfg, monkeypatch):
    monkeypatch.setattr(
        "orderflow_services.gate_value_llm_advisor.advise_gate_transition",
        lambda **_kw: _allow_advisory(),
    )
    r.set(
        cfg.report_key,
        json.dumps(
            _report(
                action="RELAX_GATE",
                passed_n=10, gated_out_n=10,
                ci_low=-0.20, ci_high=-0.02,
                passed_pf=0.9, gated_out_pf=1.3,
            )
        ),
    )
    decisions = gva.run_once(r, cfg)
    d = next(iter(decisions.values()))
    assert d.phase == "OBSERVE"
    assert "numerical_gates_fail" in d.reason
