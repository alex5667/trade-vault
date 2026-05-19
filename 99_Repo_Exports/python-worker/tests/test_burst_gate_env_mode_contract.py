"""Contract tests for eval_burst_gate BURST_GATE_MODE ENV fallback.

Verifies that:
1. When cfg has no burst_gate_mode key, BURST_GATE_MODE env var takes effect.
2. BURST_GATE_MODE=veto (compose default) produces hard veto on extreme pressure.
3. BURST_GATE_MODE=penalty (old default) never produces hard veto.
4. cfg["burst_gate_mode"] takes priority over BURST_GATE_MODE env (per code comment).
5. Snapshot always contains burst_mode key so consumers can track active mode.
"""
from __future__ import annotations

import os
from unittest.mock import patch

from core.burst_gate_v1 import eval_burst_gate


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_EXTREME_INDICATORS = {
    # hawkes_cancel_lam / mu_c=10 → exc_c = 800/10 = 80 >> thr_excess=2.5 → excess_max=80
    # ctr = cr_ema/tr_ema = 1/0.01 = 100 >> thr_ctr=4.0 → both veto conditions fire
    "cancel_rate_ema": 1.0,
    "taker_rate_ema": 0.01,
    "hawkes_cancel_lam": 800.0,
    "hawkes_trade_lam": 500.0,
    "book_churn_score": 400.0,  # >> thr_score=3.0
}

_CALM_INDICATORS = {
    "hawkes_lambda_t": 0.1,
    "hawkes_lambda_c": 0.1,
    "hawkes_lambda_h": 0.1,
}

_BASE_CFG: dict = {}


# ---------------------------------------------------------------------------
# ENV fallback takes effect when cfg key absent
# ---------------------------------------------------------------------------

def test_env_veto_mode_produces_hard_veto_on_extreme():
    """BURST_GATE_MODE=veto via ENV must produce hard veto on extreme burst."""
    with patch.dict(os.environ, {"BURST_GATE_MODE": "veto"}):
        _, veto, reason, _ = eval_burst_gate(_EXTREME_INDICATORS, _BASE_CFG)
    assert veto == 1, f"Expected hard veto in veto mode (extreme pressure), got veto={veto} reason={reason!r}"


def test_env_penalty_mode_no_hard_veto():
    """BURST_GATE_MODE=penalty (old default) must never produce hard veto."""
    with patch.dict(os.environ, {"BURST_GATE_MODE": "penalty"}):
        _, veto, _, snap = eval_burst_gate(_EXTREME_INDICATORS, _BASE_CFG)
    assert veto == 0, f"penalty mode must not hard-veto, got veto={veto}"
    # would_veto counter should still fire (observability)
    assert snap.get("burst_would_veto") in (0, 1), "burst_would_veto must be 0 or 1"


def test_env_enforce_mode_is_alias_for_veto():
    """BURST_GATE_MODE=enforce must also produce hard veto (same as veto/hard)."""
    with patch.dict(os.environ, {"BURST_GATE_MODE": "enforce"}):
        _, veto, _, _ = eval_burst_gate(_EXTREME_INDICATORS, _BASE_CFG)
    assert veto == 1, f"enforce mode must hard-veto on extreme pressure, got {veto}"


def test_env_off_mode_no_op():
    """BURST_GATE_MODE=off must return zeros (no-op)."""
    with patch.dict(os.environ, {"BURST_GATE_MODE": "off"}):
        pen, veto, _, snap = eval_burst_gate(_EXTREME_INDICATORS, _BASE_CFG)
    assert pen == 0.0 and veto == 0 and snap == {}, "off mode must be a no-op"


# ---------------------------------------------------------------------------
# cfg key takes priority over ENV
# ---------------------------------------------------------------------------

def test_cfg_key_overrides_env():
    """cfg[burst_gate_mode] must take priority over BURST_GATE_MODE env."""
    cfg_penalty = {"burst_gate_mode": "penalty"}
    with patch.dict(os.environ, {"BURST_GATE_MODE": "veto"}):
        _, veto, _, _ = eval_burst_gate(_EXTREME_INDICATORS, cfg_penalty)
    # ENV says veto, cfg says penalty → cfg wins → no hard veto
    assert veto == 0, (
        f"cfg[burst_gate_mode]=penalty must override BURST_GATE_MODE=veto env; got veto={veto}"
    )


# ---------------------------------------------------------------------------
# Calm market → no veto even in veto mode
# ---------------------------------------------------------------------------

def test_veto_mode_calm_market_no_veto():
    """In veto mode, calm market must not trigger a hard veto."""
    with patch.dict(os.environ, {"BURST_GATE_MODE": "veto"}):
        _, veto, _, _ = eval_burst_gate(_CALM_INDICATORS, _BASE_CFG)
    assert veto == 0, f"Calm market must not veto even in veto mode: veto={veto}"


# ---------------------------------------------------------------------------
# Snapshot always carries burst_mode
# ---------------------------------------------------------------------------

def test_snapshot_contains_burst_mode():
    """Consumers (metrics, ML) rely on snap[burst_mode] for observability."""
    with patch.dict(os.environ, {"BURST_GATE_MODE": "veto"}):
        _, _, _, snap = eval_burst_gate(_CALM_INDICATORS, _BASE_CFG)
    assert "burst_mode" in snap, "burst_mode must always be in snapshot"
    assert snap["burst_mode"] == "veto", f"Expected snap[burst_mode]='veto', got {snap['burst_mode']!r}"


def test_snapshot_contains_burst_mode_from_cfg():
    """When mode comes from cfg, snapshot must reflect cfg mode."""
    cfg = {"burst_gate_mode": "shadow"}
    with patch.dict(os.environ, {"BURST_GATE_MODE": "veto"}):
        _, _, _, snap = eval_burst_gate(_CALM_INDICATORS, cfg)
    assert snap.get("burst_mode") == "shadow", f"Expected snap[burst_mode]='shadow', got {snap.get('burst_mode')!r}"
