"""Static regression test: liqmap_gate_v1 wiring markers in OFConfirmEngine.

We avoid importing OFConfirmEngine here because it has heavy optional deps
in some environments. This test ensures the wiring stays in place:
  - evaluate_liqmap_gate_v1 is imported (directly or via try/except)
  - evaluate_liqmap_gate_v1 is called in build()
  - stable indicator keys are exported for meta_feat_v9 / DecisionRecord
  - hard veto is enforced when liqmap_veto==1

If this test fails, Train==Serve parity for meta_feat_v9 degrades to zeros.

Run command (from repo root):
    PYTHONPATH=./python-worker pytest -q \\
        python-worker/tick_flow_full/tests/core/test_of_confirm_engine_liqmap_gate_wiring_v1.py
"""

from pathlib import Path

# Both SoT (python-worker/core/) and reference (reference/tick_flow_full/core/) are tested.
# Walk up from this file to find the scanner_infra root (identified by having 'reference' dir).
def _find_scanner_infra_root() -> Path:
    """Walk up from this test file until we find scanner_infra root.

    Uses docker-compose-timers.yml as an unambiguous marker (only present
    at the scanner_infra root, not in python-worker sub-tree).
    """
    p = Path(__file__).resolve()
    for _ in range(10):
        p = p.parent
        if (p / "docker-compose-timers.yml").is_file():
            return p
    raise RuntimeError(f"Cannot locate scanner_infra root from {__file__}")


_ROOT = _find_scanner_infra_root()
_SOT = _ROOT / "python-worker" / "core" / "of_confirm_engine.py"
_REF = _ROOT / "reference" / "tick_flow_full" / "core" / "of_confirm_engine.py"


# Stable indicator keys that meta_feat_v9 reads at train-time and serve-time.
_STABLE_KEYS = (
    "liqmap_gate_shadow_veto",
    "liqmap_gate_veto",
    "liqmap_gate_reason",
    "liqmap_gate_risk_bps",
    "liqmap_gate_reward_bps",
    "liqmap_gate_rr",
    "liqmap_gate_adverse_peak_usd",
    "liqmap_gate_favorable_peak_usd",
    # B3: back-compat alias used by decision_record_v1 + mode/window for analytics
    "liqmap_gate_veto_reason",
    "liqmap_gate_mode",
    "liqmap_gate_window",
)


def test_of_confirm_engine_sot_has_liqmap_gate_wiring_markers():
    """SoT: python-worker/core/of_confirm_engine.py must have liqmap gate wiring.

    Ensures the SoT engine imports and calls evaluate_liqmap_gate_v1,
    exports all stable keys, and enforces hard-veto in enforce mode.
    """
    src = _SOT
    assert src.exists(), f"SoT engine file not found: {src}"
    text = src.read_text(encoding="utf-8")

    # Import: may be direct or wrapped in try/except — both are acceptable.
    assert "liqmap_gate_v1" in text, (
        "SoT engine does not import liqmap_gate_v1 — gate is not wired."
    )
    assert "evaluate_liqmap_gate_v1" in text, (
        "SoT engine does not reference evaluate_liqmap_gate_v1 — gate call is missing."
    )

    # Stable exported keys used by meta_feat_v9 (Train==Serve guarantee)
    for k in _STABLE_KEYS:
        assert k in text, (
            f"SoT engine missing stable indicator key: '{k}' — "
            "meta_feat_v9 will receive zeros instead of real gate values."
        )

    # Hard-veto must exist in enforce mode
    assert "liqmap_gate" in text and "hard_veto" in text, (
        "SoT engine is missing the hard-veto enforcement for liqmap_gate."
    )

    # B2: Gate bit constant and usage must be present
    assert "GATE_BIT_LIQMAP" in text, (
        "SoT engine missing GATE_BIT_LIQMAP class constant — B2 patch not applied."
    )
    assert "self.GATE_BIT_LIQMAP" in text, (
        "SoT engine missing self.GATE_BIT_LIQMAP usage — gate bit is never set."
    )

    # B2: Enforce mode must explicitly set ok = 0
    assert "ok = 0" in text, (
        "SoT engine enforce block does not contain ok = 0 — trades not blocked on hard-veto."
    )


def test_of_confirm_engine_ref_has_liqmap_gate_wiring_markers():
    """Reference: reference/tick_flow_full/core/of_confirm_engine.py must have liqmap gate wiring.

    Regression guard: ensures B1 patch stays committed in the reference snapshot.
    Uses static text scan to avoid any import side-effects.
    """
    src = _REF
    assert src.exists(), f"Reference engine file not found: {src}"
    text = src.read_text(encoding="utf-8")

    # Import check
    assert "evaluate_liqmap_gate_v1" in text, (
        "Reference engine does not import evaluate_liqmap_gate_v1 — B1 patch was not applied."
    )
    assert "evaluate_liqmap_gate_v1(" in text, (
        "Reference engine imports but never calls evaluate_liqmap_gate_v1."
    )

    # Stable exported keys
    for k in _STABLE_KEYS:
        assert k in text, (
            f"Reference engine missing stable indicator key: '{k}' — "
            "B1 patch is incomplete."
        )

    # Enforce mode hard-veto
    assert 'hard_veto = "liqmap_gate"' in text, (
        "Reference engine missing hard-veto assignment — "
        "enforce mode will not block trades when liqmap_veto==1."
    )

    # GATE_BIT_LIQMAP must be declared and used
    assert "GATE_BIT_LIQMAP" in text, (
        "Reference engine missing GATE_BIT_LIQMAP class constant."
    )
    assert "self.GATE_BIT_LIQMAP" in text, (
        "Reference engine missing self.GATE_BIT_LIQMAP usage — gate bit is never set."
    )

    # B2: Enforce mode must explicitly set ok = 0
    assert "ok = 0" in text, (
        "Reference engine enforce block does not contain ok = 0 — B2 patch incomplete."
    )
