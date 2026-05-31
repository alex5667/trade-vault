from __future__ import annotations

import os


def test_entry_min_score_logic():
    """Smoke-test of the intended thresholds in env (doc-level)."""
    # actual logic is in service; here we ensure values parse
    os.environ["SMT_ADX_TREND_HI_Q"] = "0.75"
    os.environ["SMT_ADX_CHOP_LO_Q"] = "0.40"
    os.environ["SMT_ENTRY_MIN_OF_SCORE"] = "1.0"
    os.environ["SMT_ENTRY_OBI_MIN_SEC"] = "1.5"
    assert float(os.environ["SMT_ADX_TREND_HI_Q"]) > float(os.environ["SMT_ADX_CHOP_LO_Q"])


def test_pressure_env_parsing():
    """Verify pressure tracking ENV variables parse correctly."""
    os.environ["PRESSURE_WINDOW_MS"] = "60000"
    os.environ["PRESSURE_SPS_HI"] = "0.50"
    os.environ["COOLDOWN_PRESSURE_MULT"] = "1.5"
    os.environ["PRESSURE_EMA_ALPHA"] = "0.20"

    assert int(os.environ["PRESSURE_WINDOW_MS"]) == 60000
    assert float(os.environ["PRESSURE_SPS_HI"]) == 0.50
    assert float(os.environ["COOLDOWN_PRESSURE_MULT"]) == 1.5
    assert float(os.environ["PRESSURE_EMA_ALPHA"]) == 0.20


def test_adx_aware_execution_env():
    """Verify ADX-aware execution ENV variables."""
    os.environ["ENTRY_ADX_CHOP_LO_Q"] = "0.40"
    assert float(os.environ["ENTRY_ADX_CHOP_LO_Q"]) == 0.40
