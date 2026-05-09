import os
import sys
from unittest.mock import MagicMock

# Add project root to path
sys.path.append(os.path.abspath("python-worker"))

from core.of_confirm_engine import OFConfirmEngine


def test_score_breakdown_export():
    engine = OFConfirmEngine()

    # Mock runtime and config
    runtime = MagicMock()
    runtime.liq_regime = "normal"
    cfg = {
        "dist_bp_threshold": 20.0,
        "exec_risk_ref_mult": 1.0,
        "w_exec_risk": 0.2,
        "w_ofi": 0.4,
        "w_fp_edge": 0.4
    }

    # Fake indicators
    indicators = {
        "spread_bps": 5.0,
        "slip_bps": 5.0,
        "ofi_score": 0.8,
        "fp_edge_score": 0.6,
        "liq_regime": "normal"
    }

    # Mocking _clamp01 as it is local in of_confirm_engine.py
    # We rely on the actual implementation since we import the file.

    evidence = {}

    # In of_confirm_engine.py: build(self, *, symbol, tf, direction, tick_ts_ms, price, delta_z, runtime, cfg, indicators, absorption=None)
    final_score, _ = engine.build(
        symbol="BTCUSDT",
        tf="1m",
        direction="buy",
        tick_ts_ms=0,
        price=50000.0,
        delta_z=0.0,
        runtime=runtime,
        cfg=cfg,
        indicators=indicators
    )

    print(f"Final Score: {final_score}")
    print(f"Indicators after build: {list(indicators.keys())}")

    # Check new indicators
    assert "exec_risk_norm" in indicators
    assert "exec_risk_ref_bps" in indicators
    assert "exec_pen" in indicators
    assert "score_breakdown" in indicators

    breakdown = indicators["score_breakdown"]
    print(f"Breakdown: {breakdown}")

    assert isinstance(breakdown, dict)
    assert "ofi" in breakdown["contrib"]
    assert "fp_edge" in breakdown["contrib"]
    assert "exec_risk_penalty" in breakdown["contrib"]

    # Check evidence enrichment within the returned object
    assert hasattr(final_score, "evidence")
    assert "score_breakdown" in final_score.evidence
    assert final_score.evidence["score_breakdown"] == breakdown

    print("Verification SUCCESS: score_breakdown correctly exported and formatted.")

if __name__ == "__main__":
    try:
        test_score_breakdown_export()
    except Exception as e:
        print(f"Verification FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
