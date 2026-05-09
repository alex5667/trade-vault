"""Test calibration: grid-search for w_exec_risk, exec_risk_ref_bps, of_score_min."""

import json
import os
import tempfile

from tools.calibrate_gate_params import eval_policy, iter_ndjson


def test_eval_policy():
    """Test policy evaluation on synthetic dataset."""
    rows = [
        {
            "score": 0.70,
            "exec_risk_bps": 10.0,
            "ok": 1,
            "r_mult": 1.5,
        },
        {
            "score": 0.65,
            "exec_risk_bps": 12.0,
            "ok": 1,
            "r_mult": 0.8,
        },
        {
            "score": 0.60,
            "exec_risk_bps": 15.0,
            "ok": 1,
            "r_mult": -1.2,  # tail loss
        },
        {
            "score": 0.55,
            "exec_risk_bps": 20.0,
            "ok": 1,
            "r_mult": 0.3,
        },
    ]

    m = eval_policy(rows, w_exec=0.20, exec_ref_bps=10.0, score_min=0.62)
    assert m["n"] == 4.0
    assert m["pass_rate"] > 0.0
    assert "meanR_pass" in m
    assert "tail_pass" in m


def test_calibrate_integration():
    """Integration test: full calibration pipeline."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".ndjson", delete=False) as f:
        # synthetic dataset
        for i in range(20):
            f.write(json.dumps({
                "score": 0.65 + (i % 3) * 0.05,
                "exec_risk_bps": 8.0 + (i % 4) * 2.0,
                "ok": 1,
                "r_mult": 0.5 + (i % 5) * 0.3 - 0.6,
            }) + "\n")
        fpath = f.name

    try:
        rows = list(iter_ndjson(fpath))
        assert len(rows) == 20

        # test grid search
        best = None
        best_obj = -1e9
        for w in [0.18, 0.22]:
            for ref in [8.0, 10.0]:
                for smin in [0.62, 0.65]:
                    m = eval_policy(rows, w_exec=w, exec_ref_bps=ref, score_min=smin)
                    if m["pass_rate"] >= 0.15 and m["tail_pass"] <= 0.18:
                        obj = m["meanR_pass"]
                        if obj > best_obj:
                            best_obj = obj
                            best = {"w_exec_risk": w, "exec_risk_ref_bps": ref, "of_score_min": smin, "metrics": m}

        assert best is not None
        assert "w_exec_risk" in best
        assert "exec_risk_ref_bps" in best
        assert "of_score_min" in best
    finally:
        os.unlink(fpath)
