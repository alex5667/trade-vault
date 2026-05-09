from __future__ import annotations

import json
from pathlib import Path

from orderflow_services.calibration_extended_exporter_v1 import Exporter, _delta_value, _metric_value


def test_metric_extractors_read_expected_fields():
    obj = {
        "arms": {
            "active": {"mce_cal": 0.11, "sharpness_mean": 0.09},
            "delta": {"mce_cal": 0.02, "sharpness_mean": -0.03},
        }
    }
    assert _metric_value(obj, "active", "mce_cal") == 0.11
    assert _delta_value(obj, "mce_cal") == 0.02


def test_exporter_step_reads_proof_and_status(tmp_path: Path, monkeypatch):
    proof = {
        "degrade_review": True,
        "arms": {
            "active": {"mce_cal": 0.11, "ece_cal": 0.04, "brier_cal": 0.20, "calibration_slope": 0.8, "calibration_intercept": 0.05, "sharpness_mean": 0.08, "sharpness_entropy": 0.92, "prob_mass_near_half": 0.55, "precision_top5p": 0.6},
            "champion": {"mce_cal": 0.08, "ece_cal": 0.03, "brier_cal": 0.19, "calibration_slope": 0.9, "calibration_intercept": 0.03, "sharpness_mean": 0.10, "sharpness_entropy": 0.88, "prob_mass_near_half": 0.50, "precision_top5p": 0.61},
            "challenger": {"mce_cal": 0.11, "ece_cal": 0.04, "brier_cal": 0.20, "calibration_slope": 0.8, "calibration_intercept": 0.05, "sharpness_mean": 0.08, "sharpness_entropy": 0.92, "prob_mass_near_half": 0.55, "precision_top5p": 0.6},
            "delta": {"mce_cal": 0.03, "sharpness_mean": -0.02, "ece_cal": 0.01, "brier_cal": 0.01, "precision_top5p": -0.01, "prob_mass_near_half": 0.05},
        },
    }
    status = {"promoted": False, "degrade_review": True}
    proof_path = tmp_path / "proof.json"
    status_path = tmp_path / "status.json"
    proof_path.write_text(json.dumps(proof), encoding="utf-8")
    status_path.write_text(json.dumps(status), encoding="utf-8")
    monkeypatch.setenv("CONF_CAL_PROOF_STATE_PATH", proof_path)
    monkeypatch.setenv("CONF_CAL_PROMOTION_STATUS_PATH", str(status_path))
    exp = Exporter()
    exp.step()  # no exception
