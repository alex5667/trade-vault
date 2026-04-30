from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest


def test_patch_b_dataset_train_bundle_scripts_exist():
    root = Path(__file__).resolve().parents[2]
    if not (root / "ml_analysis/tools/build_ofc_contextual_dataset_v1.py").exists():
        pytest.skip("Patch B not applied")
    assert (root / "ml_analysis/tools/train_ofc_exec_cost_v1.py").exists()
    assert (root / "ml_analysis/tools/train_ofc_rule_success_v1.py").exists()
    assert (root / "ml_analysis/tools/build_ofc_contextual_bundle_v1.py").exists()


@pytest.mark.skipif(not (Path(__file__).resolve().parents[2] / "ml_analysis/tools/build_ofc_contextual_dataset_v1.py").exists(), reason="Patch B not applied")
def test_nightly_bundle_guard_smoke(tmp_path: Path):
    root = Path(__file__).resolve().parents[2]
    decisions = tmp_path / "decisions.jsonl"
    outcomes = tmp_path / "outcomes.jsonl"
    decisions.write_text(json.dumps({
        "sid": "s1"
        "symbol": "BTCUSDT"
        "direction": "long"
        "decision_ts_ms": 1700000000000
        "ctx_key": "symbol=BTCUSDT|session=eu"
        "ctx_session": "eu"
        "of_score_final": 0.63
        "ctx_exec_risk_ref_bps": 4.0
        "expected_slippage_bps": 1.0
        "spread_bps": 0.8
    }) + "\n", encoding="utf-8")
    outcomes.write_text(json.dumps({
        "sid": "s1"
        "symbol": "BTCUSDT"
        "direction": "long"
        "pnl_bps_net": 1.2
        "realized_slippage_bps": 0.9
        "fill_delay_ms": 150
    }) + "\n", encoding="utf-8")
    work_dir = tmp_path / "work"
    rc = subprocess.run(
        [
            sys.executable
            str(root / "orderflow_services/nightly_ofc_contextual_ops_bundle_v1.py")
            "--decisions_jsonl", str(decisions)
            "--outcomes_jsonl", str(outcomes)
            "--work_dir", str(work_dir)
            "--registry_dir", str(work_dir / "registry")
            "--bundle_out_dir", str(work_dir / "current")
        ]
        check=False
    ).returncode
    assert rc == 0
    assert (work_dir / "current" / "manifest.json").exists()
