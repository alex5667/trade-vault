from __future__ import annotations

import py_compile
from pathlib import Path


def test_feature_drift_modules_compile() -> None:
    root = Path(__file__).resolve().parents[1]
    for rel in [
        'nightly/feature_drift_psi.py'
        'nightly/feature_drift_ks.py'
        'nightly/feature_drift_report_v1.py'
    ]:
        py_compile.compile(str(root / rel), doraise=True)


def test_bundle_and_autogen_compile_after_p3_changes() -> None:
    repo = Path(__file__).resolve().parents[2]
    for rel in [
        'ml_analysis/tools/nightly_feature_selection_loop_bundle_v1.py'
        'ml_analysis/tools/autogen_feature_denylist_proposal_v1.py'
    ]:
        py_compile.compile(str(repo / rel), doraise=True)
