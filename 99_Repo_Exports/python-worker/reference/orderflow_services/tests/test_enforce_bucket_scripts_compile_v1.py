# -*- coding: utf-8 -*-
"""Compile + YAML-syntax tests for P77/P90 enforce bucket scripts.

Tests:
  - py_compile: ensures all .py scripts in orderflow_services pass syntax check
  - yaml.safe_load: validates all P90 alert YAML files (new alert pack + updated promoter/rollback YAMLs)
"""

import py_compile
import yaml
from pathlib import Path


def test_scripts_compile():
    root = Path(__file__).resolve().parents[1]
    paths = [
        root / "enforce_bucket_promoter_rollback_controller_v1.py"
        root / "enforce_bucket_ops_validate_p78.py"
        root / "enforce_bucket_state_exporter_v1.py"
    ]
    for p in paths:
        py_compile.compile(str(p), doraise=True)


def test_alert_yamls_valid():
    """P90: All enforce-bucket alert YAML files must parse cleanly."""
    root = Path(__file__).resolve().parents[1]
    yaml_files = [
        root / "prometheus_alerts_enforce_bucket_state_exporter_p90.yml"
        root / "prometheus_alerts_enforce_bucket_promoter_v1.yml"
        root / "prometheus_alerts_enforce_bucket_promoter_rollback_v1.yml"
    ]
    for yf in yaml_files:
        assert yf.exists(), f"Alert YAML not found: {yf}"
        with open(yf, encoding="utf-8") as fh:
            doc = yaml.safe_load(fh)
        # Basic structural check: must have a 'groups' key with at least one rule group
        assert isinstance(doc, dict), f"YAML root is not a dict: {yf}"
        assert "groups" in doc, f"No 'groups' key in {yf}"
        assert len(doc["groups"]) > 0, f"Empty groups list in {yf}"
