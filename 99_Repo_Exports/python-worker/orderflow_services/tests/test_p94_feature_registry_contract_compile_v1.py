from __future__ import annotations

import py_compile
from pathlib import Path


def test_feature_registry_contract_check_compiles() -> None:
    """Contract check tool compiles without syntax errors."""
    p = Path(__file__).resolve().parents[1] / "feature_registry_contract_check_v1.py"
    py_compile.compile(str(p), doraise=True)


def test_feature_registry_contract_exporter_compiles() -> None:
    """Prometheus exporter compiles without syntax errors."""
    p = Path(__file__).resolve().parents[1] / "feature_registry_contract_exporter_v1.py"
    py_compile.compile(str(p), doraise=True)
