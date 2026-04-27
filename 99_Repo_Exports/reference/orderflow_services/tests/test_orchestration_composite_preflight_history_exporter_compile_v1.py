from __future__ import annotations

import py_compile


def test_compile_history_exporters() -> None:
    """Smoke test: both exporter files must compile cleanly (catches import-level syntax errors)."""
    py_compile.compile('orderflow_services/orchestration_composite_preflight_history_exporter_v1.py', doraise=True)
    py_compile.compile('tick_flow_full/orderflow_services/orchestration_composite_preflight_history_exporter_v1.py', doraise=True)
