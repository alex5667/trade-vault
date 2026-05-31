import py_compile


def test_compile_main_and_mirror() -> None:
    py_compile.compile('orderflow_services/strategy_research_guard_state_exporter_v1.py', doraise=True)
    py_compile.compile('tick_flow_full/orderflow_services/strategy_research_guard_state_exporter_v1.py', doraise=True)
import importlib.util
import os

import pytest


def test_strategy_research_guard_exporter_compiles():
    """
    Ensures the exporter can load cleanly without syntax errors or missing top-level imports.
    """
    path = os.path.join(os.path.dirname(__file__), "..", "strategy_research_guard_exporter_v1.py")
    spec = importlib.util.spec_from_file_location("strategy_research_guard_exporter_v1", path)
    module = importlib.util.module_from_spec(spec)

    try:
        spec.loader.exec_module(module)
        compiled = True
    except Exception as e:
        pytest.fail(f"Could not load exporter module: {e}")

    assert compiled is True
