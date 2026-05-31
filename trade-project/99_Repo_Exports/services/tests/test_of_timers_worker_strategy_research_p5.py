import pytest
import os
import sys

# To allow import from the main directory without issues
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from services.of_timers_worker import run_strategy_research_guard_bundle

def test_run_strategy_research_guard_disabled(monkeypatch):
    monkeypatch.setenv("ENABLE_STRATEGY_RESEARCH_GUARD", "0")
    # Should just return True and not crash
    assert run_strategy_research_guard_bundle() is True

# Note: We won't test the fully enabled run because it uses subprocess (run_tool) 
# and requires the bundle file. The compilation and structure are the key tests here.
