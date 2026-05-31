from __future__ import annotations

import ast
import re
import pytest


TIMER_PATH = 'services/of_timers_worker.py'


def _read_timer():
    with open(TIMER_PATH, 'r', encoding='utf-8') as f:
        return f.read()


def test_run_strategy_research_stats_bundle_defined():
    src = _read_timer()
    assert 'def run_strategy_research_stats_bundle()' in src


def test_calls_nightly_stats_bundle_module():
    src = _read_timer()
    assert 'nightly_strategy_research_stats_bundle_v1' in src


def test_enable_env_guard():
    src = _read_timer()
    assert 'ENABLE_STRATEGY_RESEARCH_STATS_BUNDLE' in src


def test_scheduled_at_0435():
    src = _read_timer()
    # Should appear as ("strategy_research_stats", 4, 35, run_strategy_research_stats_bundle)
    match = re.search(r'"strategy_research_stats".*?4.*?35.*?run_strategy_research_stats_bundle', src, re.DOTALL)
    assert match is not None, 'strategy_research_stats task not found at 04:35 in nightly tasks'


def test_gate_mode_passthrough():
    src = _read_timer()
    assert 'STRATEGY_RESEARCH_STATS_GATE_MODE' in src
