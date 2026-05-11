from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / 'quarantine_denylist.py'
SPEC = importlib.util.spec_from_file_location('quarantine_denylist', SCRIPT)
mod = importlib.util.module_from_spec(SPEC)  # type: ignore
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = mod
SPEC.loader.exec_module(mod)


def test_extract_sid_candidates_dedupes_known_fields():
    assert mod.extract_sid_candidates({'sid': 'a', 'execution_sid': 'a', 'parent_sid': 'b'}) == ['a', 'b']


def test_check_signal_against_quarantine_cache_blocks_match():
    decision = mod.check_signal_against_quarantine_cache({'sid': 'sid-1'}, {'sid-1', 'sid-2'})
    assert decision.allowed is False
    assert decision.matched_sid == 'sid-1'
