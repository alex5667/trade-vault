"""Test bench_of_confirm_engine.py runs and returns JSON (optional, not in CI)."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from tools.bench_of_confirm_engine import pctl


def test_pctl():
    """Test percentile calculation."""
    xs = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert pctl(xs, 0.5) == 3.0
    assert pctl(xs, 0.95) == 5.0
    assert pctl(xs, 0.99) == 5.0
    assert pctl([], 0.5) == 0.0

