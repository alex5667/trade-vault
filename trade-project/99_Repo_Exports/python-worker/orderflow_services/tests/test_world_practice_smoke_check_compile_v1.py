from __future__ import annotations

import py_compile
from pathlib import Path


def test_world_practice_smoke_check_compiles() -> None:
    """Smoke-check tool compiles without syntax errors."""
    p = Path(__file__).resolve().parents[1] / "world_practice_gauges_smoke_check_v1.py"
    py_compile.compile(str(p), doraise=True)
