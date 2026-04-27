# -*- coding: utf-8 -*-
"""Compile-time test: verifies nightly_slippage_calibrator_v1 can be parsed by Python."""

import py_compile
from pathlib import Path


def test_slippage_calibrator_compiles():
    root = Path(__file__).resolve().parents[1]
    p = root / "tools" / "nightly_slippage_calibrator_v1.py"
    py_compile.compile(str(p), doraise=True)
