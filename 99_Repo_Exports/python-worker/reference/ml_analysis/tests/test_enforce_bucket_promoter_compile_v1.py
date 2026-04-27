# -*- coding: utf-8 -*-

import py_compile
from pathlib import Path


def test_promoter_compiles():
    root = Path(__file__).resolve().parents[1]
    p = root / "tools" / "nightly_enforce_bucket_promoter_v1.py"
    py_compile.compile(str(p), doraise=True)
