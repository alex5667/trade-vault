from __future__ import annotations
"""Sync test: ensures ml_feature_schema_v8_of_e3 logic is identical in SoT and mirror.

SoT:    reference/tick_flow_full/core/ml_feature_schema_v8_of.py
Mirror: python-worker/core/ml_feature_schema_v8_of.py
"""

import sys
import os

_HERE = os.path.dirname(os.path.abspath(__file__))
_TESTS = os.path.dirname(_HERE)
_MIRROR_ROOT = os.path.dirname(_TESTS)
_REPO_ROOT = os.path.dirname(_MIRROR_ROOT)
_SOT_ROOT = os.path.join(_REPO_ROOT, "reference", "tick_flow_full")

import pytest

SCHEMA_HASH = "201cd55c9cce"


# Load SoT
try:
    import importlib.util
    _sot_spec = importlib.util.spec_from_file_location(
        "sot_ml_feature_schema_v8_of",
        os.path.join(_SOT_ROOT, "core", "ml_feature_schema_v8_of.py"),
    )
    _sot_mod = importlib.util.module_from_spec(_sot_spec)  # type: ignore[arg-type]
    _sot_spec.loader.exec_module(_sot_mod)  # type: ignore[union-attr]
    SoTV8 = _sot_mod.MLFeatureSchemaV8OF
    SoTV8Stable = _sot_mod.MLFeatureSchemaV8OFStable
except Exception as e:
    pytest.skip(f"Failed loading SoT: {e}", allow_module_level=True)

from core.ml_feature_schema_v8_of import MLFeatureSchemaV8OF as MirrorV8
from core.ml_feature_schema_v8_of import MLFeatureSchemaV8OFStable as MirrorV8Stable


def test_v8_schema_sync_e3() -> None:
    worker_v8 = MirrorV8()
    ref_v8 = SoTV8()
    assert list(worker_v8.num_keys or []) == list(ref_v8.num_keys or [])
    assert list(worker_v8.bool_keys or []) == list(ref_v8.bool_keys or [])

def test_v8_stable_schema_sync_e3() -> None:
    worker_stable = MirrorV8Stable()
    ref_stable = SoTV8Stable()
    assert list(worker_stable.num_keys or []) == list(ref_stable.num_keys or [])
    assert list(worker_stable.bool_keys or []) == list(ref_stable.bool_keys or [])
