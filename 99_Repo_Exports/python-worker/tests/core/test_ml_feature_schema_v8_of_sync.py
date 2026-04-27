"""Sync test: ensures ml_feature_schema_v8_of is identical in SoT and mirror.

SoT:    reference/tick_flow_full/core/ml_feature_schema_v8_of.py
Mirror: python-worker/core/ml_feature_schema_v8_of.py

Run:
    cd python-worker
    PYTHONPATH=../tick_flow_full:. python -m pytest tests/core/test_ml_feature_schema_v8_of_sync.py -v
"""
from __future__ import annotations

import sys
import os

# ---------------------------------------------------------------------------
# PYTHONPATH setup — both SoT (reference/tick_flow_full) and mirror (python-worker)
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))  # .../python-worker/tests/core
_TESTS = os.path.dirname(_HERE)                      # .../python-worker/tests
_MIRROR_ROOT = os.path.dirname(_TESTS)               # .../python-worker
_REPO_ROOT = os.path.dirname(_MIRROR_ROOT)           # .../scanner_infra
# SoT core modules live under reference/tick_flow_full/core
_SOT_ROOT = os.path.join(_REPO_ROOT, "reference", "tick_flow_full")

import pytest

SCHEMA_HASH = "cb1562b12fef"


# ---------------------------------------------------------------------------
# Import from SoT (reference/tick_flow_full)
# ---------------------------------------------------------------------------
_SOT_PATHS = [_SOT_ROOT, _REPO_ROOT]
for _p in reversed(_SOT_PATHS):
    sys.path.insert(0, _p)

try:
    import importlib
    _sot_spec = importlib.util.spec_from_file_location(
        "sot_ml_feature_schema_v8_of",
        os.path.join(_SOT_ROOT, "core", "ml_feature_schema_v8_of.py"),
    )
    _sot_mod = importlib.util.module_from_spec(_sot_spec)  # type: ignore[arg-type]
    _sot_spec.loader.exec_module(_sot_mod)  # type: ignore[union-attr]
    SoTSchema = _sot_mod.MLFeatureSchemaV8OF
except Exception as e:
    pytest.skip(f"Не удалось загрузить SoT ml_feature_schema_v8_of: {e}", allow_module_level=True)

from core.ml_feature_schema_v8_of import MLFeatureSchemaV8OF as MirrorSchema


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_v8_sot_and_mirror_have_identical_num_keys():
    """SoT and mirror must produce identical num_keys lists."""
    sot = SoTSchema()
    mirror = MirrorSchema()
    assert sot.num_keys == mirror.num_keys, (
        f"num_keys diverge:\n  SoT  : {sot.num_keys}\n  mirror: {mirror.num_keys}"
    )


def test_v8_sot_and_mirror_have_identical_bool_keys():
    """SoT and mirror must produce identical bool_keys lists."""
    sot = SoTSchema()
    mirror = MirrorSchema()
    assert sot.bool_keys == mirror.bool_keys, (
        f"bool_keys diverge:\n  SoT  : {sot.bool_keys}\n  mirror: {mirror.bool_keys}"
    )
