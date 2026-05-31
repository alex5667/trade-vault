"""test_v15_of_count_pin.py — literal-count pins for v15_of schema and registry.

Guards against:
  1. Docstring/compose drift: comments saying "v15_of (515 keys)" while the
     module invariant is `_EXPECTED_KEYS = 531`.
  2. Registry path: `get_edge_stack_feature_spec("v15_of").feature_cols` must
     reflect at least the schema key count — catches accidental dropouts
     in `get_edge_stack_feature_spec` (denylist over-removal, etc.).
  3. Trainer wire: `train_edge_stack_v1_oof.py` must expose `--feature_schema_ver`
     and stamp `feature_schema_ver` into the model pack so the runtime
     vectorizer and shape-guard have ground truth.

Run:
    python -m pytest tests/test_v15_of_count_pin.py -v
"""
from __future__ import annotations

import pathlib

import pytest


# ── 1. Literal key count — direct doc-drift guard ─────────────────────────────

def test_v15_of_count():
    """V15_OF_NUMERIC_KEYS must equal exactly 531 keys.

    If a key is intentionally added/removed, bump `_EXPECTED_KEYS` in
    `core/ml_feature_schema_v15_of.py` AND update this pin together. The two
    layers must agree so docstring drift (e.g. compose comment "515 keys")
    cannot survive code review.
    """
    from core.ml_feature_schema_v15_of import V15_OF_NUMERIC_KEYS, _EXPECTED_KEYS
    assert _EXPECTED_KEYS == 531, (
        f"_EXPECTED_KEYS={_EXPECTED_KEYS}, pinned literal=531. "
        "If you bumped _EXPECTED_KEYS, update this pin and audit all docstrings "
        "and compose comments that reference the old number."
    )
    assert len(V15_OF_NUMERIC_KEYS) == 531, (
        f"len(V15_OF_NUMERIC_KEYS)={len(V15_OF_NUMERIC_KEYS)}, expected 531."
    )


# ── 2. Registry spec parity ──────────────────────────────────────────────────

def test_feature_registry_v15_of_spec():
    """get_edge_stack_feature_spec('v15_of') must surface ≥531 feature_cols.

    Catches:
      - registry routing regression (fallback to v14_of)
      - over-aggressive denylist dropout
      - missing one-hot / derived feature expansion paths
    """
    from core.feature_registry import get_edge_stack_feature_spec, get_schema_info

    info = get_schema_info("v15_of")
    assert info.ver == "v15_of"

    spec = get_edge_stack_feature_spec("v15_of")
    assert spec.ver == "v15_of"
    assert len(spec.feature_cols) >= 531, (
        f"get_edge_stack_feature_spec('v15_of').feature_cols has "
        f"{len(spec.feature_cols)} cols, expected ≥531. "
        "Registry routing or denylist may have regressed."
    )


def test_feature_registry_v15_alias_matches_v15_of():
    """'v15' must alias to 'v15_of' (same numeric col count)."""
    from core.feature_registry import get_edge_stack_feature_spec

    a = get_edge_stack_feature_spec("v15_of")
    b = get_edge_stack_feature_spec("v15")
    assert len(a.feature_cols) == len(b.feature_cols)


# ── 3. Trainer wire — --feature_schema_ver flag exists and pack carries it ──

def _trainer_src() -> str:
    src = pathlib.Path(__file__).parent.parent / "tools" / "train_edge_stack_v1_oof.py"
    if not src.exists():
        pytest.skip("train_edge_stack_v1_oof.py not found")
    return src.read_text(encoding="utf-8")


def test_trainer_exposes_feature_schema_ver_flag():
    text = _trainer_src()
    assert '"--feature_schema_ver"' in text, (
        "train_edge_stack_v1_oof.py must expose --feature_schema_ver so the "
        "registry feature_cols path can be selected without hand-rolling JSON."
    )


def test_trainer_pack_stamps_feature_schema_ver():
    text = _trainer_src()
    assert '"feature_schema_ver"' in text, (
        "train_edge_stack_v1_oof.py model pack must include 'feature_schema_ver' "
        "so the runtime gate and shape-guard have ground truth."
    )


def test_trainer_fail_fast_on_conflicting_flags():
    """--feature_schema_ver and --feature_cols_json must be mutually exclusive."""
    text = _trainer_src()
    assert "mutually exclusive" in text, (
        "train_edge_stack_v1_oof.py must reject simultaneous "
        "--feature_schema_ver and --feature_cols_json (two conflicting truths)."
    )


def test_trainer_honors_require_feature_cols_json_env():
    """REQUIRE_FEATURE_COLS_JSON=1 must disable the legacy default 13-col list."""
    text = _trainer_src()
    assert "REQUIRE_FEATURE_COLS_JSON" in text, (
        "train_edge_stack_v1_oof.py must honor REQUIRE_FEATURE_COLS_JSON=1 "
        "env to forbid the legacy default 13-col list in production training."
    )
