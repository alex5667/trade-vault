"""P1: Train==Serve builder parity.

After the registry unification, training tools must obtain the meta-feature
builder *by name* from the central registry — not via a hardcoded import that
can silently drift from what OFConfirmEngine uses at serving time.

Locks two invariants:
  (1) For every schema in the central registry, get_schema_builder(name) is
      *the exact same callable object* used by OFConfirmEngine at serving.
  (2) The SCHEMAS map inside train_meta_model_lr_v4 is derived from the
      central registry — every name in the central registry appears with the
      same builder object.
"""
import core.of_confirm_engine as ofe
from core.meta_schema_registry import (
    META_SCHEMA_BUILDERS,
    META_SCHEMA_REGISTRY,
    get_schema_builder,
)


def test_serving_engine_imports_match_central_builders():
    """OFConfirmEngine imports each build_meta_features_vN symbol at module
    load (see import block in of_confirm_engine.py:314-373). The function
    objects exposed on the engine module must be *the same object* as the
    ones in the central registry — otherwise training picks a different
    code path than serving.
    """
    by_attr = {
        "meta_feat_v1":  "build_meta_features_v1",
        "meta_feat_v2":  "build_meta_features_v2",
        "meta_feat_v3":  "build_meta_features_v3",
        "meta_feat_v4":  "build_meta_features_v4",
        "meta_feat_v5":  "build_meta_features_v5",
        "meta_feat_v6":  "build_meta_features_v6",
        "meta_feat_v7":  "build_meta_features_v7",
        "meta_feat_v8":  "build_meta_features_v8",
        "meta_feat_v9":  "build_meta_features_v9",
        "meta_feat_v10": "build_meta_features_v10",
    }
    for name, attr in by_attr.items():
        engine_fn = getattr(ofe, attr)
        registry_fn = get_schema_builder(name)
        assert engine_fn is registry_fn, (
            f"Train==Serve drift for {name}: "
            f"engine has {engine_fn}, registry has {registry_fn}"
        )


def test_trainer_schemas_derived_from_central_registry():
    """train_meta_model_lr_v4.SCHEMAS must be derived from the central
    registry. After the refactor, every registry schema must appear in
    the trainer with the same builder object — no manual subset."""
    from tools.train_meta_model_lr_v4 import SCHEMAS

    assert set(SCHEMAS.keys()) == set(META_SCHEMA_REGISTRY.keys()), (
        f"trainer SCHEMAS keys diverge from registry: "
        f"trainer={sorted(SCHEMAS.keys())}, registry={sorted(META_SCHEMA_REGISTRY.keys())}"
    )
    for name in META_SCHEMA_REGISTRY:
        trainer_builder = SCHEMAS[name]["builder"]
        registry_builder = META_SCHEMA_BUILDERS[name]
        assert trainer_builder is registry_builder, (
            f"trainer builder for {name} drifted from registry"
        )


def test_trainer_can_build_features_for_every_registered_schema():
    """End-to-end: trainer's build_row_features must produce all expected
    cols for every schema. Catches regressions where a new schema is added
    to the registry without a matching trainer branch."""
    from tools.train_meta_model_lr_v4 import SCHEMAS, build_row_features

    sparse_row = {
        "have": 1,
        "need": 2,
        "rule_score": 0.5,
        "scenario_v4": "reversal",
        "scenario_base": "reversal",
    }
    for name, cfg in SCHEMAS.items():
        feat = build_row_features(sparse_row, cfg["builder"])
        missing = set(cfg["cols"]) - set(feat.keys())
        assert not missing, f"{name}: trainer missing cols {sorted(missing)[:5]}"
