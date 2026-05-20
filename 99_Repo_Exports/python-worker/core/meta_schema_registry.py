from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from core.meta_features_v1 import (
    META_FEAT_V1_COLS,
    META_FEAT_V1_HASH,
    META_FEAT_V1_NAME,
    META_FEAT_V1_TRANSFORMS,
    META_FEAT_V1_VERSION,
    build_meta_features_v1,
)
from core.meta_features_v2 import (
    META_FEAT_V2_COLS,
    META_FEAT_V2_HASH,
    META_FEAT_V2_NAME,
    META_FEAT_V2_TRANSFORMS,
    META_FEAT_V2_VERSION,
    build_meta_features_v2,
)
from core.meta_features_v3 import (
    META_FEAT_V3_COLS,
    META_FEAT_V3_HASH,
    META_FEAT_V3_NAME,
    META_FEAT_V3_TRANSFORMS,
    META_FEAT_V3_VERSION,
    build_meta_features_v3,
)
from core.meta_features_v4 import (
    META_FEAT_V4_COLS,
    META_FEAT_V4_HASH,
    META_FEAT_V4_NAME,
    META_FEAT_V4_TRANSFORMS,
    META_FEAT_V4_VERSION,
    build_meta_features_v4,
)
from core.meta_features_v5 import (
    META_FEAT_V5_COLS,
    META_FEAT_V5_HASH,
    META_FEAT_V5_NAME,
    META_FEAT_V5_TRANSFORMS,
    META_FEAT_V5_VERSION,
    build_meta_features_v5,
)
from core.meta_features_v6 import (
    META_FEAT_V6_COLS,
    META_FEAT_V6_HASH,
    META_FEAT_V6_NAME,
    META_FEAT_V6_TRANSFORMS,
    META_FEAT_V6_VERSION,
    build_meta_features_v6,
)
from core.meta_features_v7 import (
    META_FEAT_V7_COLS,
    META_FEAT_V7_HASH,
    META_FEAT_V7_NAME,
    META_FEAT_V7_TRANSFORMS,
    META_FEAT_V7_VERSION,
    build_meta_features_v7,
)
from core.meta_features_v8 import (
    META_FEAT_V8_COLS,
    META_FEAT_V8_HASH,
    META_FEAT_V8_NAME,
    META_FEAT_V8_TRANSFORMS,
    META_FEAT_V8_VERSION,
    build_meta_features_v8,
)
from core.meta_features_v9 import (
    META_FEAT_V9_COLS,
    META_FEAT_V9_HASH,
    META_FEAT_V9_NAME,
    META_FEAT_V9_TRANSFORMS,
    META_FEAT_V9_VERSION,
    build_meta_features_v9,
)
from core.meta_features_v10 import (
    META_FEAT_V10_COLS,
    META_FEAT_V10_HASH,
    META_FEAT_V10_NAME,
    META_FEAT_V10_TRANSFORMS,
    META_FEAT_V10_VERSION,
    build_meta_features_v10,
)
from core.meta_features_v13_of import (
    META_FEAT_V13_OF_COLS,
    META_FEAT_V13_OF_HASH,
    META_FEAT_V13_OF_NAME,
    META_FEAT_V13_OF_TRANSFORMS,
    META_FEAT_V13_OF_VERSION,
    build_meta_features_v13_of,
)
from core.meta_features_v14_of import (
    META_FEAT_V14_OF_COLS,
    META_FEAT_V14_OF_HASH,
    META_FEAT_V14_OF_NAME,
    META_FEAT_V14_OF_TRANSFORMS,
    META_FEAT_V14_OF_VERSION,
    build_meta_features_v14_of,
)
from core.meta_features_v15_of import (
    META_FEAT_V15_OF_COLS,
    META_FEAT_V15_OF_HASH,
    META_FEAT_V15_OF_NAME,
    META_FEAT_V15_OF_TRANSFORMS,
    META_FEAT_V15_OF_VERSION,
    build_meta_features_v15_of,
)


@dataclass(frozen=True)
class MetaSchemaSpec:
    """Canonical description of one meta-feature schema.

    Frozen so version/hash/cols cannot be accidentally mutated after
    module load.  The `builder` callable is the *same object* that
    OFConfirmEngine uses at serving time, enforcing Train==Serve parity.

    Note: `transforms` is NOT included in this spec — it is mutable (dict)
    and therefore stored separately in META_SCHEMA_TRANSFORMS for backward
    compatibility with training tools that pre-date this dataclass.
    """

    name: str
    version: int
    cols: tuple[str, ...]
    hash: str
    builder: Callable[..., tuple[dict[str, float], list[str]]]


META_SCHEMA_REGISTRY: dict[str, MetaSchemaSpec] = {
    META_FEAT_V1_NAME: MetaSchemaSpec(
        name=META_FEAT_V1_NAME,
        version=META_FEAT_V1_VERSION,
        cols=tuple(META_FEAT_V1_COLS),
        hash=META_FEAT_V1_HASH,
        builder=build_meta_features_v1,
    ),
    META_FEAT_V2_NAME: MetaSchemaSpec(
        name=META_FEAT_V2_NAME,
        version=META_FEAT_V2_VERSION,
        cols=tuple(META_FEAT_V2_COLS),
        hash=META_FEAT_V2_HASH,
        builder=build_meta_features_v2,
    ),
    META_FEAT_V3_NAME: MetaSchemaSpec(
        name=META_FEAT_V3_NAME,
        version=META_FEAT_V3_VERSION,
        cols=tuple(META_FEAT_V3_COLS),
        hash=META_FEAT_V3_HASH,
        builder=build_meta_features_v3,
    ),
    META_FEAT_V4_NAME: MetaSchemaSpec(
        name=META_FEAT_V4_NAME,
        version=META_FEAT_V4_VERSION,
        cols=tuple(META_FEAT_V4_COLS),
        hash=META_FEAT_V4_HASH,
        builder=build_meta_features_v4,
    ),
    META_FEAT_V5_NAME: MetaSchemaSpec(
        name=META_FEAT_V5_NAME,
        version=META_FEAT_V5_VERSION,
        cols=tuple(META_FEAT_V5_COLS),
        hash=META_FEAT_V5_HASH,
        builder=build_meta_features_v5,
    ),
    META_FEAT_V6_NAME: MetaSchemaSpec(
        name=META_FEAT_V6_NAME,
        version=META_FEAT_V6_VERSION,
        cols=tuple(META_FEAT_V6_COLS),
        hash=META_FEAT_V6_HASH,
        builder=build_meta_features_v6,
    ),
    META_FEAT_V7_NAME: MetaSchemaSpec(
        name=META_FEAT_V7_NAME,
        version=META_FEAT_V7_VERSION,
        cols=tuple(META_FEAT_V7_COLS),
        hash=META_FEAT_V7_HASH,
        builder=build_meta_features_v7,
    ),
    META_FEAT_V8_NAME: MetaSchemaSpec(
        name=META_FEAT_V8_NAME,
        version=META_FEAT_V8_VERSION,
        cols=tuple(META_FEAT_V8_COLS),
        hash=META_FEAT_V8_HASH,
        builder=build_meta_features_v8,
    ),
    META_FEAT_V9_NAME: MetaSchemaSpec(
        name=META_FEAT_V9_NAME,
        version=META_FEAT_V9_VERSION,
        cols=tuple(META_FEAT_V9_COLS),
        hash=META_FEAT_V9_HASH,
        builder=build_meta_features_v9,
    ),
    META_FEAT_V10_NAME: MetaSchemaSpec(
        name=META_FEAT_V10_NAME,
        version=META_FEAT_V10_VERSION,
        cols=tuple(META_FEAT_V10_COLS),
        hash=META_FEAT_V10_HASH,
        builder=build_meta_features_v10,
    ),
    META_FEAT_V13_OF_NAME: MetaSchemaSpec(
        name=META_FEAT_V13_OF_NAME,
        version=META_FEAT_V13_OF_VERSION,
        cols=tuple(META_FEAT_V13_OF_COLS),
        hash=META_FEAT_V13_OF_HASH,
        builder=build_meta_features_v13_of,
    ),
    META_FEAT_V14_OF_NAME: MetaSchemaSpec(
        name=META_FEAT_V14_OF_NAME,
        version=META_FEAT_V14_OF_VERSION,
        cols=tuple(META_FEAT_V14_OF_COLS),
        hash=META_FEAT_V14_OF_HASH,
        builder=build_meta_features_v14_of,
    ),
    META_FEAT_V15_OF_NAME: MetaSchemaSpec(
        name=META_FEAT_V15_OF_NAME,
        version=META_FEAT_V15_OF_VERSION,
        cols=tuple(META_FEAT_V15_OF_COLS),
        hash=META_FEAT_V15_OF_HASH,
        builder=build_meta_features_v15_of,
    ),
}

# Backward-compat flat lookups (derive from registry to stay in sync).
META_SCHEMA_BUILDERS: dict[str, Callable[..., tuple[dict[str, float], list[str]]]] = {
    name: spec.builder for name, spec in META_SCHEMA_REGISTRY.items()
}

META_SCHEMA_TRANSFORMS: dict[str, dict[str, Any]] = {
    META_FEAT_V1_NAME: dict(META_FEAT_V1_TRANSFORMS),
    META_FEAT_V2_NAME: dict(META_FEAT_V2_TRANSFORMS),
    META_FEAT_V3_NAME: dict(META_FEAT_V3_TRANSFORMS),
    META_FEAT_V4_NAME: dict(META_FEAT_V4_TRANSFORMS),
    META_FEAT_V5_NAME: dict(META_FEAT_V5_TRANSFORMS),
    META_FEAT_V6_NAME: dict(META_FEAT_V6_TRANSFORMS),
    META_FEAT_V7_NAME: dict(META_FEAT_V7_TRANSFORMS),
    META_FEAT_V8_NAME: dict(META_FEAT_V8_TRANSFORMS),
    META_FEAT_V9_NAME: dict(META_FEAT_V9_TRANSFORMS),
    META_FEAT_V10_NAME: dict(META_FEAT_V10_TRANSFORMS),
    META_FEAT_V13_OF_NAME: dict(META_FEAT_V13_OF_TRANSFORMS),
    META_FEAT_V14_OF_NAME: dict(META_FEAT_V14_OF_TRANSFORMS),
    META_FEAT_V15_OF_NAME: dict(META_FEAT_V15_OF_TRANSFORMS),
}


def get_meta_schema_spec(name: str) -> MetaSchemaSpec:
    """Return the canonical MetaSchemaSpec for *name*, or raise ValueError."""
    try:
        return META_SCHEMA_REGISTRY[name]
    except KeyError:
        raise ValueError(f"unknown_meta_schema:{name}")


def get_schema_info(name: str) -> tuple[int, list[str], str]:
    """Return (version, cols, hash) for *name*; raises KeyError if unknown."""
    spec = META_SCHEMA_REGISTRY.get(name)
    if spec is None:
        raise KeyError(f"Unknown meta schema: {name}")
    return spec.version, list(spec.cols), spec.hash


def get_schema_cols(name: str) -> list[str]:
    """Return cols list for *name*; empty list if unknown."""
    spec = META_SCHEMA_REGISTRY.get(name)
    return list(spec.cols) if spec else []


def get_schema_builder(name: str) -> Callable[..., tuple[dict[str, Any], list[str]]] | None:
    """Return the production builder for *name*, or None if unknown.

    Training tools must call this to obtain the builder — never import
    a builder directly — so Train==Serve parity is guaranteed.
    """
    spec = META_SCHEMA_REGISTRY.get(name)
    return spec.builder if spec else None


def get_schema_transforms(name: str) -> dict[str, Any]:
    """Return per-feature transform spec for *name*, or empty dict."""
    return dict(META_SCHEMA_TRANSFORMS.get(name, {}))
