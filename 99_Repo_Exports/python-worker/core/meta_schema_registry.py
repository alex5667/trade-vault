from __future__ import annotations

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

# Registry: name -> (version, cols, hash)
META_SCHEMA_REGISTRY: dict[str, tuple[int, list[str], str]] = {
    META_FEAT_V1_NAME: (META_FEAT_V1_VERSION, META_FEAT_V1_COLS, META_FEAT_V1_HASH),
    META_FEAT_V2_NAME: (META_FEAT_V2_VERSION, META_FEAT_V2_COLS, META_FEAT_V2_HASH),
    META_FEAT_V3_NAME: (META_FEAT_V3_VERSION, META_FEAT_V3_COLS, META_FEAT_V3_HASH),
    META_FEAT_V4_NAME: (META_FEAT_V4_VERSION, META_FEAT_V4_COLS, META_FEAT_V4_HASH),
    META_FEAT_V5_NAME: (META_FEAT_V5_VERSION, META_FEAT_V5_COLS, META_FEAT_V5_HASH),
    META_FEAT_V6_NAME: (META_FEAT_V6_VERSION, META_FEAT_V6_COLS, META_FEAT_V6_HASH),
    META_FEAT_V7_NAME: (META_FEAT_V7_VERSION, META_FEAT_V7_COLS, META_FEAT_V7_HASH),
    META_FEAT_V8_NAME: (META_FEAT_V8_VERSION, META_FEAT_V8_COLS, META_FEAT_V8_HASH),
    META_FEAT_V9_NAME: (META_FEAT_V9_VERSION, META_FEAT_V9_COLS, META_FEAT_V9_HASH),
    META_FEAT_V10_NAME: (META_FEAT_V10_VERSION, META_FEAT_V10_COLS, META_FEAT_V10_HASH),
}

# Builder map: name -> builder callable returning (feat_dict, missing_list)
META_SCHEMA_BUILDERS: dict[str, Callable[..., tuple[dict[str, float], list[str]]]] = {
    META_FEAT_V1_NAME: build_meta_features_v1,
    META_FEAT_V2_NAME: build_meta_features_v2,
    META_FEAT_V3_NAME: build_meta_features_v3,
    META_FEAT_V4_NAME: build_meta_features_v4,
    META_FEAT_V5_NAME: build_meta_features_v5,
    META_FEAT_V6_NAME: build_meta_features_v6,
    META_FEAT_V7_NAME: build_meta_features_v7,
    META_FEAT_V8_NAME: build_meta_features_v8,
    META_FEAT_V9_NAME: build_meta_features_v9,
    META_FEAT_V10_NAME: build_meta_features_v10,
}

# Transforms map: name -> per-feature transform spec used by MetaModelLR robust scaling.
# Exposed centrally so training tools don't re-import per-schema constants.
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
}


def get_schema_info(name: str) -> tuple[int, list[str], str]:
    """Return (version, cols, hash) for a given schema name, or raises KeyError."""
    if name not in META_SCHEMA_REGISTRY:
        raise KeyError(f"Unknown meta schema: {name}")
    return META_SCHEMA_REGISTRY[name]


def get_schema_cols(name: str) -> list[str]:
    """Return cols list for a given schema name; empty list if unknown."""
    entry = META_SCHEMA_REGISTRY.get(name)
    if entry is None:
        return []
    return list(entry[1])


def get_schema_builder(name: str) -> Callable[..., tuple[dict[str, Any], list[str]]] | None:
    """Return builder callable for a schema, or None if unknown.

    Allows training tools to use the production builder for a given schema —
    keeping Train==Serve parity.
    """
    return META_SCHEMA_BUILDERS.get(name)


def get_schema_transforms(name: str) -> dict[str, Any]:
    """Return per-feature transform spec for a schema, or empty dict if unknown.

    Used by training tools to wire MetaModelLR with the same robust scaling
    spec as the serving builder.
    """
    return dict(META_SCHEMA_TRANSFORMS.get(name, {}))
