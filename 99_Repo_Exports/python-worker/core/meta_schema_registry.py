from __future__ import annotations

from typing import Dict, List, Tuple

from core.meta_features_v1 import (
    META_FEAT_V1_NAME,
    META_FEAT_V1_VERSION,
    META_FEAT_V1_COLS,
    META_FEAT_V1_HASH,
)
from core.meta_features_v2 import (
    META_FEAT_V2_NAME,
    META_FEAT_V2_VERSION,
    META_FEAT_V2_COLS,
    META_FEAT_V2_HASH,
)
from core.meta_features_v3 import (
    META_FEAT_V3_NAME,
    META_FEAT_V3_VERSION,
    META_FEAT_V3_COLS,
    META_FEAT_V3_HASH,
)
from core.meta_features_v4 import (
    META_FEAT_V4_NAME,
    META_FEAT_V4_VERSION,
    META_FEAT_V4_COLS,
    META_FEAT_V4_HASH,
)
from core.meta_features_v5 import (
    META_FEAT_V5_NAME,
    META_FEAT_V5_VERSION,
    META_FEAT_V5_COLS,
    META_FEAT_V5_HASH,
)
from core.meta_features_v6 import (
    META_FEAT_V6_NAME,
    META_FEAT_V6_VERSION,
    META_FEAT_V6_COLS,
    META_FEAT_V6_HASH,
)
from core.meta_features_v7 import (
    META_FEAT_V7_NAME,
    META_FEAT_V7_VERSION,
    META_FEAT_V7_COLS,
    META_FEAT_V7_HASH,
)
from core.meta_features_v8 import (
    META_FEAT_V8_NAME,
    META_FEAT_V8_VERSION,
    META_FEAT_V8_COLS,
    META_FEAT_V8_HASH,
)
from core.meta_features_v9 import (
    META_FEAT_V9_NAME,
    META_FEAT_V9_VERSION,
    META_FEAT_V9_COLS,
    META_FEAT_V9_HASH,
)

# Registry: name -> (version, cols, hash)
META_SCHEMA_REGISTRY: Dict[str, Tuple[int, List[str], str]] = {
    META_FEAT_V1_NAME: (META_FEAT_V1_VERSION, META_FEAT_V1_COLS, META_FEAT_V1_HASH),
    META_FEAT_V2_NAME: (META_FEAT_V2_VERSION, META_FEAT_V2_COLS, META_FEAT_V2_HASH),
    META_FEAT_V3_NAME: (META_FEAT_V3_VERSION, META_FEAT_V3_COLS, META_FEAT_V3_HASH),
    META_FEAT_V4_NAME: (META_FEAT_V4_VERSION, META_FEAT_V4_COLS, META_FEAT_V4_HASH),
    META_FEAT_V5_NAME: (META_FEAT_V5_VERSION, META_FEAT_V5_COLS, META_FEAT_V5_HASH),
    META_FEAT_V6_NAME: (META_FEAT_V6_VERSION, META_FEAT_V6_COLS, META_FEAT_V6_HASH),
    META_FEAT_V7_NAME: (META_FEAT_V7_VERSION, META_FEAT_V7_COLS, META_FEAT_V7_HASH),
    META_FEAT_V8_NAME: (META_FEAT_V8_VERSION, META_FEAT_V8_COLS, META_FEAT_V8_HASH),
    META_FEAT_V9_NAME: (META_FEAT_V9_VERSION, META_FEAT_V9_COLS, META_FEAT_V9_HASH),
}

def get_schema_info(name: str) -> Tuple[int, List[str], str]:
    """Return (version, cols, hash) for a given schema name, or raises KeyError."""
    if name not in META_SCHEMA_REGISTRY:
        raise KeyError(f"Unknown meta schema: {name}")
    return META_SCHEMA_REGISTRY[name]
