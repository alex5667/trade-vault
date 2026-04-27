#!/usr/bin/env python3
"""
Smoke test: OFConfirmEngine schema registry wiring includes meta_feat_v5 and builders are callable.

This is intentionally lightweight (no Redis, no runtime dependencies).
"""
import sys

# Add parent directory to path (matches other test scripts in this repo)
sys.path.insert(0, "/app")

from core.of_confirm_engine import META_SCHEMA_REGISTRY, META_SCHEMA_V2P

from core.meta_features_v1 import (
    META_FEAT_V1_NAME,
    META_FEAT_V1_VERSION,
    META_FEAT_V1_HASH,
    build_meta_features_v1,
)
from core.meta_features_v2 import (
    META_FEAT_V2_NAME,
    META_FEAT_V2_VERSION,
    META_FEAT_V2_HASH,
    build_meta_features_v2,
)
from core.meta_features_v3 import (
    META_FEAT_V3_NAME,
    META_FEAT_V3_VERSION,
    META_FEAT_V3_HASH,
    build_meta_features_v3,
)
from core.meta_features_v4 import (
    META_FEAT_V4_NAME,
    META_FEAT_V4_VERSION,
    META_FEAT_V4_HASH,
    build_meta_features_v4,
)
from core.meta_features_v5 import (
    META_FEAT_V5_NAME,
    META_FEAT_V5_VERSION,
    META_FEAT_V5_HASH,
    build_meta_features_v5,
)
from core.meta_features_v6 import (
    META_FEAT_V6_NAME,
    META_FEAT_V6_VERSION,
    META_FEAT_V6_HASH,
    build_meta_features_v6,
)
from core.meta_features_v7 import (
    META_FEAT_V7_NAME,
    META_FEAT_V7_VERSION,
    META_FEAT_V7_HASH,
    build_meta_features_v7,
)


def _smoke(builder, needs_runtime: bool) -> None:
    evidence = {}
    indicators = {}
    base_kwargs = dict(
        evidence=evidence,
        indicators=indicators,
        indicators_with_v4={},
        legs={},
        have=1,
        need=1,
        ok_soft=0,
        rule_score=0.0,
        exec_risk_norm=0.0,
        exec_risk_bps=0.0,
        ml_scenario="test",
    )
    if needs_runtime:
        base_kwargs["runtime_snap"] = None
        base_kwargs["runtime_prev_snap"] = None

    feat, missing = builder(**base_kwargs)
    assert isinstance(feat, dict)
    assert isinstance(missing, list)


def main() -> None:
    # Registry must include all known schemas (including v5..v7)
    assert META_SCHEMA_REGISTRY[META_FEAT_V1_NAME] == (META_FEAT_V1_VERSION, META_FEAT_V1_HASH)
    assert META_SCHEMA_REGISTRY[META_FEAT_V2_NAME] == (META_FEAT_V2_VERSION, META_FEAT_V2_HASH)
    assert META_SCHEMA_REGISTRY[META_FEAT_V3_NAME] == (META_FEAT_V3_VERSION, META_FEAT_V3_HASH)
    assert META_SCHEMA_REGISTRY[META_FEAT_V4_NAME] == (META_FEAT_V4_VERSION, META_FEAT_V4_HASH)
    assert META_SCHEMA_REGISTRY[META_FEAT_V5_NAME] == (META_FEAT_V5_VERSION, META_FEAT_V5_HASH)
    assert META_SCHEMA_REGISTRY[META_FEAT_V6_NAME] == (META_FEAT_V6_VERSION, META_FEAT_V6_HASH)
    assert META_SCHEMA_REGISTRY[META_FEAT_V7_NAME] == (META_FEAT_V7_VERSION, META_FEAT_V7_HASH)

    # v2+ tuple used for microstructure pre-injection must include v5..v7
    assert META_FEAT_V2_NAME in META_SCHEMA_V2P
    assert META_FEAT_V3_NAME in META_SCHEMA_V2P
    assert META_FEAT_V4_NAME in META_SCHEMA_V2P
    assert META_FEAT_V5_NAME in META_SCHEMA_V2P
    assert META_FEAT_V6_NAME in META_SCHEMA_V2P
    assert META_FEAT_V7_NAME in META_SCHEMA_V2P

    # Builders callable (smoke)
    _smoke(build_meta_features_v1, needs_runtime=False)
    _smoke(build_meta_features_v2, needs_runtime=True)
    _smoke(build_meta_features_v3, needs_runtime=True)
    _smoke(build_meta_features_v4, needs_runtime=True)
    _smoke(build_meta_features_v5, needs_runtime=True)
    _smoke(build_meta_features_v6, needs_runtime=True)
    _smoke(build_meta_features_v7, needs_runtime=True)

    print("OK: meta schema registry + builders (v1..v7)")


if __name__ == "__main__":
    main()
