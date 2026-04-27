#!/usr/bin/env python3
import sys
import os

# Add parent dir to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from core.meta_features_v1 import META_FEAT_V1_NAME, META_FEAT_V1_VERSION, META_FEAT_V1_HASH
    from core.meta_features_v2 import META_FEAT_V2_NAME, META_FEAT_V2_VERSION, META_FEAT_V2_HASH
    from core.meta_features_v3 import META_FEAT_V3_NAME, META_FEAT_V3_VERSION, META_FEAT_V3_HASH
    from core.meta_features_v4 import META_FEAT_V4_NAME, META_FEAT_V4_VERSION, META_FEAT_V4_HASH
    from core.meta_features_v5 import META_FEAT_V5_NAME, META_FEAT_V5_VERSION, META_FEAT_V5_HASH
    from core.meta_features_v6 import META_FEAT_V6_NAME, META_FEAT_V6_VERSION, META_FEAT_V6_HASH
    from core.of_confirm_engine import OFConfirmEngine
except ImportError as e:
    print(f"FAILED: Import error: {e}")
    sys.exit(1)

def test_registry():
    print("--- Verifying Meta Schema Registry Wiring ---")
    
    # We check if builders are callable by mocking a confirm() call or inspecting the registry
    # In P28, the registry is local to confirm(), but we can check if builders are imported 
    # and if the top-level META_SCHEMA_REGISTRY is updated.
    
    from core.of_confirm_engine import META_SCHEMA_REGISTRY
    
    expected = [
        (META_FEAT_V1_NAME, META_FEAT_V1_VERSION),
        (META_FEAT_V2_NAME, META_FEAT_V2_VERSION),
        (META_FEAT_V3_NAME, META_FEAT_V3_VERSION),
        (META_FEAT_V4_NAME, META_FEAT_V4_VERSION),
        (META_FEAT_V5_NAME, META_FEAT_V5_VERSION),
        (META_FEAT_V6_NAME, META_FEAT_V6_VERSION),
    ]
    
    ok = True
    for name, vers in expected:
        if name not in META_SCHEMA_REGISTRY:
            print(f"[FAIL] Schema {name} missing from META_SCHEMA_REGISTRY")
            ok = False
        else:
            reg_vers, reg_hash = META_SCHEMA_REGISTRY[name]
            if reg_vers != vers:
                print(f"[FAIL] Schema {name} version mismatch: expected {vers}, got {reg_vers}")
                ok = False
            else:
                print(f"[OK] Schema {name} (v{vers}) hash={reg_hash[:8]}...")

    # Check if builders are functional (smoke test)
    # We try to import them to ensure they exist
    builders = [
        "build_meta_features_v1",
        "build_meta_features_v2",
        "build_meta_features_v3",
        "build_meta_features_v4",
        "build_meta_features_v5",
        "build_meta_features_v6",
    ]
    
    import core.of_confirm_engine as engine
    for b in builders:
        if not hasattr(engine, b):
            print(f"[FAIL] Builder {b} not found in of_confirm_engine.py")
            ok = False
        else:
            print(f"[OK] Builder {b} is wired")

    if ok:
        print("--- REGISTRY WIRING OK ---")
    else:
        print("--- REGISTRY WIRING FAILED ---")
        sys.exit(1)

if __name__ == "__main__":
    test_registry()
