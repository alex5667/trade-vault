"""
Standalone self-test for feature_registry that doesn't need pytest or python-worker on path.
Run from repo root: python3 verify_feature_registry.py
"""
import sys
import os

# Only add tick_flow_full to sys.path — avoid loading the entire python-worker
REPO = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(REPO, "python-worker"))

from core.feature_registry import get_schema_info, get_edge_stack_feature_spec, get_schema

ERRORS = []
def check(name, cond, msg=""):
    if not cond:
        ERRORS.append(f"FAIL [{name}]: {msg}")
    else:
        print(f"  PASS {name}")

# 1. All versions valid
for ver in ("v2", "v3", "v4_of"):
    i = get_schema_info(ver)
    check(f"ver-{ver}", i.ver == ver, f"ver mismatch: {i.ver!r}")
    check(f"n_features-{ver}", len(i.feature_names) >= 30, f"only {len(i.feature_names)}")
    check(f"no_colon-{ver}", all(":" not in c for c in i.column_names))
    check(f"hash_len-{ver}", len(i.schema_hash) == 64)

# 2. Hash stability
for ver in ("v2", "v3", "v4_of"):
    h1, h2 = get_schema_info(ver).schema_hash, get_schema_info(ver).schema_hash
    check(f"hash_stable-{ver}", h1 == h2)

# 3. Standard blocks
for ver in ("v2", "v3", "v4_of"):
    names = set(get_schema_info(ver).feature_names)
    for n in ("dir:LONG","dir:SHORT","bucket:trend","bucket:range","bucket:other",
              "hour:0","hour:23","dow:0","dow:6"):
        check(f"block-{n}@{ver}", n in names, f"missing {n}")

# 4. v4_of count >= 100
n4 = len(get_schema_info("v4_of").feature_names)
check("v4_of_count", n4 >= 100, f"only {n4}")
print(f"  v4_of n_features={n4}, hash={get_schema_info('v4_of').schema_hash[:16]}")

# 5. Edge stack spec
spec = get_edge_stack_feature_spec("v4_of")
check("edge_ver", spec.ver == "v4_of")
check("edge_cols>=50", len(spec.feature_cols) >= 50, f"only {len(spec.feature_cols)}")
check("edge_hash_len", len(spec.feature_cols_hash) == 64)
check("edge_buy", "direction_BUY" in spec.feature_cols)
check("edge_bucket", "bucket:trend" in spec.feature_cols)
check("edge_hour", "hour:0" in spec.feature_cols)
f_cols = [c for c in spec.feature_cols if c.startswith("f_")]
check("edge_f_cols>=30", len(f_cols) >= 30, f"only {len(f_cols)}")
print(f"  edge v4_of cols={len(spec.feature_cols)} f_*={len(f_cols)} hash={spec.feature_cols_hash[:16]}")

# 6. Alias
check("alias_get_schema", get_schema("v3").schema_hash == get_schema_info("v3").schema_hash)

# 7. Unknown version raises ValueError
try:
    get_schema_info("vBad")
    check("unknown_ver_raises", False, "should have raised ValueError")
except ValueError:
    check("unknown_ver_raises", True)

# 8. Subset assertions
v2, v3, v4 = (set(get_schema_info(v).feature_names) for v in ("v2","v3","v4_of"))
missing_23 = v2 - v3
missing_34 = v3 - v4
check("v2_subset_v3", not missing_23, f"missing: {missing_23}")
check("v3_subset_v4", not missing_34, f"missing: {missing_34}")

# 9. to_dict
d = get_schema_info("v3").to_dict()
check("to_dict_keys", all(k in d for k in ("ver","schema_hash","feature_names","column_names","n_features")))
sd = get_edge_stack_feature_spec("v3").to_dict()
check("edge_to_dict_keys", all(k in sd for k in ("ver","feature_cols_hash","feature_cols","n_cols")))

if ERRORS:
    print("\n=== FAILURES ===")
    for e in ERRORS:
        print(e)
    sys.exit(1)
else:
    print(f"\n✓ ALL {29 + len(('v2','v3','v4_of'))*9} assertions passed")
