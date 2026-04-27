import re
import os
import sys

def parse_go_keys() -> dict[str, str]:
    go_file = os.path.join(
        os.path.dirname(__file__), "..",
        "go-worker", "internal", "streams", "keys.go",
    )
    if not os.path.exists(go_file):
        print(f"Go file not found: {go_file}")
        sys.exit(1)
    content = open(go_file).read()
    pattern = re.compile(r'^\s*(?:const\s+)?([A-Za-z0-9_]+)\s*=\s*"([^"]+)"', re.MULTILINE)
    return dict(pattern.findall(content))

def py_key_map() -> dict[str, str]:
    import dataclasses
    
    # We must load from core.redis_keys 
    # Add path to sys.path
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))
    from core.redis_keys import RedisStreams, RedisKeyPrefixes
    
    mapping = {}
    for obj in [RedisStreams, RedisKeyPrefixes]:
        for f in dataclasses.fields(obj):
            val = getattr(obj, f.name)
            if isinstance(val, str):
                mapping[f.name] = val
    return mapping

go_keys = parse_go_keys()
py_keys = py_key_map()

# Normalize
go_vals = set(v.replace("%s", "{symbol}") for v in go_keys.values())
py_vals = set(v.replace("{sym}", "{symbol}") for v in py_keys.values())

in_go_not_in_py = go_vals - py_vals
in_py_not_in_go = py_vals - go_vals

print("=== In Go but not in Python ===")
for v in in_go_not_in_py:
    print(v)

print("\n=== In Python but not in Go ===")
for v in in_py_not_in_go:
    print(v)

