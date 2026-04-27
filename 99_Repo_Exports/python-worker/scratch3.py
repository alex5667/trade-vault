import re
import os
import sys

def parse_go_keys() -> dict[str, str]:
    go_file = os.path.join(
        os.path.dirname(__file__), "..",
        "go-worker", "internal", "streams", "keys.go",
    )
    if not os.path.exists(go_file):
        import pytest
        pytest.skip(f"Go file not found: {go_file}", allow_module_level=True)
    content = open(go_file).read()
    pattern = re.compile(r'^\s*(?:const\s+)?([A-Za-z0-9_]+)\s*=\s*"([^"]+)"', re.MULTILINE)
    return dict(pattern.findall(content))

def python_key_map() -> dict[str, str]:
    import dataclasses
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))
    from core.redis_keys import RedisStreams, RedisKeyPrefixes
    
    mapping = {}
    for obj in [RedisStreams, RedisKeyPrefixes]:
        for f in dataclasses.fields(obj):
            val = getattr(obj, f.name)
            if isinstance(val, str):
                mapping[f.name] = val
    return mapping

_GO_KEYS = parse_go_keys()
_PY_KEYS = python_key_map()
print(f"Parsed len go={len(_GO_KEYS)} py={len(_PY_KEYS)}")
