import re
import os

go_file = "../go-worker/internal/streams/keys.go"
py_file = "core/redis_keys.py"

go_keys = {}
for line in open(go_file):
    m = re.match(r'^\s*(?:const\s+)?([A-Za-z0-9_]+)\s*=\s*"([^"]+)"', line)
    if m:
        go_keys[m.group(1)] = m.group(2)

py_keys = {}
for line in open(py_file):
    m = re.match(r'^\s*([A-Z0-9_]+):\s*str\s*=\s*"([^"]+)"', line)
    if m:
        py_keys[m.group(1)] = m.group(2)

# Build _GO_TO_PYTHON mappings based on VALUE match
new_mappings = {}
for g_k, g_v in go_keys.items():
    matched = False
    for p_k, p_v in py_keys.items():
        if g_v == p_v:
            new_mappings[g_k] = p_k
            matched = True
            break
    if not matched:
        print(f"Missing Python key for Go key: {g_k} ('{g_v}')")

print("---------------------------------")
for k, v in new_mappings.items():
    print(f'    "{k}": "{v}",')

