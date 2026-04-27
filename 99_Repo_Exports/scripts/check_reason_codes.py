#!/usr/bin/env python3
"""
CI script to validate that all VETO_/SOFT_ reason codes used in codebase
are properly registered in _REASON_CODE_U16.

This prevents silent 0-u16 mappings in production.
"""

import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# 1) Import the registry (more reliable than parsing the file)
try:
    from signal_scoring.reason_registry import _REASON_CODE_U16
except Exception as e:
    print(f"Failed to import reason registry: {e}", file=sys.stderr)
    sys.exit(2)

known = set(_REASON_CODE_U16.keys())

# 2) grep all string literals with VETO_/SOFT_ (fallback if ripgrep not available)
try:
    rg = subprocess.run(
        ["rg", "-n", r'["\'](VETO_[A-Z0-9_]+|SOFT_[A-Z0-9_]+)["\']', str(REPO_ROOT)],
        capture_output=True,
        text=True,
    )
    if rg.returncode not in (0, 1):  # 1 = nothing found
        print(rg.stderr, file=sys.stderr)
        sys.exit(2)
    output = rg.stdout
except FileNotFoundError:
    # Fallback to grep
    rg = subprocess.run(
        ["grep", "-r", "-n", r'["\']\([A-Z]*_\)\?[A-Z_]*["\']', str(REPO_ROOT / "python-worker")],
        capture_output=True,
        text=True,
    )
    if rg.returncode not in (0, 1):  # 1 = nothing found
        print(rg.stderr, file=sys.stderr)
        sys.exit(2)
    output = rg.stdout

pat = re.compile(r'["\'](VETO_[A-Z0-9_]+|SOFT_[A-Z0-9_]+)["\']')
found: set[str] = set()

for line in output.splitlines():
    # Skip lines that look like file paths or comments
    if ':' not in line or line.startswith('#'):
        continue
    # Extract the content after the first colon (filename:line:content)
    content = ':'.join(line.split(':')[2:]) if line.count(':') >= 2 else line

    m = pat.search(content)
    if m:
        found.add(m.group(1))

unknown = sorted(found - known)

if unknown:
    print("Unknown reason codes used in codebase (not in _REASON_CODE_U16):")
    for x in unknown:
        print("  -", x)
    print("\nAdd missing codes to _REASON_CODE_U16 in signal_scoring/reason_registry.py")
    sys.exit(1)

print("OK: all VETO_/SOFT_ codes are registered.")
print(f"Found {len(found)} unique reason codes in codebase.")
