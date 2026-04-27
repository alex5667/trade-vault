#!/usr/bin/env python3
"""
sre_monitor_all.py

One entrypoint for SRE checks:
- tools/ml_sre_monitor.py (existing)
- tools/tb_sre_monitor_v2.py (from P4.1)
- tools/ml_confirm_stream_sre_monitor.py (new)

This avoids modifying existing scripts.
"""
from __future__ import annotations
import os
import subprocess
import sys

def _run(cmd: list[str]) -> int:
    try:
        p = subprocess.run(cmd, check=False)
        return int(p.returncode)
    except FileNotFoundError:
        return 2

def main() -> int:
    py = sys.executable
    base = os.path.dirname(__file__)

    rc1 = _run([py, os.path.join(base, "ml_sre_monitor.py")])
    rc2 = _run([py, os.path.join(base, "tb_sre_monitor_v2.py")])
    rc3 = _run([py, os.path.join(base, "ml_confirm_stream_sre_monitor.py")])

    # return worst
    return 2 if (rc1 or rc2 or rc3) else 0

if __name__ == "__main__":
    raise SystemExit(main())
