from __future__ import annotations
"""Wrapper: enforce tick-gate auto-apply block before running a command.

Usage:
  python -m tools.run_auto_apply_with_tick_gate_v2 -- <apply command...>

Exit codes:
  0  command succeeded
  10 command failed
  20 blocked by tick gate
  22 error (wrapper)
"""


import os
import subprocess
import sys

from services.orderflow.auto_apply_guard import assert_auto_apply_not_blocked


def main(argv: list[str]) -> int:
    if "--" not in argv:
        sys.stderr.write("Usage: python -m tools.run_auto_apply_with_tick_gate_v2 -- <cmd...>\n")
        return 22
    idx = argv.index("--")
    cmd = argv[idx + 1 :]
    if not cmd:
        sys.stderr.write("Empty command after --\n")
        return 22

    # Will exit(20) if blocked
    assert_auto_apply_not_blocked()

    env = os.environ.copy()
    try:
        p = subprocess.run(cmd, env=env)
        return 0 if p.returncode == 0 else 10
    except SystemExit as e:
        return int(getattr(e, "code", 22) or 22)
    except Exception as e:
        sys.stderr.write(f"wrapper_error: {e}\n")
        return 22


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
