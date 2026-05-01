from __future__ import annotations
"""Run a command only if auto-apply is NOT blocked (by tick gate).

This is a tiny helper wrapper for existing ApplyRunner / auto-apply jobs.
It checks Redis key:
  cfg:suggestions:entry_policy:auto_apply_block:tick_gate

If present => exits with code 20 (blocked) and prints reason meta (if any).
Else => execs the provided command.

Usage:
  python -m tools.run_auto_apply_with_tick_gate -- <your apply cmd>
"""


import json
import os
import subprocess
import sys
from typing import List, Optional

try:
    import redis  # type: ignore
except Exception:
    redis = None  # type: ignore


def _getenv_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return default if v is None else str(v)


def _key(prefix: str, suffix: str) -> str:
    return f"{prefix}:{suffix}"


def main(argv: Optional[List[str]] = None) -> int:
    argv = argv or sys.argv[1:]
    if "--" not in argv:
        print("usage: python -m tools.run_auto_apply_with_tick_gate -- <cmd>", file=sys.stderr)
        return 2
    idx = argv.index("--")
    cmd = argv[idx + 1:]
    if not cmd:
        print("missing command after --", file=sys.stderr)
        return 2

    redis_url = _getenv_str("REDIS_URL", "redis://localhost:6379/0")
    prefix = _getenv_str("AUTO_APPLY_BLOCK_PREFIX", "cfg:suggestions:entry_policy:auto_apply_block").strip()
    block_key = _key(prefix, "tick_gate")
    meta_key = _key(prefix, "tick_gate:meta")

    if redis is None:
        # If we can't check Redis, fail-open (run the command).
        p = subprocess.run(cmd)
        return int(p.returncode)

    try:
        r = redis.Redis.from_url(redis_url, decode_responses=True)
        v = r.get(block_key)
        if v:
            meta = r.get(meta_key) or ""
            try:
                meta_obj = json.loads(meta) if meta else {}
            except Exception:
                meta_obj = {"raw": meta[:2000]}
            print(json.dumps({"blocked": True, "key": block_key, "meta": meta_obj}, ensure_ascii=False, sort_keys=True))
            return 20
    except Exception:
        # Redis down => fail-open
        pass

    p = subprocess.run(cmd)
    return int(p.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
