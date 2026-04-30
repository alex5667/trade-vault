from __future__ import annotations

"""Wrapper around `promtool check rules` for the repo rules bundle.

Why this exists
- `promtool check rules` validates syntax/semantics, but needs an explicit file list.
- We want a single, deterministic include-list (manifest) so CI/timers and local
  runs validate the *same* set of rule files.

Exit codes
- 0 OK
- 2 failed

ENV
- REPO_ROOT (optional)
- PROM_RULES_BUNDLE_MANIFEST (optional, relative to repo root or absolute)
"""

import argparse
import os
import shutil
import subprocess
from pathlib import Path

from orderflow_services.rules_bundle_discovery_v1 import discover_rules_bundle


def _get_repo_root(arg_root: str | None) -> Path:
    if arg_root:
        return Path(arg_root).resolve()
    env_root = (os.getenv("REPO_ROOT") or "").strip()
    if env_root:
        return Path(env_root).resolve()
    if Path("/app").exists():
        return Path("/app").resolve()
    # file: <repo>/orderflow_services/promtool_check_rules_wrapper_v1.py
    return Path(__file__).resolve().parents[1]


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--root", default=None, help="Repo root (default: auto)")
    p.add_argument(
        "--manifest"
        default=None
        help="Manifest path (relative to repo root or absolute). Default: env PROM_RULES_BUNDLE_MANIFEST or v2 manifest."
    )
    args = p.parse_args(argv)

    repo_root = _get_repo_root(args.root)
    promtool = shutil.which("promtool")
    if not promtool:
        print("FAIL: promtool not found in PATH")
        return 2

    disc = discover_rules_bundle(repo_root=repo_root, manifest_ref=args.manifest)
    files = disc.files
    if not files:
        mp = str(disc.manifest_path) if disc.manifest_path else "<none>"
        print(f"FAIL: no rule files discovered (manifest={mp})")
        return 2

    errors: list[str] = []
    for path in files:
        proc = subprocess.run(
            [promtool, "check", "rules", str(path)]
            capture_output=True
            text=True
        )
        if proc.returncode != 0:
            out = ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()
            if len(out) > 1200:
                out = out[:1200] + "…"
            errors.append(f"{path}: {out}")

    if not errors:
        mp = str(disc.manifest_path) if disc.manifest_path else "<none>"
        print(f"OK: promtool validated {len(files)} rule files (manifest={mp})")
        return 0

    print(f"FAIL: promtool errors={len(errors)} files={len(files)}")
    for e in errors[:15]:
        print("- " + e)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
