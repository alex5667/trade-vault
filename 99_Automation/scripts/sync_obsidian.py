from __future__ import annotations

import fnmatch
import hashlib
import os
import shutil
from pathlib import Path

import yaml


def sha1_file(path: Path) -> str:
    h = hashlib.sha1()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def load_cfg(cfg_path: Path) -> dict:
    if not cfg_path.exists():
        return {}
    data = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        return {}
    return data


def matches_any(rel_path: str, patterns: list[str]) -> bool:
    rel_path = rel_path.replace("\\", "/")
    for p in patterns:
        if fnmatch.fnmatch(rel_path, p):
            return True
    return False


def should_copy(rel_path: str, include: list[str], exclude: list[str]) -> bool:
    rel_path = rel_path.replace("\\", "/")

    hard_skip_parts = [
        "/.git/",
        "/node_modules/",
        "/__pycache__/",
        "/.pytest_cache/",
        "/dist/",
        "/build/",
        "/.next/",
        "/coverage/",
        "/venv/",
        "/.venv/",
    ]
    probe = f"/{rel_path}/"
    if any(x in probe for x in hard_skip_parts):
        return False

    if exclude and matches_any(rel_path, exclude):
        return False

    if not include:
        return True

    return matches_any(rel_path, include)


def main() -> None:
    script_path = Path(__file__).resolve()
    vault_root = script_path.parents[2]
    automation_root = vault_root / "99_Automation"
    cfg_path = automation_root / "config" / "export_manifest.yaml"

    cfg = load_cfg(cfg_path)

    repo_root = Path(
        os.getenv("TRADE_REPO_ROOT", cfg.get("repo_root", "/home/alex/front/trade/scanner_infra"))
    ).expanduser().resolve()

    export_root = vault_root / "99_Repo_Exports"
    export_root.mkdir(parents=True, exist_ok=True)

    include = cfg.get("include", []) or []
    exclude = cfg.get("exclude", []) or []

    copied = 0
    skipped = 0

    for src in repo_root.rglob("*"):
        if not src.is_file():
            continue

        rel = src.relative_to(repo_root).as_posix()

        if not should_copy(rel, include, exclude):
            skipped += 1
            continue

        dst = export_root / rel
        dst.parent.mkdir(parents=True, exist_ok=True)

        if dst.exists():
            try:
                if src.stat().st_size == dst.stat().st_size and sha1_file(src) == sha1_file(dst):
                    skipped += 1
                    continue
            except Exception:
                pass

        shutil.copy2(src, dst)
        copied += 1

    print(f"done: copied={copied} skipped={skipped} export_root={export_root}")


if __name__ == "__main__":
    main()
