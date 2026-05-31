from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Dict, Tuple

try:
    import yaml
except Exception:
    yaml = None

ALLOWED_TYPES = {
    "index",
    "service",
    "stream",
    "dto",
    "runbook",
    "incident",
    "rollout",
    "document",
    "dashboard",
    "view",
    "context_pack",
    "architecture",
    "metrics",
    "research_index",
    "research_register",
    "template_like",
    "decision_log",
    "adr",
    "template",
}

SKIP_DIRS = {
    ".git",
    ".obsidian",
    ".trash",
    ".pytest_cache",
    "__pycache__",
    "node_modules",
    "99_Repo_Exports",
}

FRONTMATTER_RE = re.compile(r"^---\n(.*?)\n---\n?", re.DOTALL)
H1_RE = re.compile(r"^#\s+.+$", re.MULTILINE)


def should_skip(path: Path, include_exports: bool) -> bool:
    parts = set(path.parts)
    if not include_exports and "99_Repo_Exports" in parts:
        return True
    return any(p in parts for p in SKIP_DIRS if include_exports or p != "99_Repo_Exports")


def parse_frontmatter(text: str) -> Tuple[Dict, str]:
    m = FRONTMATTER_RE.match(text)
    if not m:
        return {}, text
    raw = m.group(1)
    body = text[m.end():]
    if yaml is None:
        return {}, body
    try:
        data = yaml.safe_load(raw) or {}
        if not isinstance(data, dict):
            return {}, body
        return data, body
    except Exception:
        return {"__invalid_yaml__": True}, body


def lint_file(path: Path) -> list[str]:
    errs: list[str] = []
    text = path.read_text(encoding="utf-8", errors="replace")
    fm, body = parse_frontmatter(text)

    if not fm:
        errs.append(f"{path}: missing frontmatter")
        return errs

    if fm.get("__invalid_yaml__"):
        errs.append(f"{path}: invalid frontmatter yaml")
        return errs

    note_type = fm.get("type")
    if not note_type:
        errs.append(f"{path}: missing field `type`")
    elif note_type not in ALLOWED_TYPES:
        errs.append(f"{path}: invalid type `{note_type}`")

    tags = fm.get("tags")
    if tags is None:
        errs.append(f"{path}: missing field `tags`")

    if path.name.lower().endswith(".md") and not H1_RE.search(body):
        errs.append(f"{path}: missing H1 title")

    return errs


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("vault", type=str, help="Path to vault root")
    ap.add_argument("--include-exports", action="store_true", help="Also lint 99_Repo_Exports")
    args = ap.parse_args()

    root = Path(args.vault).expanduser().resolve()
    if not root.exists():
        print(f"vault not found: {root}")
        return 2

    errors: list[str] = []
    checked = 0

    for path in root.rglob("*.md"):
        if should_skip(path, args.include_exports):
            continue
        checked += 1
        errors.extend(lint_file(path))

    if errors:
        print("lint failed:")
        for e in errors:
            print(f" - {e}")
        return 1

    mode = "including exports" if args.include_exports else "vault notes only"
    print(f"lint ok: checked {checked} notes under {root} ({mode})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
