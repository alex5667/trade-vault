#!/usr/bin/env python3
from __future__ import annotations

"""
Fix unsafe side->sign patterns across python-worker.

Conservative tool: applies only high-confidence rewrites that remove hidden
BUY/SELL bias when side is missing.

Default mode prints unified diffs (dry-run). Use --write to apply.
"""

import argparse
import difflib
import re
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Change:
    file: str
    rule: str


RULES: list[tuple[str, re.Pattern, str]] = [
    (
        "call_arg_side_buy_else_sell",
        re.compile(r"side\s*=\s*1\s*if\s*\(str\(tick\.get\(['\"]side['\"]\)\)\.upper\(\)\s*==\s*['\"]BUY['\"]\)\s*else\s*-1\s*,"),
        "side=side_sign_from_tick(tick)[0],  # Returns (sign, reason), we need sign",
    ),
    (
        "call_arg_side_buy_else_sell_simple",
        re.compile(r"side\s*=\s*1\s*if\s*\(tick\.get\(['\"]side['\"]\)\s*==\s*['\"]BUY['\"]\)\s*else\s*-1\s*,"),
        "side=side_sign_from_tick(tick)[0],  # Returns (sign, reason), we need sign",
    ),
    (
        "call_arg_ibm_to_sign",
        re.compile(r"side\s*=\s*-1\s*if\s*\(?tick\.get\(['\"]is_buyer_maker['\"]\)\)?\s*else\s*1\s*,"),
        "side=side_sign_from_tick(tick)[0],  # Returns (sign, reason), we need sign",
    ),
    (
        "assign_side_sign_buy_else_sell",
        re.compile(r"(?m)^(?P<indent>\s*)(?P<lhs>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*1\s*if\s*\(str\(tick\.get\(['\"]side['\"]\)\)\.upper\(\)\s*==\s*['\"]BUY['\"]\)\s*else\s*-1\s*$"),
        r"\g<indent>\g<lhs> = side_sign_from_tick(tick)[0]  # Returns (sign, reason), we need sign",
    ),
    (
        "assign_side_sign_buy_else_sell_simple",
        re.compile(r"(?m)^(?P<indent>\s*)(?P<lhs>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*1\s*if\s*\(tick\.get\(['\"]side['\"]\)\s*==\s*['\"]BUY['\"]\)\s*else\s*-1\s*$"),
        r"\g<indent>\g<lhs> = side_sign_from_tick(tick)[0]  # Returns (sign, reason), we need sign",
    ),
    (
        "assign_side_sign_inverted_not_buy",
        re.compile(r"(?m)^(?P<indent>\s*)(?P<lhs>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*-1\s*if\s*\(str\(tick\.get\(['\"]side['\"]\)\)\.upper\(\)\s*!=\s*['\"]BUY['\"]\)\s*else\s*1\s*$"),
        r"\g<indent>\g<lhs> = side_sign_from_tick(tick)[0]  # Returns (sign, reason), we need sign",
    ),
    (
        "assign_ibm_to_sign",
        re.compile(r"(?m)^(?P<indent>\s*)(?P<lhs>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*-1\s*if\s*\(?tick\.get\(['\"]is_buyer_maker['\"]\)\)?\s*else\s*1\s*$"),
        r"\g<indent>\g<lhs> = side_sign_from_tick(tick)[0]  # Returns (sign, reason), we need sign",
    ),
    (
        "default_side_buy_to_unknown",
        re.compile(r"(\"side\"\s*:\s*str\(merged\.get\(\"side\"\)\s*or\s*merged\.get\(\"trade_side\"\)\s*or\s*)\"BUY\"(\)\.upper\(\),)"),
        r"\1\"UNKNOWN\"\2",
    ),
    (
        "default_side_buy_to_unknown_dict_generic",
        re.compile(r"(?m)([\'\"]side[\'\"]\s*:\s*.*\bor\s+)[\'\"]BUY[\'\"](\s*[,}])"),
        r"\1\"UNKNOWN\"\2",
    ),
    (
        "default_side_buy_to_unknown_var",
        re.compile(
            r"(?m)^(?P<indent>\s*)side\s*=\s*(?P<rhs>.*(?:get\(['\"]side['\"]\)|get\(['\"]trade_side['\"]\)|merged\.get\(['\"]side['\"]\)|merged\.get\(['\"]trade_side['\"]\)).*)\s+or\s+['\"]BUY['\"]\s*$"
        ),
        r"\g<indent>side = \g<rhs> or \"UNKNOWN\"",
    ),
]

IMPORT_LINE = "from services.orderflow.side_sign import side_sign_from_tick\n"


def iter_py_files(root: Path) -> list[Path]:
    out: list[Path] = []
    for p in root.rglob("*.py"):
        s = str(p)
        if any(x in s for x in ("/.venv/", "/venv/", "/__pycache__/", "/dist/", "/build/", "/.git/")):
            continue
        out.append(p)
    return out


def ensure_import(text: str) -> str:
    if "side_sign_from_tick(" not in text:
        return text
    if "from services.orderflow.side_sign import side_sign_from_tick" in text:
        return text

    lines = text.splitlines(True)

    insert_at = None
    for i, ln in enumerate(lines):
        if ln.startswith("import ") or ln.startswith("from "):
            insert_at = i + 1
            continue
        if insert_at is not None and ln.strip() == "":
            continue
        if insert_at is not None:
            break

    if insert_at is None:
        return IMPORT_LINE + text

    lines.insert(insert_at, IMPORT_LINE)
    return "".join(lines)


def apply_fixes(text: str):
    applied: list[str] = []
    new = text
    for rule, pat, repl in RULES:
        if pat.search(new):
            new2 = pat.sub(repl, new)
            if new2 != new:
                applied.append(rule)
                new = new2
    new = ensure_import(new)
    return new, applied


def unified_diff(path: str, before: str, after: str) -> str:
    a = before.splitlines(True)
    b = after.splitlines(True)
    return "".join(
        difflib.unified_diff(
            a, b,
            fromfile=f"a/{path}",
            tofile=f"b/{path}",
            lineterm=""
        )
    ) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="Repository root (default: .)")
    ap.add_argument("--write", action="store_true", help="Rewrite files in place")
    ap.add_argument("--backup", action="store_true", help="Create .bak backups when writing")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    base = root / "python-worker"
    if not base.exists():
        raise SystemExit(f"python-worker directory not found under: {root}")

    files = iter_py_files(base)
    any_changes = False
    changed_files = 0

    for fp in files:
        rel = fp.relative_to(root)
        before = fp.read_text(encoding="utf-8", errors="replace")
        after, rules = apply_fixes(before)
        if after == before:
            continue

        any_changes = True
        changed_files += 1

        if args.write:
            if args.backup:
                bak = fp.with_suffix(fp.suffix + ".bak")
                if not bak.exists():
                    bak.write_text(before, encoding="utf-8")
            fp.write_text(after, encoding="utf-8")
            print(f"[WRITE] {rel}  rules={','.join(rules) if rules else '-'}")
        else:
            print(unified_diff(str(rel), before, after))

    if not any_changes:
        print("No applicable side->sign fixes found.")
        return 0

    if not args.write:
        print(f"\n[DRY-RUN] {changed_files} file(s) would be modified. Re-run with --write to apply.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

