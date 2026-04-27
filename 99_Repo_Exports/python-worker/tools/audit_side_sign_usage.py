from __future__ import annotations

"""
Static audit tool: locate risky BUY/SELL sign conversions and potential SELL-bias patterns.

Why:
  - A common production bug is implicit mapping like:
      sign = 1 if side == "BUY" else -1
    which converts UNKNOWN / empty side into SELL.

This tool scans selected directories for suspicious patterns and prints a compact report.

Usage:
  python -m tools.audit_side_sign_usage --root /app --format text
  python -m tools.audit_side_sign_usage --root . --format json
"""

import argparse
import json
import os
import re
from dataclasses import dataclass
from typing import Iterable, List, Optional


SUSPICIOUS_PATTERNS = [
    # Unknown -> SELL bias
    (re.compile(r'1\s*if\s*.*side.*==\s*[\'"]BUY[\'"]\s*else\s*-1'), "ternary_else_minus1"),
    (re.compile(r'-1\s*if\s*.*side.*!=\s*[\'"]BUY[\'"]\s*else\s*1'), "ternary_not_buy_else_plus1"),
    (re.compile(r'-1\s*if\s*.*side.*==\s*[\'"]SELL[\'"]\s*else\s*1'), "ternary_sell_else_plus1"),
    (re.compile(r'!=\s*[\'"]BUY[\'"]'), "not_buy_used_as_sell"),
    (re.compile(r'else\s*:\s*\n\s*return\s*-1', re.MULTILINE), "else_return_minus1"),
    (re.compile(r'if\s+.*side.*==\s*[\'"]BUY[\'"]\s*:\s*\n\s*return\s*1\s*\n\s*else\s*:\s*\n\s*return\s*-1', re.MULTILINE), "if_buy_return1_else_returnm1"),
    # Defaults
    (re.compile(r'or\s*[\'"]BUY[\'"]'), "default_buy"),
    (re.compile(r'or\s*[\'"]SELL[\'"]'), "default_sell"),
    # is_buyer_maker direct sign conversions (often inverted or incomplete)
    (re.compile(r'-1\s*if\s*\(?tick\.get\([\'"]is_buyer_maker[\'"]\)\)?\s*else\s*1'), "ibm_to_sign"),
    # Side lower/upper mishandling
    (re.compile(r'\.upper\(\)\s*==\s*[\'"]BUY[\'"]\s*else\s*-1'), "upper_buy_else_minus1"),
]

DEFAULT_SCAN_DIRS = [
    "python-worker/services",
    "python-worker/handlers",
    "python-worker/tools",
]


@dataclass
class Finding:
    file: str
    line: int
    kind: str
    snippet: str


def iter_py_files(root: str, rel_dirs: Iterable[str]) -> Iterable[str]:
    for d in rel_dirs:
        p = os.path.join(root, d)
        if not os.path.isdir(p):
            continue
        for dirpath, _dirnames, filenames in os.walk(p):
            for fn in filenames:
                if fn.endswith(".py"):
                    yield os.path.join(dirpath, fn)


def scan_file(path: str) -> List[Finding]:
    findings: List[Finding] = []
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        text = "".join(lines)
    except Exception:
        return findings

    for rx, kind in SUSPICIOUS_PATTERNS:
        for m in rx.finditer(text):
            start = m.start()
            line_no = text.count("\n", 0, start) + 1
            snippet = lines[line_no - 1].rstrip("\n") if 0 < line_no <= len(lines) else m.group(0)[:200]
            findings.append(Finding(file=path, line=line_no, kind=kind, snippet=snippet.strip()))

    return findings


def render_text(findings: List[Finding], root: str) -> str:
    if not findings:
        return "OK: no suspicious side->sign patterns found."
    out = []
    out.append(f"findings={len(findings)}")
    out.append("")
    for f in sorted(findings, key=lambda x: (x.file, x.line, x.kind)):
        rel = os.path.relpath(f.file, root)
        out.append(f"{rel}:{f.line} [{f.kind}] {f.snippet}")
    out.append("")
    out.append("Next step:")
    out.append("  Replace ad-hoc side->sign mappings with services.orderflow.side_sign.side_sign_from_tick().")
    return "\n".join(out)


def render_json(findings: List[Finding], root: str) -> str:
    payload = []
    for f in sorted(findings, key=lambda x: (x.file, x.line, x.kind)):
        payload.append({
            "file": os.path.relpath(f.file, root),
            "line": f.line,
            "kind": f.kind,
            "snippet": f.snippet,
        })
    return json.dumps({"findings": payload}, ensure_ascii=False, indent=2)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="Repository root (default: current directory)")
    ap.add_argument("--dirs", nargs="*", default=None, help="Relative dirs to scan (default: common python-worker dirs)")
    ap.add_argument("--format", choices=["text", "json"], default="text")
    args = ap.parse_args(argv)

    root = os.path.abspath(args.root)
    rel_dirs = args.dirs or DEFAULT_SCAN_DIRS

    all_findings: List[Finding] = []
    for fp in iter_py_files(root, rel_dirs):
        all_findings.extend(scan_file(fp))

    if args.format == "json":
        print(render_json(all_findings, root))
    else:
        print(render_text(all_findings, root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

