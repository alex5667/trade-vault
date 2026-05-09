from __future__ import annotations

"""
Batch patch generator/applicator for side->sign conversions based on audit output.

Typical workflow (run from repo root):
  python -m tools.audit_side_sign_usage --root . --format json > /tmp/side_audit.json
  python -m tools.patch_from_side_audit --root . --audit /tmp/side_audit.json --out /tmp/side_sign_patch.diff
  git apply /tmp/side_sign_patch.diff

Or apply in-place (creates .bak by default):
  python -m tools.patch_from_side_audit --root . --audit /tmp/side_audit.json --apply --backup

Design goals:
- Deterministic: patch is stable for the same repo state + audit.
- Conservative: only fixes lines that still match expected patterns.
- No external dependencies.
"""

import argparse
import difflib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Finding:
    path: str
    lineno: int  # 1-based
    kind: str
    line: str

    @staticmethod
    def from_obj(obj: dict[str, Any]) -> Finding:
        return Finding(
            path=str(obj.get("path") or obj.get("file") or ""),
            lineno=int(obj.get("lineno") or obj.get("line_no") or obj.get("line") or 0),
            kind=str(obj.get("kind") or obj.get("pattern") or obj.get("rule") or "unknown"),
            line=str(obj.get("line") or obj.get("src") or obj.get("snippet") or ""),
        )


def _load_audit_json(p: Path) -> list[Finding]:
    data = json.loads(p.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        data = data.get("findings") or data.get("results") or data.get("items") or []
    if not isinstance(data, list):
        raise ValueError("Audit JSON must be a list or dict with findings/results/items list.")
    findings: list[Finding] = []
    for it in data:
        if isinstance(it, dict):
            f = Finding.from_obj(it)
            if f.path and f.lineno > 0:
                findings.append(f)
    return findings


def _extract_var_from_cond(cond: str) -> str | None:
    """Extract tick variable name from conditional expression."""
    m = re.search(r"\b([A-Za-z_]\w*)\s*(?:\.get\(|\[\s*['\"])side", cond)
    if m:
        return m.group(1)
    m = re.search(r"\b([A-Za-z_]\w*)\s*(?:\.get\(|\[\s*['\"])is_buyer_maker", cond)
    if m:
        return m.group(1)
    return None


def _needs_side_sign_import(new_line: str) -> bool:
    return "side_sign_from_tick(" in new_line


def _safe_tri_state_expr(side_var: str) -> str:
    return f'(1 if {side_var} == "BUY" else (-1 if {side_var} == "SELL" else 0))'


def _fix_line(line: str) -> tuple[str, bool, bool]:
    original = line

    # Default side fallback: ... or "BUY" -> ... or "UNKNOWN"
    if 'or "BUY"' in line or "or 'BUY'" in line:
        if re.search(r"\bside\b", line):
            line = line.replace('or "BUY"', 'or "UNKNOWN"').replace("or 'BUY'", "or 'UNKNOWN'")

    # x = 1 if tick.get("side") == "BUY" else -1  -> side_sign_from_tick(tick)[0]
    m = re.search(r"=\s*1\s+if\s+(.+?)\s+else\s+-1(\s*(?:#.*)?)$", line)
    if m:
        cond = m.group(1)
        tick_var = _extract_var_from_cond(cond)
        if tick_var and re.search(r"['\"]BUY['\"]", cond):
            prefix = line[: m.start(0)]
            suffix = m.group(2) or ""
            line = f"{prefix}= side_sign_from_tick({tick_var})[0]{suffix}\n"

    # x = -1 if side != "BUY" else 1 -> tri-state
    m = re.search(r"=\s*-1\s+if\s+([A-Za-z_]\w*)\s*!=\s*['\"]BUY['\"]\s+else\s+1(\s*(?:#.*)?)$", line)
    if m:
        side_var = m.group(1)
        prefix = line[: m.start(0)]
        suffix = m.group(2) or ""
        line = f"{prefix}= {_safe_tri_state_expr(side_var)}{suffix}\n"

    # x = 1 if side == "BUY" else -1 -> tri-state
    m = re.search(r"=\s*1\s+if\s+([A-Za-z_]\w*)\s*==\s*['\"]BUY['\"]\s+else\s+-1(\s*(?:#.*)?)$", line)
    if m:
        side_var = m.group(1)
        prefix = line[: m.start(0)]
        suffix = m.group(2) or ""
        line = f"{prefix}= {_safe_tri_state_expr(side_var)}{suffix}\n"

    # x = -1 if tick.get("is_buyer_maker") else 1 -> side_sign_from_tick(tick)[0]
    m = re.search(r"=\s*-1\s+if\s+(.+?)\s+else\s+1(\s*(?:#.*)?)$", line)
    if m:
        cond = m.group(1)
        tick_var = _extract_var_from_cond(cond)
        if tick_var and "is_buyer_maker" in cond:
            prefix = line[: m.start(0)]
            suffix = m.group(2) or ""
            line = f"{prefix}= side_sign_from_tick({tick_var})[0]{suffix}\n"

    changed = (line != original)
    return line, changed, _needs_side_sign_import(line)


def _ensure_import(lines: list[str]) -> list[str]:
    import_stmt = "from services.orderflow.side_sign import side_sign_from_tick\n"
    if any(l.strip() == import_stmt.strip() for l in lines):
        return lines
    if any("services.orderflow.side_sign" in l for l in lines):
        return lines

    insert_at = 0
    for i, l in enumerate(lines[:200]):
        s = l.strip()
        if s.startswith("#") or s == "":
            continue
        if s.startswith('"""') or s.startswith("'''"):
            continue
        if s.startswith("import ") or s.startswith("from "):
            insert_at = i + 1
            continue
        break
    return lines[:insert_at] + [import_stmt] + lines[insert_at:]


def fix_file(path: Path, findings: list[Finding]) -> tuple[str | None, str | None, bool]:
    before = path.read_text(encoding="utf-8")
    lines = before.splitlines(keepends=True)

    by_line: dict[int, list[Finding]] = {}
    for f in findings:
        if f.lineno > 0:
            by_line.setdefault(f.lineno, []).append(f)

    changed_any = False
    needs_import = False

    for lineno, f_list in by_line.items():
        if lineno < 1 or lineno > len(lines):
            continue
        idx = lineno - 1
        old_line = lines[idx]
        audit_line = f_list[0].line or ""
        if audit_line:
            # skip if mismatch (repo changed)
            if re.sub(r"\s+", "", audit_line) not in re.sub(r"\s+", "", old_line):
                continue
        new_line, changed, req_import = _fix_line(old_line)
        if changed:
            lines[idx] = new_line
            changed_any = True
            needs_import = needs_import or req_import

    if not changed_any:
        return None, None, False

    if needs_import:
        lines = _ensure_import(lines)

    after = "".join(lines)
    return before, after, True


def make_unified_diff(rel_path: str, before: str, after: str) -> str:
    before_lines = before.splitlines(keepends=True)
    after_lines = after.splitlines(keepends=True)
    return "".join(
        difflib.unified_diff(
            before_lines,
            after_lines,
            fromfile=f"a/{rel_path}",
            tofile=f"b/{rel_path}",
            n=3,
        )
    )


def generate_patch(root: Path, findings: list[Finding]) -> str:
    root = root.resolve()
    by_file: dict[str, list[Finding]] = {}
    for f in findings:
        p = f.path.replace("\\", "/").lstrip("/")
        by_file.setdefault(p, []).append(f)

    diffs: list[str] = []
    for rel, f_list in sorted(by_file.items()):
        file_path = (root / rel).resolve()
        if not file_path.exists() or not file_path.is_file():
            continue
        before, after, changed = fix_file(file_path, f_list)
        if changed and before is not None and after is not None:
            diffs.append(make_unified_diff(rel, before, after))
    return "".join(diffs)


def apply_patch_in_place(root: Path, findings: list[Finding], backup: bool = True) -> int:
    root = root.resolve()
    by_file: dict[str, list[Finding]] = {}
    for f in findings:
        p = f.path.replace("\\", "/").lstrip("/")
        by_file.setdefault(p, []).append(f)

    changed_files = 0
    for rel, f_list in sorted(by_file.items()):
        file_path = (root / rel).resolve()
        if not file_path.exists() or not file_path.is_file():
            continue
        before, after, changed = fix_file(file_path, f_list)
        if not changed or before is None or after is None:
            continue
        if backup:
            bak = file_path.with_suffix(file_path.suffix + ".bak")
            if not bak.exists():
                bak.write_text(before, encoding="utf-8")
        file_path.write_text(after, encoding="utf-8")
        changed_files += 1
    return changed_files


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate/apply batch side-sign fixes from audit JSON.")
    ap.add_argument("--root", default=".", help="Repo root (default: .)")
    ap.add_argument("--audit", required=True, help="Path to audit JSON from tools.audit_side_sign_usage")
    ap.add_argument("--out", default="", help="Write unified diff patch to file (optional)")
    ap.add_argument("--apply", action="store_true", help="Apply fixes in-place instead of generating diff")
    ap.add_argument("--backup", action="store_true", help="When --apply, write .bak files")
    args = ap.parse_args()

    root = Path(args.root)
    findings = _load_audit_json(Path(args.audit))

    if args.apply:
        n = apply_patch_in_place(root, findings, backup=bool(args.backup))
        print(f"changed_files={n}")
        return

    patch = generate_patch(root, findings)
    if args.out:
        Path(args.out).write_text(patch, encoding="utf-8")
        print(args.out)
        return
    print(patch)


if __name__ == "__main__":
    main()

