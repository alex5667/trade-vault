# python-worker/tools/audit_code_integrity.py
from __future__ import annotations

import argparse
import ast
import json
import os
from dataclasses import dataclass

EXCLUDE_DIRS = {
    ".git", ".venv", "venv", "__pycache__", "node_modules", "dist", "build", ".mypy_cache", ".pytest_cache"
}


@dataclass
class DefInfo:
    kind: str   # "def" | "async_def" | "class"
    name: str
    lineno: int


def scan_file(path: str) -> dict[str, object]:
    """
    Detect duplicate top-level defs/classes in a single file.
    Also reports unusually high import duplication signals (heuristic).
    """
    try:
        src = open(path, encoding="utf-8").read()
    except Exception as e:
        return {"path": path, "error": f"read_error:{e}"}

    try:
        mod = ast.parse(src, filename=path)
    except Exception as e:
        return {"path": path, "error": f"parse_error:{e}"}

    defs: list[DefInfo] = []
    imports: list[tuple[str, int]] = []

    for node in mod.body:
        if isinstance(node, ast.FunctionDef):
            defs.append(DefInfo("def", node.name, getattr(node, "lineno", 0) or 0))
        elif isinstance(node, ast.AsyncFunctionDef):
            defs.append(DefInfo("async_def", node.name, getattr(node, "lineno", 0) or 0))
        elif isinstance(node, ast.ClassDef):
            defs.append(DefInfo("class", node.name, getattr(node, "lineno", 0) or 0))
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            # normalized import signature
            if isinstance(node, ast.Import):
                names = ",".join(sorted([a.name for a in node.names]))
                sig = f"import:{names}"
            else:
                modn = node.module or ""
                names = ",".join(sorted([a.name for a in node.names]))
                sig = f"from:{modn}:{names}"
            imports.append((sig, getattr(node, "lineno", 0) or 0))

    # duplicates
    by_name: dict[str, list[DefInfo]] = {}
    for d in defs:
        by_name.setdefault(d.name, []).append(d)
    dup_defs = {k: v for k, v in by_name.items() if len(v) > 1}

    # import dup heuristic
    imp_map: dict[str, list[int]] = {}
    for sig, ln in imports:
        imp_map.setdefault(sig, []).append(ln)
    dup_imports = {k: v for k, v in imp_map.items() if len(v) > 1}

    out = {
        "path": path,
        "dup_defs": {
            name: [{"kind": x.kind, "lineno": x.lineno} for x in infos]
            for name, infos in dup_defs.items()
        },
        "dup_imports": {
            sig: lines for sig, lines in list(dup_imports.items())[:20]
        },
        "n_defs": len(defs),
        "n_imports": len(imports),
    }
    return out


def walk_py(root: str) -> list[str]:
    out: list[str] = []
    for base, dirs, files in os.walk(root):
        # prune
        dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]
        for fn in files:
            if fn.endswith(".py"):
                out.append(os.path.join(base, fn))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="root directory to scan")
    ap.add_argument("--out", default="", help="write JSON report")
    ap.add_argument("--fail-on-dup", type=int, default=0, help="exit non-zero if duplicates found")
    ap.add_argument("--max-files", type=int, default=20000)
    args = ap.parse_args()

    files = walk_py(args.root)[: args.max_files]

    report = []
    dup_count = 0
    parse_err = 0

    for p in files:
        r = scan_file(p)
        if "error" in r:
            parse_err += 1
            report.append(r)
            continue
        if r.get("dup_defs") and len(r["dup_defs"]) > 0:
            dup_count += 1
            report.append(r)
        else:
            # include import duplication only if heavy
            if r.get("dup_imports") and len(r["dup_imports"]) >= 5:
                report.append(r)

    summary = {
        "root": args.root,
        "files_scanned": len(files),
        "files_with_dup_defs": dup_count,
        "files_with_parse_errors": parse_err,
    }

    out = {"summary": summary, "findings": report}

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
    else:
        print(json.dumps(out, ensure_ascii=False, indent=2))

    if args.fail_on_dup == 1 and dup_count > 0:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
