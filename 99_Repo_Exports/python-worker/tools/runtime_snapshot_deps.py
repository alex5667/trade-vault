from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


@dataclass
class Requirements:
    top: Set[str]
    nested: Dict[str, Set[str]]
    files: List[str]


def _is_name(n: ast.AST, name: str) -> bool:
    return isinstance(n, ast.Name) and n.id == name


def _str_const(n: ast.AST) -> Optional[str]:
    if isinstance(n, ast.Constant) and isinstance(n.value, str):
        return n.value
    return None


def _find_function(tree: ast.AST, name: str) -> Optional[ast.FunctionDef]:
    for n in ast.walk(tree):
        if isinstance(n, ast.FunctionDef) and n.name == name:
            return n
    return None


def _collect_param_fields(node: ast.AST, param: str) -> Set[str]:
    """Collect attribute / dict-key reads on a parameter name."""
    out: Set[str] = set()
    ignore_attrs = {"get", "items", "keys", "values"}

    class V(ast.NodeVisitor):
        def visit_Attribute(self, n: ast.Attribute) -> Any:
            if _is_name(n.value, param) and str(n.attr) not in ignore_attrs:
                out.add(str(n.attr))
            self.generic_visit(n)

        def visit_Subscript(self, n: ast.Subscript) -> Any:
            if _is_name(n.value, param):
                k = _str_const(n.slice) if not isinstance(n.slice, ast.Slice) else None
                if k:
                    out.add(k)
            self.generic_visit(n)

        def visit_Call(self, n: ast.Call) -> Any:
            if isinstance(n.func, ast.Name) and n.func.id == "getattr" and len(n.args) >= 2:
                if _is_name(n.args[0], param):
                    k = _str_const(n.args[1])
                    if k:
                        out.add(k)
            if isinstance(n.func, ast.Attribute) and n.func.attr == "get" and len(n.args) >= 1:
                if _is_name(n.func.value, param):
                    k = _str_const(n.args[0])
                    if k:
                        out.add(k)
            if isinstance(n.func, ast.Name) and n.func.id in {"_get", "_get_attr_or_key"} and len(n.args) >= 2:
                if _is_name(n.args[0], param):
                    k = _str_const(n.args[1])
                    if k:
                        out.add(k)
            self.generic_visit(n)

    V().visit(node)
    return out


def _collect_engine_runtime_deps(engine_py: Path) -> Tuple[Set[str], Dict[str, Set[str]]]:
    src = engine_py.read_text(encoding="utf-8")
    tree = ast.parse(src, filename=str(engine_py))
    top: Set[str] = set()
    nested: Dict[str, Set[str]] = {}
    alias: Dict[str, str] = {}

    def add_nested(key: str, field: str) -> None:
        if not key.startswith("last_"):
            return
        if field:
            nested.setdefault(key, set()).add(field)

    class V(ast.NodeVisitor):
        def visit_Assign(self, n: ast.Assign) -> Any:
            if isinstance(n.value, ast.Call) and isinstance(n.value.func, ast.Name) and n.value.func.id == "getattr":
                if len(n.value.args) >= 2 and _is_name(n.value.args[0], "runtime"):
                    key = _str_const(n.value.args[1])
                    if key:
                        for t in n.targets:
                            if isinstance(t, ast.Name):
                                alias[t.id] = key
                                top.add(key)
            if isinstance(n.value, ast.Attribute) and _is_name(n.value.value, "runtime"):
                key = n.value.attr
                for t in n.targets:
                    if isinstance(t, ast.Name):
                        alias[t.id] = key
                        top.add(key)
            self.generic_visit(n)

        def visit_Attribute(self, n: ast.Attribute) -> Any:
            if isinstance(n.value, ast.Name) and n.value.id == "runtime":
                top.add(str(n.attr))
            if isinstance(n.value, ast.Name) and n.value.id in alias:
                add_nested(alias[n.value.id], str(n.attr))
            self.generic_visit(n)

        def visit_Call(self, n: ast.Call) -> Any:
            if isinstance(n.func, ast.Name) and n.func.id == "getattr" and len(n.args) >= 2:
                if _is_name(n.args[0], "runtime"):
                    k = _str_const(n.args[1])
                    if k:
                        top.add(k)
                if isinstance(n.args[0], ast.Name) and n.args[0].id in alias:
                    k = _str_const(n.args[1])
                    if k:
                        add_nested(alias[n.args[0].id], k)
            if isinstance(n.func, ast.Attribute) and n.func.attr == "get" and len(n.args) >= 1:
                if isinstance(n.func.value, ast.Name) and n.func.value.id in alias:
                    k = _str_const(n.args[0])
                    if k:
                        add_nested(alias[n.func.value.id], k)
            self.generic_visit(n)

    V().visit(tree)
    return top, nested


def collect_requirements(repo_root: str | Path) -> Requirements:
    root = Path(repo_root)
    files_scanned: List[str] = []

    engine_py = root / "core" / "of_confirm_engine.py"
    top, nested = _collect_engine_runtime_deps(engine_py)
    files_scanned.append(str(engine_py.relative_to(root)))

    evidence_map: Dict[str, Tuple[Path, str, str]] = {
        "last_sweep": (root / "core" / "of_evidence.py", "compute_sweep_recent", "last_sweep"),
        "last_reclaim": (root / "core" / "of_evidence.py", "compute_reclaim_recent", "last_reclaim"),
        "last_fp_edge": (root / "core" / "fp_edge_evidence.py", "compute_fp_edge_absorb", "last_edge"),
        "last_bar": (root / "core" / "absorption_level_score.py", "compute_absorption_level_score", "bar"),
        "last_obi_event": (root / "core" / "book_evidence.py", "compute_obi_flags", "last_event"),
        "last_iceberg_event": (root / "core" / "book_evidence.py", "compute_iceberg_flags", "last_event"),
        "last_ofi_event": (root / "core" / "book_evidence.py", "compute_ofi_flags", "last_event"),
    }

    for key, (path, fn, param) in evidence_map.items():
        if not path.exists():
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        f = _find_function(tree, fn)
        if not f:
            continue
        req = _collect_param_fields(f, param)
        if req:
            nested.setdefault(key, set()).update(req)
        files_scanned.append(str(path.relative_to(root)))

    return Requirements(top=top, nested=nested, files=sorted(set(files_scanned)))


def to_json(req: Requirements) -> Dict[str, Any]:
    return {
        "top": sorted(req.top),
        "nested": {k: sorted(v) for k, v in sorted(req.nested.items())},
        "files": req.files,
    }


def main(argv: Optional[List[str]] = None) -> int:
    import argparse
    import json
    ap = argparse.ArgumentParser(description="Extract runtime_snapshot field requirements from code (AST scan)")
    ap.add_argument("--root", default=".", help="repo root")
    ap.add_argument("--json", action="store_true", help="print JSON")
    args = ap.parse_args(argv)
    req = collect_requirements(args.root)
    if args.json:
        print(json.dumps(to_json(req), indent=2, sort_keys=True))
    else:
        print("top:", sorted(req.top))
        for k in sorted(req.nested):
            print(k, ":", sorted(req.nested[k]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

