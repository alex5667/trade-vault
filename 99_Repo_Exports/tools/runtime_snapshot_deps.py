#!/usr/bin/env python3
import argparse
import ast
import json
from pathlib import Path
from typing import Dict, Set


def _collect_runtime_accesses(tree: ast.AST) -> Set[str]:
    keys: Set[str] = set()

    class V(ast.NodeVisitor):
        def visit_Call(self, node: ast.Call) -> None:
            # getattr(runtime, "field", ...)
            try:
                if isinstance(node.func, ast.Name) and node.func.id == "getattr" and len(node.args) >= 2:
                    a0, a1 = node.args[0], node.args[1]
                    if isinstance(a0, ast.Name) and a0.id == "runtime":
                        if isinstance(a1, ast.Constant) and isinstance(a1.value, str):
                            keys.add(a1.value)
            except Exception:
                pass
            self.generic_visit(node)

        def visit_Attribute(self, node: ast.Attribute) -> None:
            # runtime.field
            try:
                if isinstance(node.value, ast.Name) and node.value.id == "runtime":
                    keys.add(node.attr)
            except Exception:
                pass
            self.generic_visit(node)

    V().visit(tree)
    return keys


def scan_file(path: Path) -> Set[str]:
    src = path.read_text(encoding="utf-8")
    tree = ast.parse(src, filename=str(path))
    return _collect_runtime_accesses(tree)


def main() -> int:
    ap = argparse.ArgumentParser(description="Scan OFConfirmEngine for runtime field dependencies.")
    ap.add_argument("--engine-path", default="core/of_confirm_engine.py", help="Path to of_confirm_engine.py")
    ap.add_argument("--json", action="store_true", help="Output JSON")
    ap.add_argument("--fail-on-missing", action="store_true", help="Exit non-zero if schema misses any deps")
    args = ap.parse_args()

    engine_path = Path(args.engine_path)
    deps = sorted(scan_file(engine_path))

    out: Dict[str, object] = {"engine_path": str(engine_path), "deps": deps}

    schema: Dict[str, str] = {}
    try:
        # Lazy import (repo root on sys.path when run via -m)
        from core.of_confirm_engine import OFConfirmEngine  # type: ignore

        schema = OFConfirmEngine.runtime_snapshot_schema()  # type: ignore
        out["schema_keys"] = sorted(schema.keys())
        missing = sorted(set(deps) - set(schema.keys()))
        out["missing_in_schema"] = missing
        out["ok"] = len(missing) == 0
        if args.fail_on_missing and missing:
            print(json.dumps(out, ensure_ascii=False, indent=2))
            return 1
    except Exception as e:
        out["schema_error"] = f"{type(e).__name__}: {e}"

    if args.json:
        print(json.dumps(out, ensure_ascii=False, indent=2))
    else:
        for k in deps:
            print(k)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
