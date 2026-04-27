import ast
import os
import sys

def check_file(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
    except Exception:
        return
    try:
        tree = ast.parse(content)
    except Exception:
        return
    
    # 1. Check for empty function bodies (likely merged issues)
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            real_stmts = [n for n in node.body if not isinstance(n, (ast.Pass, ast.Expr))]
            if not real_stmts and "test" not in path and "tests" not in path:
                print(f"EMPTY_BODY {path}:{node.lineno} {node.name}")
    
    # 2. Check for dead code (return/break/continue/raise) not at the end of a block
    for node in ast.walk(tree):
        for fieldname, value in ast.iter_fields(node):
            if isinstance(value, list):
                for i, child in enumerate(value):
                    if isinstance(child, (ast.Return, ast.Break, ast.Continue, ast.Raise)):
                        if i < len(value) - 1:
                            # Code follows an unconditional control flow break
                            dead_node = value[i+1]
                            print(f"DEAD_CODE {path}:{dead_node.lineno} Following a {child.__class__.__name__} at line {child.lineno}")

if __name__ == "__main__":
    search_dir = sys.argv[1]
    for root, _, files in os.walk(search_dir):
        if 'reference' in root or '.venv' in root:
            continue
        for f in files:
            if f.endswith('.py'):
                check_file(os.path.join(root, f))
