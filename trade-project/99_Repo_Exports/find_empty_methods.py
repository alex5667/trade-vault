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
    
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            # Check if body consists only of Pass or Expr (docstring)
            real_stmts = [n for n in node.body if not isinstance(n, (ast.Pass, ast.Expr))]
            if not real_stmts:
                print(f"{path}:{node.lineno} Function '{node.name}' has an empty body (or only docstring)!")

if __name__ == "__main__":
    search_dir = sys.argv[1]
    for root, _, files in os.walk(search_dir):
        if 'reference' in root or '.venv' in root:
            continue
        for f in files:
            if f.endswith('.py'):
                check_file(os.path.join(root, f))
