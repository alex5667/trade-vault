import ast
import sys

filename = 'services/crypto_orderflow_service.py'

try:
    with open(filename) as f:
        tree = ast.parse(f.read())
except Exception as e:
    print(f"Error parsing file: {e}")
    sys.exit(1)

class UnreachableCodeFinder(ast.NodeVisitor):
    def visit_FunctionDef(self, node):
        self.check_body(node.body, f"Function: {node.name}")

    def visit_AsyncFunctionDef(self, node):
        self.check_body(node.body, f"AsyncFunction: {node.name}")

    def check_body(self, body, context):
        has_returned = False
        for i, stmt in enumerate(body):
            if has_returned:
                print(f"Unreachable code found in {context} at line {stmt.lineno}")
                print(f"  Statement: {ast.dump(stmt)}")
                return # Stop reporting for this block

            if isinstance(stmt, ast.Return):
                has_returned = True

            # Recurse into blocks (if, for, while, etc.)
            if isinstance(stmt, ast.If):
                self.check_body(stmt.body, f"{context} -> If body")
                self.check_body(stmt.orelse, f"{context} -> If orelse")
            elif isinstance(stmt, ast.For):
                self.check_body(stmt.body, f"{context} -> For body")
                self.check_body(stmt.orelse, f"{context} -> For orelse")
            elif isinstance(stmt, ast.While):
                self.check_body(stmt.body, f"{context} -> While body")
                self.check_body(stmt.orelse, f"{context} -> While orelse")
            elif isinstance(stmt, ast.Try):
                self.check_body(stmt.body, f"{context} -> Try body")
                for handler in stmt.handlers:
                    self.check_body(handler.body, f"{context} -> Except body")
                self.check_body(stmt.orelse, f"{context} -> Try orelse")
                self.check_body(stmt.finalbody, f"{context} -> Try finalbody")
            elif isinstance(stmt, ast.With):
                 self.check_body(stmt.body, f"{context} -> With body")
            elif isinstance(stmt, ast.AsyncWith):
                 self.check_body(stmt.body, f"{context} -> AsyncWith body")

finder = UnreachableCodeFinder()
finder.visit(tree)
