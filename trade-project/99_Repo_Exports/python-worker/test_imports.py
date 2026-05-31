import ast

with open("services/crypto_orderflow_service.py", "r") as f:
    source = f.read()

tree = ast.parse(source)
for node in ast.walk(tree):
    if isinstance(node, ast.ImportFrom) and node.module == "services.orderflow.metrics":
        names = [n.name for n in node.names]
        break

missing = []
for name in names:
    try:
        exec(f"from services.orderflow.metrics import {name}")
    except ImportError:
        missing.append(name)

print("MISSING:", ",".join(missing))
