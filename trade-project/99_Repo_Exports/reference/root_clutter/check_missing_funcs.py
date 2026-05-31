import ast

with open("python-worker/services/binance_executor.py") as f:
    tree = ast.parse(f.read())

classes = [n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "BinanceExecutor"]
methods = [n.name for n in classes[0].body if isinstance(n, ast.FunctionDef)]
expected = ["_resolve_execution_policy", "_position_qty_tolerance", "_emit_tp_state", "_submit_reduce_only_market_exit", "_emergency_flatten_position", "_place_protective", "_start_maker_tp_watchdogs"]
print("Missing class methods:", [m for m in expected if m not in methods])
funcs = [n.name for n in tree.body if isinstance(n, ast.FunctionDef)]
expected_funcs = ["compute_limit_tp_price", "compute_trailing_activate_price", "_tp_state_name"]
print("Missing top-level funcs:", [m for m in expected_funcs if m not in funcs])
