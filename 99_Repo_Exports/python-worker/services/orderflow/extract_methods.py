import ast

with open('/home/alex/front/trade/scanner_infra/python-worker/services/orderflow_strategy.py', 'r') as f:
    code = f.read()

tree = ast.parse(code)
class_node = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'OrderFlowStrategy')

def get_method_source(name):
    for node in class_node.body:
        if isinstance(node, ast.AsyncFunctionDef) or isinstance(node, ast.FunctionDef):
            if node.name == name:
                return ast.get_source_segment(code, node)
    return ""

def write_method(file_path, methods):
    imports = "import logging\nimport json\nimport time\nimport math\nfrom typing import Any, Sequence\nfrom utils.time_utils import get_ny_time_millis\n\n"
    content = imports + "class ExtractedMethods:\n"
    for name in methods:
        src = get_method_source(name)
        if src:
            indented = "\n".join("    " + line if line else line for line in src.split("\n"))
            content += indented + "\n\n"
    with open(file_path, 'w') as f:
        f.write(content)

write_method('/home/alex/front/trade/scanner_infra/python-worker/services/orderflow/tick_decision_engine.py', 
             ['process_tick', '_on_microbar_closed', '_parse_tick_payload', '_parse_book_payload', '_get_atr_for_symbol'])
