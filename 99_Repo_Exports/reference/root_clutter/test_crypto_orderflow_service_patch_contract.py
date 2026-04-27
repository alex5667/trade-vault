import ast
import os
from pathlib import Path


def _find_service_path() -> Path:
    # Allow overriding for your repo layout
    candidates = [
        Path(os.getenv("CRYPTO_OF_SERVICE_PATH", "")),
        Path("python-worker/services/crypto_orderflow_service.py"),
        Path("services/crypto_orderflow_service.py"),
        Path("services/orderflow/crypto_orderflow_service.py"),
        Path("crypto_orderflow_service.py"),
    ]
    for p in candidates:
        if p and p.exists() and p.is_file():
            return p
    raise FileNotFoundError(
        "Cannot find crypto_orderflow_service.py. Set CRYPTO_OF_SERVICE_PATH env var to the file path."
    )


SERVICE_PATH = _find_service_path()
SERVICE_SRC = SERVICE_PATH.read_text(encoding="utf-8")
TREE = ast.parse(SERVICE_SRC)


def _find_class(tree: ast.AST, name: str) -> ast.ClassDef:
    for node in tree.body:  # type: ignore[attr-defined]
        if isinstance(node, ast.ClassDef) and node.name == name:
            return node
    raise AssertionError(f"Class {name} not found")


def _find_method(cls: ast.ClassDef, name: str):
    for node in cls.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    raise AssertionError(f"Method {cls.name}.{name} not found")


def _source_segment(node: ast.AST) -> str:
    seg = ast.get_source_segment(SERVICE_SRC, node)
    if not seg:
        raise AssertionError("Failed to extract source segment")
    return seg


def _build_test_class(*method_nodes):
    # Build a tiny class that reuses the exact method implementations from the service file
    methods_src = "\n\n".join(_source_segment(m) for m in method_nodes)
    # Check if _msgid_ms is a staticmethod and preserve that
    has_staticmethod = any("@staticmethod" in _source_segment(m).split("\n")[0] or 
                          (isinstance(m, ast.FunctionDef) and 
                           any(isinstance(d, ast.Name) and d.id == "staticmethod" 
                               for d in m.decorator_list))
                          for m in method_nodes if hasattr(m, "name") and m.name == "_msgid_ms")
    
    code = (
        "class _T:\n"
        "    def __init__(self, max_ts_skew_ms: int):\n"
        "        self._max_ts_skew_ms = max_ts_skew_ms\n\n"
    )
    # Add staticmethod decorator if needed
    for m in method_nodes:
        src = _source_segment(m)
        if m.name == "_msgid_ms":
            # Ensure it's a staticmethod
            if "@staticmethod" not in src.split("\n")[0]:
                code += "    @staticmethod\n"
        code += "\n".join("    " + line if line.strip() else line for line in src.splitlines()) + "\n\n"
    
    ns = {}
    exec(code, ns, ns)
    return ns["_T"]


def test_methods_exist_and_defaults_are_safe():
    cls = _find_class(TREE, "CryptoOrderflowService")

    _find_method(cls, "_msgid_ms")
    _find_method(cls, "_coerce_event_ts_ms")
    _find_method(cls, "_xack_pipeline")

    # Ensure pressure-drop default is OFF by default (safety)
    init = _find_method(cls, "__init__")
    init_src = _source_segment(init)
    assert "CRYPTO_OF_DROP_ON_LAG" in init_src
    assert "\"false\"" in init_src or "'false'" in init_src


def test_msgid_ms_parsing():
    cls = _find_class(TREE, "CryptoOrderflowService")
    m_msgid = _find_method(cls, "_msgid_ms")
    m_coerce = _find_method(cls, "_coerce_event_ts_ms")

    T = _build_test_class(m_msgid, m_coerce)
    t = T(max_ts_skew_ms=1000)

    # _msgid_ms is a staticmethod, call it on the class
    assert T._msgid_ms("1700000000000-0") == 1700000000000
    assert T._msgid_ms("1700000000000-123") == 1700000000000
    assert T._msgid_ms("bad") == 0


def test_coerce_event_ts_ms_prefers_sane_payload():
    cls = _find_class(TREE, "CryptoOrderflowService")
    m_msgid = _find_method(cls, "_msgid_ms")
    m_coerce = _find_method(cls, "_coerce_event_ts_ms")

    T = _build_test_class(m_msgid, m_coerce)
    t = T(max_ts_skew_ms=500)

    now = 1_000_000
    payload = now - 200  # sane
    assert t._coerce_event_ts_ms(msg_id="1700000000000-0", payload_ts_ms=payload, now_ms=now) == payload


def test_coerce_event_ts_ms_falls_back_to_msgid_on_poisoned_payload():
    cls = _find_class(TREE, "CryptoOrderflowService")
    m_msgid = _find_method(cls, "_msgid_ms")
    m_coerce = _find_method(cls, "_coerce_event_ts_ms")

    T = _build_test_class(m_msgid, m_coerce)
    t = T(max_ts_skew_ms=500)

    now = 1_000_000
    poisoned = now + 999_999  # far beyond skew
    assert t._coerce_event_ts_ms(msg_id="1700000000000-0", payload_ts_ms=poisoned, now_ms=now) == 1700000000000


def test_coerce_event_ts_ms_last_resort_wall_clock():
    cls = _find_class(TREE, "CryptoOrderflowService")
    m_msgid = _find_method(cls, "_msgid_ms")
    m_coerce = _find_method(cls, "_coerce_event_ts_ms")

    T = _build_test_class(m_msgid, m_coerce)
    t = T(max_ts_skew_ms=500)

    now = 1_000_000
    # bad msg_id and missing payload -> wall clock
    assert t._coerce_event_ts_ms(msg_id="bad", payload_ts_ms=0, now_ms=now) == now


def test_consume_ticks_uses_batch_ack():
    cls = _find_class(TREE, "CryptoOrderflowService")
    try:
        consume = _find_method(cls, "consume_ticks")
    except AssertionError:
        # Method might be async, try to find it anyway
        for node in cls.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "consume_ticks":
                consume = node
                break
        else:
            raise AssertionError("Method CryptoOrderflowService.consume_ticks not found")

    # Cheap structural check: consume_ticks should call _xack_pipeline at least once
    class CallFinder(ast.NodeVisitor):
        def __init__(self):
            self.found = 0

        def visit_Call(self, node: ast.Call):
            # Look for self._xack_pipeline(...)
            if isinstance(node.func, ast.Attribute) and node.func.attr == "_xack_pipeline":
                self.found += 1
            self.generic_visit(node)

    v = CallFinder()
    v.visit(consume)
    assert v.found >= 1, "consume_ticks must call _xack_pipeline for batch ACK"
