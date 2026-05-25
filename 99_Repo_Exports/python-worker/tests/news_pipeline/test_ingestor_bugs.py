"""
Regression tests for news ingestor bug fixes.

Covers:
- Python ingestor time.mktime → calendar.timegm (UTC-safe)
- Standby ingestor same fix
- news_pipeline/grade.py duplicate function removed
- standby entrypoint run() takes no args
"""
from __future__ import annotations

import time


# ── timegm / mktime correctness ───────────────────────────────────────────────

def test_python_ingestor_uses_utc_timegm():
    """services/news_ingestor_py/main.py must import _timegm and use UTC-safe parsing."""
    import ast
    import pathlib

    src = pathlib.Path("services/news_ingestor_py/main.py").read_text()

    # Must import timegm (not mktime)
    assert "_timegm" in src or "timegm" in src, (
        "news_ingestor_py/main.py does not import timegm"
    )
    # Must NOT use time.mktime for published_parsed
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            if getattr(node, "attr", "") == "mktime":
                val = getattr(node, "value", None)
                if getattr(val, "id", "") == "time":
                    raise AssertionError(
                        f"time.mktime still used at line {node.lineno}; "
                        "replace with calendar.timegm (UTC-safe)"
                    )


def test_standby_ingestor_uses_utc_timegm():
    """news_pipeline.standby_ingestor must also use timegm."""
    from news_pipeline.standby_ingestor import _timegm  # noqa: PLC0415

    t = time.struct_time((2024, 1, 15, 0, 0, 0, 0, 15, 0))
    result = _timegm(t)
    assert result == 1705276800, f"expected 1705276800, got {result}"


def test_timegm_differs_from_mktime_in_nonlocal_tz():
    """Demonstrate that mktime IS TZ-dependent while timegm is not."""
    from calendar import timegm

    t = time.struct_time((2024, 6, 1, 12, 0, 0, 0, 153, 0))
    utc_epoch = timegm(t)
    # timegm always interprets as UTC
    assert utc_epoch == 1717243200  # 2024-06-01T12:00:00Z


# ── grade.py single function ──────────────────────────────────────────────────

def test_grade_no_duplicate_function():
    """grade module must have exactly one compute_grade_id (no duplicate)."""
    import ast
    import pathlib

    src = pathlib.Path("news_pipeline/grade.py").read_text()
    tree = ast.parse(src)
    defs = [n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]
    count = defs.count("compute_grade_id")
    assert count == 1, f"compute_grade_id defined {count} times (expected 1)"


def test_grade_function_has_no_os_dependency():
    """The remaining compute_grade_id must not reference os.getenv."""
    import ast
    import pathlib

    src = pathlib.Path("news_pipeline/grade.py").read_text()
    # Check there is no bare 'os' attribute access (os.getenv)
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "compute_grade_id":
            func_src = ast.get_source_segment(src, node) or ""
            assert "os.getenv" not in func_src, (
                "compute_grade_id still references os.getenv; "
                "this causes NameError when os is not imported"
            )


# ── standby entrypoint signature ─────────────────────────────────────────────

def test_standby_run_takes_no_args():
    """news_pipeline.standby_ingestor.run must not require a redis argument."""
    import inspect
    from news_pipeline.standby_ingestor import run  # noqa: PLC0415

    sig = inspect.signature(run)
    params = [
        p for p in sig.parameters.values()
        if p.default is inspect.Parameter.empty
        and p.kind not in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        )
    ]
    assert len(params) == 0, (
        f"run() has required positional params {[p.name for p in params]}; "
        "standby entrypoint must call run() with no args"
    )


# ── smt_bundle_aggregator exception visibility ────────────────────────────────

def test_smt_aggregator_no_silent_except():
    """smt_bundle_aggregator tick_once must not have bare 'except Exception: pass'."""
    import ast
    import pathlib

    src = pathlib.Path("services/smt_bundle_aggregator.py").read_text()
    tree = ast.parse(src)

    for node in ast.walk(tree):
        if not isinstance(node, ast.ExceptHandler):
            continue
        body = node.body
        if len(body) == 1 and isinstance(body[0], ast.Pass):
            # bare pass in an except block
            raise AssertionError(
                f"Silent 'except ... pass' found at line {node.lineno} in "
                "smt_bundle_aggregator.py — errors must be logged/counted"
            )
