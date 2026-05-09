"""P86: Compile test for enforce_bucket_state_exporter_v1.py with DB residual stats.

Verifies that the exporter module parses correctly after the P86 additions
(new Gauges, _safe_ident helper, _export_exec_slip_residual_stats method).
"""
import py_compile
from pathlib import Path


def test_enforce_bucket_state_exporter_compiles() -> None:
    """Ensure the exporter file compiles without syntax errors."""
    p = Path(__file__).resolve().parents[1] / "enforce_bucket_state_exporter_v1.py"
    py_compile.compile(str(p), doraise=True)
