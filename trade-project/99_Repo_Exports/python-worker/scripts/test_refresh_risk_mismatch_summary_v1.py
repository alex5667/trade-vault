"""P4.8 test: verify refresh_risk_mismatch_summary.py loads and has expected structure."""
import importlib.util
import sys
from pathlib import Path

# Load the script as a module without executing __main__
_p = Path(__file__).resolve().parents[1] / 'scripts' / 'refresh_risk_mismatch_summary.py'
_spec = importlib.util.spec_from_file_location('refresh_risk_mismatch_summary', _p)
_mod = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _mod
_spec.loader.exec_module(_mod)


def test_script_exists_and_loads():
    """The refresh_risk_mismatch_summary module must export a main() function."""
    assert hasattr(_mod, 'main'), 'refresh_risk_mismatch_summary.py must have a main() function'


def test_write_atomic_function_present():
    """_write_atomic helper must be present."""
    assert hasattr(_mod, '_write_atomic'), '_write_atomic helper must be present'
