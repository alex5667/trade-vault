"""P4: Runtime integrity check tests.

Verifies:
  1. No duplicate definitions of critical methods in runtime source files.
  2. CLI main() returns exit code 0 (clean state after P4 canonicalization).
"""
from pathlib import Path
import importlib.util
import sys

# Locate repo root (python-worker parent) so imports work regardless of CWD
_root = Path(__file__).resolve()
for _p in _root.parents:
    if (_p / "python-worker").is_dir() and (_p / "services" if (_p / "services").is_dir() else (_p / "python-worker" / "services")).is_dir():
        # prefer python-worker as base
        if (_p / "python-worker" / "services").is_dir():
            root = _p / "python-worker"
            break
        root = _p
        break
else:
    root = Path(__file__).resolve().parents[2]

if str(root) not in sys.path:
    sys.path.insert(0, str(root))

mod_path = root / "services" / "binance_runtime_integrity_check.py"
spec = importlib.util.spec_from_file_location("services.binance_runtime_integrity_check_p4", mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)


def test_runtime_source_has_no_duplicate_critical_defs():
    """After P4 canonicalization, zero critical methods must appear > once."""
    base = root / "services"
    for filename, method_names in mod.CRITICAL_METHODS.items():
        path = base / filename
        if not path.exists():
            # Skip if file is not present in this layout
            continue
        duplicates = mod.scan_duplicate_method_defs(path, method_names)
        assert duplicates == {}, (
            f"{filename} still has duplicate defs for: "
            + ", ".join(f"{k}@{v}" for k, v in sorted(duplicates.items()))
        )


def test_runtime_integrity_cli_returns_success():
    """CLI must exit 0 — means zero duplicates and no blockers."""
    assert mod.main() == 0
