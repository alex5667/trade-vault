"""conftest.py for ml_analysis/tests.

Добавляет tick_flow_full в sys.path, чтобы тесты могли импортировать
core.feature_registry без ручной установки PYTHONPATH.
"""
import sys
from pathlib import Path

# ml_analysis/tests/conftest.py → ml_analysis → python-worker → scanner_infra
_HERE = Path(__file__).resolve()

# Robust repo-root detection: walk upwards until we find tick_flow_full.
_REPO_ROOT = None
for _p in _HERE.parents:
    if (_p / "tick_flow_full").is_dir():
        _REPO_ROOT = _p
        break
if _REPO_ROOT is None:
    # Fallback to previous behavior (kept for compatibility in unusual layouts).
    _REPO_ROOT = _HERE.parents[3]

_TFF = Path(_REPO_ROOT) / "tick_flow_full"
_MLA = Path(_REPO_ROOT) / "ml_analysis"

if _TFF.is_dir() and str(_TFF) not in sys.path:
    sys.path.insert(0, str(_TFF))

# Add ml_analysis itself so tests can import offline tooling packages
# (tools.*, common.*) without external PYTHONPATH.
if _MLA.is_dir() and str(_MLA) not in sys.path:
    sys.path.insert(0, str(_MLA))
