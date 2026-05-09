from __future__ import annotations

"""
conftest.py for tick_flow_full/orderflow_services/tests
Ensures both tick_flow_full (core/, services/) and the main python-worker root
are on sys.path so TickProcessor and related modules import correctly.
"""

import sys
from pathlib import Path


def _setup_paths() -> None:
    """Add tick_flow_full and python-worker root to sys.path.

    Layout:
        <root>/tick_flow_full/{core,services,common,...}   ← PYTHONPATH[0]
        <root>/                                             ← PYTHONPATH[1]

    The test file already calls _ensure_tick_flow_full_on_path() but conftest
    runs before collection so the path is available for all import statements.
    """
    # Assume: this file is at <root>/tick_flow_full/orderflow_services/tests/conftest.py
    #   parents[0] = tests/
    #   parents[1] = orderflow_services/
    #   parents[2] = tick_flow_full/
    #   parents[3] = <root> (python-worker)
    here = Path(__file__).resolve()
    tff = here.parents[2]           # tick_flow_full/
    root = here.parents[3]          # python-worker/

    for p in (str(tff), str(root)):
        if p not in sys.path:
            sys.path.insert(0, p)


_setup_paths()
