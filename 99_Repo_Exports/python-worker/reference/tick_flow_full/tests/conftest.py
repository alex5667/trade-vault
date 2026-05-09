# tick_flow_full/tests/conftest.py
"""
Pytest configuration for tick_flow_full unit tests.
Adds tick_flow_full/ to sys.path so that `from core.xxx import ...` resolves
to tick_flow_full/core/ regardless of where pytest is invoked from.
"""
import os
import sys

# Resolve the tick_flow_full package root (two levels up from this file)
_TFF_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _TFF_ROOT not in sys.path:
    sys.path.insert(0, _TFF_ROOT)
