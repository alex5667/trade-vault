from __future__ import annotations

"""Backward-compatible wrapper for the unified risk policy engine.

Existing code paths historically imported portfolio_risk_engine. The new source
of truth is risk_policy_engine.py; this module re-exports the updated API and
also works when loaded as a standalone file in bundle-style tests.

All class names, constants, and function signatures from the old module remain
available unchanged so that no existing consumer needs to change its imports.
"""

try:
    # Normal in-package usage (services.risk package)
    from .risk_policy_engine import *  # type: ignore  # noqa: F401,F403
except Exception:  # pragma: no cover
    # Standalone / bundle test load — resolve sibling file via importlib
    import importlib.util
    import sys
    from pathlib import Path

    _path = Path(__file__).resolve().with_name("risk_policy_engine.py")
    _spec = importlib.util.spec_from_file_location("risk_policy_engine", _path)
    _mod = importlib.util.module_from_spec(_spec)  # type: ignore
    sys.modules[_spec.name] = _mod  # type: ignore
    assert _spec.loader is not None  # type: ignore
    _spec.loader.exec_module(_mod)  # type: ignore
    for _name in dir(_mod):
        if not _name.startswith("_"):
            globals()[_name] = getattr(_mod, _name)
