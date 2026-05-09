from __future__ import annotations

"""Startup ML Confirm config initializer — ml_analysis.tools wrapper.

This module exists to satisfy imports like:
  from tools.init_ml_confirm_on_startup import ensure_ml_confirm_config

Implementation is kept in-sync with `utilities/init_ml_confirm_on_startup.py`.
The canonical implementation lives in `utilities/`; this wrapper simply re-exports it.
"""


# Prefer the canonical utilities implementation
try:
    from utilities.init_ml_confirm_on_startup import ensure_ml_confirm_config  # type: ignore
except Exception:  # pragma: no cover
    # Fallback: try tools/ parallel
    try:
        from tools.init_ml_confirm_on_startup import ensure_ml_confirm_config  # type: ignore
    except Exception:
        ensure_ml_confirm_config = None  # type: ignore

__all__ = ["ensure_ml_confirm_config"]
