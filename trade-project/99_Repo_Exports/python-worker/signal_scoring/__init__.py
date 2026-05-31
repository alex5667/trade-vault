import logging as _logging

from .config import ScoringConfig
from .ctx import SignalContext

_log = _logging.getLogger(__name__)

# Try to import weak_progress
try:
    from . import weak_progress
    WEAK_PROGRESS_AVAILABLE = True
except ImportError:
    WEAK_PROGRESS_AVAILABLE = False
    # Stub module
    import types
    weak_progress = types.ModuleType("weak_progress")

# Engine requires database dependencies, import separately when needed
try:
    from .engine import SignalScoringEngine
    if WEAK_PROGRESS_AVAILABLE:
        __all__ = ["ScoringConfig", "SignalContext", "SignalScoringEngine", "weak_progress"]
    else:
        __all__ = ["ScoringConfig", "SignalContext", "SignalScoringEngine"]
except ImportError as e:
    _log.warning("Failed to import SignalScoringEngine: %s", e)
    if WEAK_PROGRESS_AVAILABLE:
        __all__ = ["ScoringConfig", "SignalContext", "weak_progress"]
    else:
        __all__ = ["ScoringConfig", "SignalContext"]
except Exception as e:
    _log.warning("Unexpected error importing SignalScoringEngine: %s", e)
    if WEAK_PROGRESS_AVAILABLE:
        __all__ = ["ScoringConfig", "SignalContext", "weak_progress"]
    else:
        __all__ = ["ScoringConfig", "SignalContext"]
