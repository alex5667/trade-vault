# orderflow/__init__.py

# Lazy imports to avoid circular dependencies
__all__ = [
    "BaseOrderFlowHandler",
    "Candidate",
    "ScoredCandidate",
]

def __getattr__(name):
    if name == "BaseOrderFlowHandler":
        from .base_handler_legacy import BaseOrderFlowHandler
        return BaseOrderFlowHandler
    elif name == "Candidate":
        from .candidates import Candidate
        return Candidate
    elif name == "ScoredCandidate":
        from .candidates import ScoredCandidate
        return ScoredCandidate
    raise AttributeError(f"module 'orderflow' has no attribute '{name}'")
